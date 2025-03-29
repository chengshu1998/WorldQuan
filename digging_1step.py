import os

# 导入自定义库和模块
from machine_lib import login, async_login, simulate_single, get_datafields, process_datafields, first_order_factory, ts_ops, basic_ops
# login 同步登录 async_login 异步登录 get_datafields 获取因子数据字段 process_datafields 处理数据字段 
# first_order_factory 生成一阶因子表达式 simulate_single 单因子回测 ts_ops 时序运算符 如 Delay, Mean, Std 等 basic_ops 基础运算符
from fields import *
import time
import random
import asyncio
# Python标准库中的一个模块，用于编写异步并发代码 用于提高因子回测的效率
from config import *

# 定义会话管理器类，用于管理登录会话
class SessionManager:
    def __init__(self, session, start_time, expiry_time):
        """
        初始化会话管理器
        :param session: 登录会话对象
        :param start_time: 会话开始时间
        :param expiry_time: 会话过期时间
        """
        self.session = session
        self.start_time = start_time
        self.expiry_time = expiry_time

    async def refresh_session(self):
        """
        刷新会话，当会话过期时重新登录
        """
        print("Session expired, logging in again...")
        await self.session.close()  # 关闭当前会话
        self.session = await async_login()  # 异步重新登录
        self.start_time = time.time()  # 更新会话开始时间

# 定义异步函数，用于模拟多个因子
async def simulate_multiple_alphas(alpha_list, region_list, decay_list, delay_list, name, neut, stone_bag=[], n_jobs=5):
    """
    异步模拟多个因子
    :param alpha_list: 因子列表
    :param region_list: 区域列表
    :param decay_list: 衰减参数列表
    :param delay_list: 延迟参数列表
    :param name: 因子名称
    :param neut: 中性化参数
    :param stone_bag: 其他参数
    :param n_jobs: 并发任务数量
    """
    n = n_jobs  # 并发任务数量
    semaphore = asyncio.Semaphore(n)  # 信号量，用于限制并发任务数量
    tasks = []  # 存储所有任务
    tags = [name]  # 标签列表

    # 创建多个会话管理器
    session_managers = []
    for _ in range(n):
        # 记录登录的开始时间，并为每个 session_manager 创建独立的 session
        session_start_time = time.time()
        session = await async_login()  # 异步登录
        session_expiry_time = 3 * 60 * 60  # 会话过期时间为 3 小时
        session_manager = SessionManager(session, session_start_time, session_expiry_time)
        session_managers.append(session_manager)

    # 将任务划分成 n 份
    chunk_size = (len(alpha_list) + n - 1) // n  # 向上取整，计算每个任务块的大小
    task_chunks = [alpha_list[i:i + chunk_size] for i in range(0, len(alpha_list), chunk_size)]  # 分割因子列表
    region_chunks = [region_list[i:i + chunk_size] for i in range(0, len(region_list), chunk_size)]  # 分割区域列表
    decay_chunks = [decay_list[i:i + chunk_size] for i in range(0, len(decay_list), chunk_size)]  # 分割衰减列表
    delay_chunks = [delay_list[i:i + chunk_size] for i in range(0, len(delay_list), chunk_size)]  # 分割延迟列表

    # 遍历每个任务块，并为每个任务块分配一个会话管理器
    for i, (alpha_chunk, region_chunk, decay_chunk, delay_chunk) in (
            enumerate(zip(task_chunks, region_chunks, decay_chunks, delay_chunks))):
        # 获取当前 chunk 对应的 session_manager
        current_session_manager = session_managers[i]
        for alpha, region, decay, delay in zip(alpha_chunk, region_chunk, decay_chunk, delay_chunk):
            # 将任务与当前的 session_manager 关联
            task = simulate_single(current_session_manager, alpha, region, name, neut, decay, delay, stone_bag, tags, semaphore)
            tasks.append(task)

    # 异步执行所有任务
    await asyncio.gather(*tasks)

    # 关闭所有会话
    for session_manager in session_managers:
        await session_manager.session.close()

# 定义函数，用于读取已完成的因子表达式
def read_completed_alphas(filepath):
    """
    从指定文件中读取已经完成的alpha表达式
    :param filepath: 文件路径
    :return: 已完成的因子集合
    """
    completed_alphas = set()  # 使用集合存储已完成的因子
    try:
        with open(filepath, mode='r') as f:
            for line in f:
                completed_alphas.add(line.strip())  # 去除换行符并添加到集合中
    except FileNotFoundError:
        print(f"File {filepath} not found.")  # 如果文件不存在，打印提示信息
    return completed_alphas

# 主程序入口
if __name__ == '__main__':
    # 配置区域
    dataset_id = 'analyst4'  # 数据集 ID
    step1_tag = "analyst4_usa_1step"  # 标签名称

    # 同步登录
    s = login()
    # 获取因子数据字段
    df = get_datafields(s, dataset_id=dataset_id, region='USA', universe='TOP3000', delay=1)
    # 处理数据字段，生成矩阵和向量字段
    pc_fields = process_datafields(df, "matrix") + process_datafields(df, "vector")
    # pc_fields = recommended_fields_1  # 这个是推荐字段，可以取消注释直接使用

    # 生成一阶因子表达式
    first_order = first_order_factory(pc_fields, ts_ops + basic_ops)

    # 用 region_dict 去找到对应 region 和 universe 作为 simulation 的设置
    region_dict = {"usa": ("USA", "TOP3000"), "asi": ("ASI", "MINVOL1M"), "eur": ("EUR", "TOP1200"),
                   "glb": ("GLB", "TOP3000"), "hkg": ("HKG", "TOP800"), "twn": ("TWN", "TOP500"), "jpn": ("JPN", "TOP1600"),
                   "kor": ("KOR", "TOP600"), "chn": ("CHN", "TOP2000U"), "amr": ("AMR", "TOP600")}

    # 读取已完成的 alpha 表达式
    completed_alphas = read_completed_alphas(f'records/{step1_tag}_simulated_alpha_expression.txt')
    # 原始 alpha 列表
    alpha_list = first_order
    # 排除已完成的 alpha 表达式
    alpha_list = [alpha for alpha in alpha_list if alpha not in completed_alphas]
    print(len(alpha_list), "Waiting for Simulate")  # 打印待模拟的因子数量
    # 打乱 alpha 列表顺序
    random.shuffle(alpha_list)

    # 扩展 region_list、decay_list 和 delay_list，使其与 alpha_list 长度一致
    region_list = [('USA', 'TOP3000')] * len(alpha_list)
    decay_list = [6] * len(alpha_list)
    delay_list = [1] * len(alpha_list)

    stone_bag = []  # 其他参数

    # 执行异步模拟，并控制并发数量为 3
    asyncio.run(simulate_multiple_alphas(alpha_list, region_list, decay_list, delay_list,
                                         step1_tag, 'SUBINDUSTRY',
                                         stone_bag, n_jobs=3))

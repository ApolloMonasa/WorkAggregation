# /analysis/__init__.py

# ==============================================================================
#  分析模块 (analysis) 包初始化文件
# ==============================================================================
#
#  说明:
#  此文件是 `analysis` 包的入口。当程序中执行 `import analysis` 时，此文件中的
#  代码会被执行。
#
#  主要职责:
#  1. 定义并实例化一个全局应用上下文 `AppContext`，用于管理整个分析模块
#     共享的状态和资源，如数据库连接、配置信息和函数注册表。
#  2. 在包被导入时，尝试建立一个全局的数据库连接。
#  3. 从各个子模块中导入核心类和函数，将它们提升到包的顶层命名空间，
#     使得外部可以通过 `analysis.Analyze` 这样的方式直接访问，简化调用。
#
# ==============================================================================

import configparser
import pymysql
import os


class AppContext:
    """
    全局应用上下文类。
    作为整个分析模块的单例容器，存储共享的配置、数据库连接和动态注册的函数。
    """
    # --- 函数注册表 ---
    # 用于在不同模块中注册处理函数，实现解耦和动态扩展。
    process_fn_list = []  # 数据预处理函数列表
    analyze_fn_list = []  # 数据分析函数列表
    chart_fn_list = []    # 图表生成函数列表

    # --- 共享配置 ---
    conf = configparser.ConfigParser()

    # --- 数据库连接信息 ---
    # 注意: 在生产环境中，建议将这些敏感信息移至安全的配置文件或环境变量中。
    user = "pyuser"
    password = "123456"
    db_name = "ujn_a"

    # --- 数据库连接实例 ---
    # 在包加载时尝试建立全局数据库连接。
    try:
        db = pymysql.connect(
            host="localhost",
            user=user,
            password=password,
            charset="utf8",
            autocommit=True
        )
        cursor = db.cursor()
        cursor.execute(f'USE `{db_name}`;')
    except pymysql.Error as e:
        # 如果连接失败，将 db 和 cursor 设置为 None，以便其他模块可以进行安全检查。
        print(f"数据库连接失败: {e}")
        db, cursor = None, None

    # --- 项目根路径 ---
    # 计算并存储项目的根目录路径，方便进行文件读写。
    path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..')).replace('\\', '/')


# 1. 创建全局唯一的 AppContext 实例，命名为 `app`，供包内所有模块共享。
app = AppContext()

# 2. 从子模块中导入核心组件，将它们提升到包的顶层命名空间。
#    这样做可以使用户通过 `from analysis import Analyze` 而不是更长的
#    `from analysis.analysis_main import Analyze` 来导入。
from .analysis_main import Analyze
from . import create_chart
from . import interaction
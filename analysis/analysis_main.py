# /analysis/analysis_main.py

import configparser
import os
import pymysql
import sys


class Analyze:
    """
    主分析流程控制器和上下文容器。
    此类作为静态类使用，不需实例化。它负责：
    1. 存储全局配置和数据库连接。
    2. 提供一个统一的入口方法 `main` 来按顺序执行整个数据分析流程。
    3. 作为子模块（input_data, process_data, analyze_data）的中心协调者。
    """
    # --- 函数注册表 ---
    # 这些列表用于在不同模块中动态注册处理函数，实现模块间的解耦。
    process_fn_list = []
    analyze_fn_list = []
    chart_fn_list = []

    # 用于存储所有可用的数据视图名称，可能用于数据清洗或校验。
    available_views = []

    # --- 共享配置 ---
    conf = configparser.ConfigParser()

    # --- 数据库配置 ---
    # 注意: 在生产环境中，建议将用户名和密码移至更安全的位置，如环境变量或加密的配置文件。
    user = "pyuser"
    password = "123456"

    # 在类加载时尝试建立全局数据库连接。
    try:
        db = pymysql.connect(host="localhost", user=user, password=password, charset="utf8")
        cursor = db.cursor()
        cursor.execute('USE `ujn_a`;')
    except pymysql.Error as e:
        print(f"!!! 数据库连接失败: {e}")
        print("!!! 请检查 analysis/analysis_main.py 中的 user 和 password 配置是否正确。")
        db, cursor = None, None

    # --- 路径配置 ---
    # 获取当前工作目录。注意：这依赖于脚本的启动位置。
    path = os.getcwd().replace('\\', '/')

    @classmethod
    def main(cls):
        """
        数据分析流程的主入口。
        按顺序执行数据导入、数据处理和数据分析三个核心步骤。
        """
        # --- 动态导入子模块 ---
        # 【设计说明】将 import 语句置于方法内部是一种特殊设计，通常用于以下目的：
        # 1. 避免循环导入：子模块（如 process_data）可能需要从本文件导入 Analyze 类
        #    来访问共享资源。如果此处的 import 在顶层，会导致循环依赖错误。
        # 2. 延迟加载：仅在需要时才加载模块，可能略微加快程序启动速度。
        script_path = os.path.realpath(__file__)
        script_dir = os.path.dirname(script_path)
        if script_dir not in sys.path:
            sys.path.append(script_dir)

        import input_data
        import process_data
        import analyze_data

        # 安全检查：如果数据库未连接，则终止后续所有操作。
        if not cls.db:
            print("!!! 分析流程终止，因为数据库未连接。")
            return

        # --- 步骤 1: 数据导入 ---
        print("开始执行 input_data...")
        input_data.main()

        # --- 步骤 2: 数据预处理 ---
        print("开始执行 process_data...")
        process_data.main()

        # --- 步骤 3: 数据分析与计算 ---
        print("开始执行 analyze_data...")
        analyze_data.main()


# --- 模块测试入口 ---
if __name__ == '__main__':
    """
    当此脚本作为主程序直接运行时，执行此代码块。
    """
    Analyze.main()
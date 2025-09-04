# /analysis/process_data.py

# ==============================================================================
#  数据分析模块 - 步骤 2: 数据预处理
# ==============================================================================
#
#  说明:
#  此模块负责对从 CSV 导入到数据库的原始数据进行清洗、格式化和分类。
#  它是数据分析前至关重要的一步，确保了后续分析的数据质量和规整性。
#
#  核心功能:
#  1. 使用装饰器 `@ways` 动态注册所有预处理函数。
#  2. `main` 函数按注册顺序依次调用这些函数。
#  3. 清洗薪资字段，从中提取并计算最低、最高和平均薪资（月薪）。
#  4. 统一工作经验字段的格式，将其转换为数字。
#  5. 基于职位名称中的关键词，创建多个 SQL 视图 (VIEW)，对职位进行分类。
#     这样做的好处是避免了修改原始数据，并且可以灵活地进行多维度分析。
#
# ==============================================================================

import re
import jieba  # 注意：jieba 模块被导入但在此文件中未被使用。
import analysis_main as A  # 导入中心枢纽以访问共享资源。


def ways(func):
    """
    装饰器：将函数注册到 `Analyze` 类的 `process_fn_list` 中。
    这使得 `main` 函数可以自动发现并执行所有被此装饰器标记的函数。
    """
    A.Analyze.process_fn_list.append(func)

    def wrapper(*args, **kw):
        return func(*args, **kw)
    return wrapper


def main():
    """
    数据预处理流程的主入口函数。
    """
    global cursor, db
    cursor = A.Analyze.cursor
    db = A.Analyze.db

    # --- 1. 清理环境：删除上一次运行时创建的所有视图 ---
    try:
        cursor.execute("SHOW FULL TABLES WHERE TABLE_TYPE LIKE 'VIEW';")
        views = cursor.fetchall()
        for view in views:
            cursor.execute(f"DROP VIEW IF EXISTS `{view[0]}`;")
        db.commit()
    except Exception as e:
        print(f"清空旧视图时发生错误（可忽略）: {e}")

    # --- 2. 依次执行所有已注册的预处理函数 ---
    print("开始执行数据预处理...")
    for fn in A.Analyze.process_fn_list:
        print(f"正在执行: {fn.__name__}...")
        fn()
        db.commit()  # 每个步骤后提交事务，确保数据更改生效。
    print("数据预处理完成！")


@ways
def qcwy_clean_salary_and_experience():
    """
    负责清洗 `qcwy` 表中的薪资 (salary) 和工作经验 (experience) 字段。
    将非结构化的文本转换为结构化的数值，并填充到 `min_pay`, `max_pay`, `ave_pay` 等列。
    """
    print("  -> 正在清洗薪资和经验数据...")
    # 将 NULL 值更新为空字符串，便于后续处理。
    cursor.execute("UPDATE qcwy SET salary = '' WHERE salary IS NULL;")
    cursor.execute("UPDATE qcwy SET experience = '' WHERE experience IS NULL;")

    cursor.execute("SELECT id, salary, experience FROM qcwy")
    all_data = cursor.fetchall()

    update_salary_list = []      # 存储待更新的薪资数据 (min, max, avg, id)
    update_experience_list = []  # 存储待更新的经验数据 (exp_str, id)

    for row in all_data:
        row_id, salary_str, exp_str = row
        min_pay, max_pay, ave_pay = None, None, None

        # --- 薪资清洗逻辑 ---
        if salary_str:
            multiplier = 1
            if '年' in salary_str:
                multiplier = 1 / 12  # 年薪转月薪
            elif '天' in salary_str:
                multiplier = 30      # 日薪转月薪
            unit_multiplier = 1
            if '万' in salary_str:
                unit_multiplier = 10000
            elif '千' in salary_str:
                unit_multiplier = 1000

            # 提取所有数字
            numbers = re.findall(r'(\d+\.?\d*)', salary_str)
            if len(numbers) == 2:  # 处理范围薪资，如 "1.5-2万/月"
                try:
                    min_val = float(numbers[0]) * unit_multiplier * multiplier
                    max_val = float(numbers[1]) * unit_multiplier * multiplier
                    min_pay, max_pay, ave_pay = round(min_val), round(max_val), round((min_val + max_val) / 2)
                except (ValueError, IndexError):
                    pass
            elif len(numbers) == 1:  # 处理固定薪资，如 "15万/年"
                try:
                    val = float(numbers[0]) * unit_multiplier * multiplier
                    min_pay = max_pay = ave_pay = round(val)
                except (ValueError, IndexError):
                    pass

        if ave_pay is not None:
            update_salary_list.append((min_pay, max_pay, ave_pay, row_id))

        # --- 工作经验清洗逻辑 ---
        exp_num_str = None
        if exp_str:
            exp_numbers = re.findall(r'\d+', exp_str)
            if len(exp_numbers) == 2:  # 处理范围经验，如 "3-4年" -> "3"
                exp_num_str = str((int(exp_numbers[0]) + int(exp_numbers[1])) // 2)
            elif len(exp_numbers) == 1:  # 处理固定经验，如 "1年" -> "1"
                exp_num_str = exp_numbers[0]
            elif any(kw in exp_str for kw in ['无', '不限', '应届']) or exp_str == '':
                exp_num_str = '0'  # 无经验统一为 0

        if exp_num_str is not None:
            update_experience_list.append((exp_num_str, row_id))

    # --- 批量更新数据库 ---
    # 使用 executemany 进行批量更新，比单条循环更新效率高得多。
    if update_salary_list:
        cursor.executemany("UPDATE qcwy SET min_pay=%s, max_pay=%s, ave_pay=%s WHERE id=%s", update_salary_list)
        print(f"  -> 已更新 {len(update_salary_list)} 条薪资数据。")

    if update_experience_list:
        cursor.executemany("UPDATE qcwy SET experience=%s WHERE id=%s", update_experience_list)
        print(f"  -> 已更新 {len(update_experience_list)} 条经验数据。")


@ways
def qcwy_create_job_views():
    """
    根据职位标题的关键词，创建一系列 SQL 视图 (VIEW) 来对工作进行分类。
    """
    print("  -> 正在创建职位分类视图...")

    # 定义视图名称与关键词的映射关系
    # 格式: '视图名称': (['包含的关键词列表'], ['排除的关键词列表' or None])
    views_to_create = {
        'XXXX讲师': (['讲师'], None), '项目开发经理': (['经理'], None), '技术/研发总监': (['总监'], None),
        '大数据开发工程师': (['大数据'], None), '技术/研究/项目负责人': (['负责人'], None), '服务器工程师': (['服务器'], None),
        '数据库工程师': (['数据库'], None), '软件开发工程师': (['软件'], ['测试']), '建模工程师': (['建模'], None),
        '硬件工程师': (['硬件'], None), '网络工程师': (['网络'], None), '人工智能开发工程师': (['人工智能'], None),
        '后端工程师': (['后端'], None), '机器学习工程师': (['机器学习', '学习'], None), '数据挖掘/分析/处理工程师': (['数据'], ['管理']),
        '数据管理工程师': (['数据管理'], None), 'Web前端工程师': (['前端'], None), '计算机维修/维护工程师': (['维修', '维护'], None),
        'Java工程师': (['Java'], None), 'C++工程师': (['C++'], None), 'PHP工程师': (['PHP'], None),
        'C#工程师': (['C#'], None), '.NET工程师': (['.Net'], None), 'Hadoop工程师': (['Hadoop'], None),
        'Python工程师': (['Python'], None), 'Go工程师': (['Go'], None), 'Javascript工程师': (['Javascript'], None),
        'Android开发工程师': (['Android'], None), 'IOS开发工程师': (['IOS'], None), 'BI工程师': (['BI'], None),
        '软件开发': (['软件'], ['测试']), '人工智能': (['人工智能'], None),
        '深度\\机器学习': (['学习'], None),
        '数据': (['数据'], None), '算法': (['算法'], None), '测试': (['测试'], None),
        '安全': (['安全'], None), '运维': (['运维'], None), 'UI': (['UI'], ['GUI']),
        '区块链': (['区块链'], None), '网络': (['网络'], None), '硬件': (['硬件'], None), '物联网': (['物联网'], None), '游戏': (['游戏'], None)
    }

    for view_name, (include_kws, exclude_kws) in views_to_create.items():
        include_cond = " OR ".join([f"title LIKE '%{kw}%'" for kw in include_kws])
        exclude_cond = ""
        if exclude_kws:
            exclude_cond = " AND " + " AND ".join([f"title NOT LIKE '%{kw}%'" for kw in exclude_kws])

        # 对视图名称进行转义，防止SQL注入（虽然这里是内部定义，但仍是好习惯）
        safe_view_name = view_name.replace("'", "''")
        sql = f"CREATE OR REPLACE VIEW `{safe_view_name}` AS SELECT * FROM qcwy WHERE ({include_cond}) {exclude_cond}"

        try:
            cursor.execute(sql)
            # 将成功创建的视图名称添加到全局可用视图列表中
            A.Analyze.available_views.append(view_name)
        except Exception as e:
            print(f"  -> !!! 创建视图 '{view_name}' 失败: {e}")


@ways
def qcwy_create_other_views():
    """
    创建一些特定的、用于宏观分析的视图，如“新兴职业”与“传统职业”。
    """
    print("  -> 正在创建其他分析视图...")
    emerging_keywords = ['学习', '人工智能', '数据', '算法', '区块链', '视觉', '物联网', '自然语言']
    emerging_cond = " OR ".join([f"title LIKE '%{kw}%'" for kw in emerging_keywords])

    # 创建新兴职业视图
    cursor.execute(f"CREATE OR REPLACE VIEW `新兴职业` AS SELECT * FROM qcwy WHERE {emerging_cond}")
    # 创建传统职业视图
    cursor.execute(f"CREATE OR REPLACE VIEW `传统职业` AS SELECT * FROM qcwy WHERE NOT ({emerging_cond})")
    # 创建大数据职位视图
    cursor.execute("CREATE OR REPLACE VIEW `大数据职位` AS SELECT * FROM qcwy WHERE title LIKE '%数据%'")
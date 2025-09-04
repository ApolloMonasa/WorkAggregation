# /analysis/interaction.py

# ==============================================================================
#  数据分析模块 - 交互式前景分析 API
# ==============================================================================
#
#  说明:
#  此模块提供了交互式职业前景分析功能的后端 API 逻辑。
#  它接收前端用户通过表单提交的筛选条件，动态查询数据库，
#  进行实时的统计分析，并返回结构化的 JSON 数据。
#
#  核心功能:
#  1. 动态构建安全的 SQL 查询语句，以应对用户不同的筛选组合。
#  2. 查询数据库，获取符合条件的职位数据样本。
#  3. 使用 Pandas 库对样本数据进行快速的统计分析，计算平均薪资、
#     学历和经验要求分布等。
#  4. 生成一段描述性的“用户画像”文本。
#  5. 【增强功能】将匹配到的职位列表一并返回，供前端展示具体职位信息。
#
# ==============================================================================

from . import app
import pandas as pd
import re


def analyze_prospects(filters: dict) -> dict:
    """
    根据用户提供的筛选条件，分析职业前景并返回匹配的职位列表。

    Args:
        filters (dict): 包含筛选条件的字典，可能包含以下键:
            'jobTitle', 'location', 'education', 'major', 'experience'。

    Raises:
        ConnectionError: 如果数据库未连接。

    Returns:
        dict: 包含分析结果的字典。
              - 如果没有匹配项或未提供条件，返回相应的提示信息。
              - 成功时返回结构:
                {
                  "salary": { "avg": int, "min": int, "max": int, "count": int },
                  "portrait": str,
                  "jobs": list[dict]
                }
    """
    # 确保数据库连接可用
    if not app.db:
        raise ConnectionError("数据库未连接，无法进行分析。")

    # --- 1. 构建动态 SQL 查询 ---
    # 基础查询语句，用于获取职位列表以供前端展示
    base_query = "SELECT title, place, salary, experience, education, companytype, industry FROM qcwy WHERE 1=1"
    conditions = []
    params = []

    # 动态地根据用户输入的 filters 构建 SQL 的 WHERE 子句和参数列表
    # 这种方式可以有效防止 SQL 注入
    if filters.get('jobTitle'):
        conditions.append("title LIKE %s")
        params.append(f"%{filters['jobTitle']}%")
    if filters.get('location'):
        conditions.append("place LIKE %s")
        params.append(f"%{filters['location']}%")
    if filters.get('education') and filters['education'] != '不限':
        conditions.append("education LIKE %s")
        params.append(f"%{filters['education']}%")
    if filters.get('major'):
        conditions.append("(description LIKE %s OR title LIKE %s)")
        params.extend([f"%{filters['major']}%", f"%{filters['major']}%"])
    if filters.get('experience') and filters['experience'] != '不限':
        # 对经验关键词做简单处理，以匹配数据库中的格式
        exp_keyword = filters['experience'].replace('经验', '')
        conditions.append("experience LIKE %s")
        params.append(f"%{exp_keyword}%")

    # 如果用户未提供任何筛选条件，则直接返回提示信息
    if not conditions:
        return {"message": "请输入至少一个查询条件。"}

    # 组合最终的 SQL 查询语句
    base_query += " AND " + " AND ".join(conditions)
    # 限制返回的职位数量，避免一次性返回过多数据导致前端卡顿或浏览器崩溃
    base_query += " LIMIT 100"

    # --- 2. 执行查询并获取数据 ---
    print("执行前景分析查询:", base_query)
    print("查询参数:", params)

    # 【设计说明】这里执行了两次查询：
    # 1. df_for_analysis: 用于后端统计分析，查询的是处理过的数值型薪资字段 (min_pay, max_pay)。
    # 2. df_for_display: 用于前端展示，查询的是原始的文本型薪资字段 (salary)。
    df_for_analysis = pd.read_sql_query(base_query.replace('salary', 'min_pay, max_pay, ave_pay'), app.db,
                                        params=params)
    df_for_display = pd.read_sql_query(base_query, app.db, params=params)

    # 统一列名为小写，便于后续处理
    df_for_analysis.columns = [c.lower() for c in df_for_analysis.columns]

    sample_size = len(df_for_analysis)

    # 如果查询结果为空，返回一个默认的空结构体
    if sample_size == 0:
        return {
            "salary": {"avg": 0, "min": 0, "max": 0, "count": 0},
            "portrait": "抱歉，根据您的条件没有找到匹配的职位数据。",
            "jobs": []
        }

    # --- 3. 计算薪资统计与生成用户画像 ---
    # 计算薪资统计信息，使用 .dropna() 避免空值对计算的影响
    salary_stats = {
        "avg": int(df_for_analysis['ave_pay'].dropna().mean()) if not df_for_analysis['ave_pay'].dropna().empty else 0,
        "min": int(df_for_analysis['min_pay'].dropna().min()) if not df_for_analysis['min_pay'].dropna().empty else 0,
        "max": int(df_for_analysis['max_pay'].dropna().max()) if not df_for_analysis['max_pay'].dropna().empty else 0,
        "count": sample_size
    }

    # 使用 value_counts 快速计算学历和经验的分布，并取前三名
    edu_dist = df_for_analysis['education'].value_counts(normalize=True).nlargest(3)
    edu_portrait = ", ".join(
        [f"{idx}({val:.0%})" for idx, val in edu_dist.items()]) if not edu_dist.empty else "暂无数据"
    exp_dist = df_for_analysis['experience'].value_counts(normalize=True).nlargest(3)
    exp_portrait = ", ".join(
        [f"{idx}({val:.0%})" for idx, val in exp_dist.items()]) if not exp_dist.empty else "暂无数据"

    portrait_text = f"在符合条件的职位中：\n- 学历要求主要集中在: {edu_portrait}。\n- 经验要求主要集中在: {exp_portrait}。"

    # --- 4. 准备职位列表用于前端展示 ---
    # 将用于展示的 DataFrame 转换为字典列表，这是标准的 JSON API 格式
    job_list = df_for_display.to_dict('records')

    # --- 5. 组合并返回最终结果 ---
    return {
        "salary": salary_stats,
        "portrait": portrait_text,
        "jobs": job_list
    }
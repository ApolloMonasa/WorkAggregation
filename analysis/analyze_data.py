# /analysis/analyze_data.py

# ==============================================================================
#  数据分析模块 - 步骤 3: 数据分析与计算
# ==============================================================================
#
#  说明:
#  此模块是数据分析流程的最后一步。它负责查询经过预处理的数据库，
#  执行各种统计和聚合计算，并将最终用于生成图表的数据写入 `conf.ini` 文件。
#
#  核心功能:
#  1. 使用装饰器 `@ways` 自动注册所有分析函数。
#  2. `main` 函数作为调度器，按顺序执行所有已注册的分析函数。
#  3. 每个 `fX` 函数对应一个独立的分析任务，通常为一个图表准备数据。
#  4. 大量使用 Pandas 和 NumPy 库进行高效的数据处理和计算。
#  5. 最终产出是更新后的 `conf.ini` 文件，其中的 `[chart]` 部分包含了
#     所有图表所需的数据。
#
# ==============================================================================

import analysis_main as A  # 导入中心枢纽以访问共享资源
import numpy as np
import pandas as pd
import pyecharts
import re
import traceback  # 仅在异常处理时导入，以减少不必要的加载


def ways(func):
    """
    装饰器：将分析函数注册到 `Analyze` 类的 `analyze_fn_list` 中。
    这使得 `main` 函数可以自动发现并执行所有被此装饰器标记的函数。
    """
    A.Analyze.analyze_fn_list.append(func)

    def wrapper(*args, **kw):
        return func(*args, **kw)
    return wrapper


def main():
    """
    数据分析流程的主入口函数。
    负责初始化环境、调度并执行所有注册的分析函数。
    """
    global cursor, db, conf
    # 从共享上下文中获取数据库连接和配置对象
    cursor = A.Analyze.cursor
    db = A.Analyze.db
    conf = A.Analyze.conf

    # 在每次运行时，清空旧的图表配置，确保生成全新的配置
    if conf.has_section('chart'):
        conf.remove_section('chart')
    conf.add_section('chart')

    print("开始执行分析...")
    # 遍历并执行所有通过 @ways 装饰器注册的分析函数
    for fn in A.Analyze.analyze_fn_list:
        try:
            fn()
            print(f"  -> {fn.__name__} 完成")
        except Exception as e:
            # 捕获单个分析函数的异常，防止整个流程中断
            print(f"  -> !!! 分析函数 {fn.__name__} 执行出错: {e}")
            traceback.print_exc()

    # 将所有分析结果写入配置文件
    with open('conf.ini', 'w', encoding='utf-8') as configfile:
        conf.write(configfile)
    print("分析完成！")


def execute_and_fetch_with_mock_number(sql_query):
    """
    执行SQL查询并为每行结果附加一个模拟的计数值(1)。
    这是一个辅助函数，用于统一处理那些本身不包含计数的查询结果，
    使其数据结构与包含计数的查询结果一致。
    """
    cursor.execute(sql_query)
    results = cursor.fetchall()
    return [row + (1,) for row in results]


@ways
def f1():
    """为图表1：传统职业与新兴职业的薪资分布箱线图准备数据。"""
    # 查询传统职业薪资
    re1 = execute_and_fetch_with_mock_number("select ave_pay from 传统职业 where ave_pay is not null limit 10000")
    m = [x[0] for x in re1]
    n = [x[1] for x in re1]
    a = np.repeat(m, n) if m else np.array([])
    a1 = a[(a != a.max()) & (a != a.min())].tolist() if a.size > 0 else []

    # 查询新兴职业薪资
    re2 = execute_and_fetch_with_mock_number("select ave_pay from 新兴职业 where ave_pay is not null limit 10000")
    m = [x[0] for x in re2]
    n = [x[1] for x in re2]
    a = np.repeat(m, n) if m else np.array([])
    b1 = a[(a != a.max()) & (a != a.min())].tolist() if a.size > 0 else []

    # 使用pyecharts工具准备箱线图数据格式
    q = [a1, b1]
    re1 = pyecharts.Boxplot.prepare_data(q)
    conf.set('chart', 'chart.1.1', str(re1))


@ways
def f2():
    """为图表2：大数据职位的行业分布条形图准备数据。"""
    results = execute_and_fetch_with_mock_number("select industry from 大数据职位 where industry is not null and industry != ''")
    industries = [row[0] for row in results]
    nums = [row[1] for row in results]

    # 清洗和聚合行业数据，处理 "互联网/游戏" 这样的组合字段
    a = {}
    for i, industry_str in enumerate(industries):
        x = re.split(r'[,/]', industry_str)
        for k in x:
            k = k.strip()
            if k: a[k] = a.get(k, 0) + nums[i]

    b = sorted(a.items(), key=lambda item: item[1], reverse=True)
    hy = [x[0] for x in b[:10]]
    n = [x[1] for x in b[:10]]
    conf.set('chart', 'chart.2.1', str(hy))
    conf.set('chart', 'chart.2.2', str(n))


@ways
def f3():
    """为图表3：热门职位-城市需求热力图准备数据。"""
    l_views = [v for v in A.Analyze.available_views if '工程师' in v or '经理' in v or '总监' in v or '负责人' in v]
    if not l_views: return

    city = ['上海', '深圳', '广州', '北京', '武汉', '成都', '杭州', '南京', '西安', '苏州']
    v = []
    # 1. 找出职位数量排名前10的职位类别
    for view_name in l_views:
        safe_view_name = '`' + view_name.replace("'", "''") + '`'
        re_data = execute_and_fetch_with_mock_number(f"select 1 from {safe_view_name} limit 10000")
        s = sum(row[1] for row in re_data)
        if s > 0: v.append([s, view_name])

    v.sort(key=lambda item: item[0], reverse=True)
    if not v: return
    top_10_jobs = [item[1] for item in v[:10]]

    # 2. 对每个热门职位，统计其在主要城市的分布
    x = []
    for job_name in top_10_jobs:
        safe_view_name = '`' + job_name.replace("'", "''") + '`'
        re_data = execute_and_fetch_with_mock_number(f"select place from {safe_view_name} limit 10000")
        a = {}
        for row in re_data:
            if row[0] in city:
                a[row[0]] = a.get(row[0], 0) + row[1]
        for key, value in a.items():
            # 格式化为 [城市, 职位, 数量] 的热力图数据格式
            x.append([key, job_name, value])

    ct = list(set([w[0] for w in x]))
    conf.set('chart', 'chart.3.1', str(ct))
    conf.set('chart', 'chart.3.2', str(top_10_jobs))
    conf.set('chart', 'chart.3.3', str(x))


@ways
def f4():
    """为图表4：全国平均薪资Top10城市条形图准备数据。"""
    re = execute_and_fetch_with_mock_number("select place, ave_pay from qcwy where ave_pay is not null limit 10000")
    df = pd.DataFrame(re, columns=["place", "ave_pay", "number"])
    df = df.dropna()

    # 按城市分组计算平均薪资并排序
    avg_pay_by_city = df.groupby('place')['ave_pay'].mean().round(2).sort_values(ascending=False)
    # 过滤掉省级、自治区等非城市单位
    valid_cities = [city for city in avg_pay_by_city.index if '省' not in city and '自治' not in city and '台湾' not in city and '国外' not in city]

    top_10 = avg_pay_by_city[valid_cities][:10]
    conf.set('chart', 'chart.4.1', str(top_10.index.tolist()))
    conf.set('chart', 'chart.4.2', str(top_10.values.tolist()))


@ways
def f5():
    """为图表5：大数据职位需求量Top10城市条形图准备数据。"""
    re = execute_and_fetch_with_mock_number("select place from 大数据职位")
    df = pd.DataFrame(re, columns=['place', 'num'])
    df = df.dropna()
    a = df.groupby('place')['num'].sum().sort_values(ascending=False)

    c = a.index[:10].tolist()
    b = a.values[:10].tolist()
    conf.set('chart', 'chart.5.1', str(c))
    conf.set('chart', 'chart.5.2', str(b))


@ways
def f6():
    """为图表6：学历-经验与薪资关系3D散点图准备数据。"""
    sql = "SELECT education, experience, ave_pay FROM qcwy WHERE ave_pay IS NOT NULL AND experience IS NOT NULL AND experience REGEXP '^[0-9]+$' LIMIT 10000"
    re = execute_and_fetch_with_mock_number(sql)
    if not re: return

    df = pd.DataFrame(re, columns=['education', 'experience', 'ave_pay', 'number'])
    df['experience'] = pd.to_numeric(df['experience'], errors='coerce')
    df.dropna(subset=['experience'], inplace=True)
    df['experience'] = df['experience'].astype(int)
    if df.empty: return

    # 定义学历和经验的展示顺序
    p = ['', '中专', '大专', '本科', '硕士']
    w = sorted(df['experience'].unique())

    # 按学历和经验分组，计算平均薪资
    grouped = df.groupby(['education', 'experience'])['ave_pay'].mean().round(2)

    t = []
    for i in p:
        for j in w:
            try:
                if (i, j) in grouped.index:
                    v = grouped.loc[(i, j)]
                    j_str = str(j) + '年'
                    i_str = i if i else '不限'
                    t.append([i_str, j_str, v])
            except KeyError:
                continue

    w_str = [str(exp) + '年' for exp in w]
    p[0] = '不限'  # 将空字符串替换为更友好的标签
    conf.set('chart', 'chart.6.1', str(p))
    conf.set('chart', 'chart.6.2', str(w_str))
    conf.set('chart', 'chart.6.3', str(t))


@ways
def f7():
    """为图表7：学历与薪资、需求量关系图准备数据。"""
    re = execute_and_fetch_with_mock_number("select education, ave_pay from qcwy where ave_pay is not null and education is not null and education != '' limit 10000")
    if not re: return
    df = pd.DataFrame(re, columns=['education', 'pay', 'num'])

    # 按学历分组，聚合计算平均薪资和职位总数
    result = df.groupby('education').agg({'pay': 'mean', 'num': 'sum'}).round(2)
    result.index = [idx if idx else '不限' for idx in result.index]

    conf.set('chart', 'chart.7.1', str(result.index.tolist()))
    conf.set('chart', 'chart.7.2', str(result['pay'].tolist()))
    conf.set('chart', 'chart.7.3', str(result['num'].tolist()))


@ways
def f10():
    """为图表10：传统与新兴职业对学历、经验要求的对比饼图准备数据。"""
    re1 = execute_and_fetch_with_mock_number("select experience, education from 传统职业 where experience is not null and education is not null and experience != '' and education != '' limit 10000")
    if not re1: return
    df1 = pd.DataFrame(re1, columns=['experience', 'education', 'number'])
    df1['experience'] = pd.to_numeric(df1['experience'], errors='coerce').dropna().astype(int)

    re2 = execute_and_fetch_with_mock_number("select experience, education from 新兴职业 where experience is not null and education is not null and experience != '' and education != '' limit 10000")
    if not re2: return
    df2 = pd.DataFrame(re2, columns=['experience', 'education', 'number'])
    df2['experience'] = pd.to_numeric(df2['experience'], errors='coerce').dropna().astype(int)

    a = ['', '中专', '大专', '本科', '硕士']
    # 传统职业学历分布
    q1 = df1.groupby('education')['number'].sum()
    b = [idx for idx in q1.index if idx in a]
    c = q1[b].values.tolist()
    if '' in b: b[b.index('')] = '不限'

    # 新兴职业学历分布
    q2 = df2.groupby('education')['number'].sum()
    d = [idx for idx in q2.index if idx in a]
    f = q2[d].values.tolist()
    if '' in d: d[d.index('')] = '不限'

    # 传统职业经验分布
    p1 = df1.groupby('experience')['number'].sum()
    k = [str(idx) + '年' for idx in p1.index]

    # 新兴职业经验分布
    p2 = df2.groupby('experience')['number'].sum()
    j = [str(idx) + '年' for idx in p2.index]

    conf.set('chart', 'chart.10.1', str(b))
    conf.set('chart', 'chart.10.2', str(c))
    conf.set('chart', 'chart.10.3', str(d))
    conf.set('chart', 'chart.10.4', str(f))
    conf.set('chart', 'chart.10.5', str(k))
    conf.set('chart', 'chart.10.6', str(p1.values.tolist()))
    conf.set('chart', 'chart.10.7', str(j))
    conf.set('chart', 'chart.10.8', str(p2.values.tolist()))


@ways
def f11():
    """为图表11：热门职位对工作经验要求条形图准备数据。"""
    l_views = [v for v in A.Analyze.available_views if '工程师' in v or '经理' in v or '总监' in v or '负责人' in v]
    if not l_views: return
    a = []
    for view_name in l_views:
        safe_view_name = '`' + view_name.replace("'", "''") + '`'
        re_data = execute_and_fetch_with_mock_number(f"select experience from {safe_view_name} where experience is not null and experience != '' limit 10000")
        if not re_data: continue
        df = pd.DataFrame(re_data, columns=['experience', 'number'])
        df['experience'] = pd.to_numeric(df['experience'], errors='coerce').dropna().astype(int)
        if df.empty: continue
        # 加权平均计算平均经验要求
        avg_exp = (df['experience'] * df['number']).sum() / df['number'].sum()
        a.append([avg_exp, view_name])

    a.sort(key=lambda item: item[0], reverse=True)
    if not a: return
    x = [item[1] for item in a[:10]]
    y = [round(item[0], 2) for item in a[:10]]
    conf.set('chart', 'chart.11.1', str(x))
    conf.set('chart', 'chart.11.2', str(y))


@ways
def f12():
    """为图表12：工作经验与薪资、需求量关系气泡图准备数据。"""
    re = execute_and_fetch_with_mock_number("select experience, ave_pay from qcwy where experience is not null and experience != '' and ave_pay is not null limit 10000")
    if not re: return
    df = pd.DataFrame(re, columns=['experience', 'pay', 'num'])
    df['experience'] = pd.to_numeric(df['experience'], errors='coerce').dropna().astype(int)

    result = df.groupby('experience').agg({'pay': 'mean', 'num': 'sum'}).round(2)

    # 格式化为 [经验, 需求量, 平均薪资] 的气泡图数据
    data = [[idx, row['num'], row['pay']] for idx, row in result.iterrows()]
    conf.set('chart', 'chart.12.1', str(data))


@ways
def f13():
    """为图表13：热门职位薪资词云图准备数据。"""
    # 此函数复用 f15 的计算结果来生成词云图的数据。
    f15()
    if A.Analyze.conf.has_option('chart', 'chart.15.1'):
        conf.set('chart', 'chart.13.1', conf.get('chart', 'chart.15.1'))
        conf.set('chart', 'chart.13.2', conf.get('chart', 'chart.15.2'))
        conf.set('chart', 'chart.13.3', str([1] * 10))  # 词云权重，此处简化为相同权重


@ways
def f14():
    """为图表14：非技术岗位的薪资排行条形图准备数据。"""
    # 筛选出非“工程师”类的职位视图
    l_views = [v for v in A.Analyze.available_views if 'view' not in v and ('工程师' not in v or '师' not in v)]
    if not l_views: return
    a = {}
    for view_name in l_views:
        safe_view_name = '`' + view_name.replace("'", "''") + '`'
        re_data = execute_and_fetch_with_mock_number(f"select ave_pay from {safe_view_name} where ave_pay is not null limit 10000")
        if not re_data: continue
        df = pd.DataFrame(re_data, columns=['ave_pay', 'number'])
        avg_pay = (df['ave_pay'] * df['number']).sum() / df['number'].sum()
        a[view_name.replace('\\', '/')] = avg_pay

    list_words = sorted(a.items(), key=lambda item: item[1], reverse=True)
    if not list_words: return
    p = [item[0] for item in list_words[:10]]
    q = [round(item[1]) for item in list_words[:10]]
    conf.set('chart', 'chart.14.1', str(p))
    conf.set('chart', 'chart.14.2', str(q))


@ways
def f15():
    """为图表15：热门职位薪资排行条形图准备数据。"""
    l_views = [v for v in A.Analyze.available_views if '工程师' in v or '经理' in v or '总监' in v or '负责人' in v]
    if not l_views: return
    a = []
    for view_name in l_views:
        safe_view_name = '`' + view_name.replace("'", "''") + '`'
        re_data = execute_and_fetch_with_mock_number(f"select ave_pay from {safe_view_name} where ave_pay is not null limit 10000")
        if not re_data: continue
        df = pd.DataFrame(re_data, columns=['ave_pay', 'number'])
        avg_pay = (df['ave_pay'] * df['number']).sum() / df['number'].sum()
        a.append([avg_pay, view_name])

    a.sort(key=lambda item: item[0], reverse=True)
    if not a: return
    x = [item[1] for item in a[:10]]
    y = [round(item[0]) for item in a[:10]]
    conf.set('chart', 'chart.15.1', str(x))
    conf.set('chart', 'chart.15.2', str(y))


@ways
def f16():
    """为图表16：全国职位需求量Top10城市条形图准备数据。"""
    re = execute_and_fetch_with_mock_number("select place from qcwy where place is not null")
    df = pd.DataFrame(re, columns=['place', 'num']).dropna()
    w = df.groupby('place')['num'].sum().sort_values(ascending=False)

    c = w.index[:10].tolist()
    d = w.values[:10].tolist()
    conf.set('chart', 'chart.16.1', str(c))
    conf.set('chart', 'chart.16.2', str(d))


@ways
def f17():
    """为图表17：热门技术岗位薪资排行条形图准备数据。"""
    l_views = [v for v in A.Analyze.available_views if '工程师' in v]
    if not l_views: return
    x = []
    for view_name in l_views:
        safe_view_name = '`' + view_name.replace("'", "''") + '`'
        re_data = execute_and_fetch_with_mock_number(f"select ave_pay from {safe_view_name} where ave_pay is not null")
        if not re_data: continue
        df = pd.DataFrame(re_data, columns=['ave_pay', 'number'])
        avg_pay = (df['ave_pay'] * df['number']).sum() / df['number'].sum()
        x.append([avg_pay, view_name])

    x.sort(key=lambda item: item[0], reverse=True)
    if not x: return
    jn = [item[1] for item in x[:10]]
    mo = [round(item[0]) for item in x[:10]]
    conf.set('chart', 'chart.17.1', str(jn))
    conf.set('chart', 'chart.17.2', str(mo))


@ways
def f18():
    """为图表18：新兴职业内部构成饼图准备数据。"""
    re = execute_and_fetch_with_mock_number("select title from 新兴职业 limit 10000")
    if not re: return
    df = pd.DataFrame(re, columns=['job', 'number'])

    keywords = ['学习', '人工智能', '数据', '区块链', '算法', '物联网', '视觉', '自然语言']
    b = {}
    total_num = df['number'].sum()
    if total_num == 0: return

    for kw in keywords:
        count = df[df['job'].str.contains(kw)]['number'].sum()
        if count > 0:
            b[kw] = count

    # 格式化标签名称
    job = [k.replace('数据', '大数据').replace('学习', '机器学习').replace('视觉', '机器视觉') for k in b.keys()]
    num = [round(v / total_num, 2) for v in b.values()]
    conf.set('chart', 'chart.18.1', str(job))
    conf.set('chart', 'chart.18.2', str(num))
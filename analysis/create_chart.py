# /analysis/create_chart.py

# ==============================================================================
#  数据分析模块 - 图表生成
# ==============================================================================
#
#  说明:
#  此模块是所有 pyecharts 图表对象的定义中心。每个以 `t` 开头的函数 (`t1`, `t2`, ...)
#  都负责创建一个特定的图表实例。
#
#  核心机制:
#  1. `@ways` 装饰器: 自动将每个图表生成函数注册到一个全局列表中
#     (`A.Analyze.chart_fn_list`)。这使得服务器端代码可以按 ID 动态调用它们。
#
#  2. 数据传递: 每个图表函数接收一个生成器 `pa` 作为参数。这个生成器
#     从 `conf.ini` 文件中读取由 `analyze_data.py` 模块计算并存储的数据。
#     通过 `next(pa)` 依次获取所需的数据片段。
#
# ==============================================================================

import os
import sys
import pyecharts as p
import configparser
import random

from pyecharts import Style

# 确保可以从父目录导入 analysis_main 模块
script_path = os.path.realpath(__file__)
script_dir = os.path.dirname(script_path)
sys.path.append(script_dir)

# 导入中心枢纽，以访问共享的应用上下文（特别是函数注册列表）
import analysis_main as A


def ways(func):
    """
    装饰器：将一个函数注册为图表生成函数。
    所有被此装饰器标记的函数都会被添加到 `A.Analyze.chart_fn_list` 中，
    以便服务器可以按索引动态调用它们。
    """
    A.Analyze.chart_fn_list.append(func)

    def wrapper(*args, **kw):
        return func(*args, **kw)
    return wrapper


def parameter(fn):
    """
    (测试/旧版功能) 参数生成器。
    从配置文件中读取与指定函数相关的数据。
    注意：在 Web 服务器中，此逻辑被 `.server.py` 中的 `parameter_generator` 替代。
    """
    name = fn.__name__.replace('t', '')
    for i in range(1, 50):
        pa = 'chart.' + name + '.' + str(i)
        yield eval(conf_chart[pa])


def main():
    """
    (测试/旧版功能) 主函数。
    用于在命令行环境中一次性生成所有已注册的图表。
    不用于 Web 服务器的正常运行。
    """
    global conf_chart
    conf = configparser.ConfigParser()
    conf.read('./conf/conf.ini')
    conf_chart = conf['chart']
    p.configure(global_theme='macarons')  # 设置全局主题

    charts = []
    for fn in A.Analyze.chart_fn_list:
        pa = parameter(fn)
        x = fn(pa)
        x.width = '100%'
        # 为特定图表设置自定义尺寸
        if fn.__name__ == 't3':
            x.width, x.height = 650, 500
        if fn.__name__ == 't12':
            x.width, x.height = 700, 500
        if fn.__name__ == 't21':
            x.width, x.height = 700, 500

        charts.append(x)
    return charts


# --- 图表定义区 ---
# 每个 `tX` 函数都对应一个图表的生成逻辑。

@ways
def t1(pa):
    """生成图表1：新兴职业与传统职业薪水对比的箱线图。"""
    y = next(pa)
    box = p.Boxplot('新兴与传统职业薪水对比')
    box.add('传统职业', ['薪水'], [y[0]], is_toolbox_show=False)
    box.add('新兴职业', ['薪水'], [y[1]], is_toolbox_show=False)
    return box


@ways
def t2(pa):
    """生成图表2：需求量前10的行业的条形图。"""
    bar = p.Bar('需求前10的行业')
    bar.add('需求', next(pa), next(pa), is_toolbox_show=False)
    return bar


@ways
def t3(pa):
    """生成图表3：热门职位在主要城市的需求分布热力图。"""
    hm = p.HeatMap('地区职位与需求关系', width=1500, height=600)
    hm.add("需求量", next(pa), next(pa), next(pa), is_visualmap=True, visual_range=[350, 25000],
           visual_text_color="#000", visual_orient='horizontal', yaxis_label_textsize=8,
           yaxis_rotate=-45, is_toolbox_show=False)
    return hm


@ways
def t4(pa):
    """生成图表4：全国平均薪资Top10城市的条形图。"""
    bar = p.Bar("薪资前10城市")
    bar.add("薪资", next(pa), next(pa), mark_line=["average"], is_toolbox_show=False)
    return bar


@ways
def t5(pa):
    """生成图表5：大数据职位需求量Top10城市的条形图。"""
    bar = p.Bar("大数据职位需求前10城市")
    bar.add("需求量", next(pa), next(pa), mark_line=["average"], is_toolbox_show=False)
    return bar


@ways
def t6(pa):
    """生成图表6：学历、经验与薪资关系的3D柱状图。"""
    bar3d = p.Bar3D("学历经验与薪水关系", width=1200, height=500)
    range_color = ['#313695', '#4575b4', '#74add1', '#abd9e9', '#e0f3f8', '#ffffbf',
                   '#fee090', '#fdae61', '#f46d43', '#d73027', '#a50026']
    bar3d.add("学历经验薪水", next(pa), next(pa), next(pa),
              is_visualmap=True, visual_range=[4000, 30000],
              visual_range_color=range_color, grid3d_width=150, grid3d_depth=80,
              is_grid3d_rotate=True, grid3d_shading='realistic', grid3d_rotate_speed=30, is_toolbox_show=False)
    return bar3d


@ways
def t7(pa):
    """生成图表7：学历与需求量、薪资关系的散点图。"""
    scatter = p.Scatter("学历与需求量、薪水关系")
    scatter.add("薪水", next(pa), next(pa), extra_data=next(pa), is_visualmap=True,
                xaxis_type="category", visual_dimension=2, visual_range=[500, 500000],
                is_toolbox_show=False, visual_top=9999)
    return scatter


@ways
def t8(pa):
    """生成图表8：山东省内薪资前10的城市排名的条形图。"""
    bar = p.Bar("山东薪水前10的城市排名")
    bar.add("薪水", next(pa), next(pa), mark_line=['average'], is_toolbox_show=False)
    return bar


@ways
def t9(pa):
    """生成图表9：山东省计算机职位地理分布图。"""
    # 辅助函数，用于解决 pyecharts 旧版本地图标签不显示数值的 bug
    def label_formatter(params):
        return params.value[2]

    style = p.Style(
        title_color="#fff",
        title_pos="center",
        width=1200,
        height=600,
        background_color='#404a59'
    )
    chart = p.Geo("山东省计算机职位分布", '数据来自齐鲁人才网，部分地区数据不准确', **style.init_style, subtitle_text_size=18)
    city = [i.replace('市', '') for i in next(pa)]
    chart.add("", city, next(pa), maptype='山东', visual_range=[0, 700], label_formatter=label_formatter,
              visual_text_color="#fff", is_legend_show=True,
              symbol_size=15, is_visualmap=True,
              tooltip_formatter='{b}',
              label_emphasis_textsize=15,
              label_emphasis_pos='right', is_toolbox_show=False)
    return chart


@ways
def t10(pa):
    """生成图表10：新兴与传统职业对学历、经验需求对比的多重饼图。"""
    pie = p.Pie("新兴与传统职业学历经验需求对比", width=700, height=400)
    pie.add("传统学历需求", next(pa), next(pa), radius=[50, 55], center=[35, 53])
    pie.add("新兴学历需求", next(pa), next(pa), radius=[0, 45], center=[35, 53], rosetype='radius', is_random=True)
    pie.add("传统经验需求", next(pa), next(pa), radius=[50, 55], center=[70, 53])
    pie.add("新兴经验需求", next(pa), next(pa), radius=[0, 45], center=[70, 53], rosetype='radius',
            legend_orient='vertical', legend_pos='left', legend_top='center', is_random=True, is_toolbox_show=False)
    return pie


@ways
def t11(pa):
    """生成图表11：经验要求前十的职位的条形图。"""
    bar = p.Bar('经验要求前十的职位')
    bar.add('经验', next(pa), next(pa), mark_line=["average"], is_toolbox_show=False)
    return bar


@ways
def t12(pa):
    """生成图表12：工作经验、需求量与薪资关系的3D气泡图。"""
    range_color = [
        '#313695', '#4575b4', '#74add1', '#abd9e9', '#e0f3f8', '#ffffbf',
        '#fee090', '#fdae61', '#f46d43', '#d73027', '#a50026']
    scatter3D = p.Scatter3D("经验需求薪水")
    scatter3D.add("3D", next(pa), is_visualmap=True, visual_range_color=range_color, is_grid3d_rotate=True,
                  visual_range=[7000, 35000], xaxis3d_name='经验', yaxis3d_name='需求', zaxis3d_name='薪水',
                  is_toolbox_show=False, visual_top=9999)
    return scatter3D


@ways
def t13(pa):
    """生成图表13：职位与需求量、薪水关系的特效散点图。"""
    scatter = p.EffectScatter("职位与需求量、薪水关系")
    next(pa)
    y = next(pa)
    y2 = next(pa)
    for i in range(len(y)):
        scatter.add("薪水", [y[i]], [y2[i]], is_toolbox_show=False, symbol_size=15,
                    effect_scale=3, effect_period=3, symbol=random.choice(['roundRect', 'pin']),
                    effect_brushtype='fill')
    return scatter


@ways
def t14(pa):
    """生成图表14：计算机专业薪资前10方向的条形图。"""
    bar = p.Bar('计算机专业薪水前10方向')
    y = next(pa)
    y_values = next(pa)
    for i in range(len(y) - 4):
        bar.add(y[i], ['薪水'], [y_values[i]], is_show=True, is_toolbox_show=False, is_label_show=True,
                label_formatter='{a}')
    bar.add(y[-1], ['薪水'], [y_values[-1]], is_legend_show=False, is_toolbox_show=False, is_label_show=True,
            label_formatter='{a}')
    return bar


@ways
def t15(pa):
    """生成图表15：薪水前十的职位的条形图。"""
    bar = p.Bar('薪水前十的职位')
    bar.add('薪水', next(pa), next(pa), mark_line=["average"], is_toolbox_show=False)
    return bar


@ways
def t16(pa):
    """生成图表16：计算机专业需求前十城市的条形图。"""
    bar = p.Bar('计算机专业需求前十城市')
    bar.add('需求量', next(pa), next(pa), mark_line=['average'], is_toolbox_show=False)
    return bar


@ways
def t17(pa):
    """生成图表17：高薪水技能的条形图。"""
    bar = p.Bar("高薪水技能")
    y = next(pa)
    y_values = next(pa)
    for i in range(len(y) - 4):
        if i % 3 == 1:
            bar.add(y[i], ['薪水'], [y_values[i]], is_show=True, mark_line=['average'], is_toolbox_show=False)
        else:
            bar.add(y[i], ['薪水'], [y_values[i]], is_show=True, is_toolbox_show=False)
    return bar


@ways
def t18(pa):
    """生成图表18：各新兴职业所占比例的水球图。"""
    shape = ("path://M367.855,428.202c-3.674-1.385-7.452-1.966-11.146-1"
             ".794c0.659-2.922,0.844-5.85,0.58-8.719 c-0.937-10.407-7."
             "663-19.864-18.063-23.834c-10.697-4.043-22.298-1.168-29.9"
             "02,6.403c3.015,0.026,6.074,0.594,9.035,1.728 c13.626,5."
             "151,20.465,20.379,15.32,34.004c-1.905,5.02-5.177,9.115-9"
             ".22,12.05c-6.951,4.992-16.19,6.536-24.777,3.271 c-13.625"
             "-5.137-20.471-20.371-15.32-34.004c0.673-1.768,1.523-3.423"
             ",2.526-4.992h-0.014c0,0,0,0,0,0.014 c4.386-6.853,8.145-14"
             ".279,11.146-22.187c23.294-61.505-7.689-130.278-69.215-153"
             ".579c-61.532-23.293-130.279,7.69-153.579,69.202 c-6.371,"
             "16.785-8.679,34.097-7.426,50.901c0.026,0.554,0.079,1.121,"
             "0.132,1.688c4.973,57.107,41.767,109.148,98.945,130.793 c58."
             "162,22.008,121.303,6.529,162.839-34.465c7.103-6.893,17.826"
             "-9.444,27.679-5.719c11.858,4.491,18.565,16.6,16.719,28.643 "
             "c4.438-3.126,8.033-7.564,10.117-13.045C389.751,449.992,"
             "382.411,433.709,367.855,428.202z")
    liquid = p.Liquid("各新兴职业所占比例")
    liquid.add(next(pa), next(pa), is_liquid_outline_show=False, shape=shape, is_toolbox_show=False)
    return liquid


@ways
def t19(pa):
    """生成图表19：大公司对学历要求的微型饼图矩阵。"""
    l1 = next(pa)
    pp = next(pa)
    data = next(pa)
    pie = p.Pie('大公司学历要求')
    style = Style()
    pie_style = style.add(
        is_label_show=True,
        label_pos="center",
        is_label_emphasis=False,
        label_formatter='{b}',
        label_text_size=16,
        is_legend_show=False,
        label_text_color="#000"
    )
    # 此处数据操作是为了适配 pyecharts 的多饼图布局
    for i in range(len(data)):
        data[i][0] = 0
        data[i][1] = 0
    for i in range(len(l1)):
        l1[i] = [l1[i], '', '', '', '', '', '', '']

    # 通过多次 add 创建网格布局的微型图
    pie.add('', l1[0], data[0], center=[10, 25], radius=[13, 18], **pie_style)
    pie.add('', l1[1], data[1], center=[20, 25], radius=[13, 18], legend_pos='left', **pie_style)
    # ... (省略了其余重复的 add 调用)
    pie.add('', l1[26], data[26], center=[90, 80], radius=[13, 18], **pie_style, is_toolbox_show=False)
    return pie


@ways
def t20(pa):
    """生成图表20：各大公司薪资与经验关系的散点图。"""
    scatter = p.Scatter('各大公司工资经验')
    scatter.add('工资', next(pa), next(pa), extra_data=next(pa), is_visualmap=True, visual_dimension=2,
                xaxis_type="category", visual_range=[0, 6], is_toolbox_show=False)
    return scatter


@ways
def t21(pa):
    """生成图表21：大公司福利关键词的词云图。"""
    wordcloud = p.WordCloud('大公司福利', width=1300, height=620)
    wordcloud.add("", next(pa), next(pa), word_size_range=[20, 100], is_toolbox_show=False)
    return wordcloud
# /.server.py

import logging
import os
import threading
import configparser
from flask import Flask, render_template, request, url_for, jsonify

# 导入项目内自定义模块
from analysis import analysis_main, create_chart, interaction
from spider import spider_main

# --- Flask App 初始化与配置 ---

app = Flask(__name__)
# 设置静态文件缓存过期时间为0，确保在开发过程中对静态文件的修改能立即生效
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 0
# 为模板中引用的JS文件设置远程主机路径
REMOTE_HOST = "/static/js"

# --- 日志配置 ---

# 过滤掉 Flask 内置 Web 服务器 (Werkzeug) 的常规 INFO 级别日志，只显示错误
logging.getLogger('werkzeug').setLevel(logging.ERROR)
# 创建一个文件处理器，用于将应用的警告及以上级别的日志写入到 log.txt 文件
handler = logging.FileHandler(filename='log.txt', mode='a', encoding='utf-8')
handler.setLevel(logging.WARNING)
app.logger.addHandler(handler)

# --- 路由定义 ---


@app.route("/")
def index():
    """
    渲染项目首页。
    """
    return render_template('index.html')


@app.route("/爬虫")
def ready_spider():
    """
    渲染爬虫任务配置页面。
    """
    return render_template('spider.html')


@app.route('/爬虫完成', methods=['POST'])
def get_spider():
    """
    处理爬虫配置表单提交，并在后台启动爬虫任务。
    接收 POST 请求，包含城市、职位、数量限制、并发和定时设置。
    """
    # 从表单中获取用户提交的参数
    city_list = request.form.getlist('city')
    job_list = request.form.getlist('job')
    limit_per_task = int(request.form.get('limit', '999999'))
    use_concurrent = 'multithread' in request.form
    enable_timer = 'enable_timer' in request.form

    # 构造定时器设置字典
    timer_settings = {
        "enable": enable_timer,
        "begin_hour": int(request.form.get('begin_hour', 2)),
        "begin_minute": int(request.form.get('begin_minute', 0)),
        "end_hour": int(request.form.get('end_hour', 5)),
        "end_minute": int(request.form.get('end_minute', 0)),
        "interval": int(request.form.get('interval', 60))
    }
    # 构造传递给爬虫主函数的总参数字典
    dict_parameter = {
        "city": city_list,
        "job": job_list,
        "limit": limit_per_task,
        "concurrent": use_concurrent,
        "timer": timer_settings
    }

    # 使用新线程在后台执行爬虫任务，避免阻塞 Web 服务器
    spider_thread = threading.Thread(target=spider_main.main, args=(dict_parameter,), daemon=True)
    spider_thread.start()

    # 根据是否启用定时任务，向用户返回不同的反馈信息
    if enable_timer:
        title, message = "定时任务已启动", "服务器将按时自动执行。"
    else:
        title, message = "爬虫任务已在后台执行", "请稍后查看结果。"

    return render_template('task_feedback.html', title=title, message=message, back_url=url_for('ready_spider'))


@app.route("/爬虫结果")
def result_spider():
    """
    展示爬虫抓取到的原始数据 HTML 文件。
    如果文件不存在，则显示一个提示页面。
    """
    file_path = os.path.join(app.root_path, 'static', 'html', 'data.html')
    if os.path.exists(file_path):
        # send_static_file 会处理正确的 MIME 类型
        return app.send_static_file('html/data.html')
    else:
        return render_template('no_res.html')


@app.route("/分析")
def analyse():
    """
    启动后台数据分析任务，并立即返回一个任务启动成功的反馈页面。
    """
    def run_analysis_in_thread():
        """
        封装分析逻辑，以便在新线程中执行。
        """
        print("后台分析任务开始...")
        try:
            analysis_main.Analyze.main()
            print("后台分析任务完成！")
        except Exception as e:
            app.logger.error(f"后台分析任务出错: {e}", exc_info=True)
            print(f"后台分析任务出错: {e}")
            import traceback
            traceback.print_exc()

    # 创建并启动后台分析线程
    thread = threading.Thread(target=run_analysis_in_thread, daemon=True)
    thread.start()
    return render_template('analysis_feedback.html',
                           title="数据分析任务已启动",
                           message="我们正在后台处理数据，请稍后点击下方按钮查看结果。")


@app.route("/展示")
def showresult():
    """
    渲染数据可视化的主展示页面。
    该页面会动态加载各个图表。
    """
    # 定义需要加载的 ECharts 相关 JS 库
    js_files = ['echarts.min', 'echarts-gl.min', 'macarons', 'echarts-wordcloud.min', 'echarts-liquidfill.min']
    return render_template("show_original.html", script_list=js_files, host=REMOTE_HOST)


@app.route('/chart/<id>')
def showresult1(id):
    """
    根据提供的图表 ID, 动态生成并返回该图表的 HTML 片段。
    这是一个被 /展示 页面 AJAX 请求的接口。
    """
    try:
        chart_id = int(id)
        all_chart_functions = create_chart.A.Analyze.chart_fn_list
        conf = configparser.ConfigParser()
        conf.read('conf.ini', encoding='utf-8')

        if not conf.has_section('chart'):
            raise FileNotFoundError("conf.ini中未找到[chart]节")

        if chart_id >= len(all_chart_functions):
            return f"错误: 图表ID {chart_id} 超出范围。", 404

        # 根据 ID 获取对应的图表生成函数
        target_fn = all_chart_functions[chart_id]

        def parameter_generator(fn, config):
            """
            一个生成器，用于从 conf.ini 文件中动态解析并提供图表函数所需的参数。
            """
            conf_chart = config['chart']
            # 从函数名推断配置项的前缀，例如 't3' -> 'chart.3'
            name = fn.__name__.replace('t', '')
            i = 1
            while True:
                # 构造配置项的 key，如 chart.3.1, chart.3.2 ...
                pa = f'chart.{name}.{i}'
                value = conf_chart.get(pa)
                if value is None:
                    break  # 如果找不到配置项，则停止生成
                # 使用 eval 执行字符串形式的参数，以支持列表、元组等复杂类型
                yield eval(value)
                i += 1

        # 获取参数生成器
        pa = parameter_generator(target_fn, conf)

        # 调用图表函数并传入参数
        chart_obj = target_fn(pa)
        chart_obj.width = '100%'

        # 对特定图表应用自定义的尺寸
        if target_fn.__name__ in ['t3', 't12', 't21']:
            chart_obj.width = 650
            chart_obj.height = 500
        elif target_fn.__name__ == 't6':
            chart_obj.width = 1200
            chart_obj.height = 600

        # 渲染图表为可嵌入的 HTML
        return chart_obj.render_embed()

    except (StopIteration, KeyError, FileNotFoundError):
        return f"<p style='color:red; text-align:center;'>生成图表(ID:{id})失败：分析数据不足或不存在。</p>"
    except Exception as e:
        app.logger.error(f"生成图表 {id} 时发生错误: {e}", exc_info=True)
        return f"生成图表时发生未知错误: {e}", 500


@app.route("/互动")
def interaction_page():
    """
    渲染交互式分析页面。
    """
    return render_template('interaction.html')


@app.route("/api/analyze_prospects", methods=['POST'])
def analyze_prospects_api():
    """
    处理交互式前景分析的 API 请求。
    接收包含筛选条件的 JSON 数据，返回分析结果。
    """
    filters = request.json
    print("接收到前景分析请求:", filters)
    try:
        results = interaction.analyze_prospects(filters)
        return jsonify(success=True, data=results)
    except Exception as e:
        print(f"前景分析API出错: {e}")
        import traceback
        traceback.print_exc()
        return jsonify(success=False, message=str(e))


@app.route("/us")
def us():
    """
    渲染 "关于我们" 页面。
    """
    return render_template('us.html')


# --- 应用启动入口 ---
if __name__ == '__main__':
    # host='0.0.0.0' 使服务可以被局域网内其他设备访问
    # port=80 使用 HTTP 协议的默认端口
    # debug=False 在生产环境中关闭调试模式
    app.run(debug=False, host='0.0.0.0', port=80)
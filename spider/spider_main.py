# /spider/spider_main.py

import time
import datetime
import threading
import queue
import csv
import os
import configparser
from multiprocessing import Process, Queue, freeze_support
from selenium import webdriver

# 导入自定义工具模块
# from spider.tool import timer # 注意：此模块在当前代码中未被使用

# --- 1. 读取城市代码配置 ---

# 创建配置解析器实例
config = configparser.ConfigParser()
# 获取当前文件所在目录
current_dir = os.path.dirname(os.path.abspath(__file__))
# 构造配置文件的完整路径
conf_path = os.path.join(current_dir, 'conf.ini')
# 读取配置文件
config.read(conf_path, encoding='utf-8')


def get_city_code(city_name: str) -> str:
    """
    根据城市名称从 conf.ini 文件中获取对应的城市代码。

    Args:
        city_name (str): 城市的中文名称。

    Returns:
        str: 对应的城市代码。如果找不到，则返回全国代码 '000000' 并打印警告。
    """
    try:
        return config.get('citycode', city_name)
    except (configparser.NoSectionError, configparser.NoOptionError):
        print(f"警告: 在 conf.ini 中未找到城市 '{city_name}' 的代码，将使用全国代码 '000000'。")
        return "000000"


# --- 2. 配置 Selenium WebDriver 选项 ---

options = webdriver.ChromeOptions()
# 禁用 'navigator.webdriver' 标志，防止被网站检测为自动化程序
options.add_argument("--disable-blink-features=AutomationControlled")
# options.add_argument("--headless")  # 无头模式，后台运行浏览器，可根据需要启用
options.add_argument("--start-maximized")  # 启动时最大化窗口
options.add_argument("--no-sandbox")  # 在容器化环境中运行时需要
options.add_argument("--disable-gpu")  # 禁用GPU加速，某些环境下可避免问题
options.add_argument("--disable-dev-shm-usage")  # 解决 Docker 或 CI 环境中的资源限制问题
# 禁用不必要的日志输出
options.add_experimental_option('excludeSwitches', ['enable-logging'])


# ==============================
#  Spider 基类
# ==============================
class BaseSpider(object):
    """
    爬虫基类，封装通用属性和方法。
    """

    def __init__(self, city, job, city_code, queue, driver):
        self.city = city
        self.job = job
        self.city_code = city_code
        self.queue = queue
        self.driver = driver

    def request_json(self, keyword, page_num=1, jobArea="000000"):
        """
        通过执行异步 JavaScript `fetch` 请求来获取招聘数据。
        这种方式比 Selenium 直接操作页面元素更高效且不易被检测。

        Args:
            keyword (str): 搜索的职位关键词。
            page_num (int): 请求的页码。
            jobArea (str): 城市代码。

        Returns:
            dict: API返回的JSON数据，或在出错时返回包含'error'键的字典。
        """
        script = f"""
        var done = arguments[0];
        fetch("https://we.51job.com/api/job/search-pc?api_key=51job&keyword={keyword}&searchType=2&sortType=0&pageNum={page_num}&pageSize=20&jobArea={jobArea}")
            .then(r => r.json()).then(data => done(data)).catch(err => done({{'error': err.toString()}}));
        """
        return self.driver.execute_async_script(script)


# ==============================
#  51job 爬虫实现类
# ==============================
class Job51Spider(BaseSpider):
    """
    针对前程无忧网（51job.com）的爬虫实现。
    继承自 BaseSpider，并添加了抓取数量限制的逻辑。
    """

    def __init__(self, city, job, city_code, queue, driver, limit):
        super().__init__(city, job, city_code, queue, driver)
        self.limit = limit  # 每个任务的最大抓取数量
        self.count = 0      # 当前任务已抓取数量

    def run(self):
        """
        爬虫主执行逻辑。
        循环翻页，直到没有更多数据或达到数量上限。
        """
        # 初始访问页面，主要是为了建立会话和获取cookies
        self.driver.get(f"https://we.51job.com/pc/search?jobArea={self.city_code}")
        time.sleep(3)
        page = 1
        while True:
            # 检查是否已达到抓取数量上限
            if self.count >= self.limit:
                print(f"[数量限制] {self.city}-{self.job} 已抓取 {self.count} 条，达到 {self.limit} 的上限，任务提前结束。")
                break

            data = self.request_json(self.job, page, self.city_code)

            if "error" in data:
                print(f"[错误] 请求API失败: {data['error']}")
                break

            jobs = data.get("resultbody", {}).get("job", {}).get("items", [])
            if not jobs:
                print(f"[完成] {self.city}-{self.job} 所有页面已爬取完毕.")
                break

            for job in jobs:
                if self.count >= self.limit:
                    break
                # 解析并构造结果字典
                result = {
                    "provider": "前程无忧网", "keyword": self.job, "title": job.get("jobName"),
                    "place": job.get("jobAreaString"), "salary": job.get("provideSalaryString"),
                    "experience": job.get("workYearString"), "education": job.get("degreeString"),
                    "companytype": job.get("companyTypeString"),
                    "industry": f"{job.get('companyIndustryType1Str')} / {job.get('companyIndustryType2Str')}",
                    "description": job.get("jobDescribe")
                }
                self.queue.put(result)
                self.count += 1

            print(f"[进度] {self.city}-{self.job} 第 {page} 页抓取成功，当前已抓取 {self.count}/{self.limit} 条")
            page += 1
        return "over"


# ==============================
#  进程：爬虫任务 (生产者)
# ==============================
class SpiderProcess(Process):
    """
    将单个爬虫任务（例如“北京-Java”）封装在一个独立的进程中。
    这是实现并发爬取的关键，每个进程都有自己的 WebDriver 实例，避免了线程安全问题。
    """

    def __init__(self, city, job, queue, limit):
        super().__init__()
        self.city = city
        self.job = job
        self.queue = queue
        self.limit = limit

    def run(self):
        """
        进程启动后执行的方法。
        """
        process_driver = None
        try:
            # 每个进程必须创建自己的 WebDriver 实例
            process_driver = webdriver.Chrome(options=options)
            city_code = get_city_code(self.city)
            print(f"启动爬虫进程: 城市='{self.city}', 职位='{self.job}', 数量上限={self.limit}")
            spider = Job51Spider(self.city, self.job, city_code, self.queue, process_driver, self.limit)
            spider.run()
        except Exception as e:
            print(f"爬虫进程 '{self.city}-{self.job}' 发生严重错误: {e}")
        finally:
            # 确保进程结束时浏览器被正确关闭
            if process_driver:
                process_driver.quit()


# ==============================
#  进程：写入 CSV (消费者)
# ==============================
class WriterProcess(Process):
    """
    独立的写文件进程。
    从队列中获取爬虫进程产生的数据，并写入CSV文件。
    这种“生产者-消费者”模式可以避免多进程写文件冲突，并提高效率。
    """

    def __init__(self, queue, filename="data/qcwy.csv"):
        super().__init__()
        self.queue = queue
        self.filename = filename

    def run(self):
        """
        进程启动后执行的方法。
        """
        # 确保目录存在
        os.makedirs(os.path.dirname(self.filename), exist_ok=True)
        with open(self.filename, "w", newline="", encoding="utf-8-sig") as f:
            fieldnames = ["provider", "keyword", "title", "place", "salary", "experience", "education",
                          "companytype", "industry", "description"]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()

            while True:
                try:
                    # 从队列中获取数据，设置60秒超时
                    item = self.queue.get(timeout=60)
                    # 收到停止信号，结束循环
                    if item == "STOP":
                        print("写入进程收到停止信号，即将退出。")
                        break
                    writer.writerow(item)
                except queue.Empty:
                    # 如果60秒内队列中没有新数据，则认为所有爬虫已结束
                    print("写入进程长时间未收到数据，自动停止。")
                    break


# ==============================
#  HTML 生成函数
# ==============================
def generate_html_from_csv(csv_file="data/qcwy.csv", html_file="static/html/data.html"):
    """
    读取CSV文件内容，并生成一个简单的HTML表格页面用于数据预览。
    """
    os.makedirs(os.path.dirname(html_file), exist_ok=True)
    rows = []
    headers = []
    try:
        with open(csv_file, 'r', encoding='utf-8-sig') as f:
            reader = csv.reader(f)
            headers = next(reader)
            rows = list(reader)
    except (FileNotFoundError, StopIteration):
        print(f"警告: CSV文件 '{csv_file}' 为空或不存在，将生成一个空的HTML表格。")
        if not headers:
            headers = ["provider", "keyword", "title", "place", "salary", "experience",
                       "education", "companytype", "industry", "description"]

    # 拼接HTML字符串
    html_content = [
        '<!DOCTYPE html>', '<html lang="zh-CN">', '<head>',
        '    <meta charset="UTF-8">', '<meta name="viewport" content="width=device-width, initial-scale=1.0">',
        '<title>招聘数据展示</title>', '<style>',
        'body { font-family: Arial, sans-serif; margin: 20px; }',
        'table { width: 100%; border-collapse: collapse; margin-top: 20px; }',
        'th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }',
        'th { background-color: #f2f2f2; }',
        'tr:nth-child(even) { background-color: #f9f9f9; }',
        'tr:hover { background-color: #f1f1f1; }', '</style>',
        '</head>', '<body>', '<h1>招聘数据展示</h1>', '<table>', '<thead>', '<tr>'
    ]
    for header in headers:
        html_content.append(f'<th>{header}</th>')
    html_content.extend(['</tr>', '</thead>', '<tbody>'])
    for row in rows:
        html_content.append('<tr>')
        for cell in row:
            html_content.append(f'<td>{cell}</td>')
        html_content.append('</tr>')
    html_content.extend(['</tbody>', '</table>', '</body>', '''
        <!-- 通用导航栏 -->
        <div class="navbar">
            <a href="/" class="nav-button">返回主页</a>
        </div>
        <style>
            .navbar {
                position: fixed; top: 0; left: 0; width: 100%;
                background-color: #333; padding: 10px 20px;
                box-shadow: 0 2px 5px rgba(0,0,0,0.2); z-index: 1000;
                box-sizing: border-box; /* 确保 padding 不会撑大宽度 */
            }
            .nav-button {
                color: white; text-decoration: none; padding: 8px 15px;
                border-radius: 5px; transition: background-color 0.3s;
            }
            .nav-button:hover { background-color: #555; }
            /* 为页面主体增加上边距，防止被导航栏遮挡 */
            body { padding-top: 60px; }
        </style>
        ''', '</html>'])

    with open(html_file, 'w', encoding='utf-8') as f:
        f.write('\n'.join(html_content))
    print(f"HTML报告已生成: {html_file}")


# ==============================
#  串行任务执行函数
# ==============================
def run_single_task(city, job, queue, limit):
    """
    在主进程中按顺序执行单个爬虫任务。用于非并发模式。
    """
    process_driver = None
    try:
        process_driver = webdriver.Chrome(options=options)
        city_code = get_city_code(city)
        print(f"【串行模式】启动任务: 城市='{city}', 职位='{job}', 数量上限={limit}")
        spider = Job51Spider(city, job, city_code, queue, process_driver, limit)
        spider.run()
    except Exception as e:
        print(f"串行任务 '{city}-{job}' 发生严重错误: {e}")
    finally:
        if process_driver:
            process_driver.quit()


# ==============================
#  封装单次爬取的核心逻辑
# ==============================
def run_crawl_once(dict_parameter: dict):
    """
    执行一次完整的爬取流程。
    该函数负责解析参数、准备文件、启动生产者（爬虫）和消费者（写入）进程，
    并最终生成HTML报告。
    """
    # --- 1. 定义默认的城市和职位关键词列表 ---
    # 如果用户没有提供，则使用这些丰富的默认值
    DEFAULT_CITIES = [
        "北京", "上海", "广州", "深圳", "杭州", "成都", "南京", "武汉", "西安", "苏州",
        "重庆", "长沙", "天津", "青岛", "厦门", "宁波", "大连", "福州", "济南", "无锡",
        "合肥", "郑州", "沈阳", "昆明", "哈尔滨", "石家庄", "南昌", "东莞", "佛山", "珠海",
        "常州", "温州", "全国"
    ]
    DEFAULT_JOBS = [
        "Java", "Python", "Go", "C++", "PHP", "后端开发", "服务器",
        "前端开发", "JavaScript", "Vue", "React", "小程序", "Android", "iOS",
        "数据分析", "数据挖掘", "大数据", "算法工程师", "机器学习", "人工智能", "AI",
        "深度学习", "自然语言处理", "NLP", "推荐系统",
        "软件测试", "测试开发", "自动化测试", "运维", "DevOps", "SRE",
        "游戏开发", "Unity", "UE4", "UE5", "游戏策划",
        "嵌入式", "物联网", "IoT", "硬件",
        "产品经理", "项目经理", "技术支持", "网络安全", "爬虫", "可视化",
        "UI设计师", "销售", "运营"
    ]

    # --- 2. 智能地获取城市和职位列表 ---
    city_list = dict_parameter.get("city") or DEFAULT_CITIES
    job_list = dict_parameter.get("job") or DEFAULT_JOBS

    print("=" * 50)
    print("即将开始的爬取任务:")
    print(f"  -> 城市 ({len(city_list)}个): {', '.join(city_list)}")
    print(f"  -> 职位关键词 ({len(job_list)}个): {', '.join(job_list)}")
    print("=" * 50)

    # --- 3. 获取其他参数 ---
    limit_per_task = dict_parameter.get("limit", 999999)
    use_concurrent = dict_parameter.get("concurrent", True)

    # --- 4. 准备文件和启动写入进程 ---
    csv_file = "data/qcwy.csv"
    html_file = "static/html/data.html"
    os.makedirs("data", exist_ok=True)
    os.makedirs("static/html", exist_ok=True)
    if os.path.exists(csv_file): os.remove(csv_file)
    if os.path.exists(html_file): os.remove(html_file)

    q = Queue()
    writer = WriterProcess(q, filename=csv_file)
    writer.start()

    # --- 5. 根据配置启动爬虫（并发或串行） ---
    if use_concurrent:
        processes = []
        for city in city_list:
            for job in job_list:
                p = SpiderProcess(city, job, q, limit_per_task)
                processes.append(p)
                p.start()
                # 增加短暂延时，避免瞬间启动大量浏览器实例，降低被封禁风险
                time.sleep(1.5)
        # 等待所有爬虫进程执行完毕
        for p in processes:
            p.join()
        print("所有并发爬虫进程已执行完毕。")
    else:
        # 串行执行
        for city in city_list:
            for job in job_list:
                run_single_task(city, job, q, limit_per_task)
        print("所有串行爬虫任务已执行完毕。")

    # --- 6. 结束写入进程并生成HTML报告 ---
    q.put("STOP")  # 发送停止信号
    writer.join()  # 等待写入进程结束
    print("写入进程已结束.")

    generate_html_from_csv(csv_file, html_file)


# ==============================
#  主函数 (最终调度器)
# ==============================
def main(dict_parameter: dict):
    """
    项目的主入口函数，负责调度爬虫任务。
    根据参数决定是立即执行一次，还是按设定的时间表定时循环执行。
    """
    # 在Windows上，多进程代码必须放在 freeze_support() 调用之下。
    # 把它放在 main 函数的入口处是最稳妥的做法。
    freeze_support()

    timer_settings = dict_parameter.get("timer", {"enable": False})

    # 如果未启用定时器，则直接执行一次爬取
    if not timer_settings.get("enable"):
        print("模式: 立即执行单次爬取")
        run_crawl_once(dict_parameter)
        print("所有任务完成。")
        return

    # 如果启用定时器，则进入定时循环模式
    print("模式: 定时循环爬取已启动")
    bh = timer_settings["begin_hour"]
    bm = timer_settings["begin_minute"]
    eh = timer_settings["end_hour"]
    em = timer_settings["end_minute"]
    interval_minutes = timer_settings["interval"]

    while True:
        now = datetime.datetime.now()
        begin_time = now.replace(hour=bh, minute=bm, second=0, microsecond=0)
        end_time = now.replace(hour=eh, minute=em, second=0, microsecond=0)

        # 判断当前时间是否在允许的运行时间段内
        if begin_time <= now < end_time:
            print(f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] 当前处于允许运行时间段，执行爬取任务。")
            run_crawl_once(dict_parameter)
            print(f"本次任务完成，将休眠 {interval_minutes} 分钟后再次检查。")
            time.sleep(interval_minutes * 60)
        else:
            print(f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] 当前非运行时间，等待... (下次检查将在1分钟后)")
            time.sleep(60)


if __name__ == '__main__':
    # 确保直接运行此脚本时，多进程功能也能正常工作。
    freeze_support()

    # --- 用于直接运行脚本时的测试配置 ---
    test_conf = {
        "city": ["北京"],
        "job": ["软件"],
        "limit": 50,
        "concurrent": False,  # 测试串行模式
        "timer": {
            "enable": False  # 测试单次立即执行
        }
    }
    main(test_conf)
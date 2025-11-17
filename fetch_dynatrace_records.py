import json
import logging
import os
import random
import re
import time
import traceback
from datetime import datetime, timedelta
from urllib.parse import urlencode

import pandas as pd
import requests


##################### 全局变量开始 #####################

def get_previous_workday(current_date):
    """
    获取指定日期的上一个工作日
    跳过周末（周六、周日）
    
    Args:
        current_date: datetime对象，当前日期
    
    Returns:
        datetime对象，上一个工作日
    """
    previous_day = current_date - timedelta(days=1)

    # 如果上一天是周六(5)或周日(6)，继续往前找
    while previous_day.weekday() >= 5:  # 周一=0, 周日=6
        previous_day = previous_day - timedelta(days=1)

    return previous_day


def get_workday_dates():
    """
    获取当前日期和上一个工作日的日期
    
    Returns:
        tuple: (当前日期, 上一个工作日)
    """
    today = datetime.now()
    previous_workday = get_previous_workday(today)

    return today, previous_workday


#  TODO 测试
# TODAY_STR = "20251014"
today, previous_workday = get_workday_dates()
TODAY_STR = today.strftime('%Y%m%d')
PREVIOUS_WORKDAY_STR = previous_workday.strftime('%Y%m%d')

OUTPUT_DIR = f"output/{TODAY_STR}"
PREVIOUS_WORKDAY_OUTPUT_DIR = f"output/{PREVIOUS_WORKDAY_STR}"

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(PREVIOUS_WORKDAY_OUTPUT_DIR, exist_ok=True)

# 查询结果轮询间隔（秒）
POLL_INTERVAL = 10

# 异常消息的分类规则，定义为正则表达式模式
# 可以根据需要扩展规则
FUZZY_RULES = [
    "Call entry with interaction_id='.*' not found",
    "Unexpected response status: 500 for post /calls-router/handoff/.*: #<OAuth2::Response:.*>",
    "undefined method `.*' for #<.*>",
    "Request waited .*ms, then ran for longer than .*ms",
    "OpenSSL::SSL::SSLError: SSL_read: unexpected eof while reading .*",
    "OpenSSL::SSL::SSLError: SSL_read: no response data.*",
    "Failed to open TCP connection to .* \(execution expired\)",
    "Errno::ETIMEDOUT: Connection timed out .*",
    "Could not find call_flow_class for .*",
    "Cannot dial sip:.*reason: add external contact only supports phone numbers",
    "CallQueueEvictor couldn't continue the waiting flow for call .*",
    # Unable to redirect call CA3fefa01bd417d244e1fa1c21bd88a105: 400 - [400] {"code"=>20001, "message"=>"Bad Request", "more_info"=>"https://www.twilio.com/docs/errors/20001", "status"=>400}
    "Unable to redirect call .*",
    "No primary server is available in cluster: #<Cluster topology=ReplicaSetNoPrimary.*",
    "The socket took over .* seconds to connect .*",
    # Account '63cac9bd8cfef70a9dfdc194' has no UC Configs
    "Account .* has no UC Configs",
    # Errno::ECONNRESET: Connection reset by peer (for 89.194.204.150:27017 (infra-prd-td-us-1-gener-shard-00-04.9c2kr.mongodb.net:27017, TLS)) (on infra-prd-td-us-1-gener-shard-00-04.9c2kr.mongodb.net:27017, connection 3:5)
    "Errno::ECONNRESET: Connection reset by peer .*",
    r"\[HTTP 400\] 400 : Unable to update record.*",
]


##################### 全局变量结束 #####################


def setup_logging():
    """配置日志记录到文件和控制台。"""
    os.makedirs('logs', exist_ok=True)
    log_filename = f"logs/log_{TODAY_STR}.log"
    logging.basicConfig(
        level=logging.INFO,
        handlers=[
            logging.FileHandler(log_filename, encoding='utf-8'),
            logging.StreamHandler()
        ],
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    return log_filename


# 用户代理列表
agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1"
]


def make_request(query, cookie, csrftoken, start_time_str, end_time_str, day_num, output_dir):
    # 执行 DQL 请求
    api1_url = "https://wyv31614.live.dynatrace.com/rest/v2/logmonitoring/dql/query:execute"
    user_agent = random.choice(agents)
    api1_headers = {
        "User-Agent": user_agent,
        "x-csrftoken": csrftoken,
        "cookie": cookie,
    }
    api1_body = {
        "query": query,
        "defaultTimeframeStart": start_time_str,
        "defaultTimeframeEnd": end_time_str,
        "requestTimeoutMilliseconds": 1,
        "maxResultBytes": 64000000,
        "timezone": "Asia/Shanghai",
    }

    try:
        logging.info("正在发送 DQL 执行请求...")
        response1 = requests.post(api1_url, headers=api1_headers, json=api1_body)
        response1.raise_for_status()
        api1_result = response1.json()

        output_filename = f'{output_dir}/dql_result_for_day_{day_num}.json'

        request_token = api1_result.get("requestToken")

        logging.info("DQL 执行请求成功，响应已保存，requestToken=%s", request_token)
        if not request_token:
            logging.error("DQL 执行响应中未找到 requestToken。")
            return None

        # 轮询执行结果
        request_token = urlencode({'': request_token})[1:]
        api2_url = f"https://wyv31614.live.dynatrace.com/rest/v2/logmonitoring/dql/query:poll?request-token={request_token}&request-timeout-milliseconds=30000"
        start_time = datetime.now()
        timeout = timedelta(minutes=6)

        while datetime.now() - start_time < timeout:
            try:
                logging.info("正在轮询 DQL 执行结果...")
                response2 = requests.get(api2_url, headers=api1_headers)
                response2.raise_for_status()
                api2_result = response2.json()

                state = api2_result.get("state")
                if state == "SUCCEEDED":
                    logging.info("DQL 执行结果成功。")

                    records = api2_result.get("result", {}).get("records", [])
                    with open(output_filename, 'w') as f:
                        json.dump(records, f, indent=4)

                    execution_time_milliseconds = api2_result.get("result", {}).get("metadata", {}).get("grail",
                                                                                                        {}).get(
                        "executionTimeMilliseconds", {})
                    if execution_time_milliseconds:
                        # 将执行时间毫秒转换为分钟
                        logging.info("分析时间范围持续时间：%.2f 分钟", execution_time_milliseconds / 60000)
                    scanned_bytes = api2_result.get("result", {}).get("metadata", {}).get("grail", {}).get(
                        "scannedBytes")
                    if scanned_bytes:
                        # 将字节转换为 TB 用于日志记录
                        scanned_tb = scanned_bytes / (1024 ** 4)
                        logging.info("扫描数据大小：%.2f TB", scanned_tb)

                    logging.info("DQL 执行结果已保存到 %s", output_filename)
                    return output_filename

                logging.info(f"DQL 执行状态：{state}。{POLL_INTERVAL} 秒后重试...")
                time.sleep(POLL_INTERVAL)

            except requests.RequestException as e:
                logging.error(f"轮询 DQL 执行结果时出错：{traceback.print_exc()}")

        logging.error("轮询 DQL 执行结果在 6 分钟后超时。")

    except requests.RequestException as e:
        logging.error(f"执行 DQL 请求时出错：{e}")
        logging.error(traceback.format_exc())

    return None


def handle_data():
    # 聚合：合并输出目录中的所有 JSON 文件
    # 今天往前推7天的数据
    records_current_7_days = []
    for day in range(1, 8):
        with open(f"{OUTPUT_DIR}/dql_result_for_day_{day}.json", 'r') as f:
            day_records = json.load(f)
            records_current_7_days.extend(day_records)
    output_filename_current = f"{OUTPUT_DIR}/merged_current_7_days.json"
    with open(output_filename_current, 'w') as f:
        json.dump(records_current_7_days, f, indent=4)
    logging.info(f"已合并今天往前推7天的数据到 {output_filename_current}")

    # 上一个工作日往前推7天的数据
    records_previous_7_days = []
    for day in range(1, 8):
        with open(f"{PREVIOUS_WORKDAY_OUTPUT_DIR}/dql_result_for_day_{day}.json", 'r') as f:
            day_records = json.load(f)
            records_previous_7_days.extend(day_records)
    output_filename_previous = f"{OUTPUT_DIR}/merged_previous_workday_7_days.json"
    with open(output_filename_previous, 'w') as f:
        json.dump(records_previous_7_days, f, indent=4)
    logging.info(f"已合并上一个工作日往前推7天的数据到 {output_filename_previous}")

    # 今天往前推1天的数据（最新的1天）
    records_current_1_day = []
    with open(f"{OUTPUT_DIR}/dql_result_for_day_7.json", 'r') as f:
        records_current_1_day = json.load(f)

    # 上一个工作日往前推1天的数据（上一个工作日的最新1天）
    records_previous_1_day = []
    with open(f"{PREVIOUS_WORKDAY_OUTPUT_DIR}/dql_result_for_day_7.json", 'r') as f:
        records_previous_1_day = json.load(f)

    # 处理数据：按 span.events.exception.message 分组，合并唯一的应用和堆栈跟踪值，求和 count()
    result = {}
    all_records = records_current_7_days + records_previous_7_days
    for record in all_records:
        app = record.get("app", "Unknown App")
        message = record.get("span.events.exception.message", "No Exception Message") or ""
        stacktrace = record.get("span.events.exception.stack_trace", "No Exception Stacktrace") or ""

        if message == "":
            logging.warning("发现空异常消息，记录：%s", record)
            break

        if message not in result:
            result[message] = {
                "apps": set(),
                "stacktraces": set(),
                "current_7_days_count": 0,
                "previous_workday_7_days_count": 0,
                "last_1_day_count": 0,
                "pre_last_1_day_count": 0,
            }

        result[message]["apps"].add(app)
        result[message]["stacktraces"].add(stacktrace)

    # 计算数量
    # 今天往前推7天的数据
    for message in result.keys():
        for record in records_current_7_days:
            record_message = record.get("span.events.exception.message", "No Exception Message") or ""
            if record_message == message:
                result[message]["current_7_days_count"] += int(record.get("count()", 0))

        # 上一个工作日往前推7天的数据
        for record in records_previous_7_days:
            record_message = record.get("span.events.exception.message", "No Exception Message") or ""
            if record_message == message:
                result[message]["previous_workday_7_days_count"] += int(record.get("count()", 0))

        # 今天往前推1天的数据
        for record in records_current_1_day:
            record_message = record.get("span.events.exception.message", "No Exception Message") or ""
            if record_message == message:
                result[message]["last_1_day_count"] += int(record.get("count()", 0))

        # 上一个工作日往前推1天的数据
        for record in records_previous_1_day:
            record_message = record.get("span.events.exception.message", "No Exception Message") or ""
            if record_message == message:
                result[message]["pre_last_1_day_count"] += int(record.get("count()", 0))

    # 聚合消息分类
    categorized_result = {}
    for message, details in result.items():
        new_message = apply_fuzzy_rules(message)
        if new_message not in categorized_result:
            categorized_result[new_message] = {
                "apps": set(),
                "stacktraces": set(),
                "raw_messages": set(),
                "current_7_days_count": 0,
                "previous_workday_7_days_count": 0,
                "last_1_day_count": 0,
                "pre_last_1_day_count": 0,
            }

        categorized_result[new_message]["raw_messages"].add(message)
        categorized_result[new_message]["apps"].update(details["apps"])
        categorized_result[new_message]["stacktraces"].update(details["stacktraces"])
        categorized_result[new_message]["current_7_days_count"] += details["current_7_days_count"]
        categorized_result[new_message]["previous_workday_7_days_count"] += details["previous_workday_7_days_count"]
        categorized_result[new_message]["last_1_day_count"] += details["last_1_day_count"]
        categorized_result[new_message]["pre_last_1_day_count"] += details["pre_last_1_day_count"]
    result = categorized_result
    logging.info(f"分类后，有 {len(result)} 种异常消息类型。")

    # 将结果转换为列表并排序
    sorted_result = sorted(result.items(), key=lambda x: x[1]["current_7_days_count"], reverse=True)
    # 输出到 Excel 文件
    output_data = []
    for message, details in sorted_result:
        # 考虑异常堆栈跟踪的 None/空值
        if not details["stacktraces"]:
            details["stacktraces"] = {""}
        if not details["raw_messages"]:
            details["raw_messages"] = {""}
        # 将 None 替换为字符串 ""
        details["apps"] = {app if app is not None else "" for app in details["apps"]}
        details["stacktraces"] = {st if st is not None else "" for st in details["stacktraces"]}
        details["raw_messages"] = {rm if rm is not None else "" for rm in details["raw_messages"]}

        # 判断是否为新异常：如果当前7天有出现，但上一个工作日往前7天没有出现
        if details["current_7_days_count"] > 0 and details["previous_workday_7_days_count"] == 0:
            details["is_new"] = True
        else:
            details["is_new"] = False

        output_data.append({
            "app": ", ".join(details["apps"]),
            "exception message(exp)": message.replace(".*", "******").strip(),
            "raw messages": "\n\n".join(details["raw_messages"]).strip(),
            "exception stacktrace": "\n\n".join(details["stacktraces"]).strip(),
            "quantity for the last 7 days": str(details["current_7_days_count"]) + "\n" +
                                            f"prev: {str(details['previous_workday_7_days_count'])}",
            "quantity for the previous day": str(details.get("last_1_day_count", 0)) + "\n" +
                                             f"prev: {str(details['pre_last_1_day_count'])}",
            "is_new": "YES" if details.get("is_new") else ""
        })
    df = pd.DataFrame(output_data)
    excel_filename = f"{OUTPUT_DIR}/summary.xlsx"
    df.to_excel(excel_filename, index=False)
    logging.info(f"处理完成，结果已保存到 {excel_filename}")
    logging.info(f"对比数据说明：")
    logging.info(f"  - 当前7天数据：{today.strftime('%Y-%m-%d')} 往前推7天")
    logging.info(f"  - 上一个工作日7天数据：{previous_workday.strftime('%Y-%m-%d')} 往前推7天")


# 功能：应用模糊匹配规则
def apply_fuzzy_rules(message):
    for rule in FUZZY_RULES:
        if re.fullmatch(rule, message.strip(), flags=re.DOTALL):
            return rule
    return message


def get_unique_date_ranges():
    """
    计算需要获取数据的所有唯一日期，避免重复请求
    
    Returns:
        dict: {date_str: {'date': date_obj, 'needed_for': [('current', day_num), ('previous', day_num)]}}
    """
    unique_dates = {}

    # 今天往前推7天的数据
    today_start = today - timedelta(days=7)
    for i in range(7):
        date_obj = today_start + timedelta(days=i)
        date_str = date_obj.strftime('%Y-%m-%d')

        if date_str not in unique_dates:
            unique_dates[date_str] = {
                'date': date_obj,
                'needed_for': []
            }
        unique_dates[date_str]['needed_for'].append(('current', i + 1))

    # 上一个工作日往前推7天的数据
    previous_start = previous_workday - timedelta(days=7)
    for i in range(7):
        date_obj = previous_start + timedelta(days=i)
        date_str = date_obj.strftime('%Y-%m-%d')

        if date_str not in unique_dates:
            unique_dates[date_str] = {
                'date': date_obj,
                'needed_for': []
            }
        unique_dates[date_str]['needed_for'].append(('previous', i + 1))

    return unique_dates


def main():
    # 设置日志记录
    log_file = setup_logging()
    logging.info(f"日志文件：{log_file}")

    # 从 resources 读取查询、cookie 和 csrftoken
    with open('resources/query.txt', 'r', encoding='utf-8') as f:
        query = f.read()
    with open('resources/cookie.txt', 'r', encoding='utf-8') as f:
        cookie = f.read().strip()
    with open('resources/csrftoken.txt', 'r', encoding='utf-8') as f:
        csrftoken = f.read().strip()
    if not query or not cookie:
        logging.error("查询或 cookie 为空，请检查 resources/query.txt 和 resources/cookie.txt。")
    logging.info("已读取查询和 cookie。")

    logging.info(f"今天是：{today.strftime('%Y-%m-%d %A')}")
    logging.info(f"上一个工作日是：{previous_workday.strftime('%Y-%m-%d %A')}")

    # 计算所有需要获取的唯一日期2
    unique_dates = get_unique_date_ranges()
    total_requests = len(unique_dates)
    duplicate_saved = 14 - total_requests  # 总共14个请求减去实际需要的请求数

    logging.info(f"优化后需要 {total_requests} 个请求（节省了 {duplicate_saved} 个重复请求）")

    # 存储请求结果的映射
    date_to_data = {}

    # 按日期排序，统一获取数据
    sorted_dates = sorted(unique_dates.items())

    for date_str, date_info in sorted_dates:
        date_obj = date_info['date']
        needed_for = date_info['needed_for']

        # 设置开始、结束时间的小时数为 10:00:00
        start_time_str = date_obj.strftime("%Y-%m-%dT10:00:00.000")
        end_time_str = (date_obj + timedelta(days=1)).strftime("%Y-%m-%dT10:00:00.000")

        logging.info(f"正在获取 {date_str} 的数据，需要用于：{needed_for}")

        # 使用临时文件名，稍后会复制到相应位置
        temp_output_dir = "temp_data"
        os.makedirs(temp_output_dir, exist_ok=True)

        temp_filename = make_request(query, cookie, csrftoken, start_time_str, end_time_str,
                                     date_str.replace('-', ''), temp_output_dir)

        if temp_filename:
            # 读取数据
            with open(temp_filename, 'r') as f:
                data = json.load(f)
            date_to_data[date_str] = data
            logging.info(f"{date_str} 的数据获取成功")
        else:
            logging.error(f"{date_str} 的数据获取失败")
            date_to_data[date_str] = []

    # 将数据复制到相应的目录和文件名
    for date_str, date_info in unique_dates.items():
        data = date_to_data.get(date_str, [])

        for dataset_type, day_num in date_info['needed_for']:
            if dataset_type == 'current':
                output_dir = OUTPUT_DIR
                logging.info(f"保存 {date_str} 数据到今天数据集第 {day_num} 天")
            else:  # previous
                output_dir = PREVIOUS_WORKDAY_OUTPUT_DIR
                logging.info(f"保存 {date_str} 数据到上一个工作日数据集第 {day_num} 天")

            output_filename = f'{output_dir}/dql_result_for_day_{day_num}.json'
            with open(output_filename, 'w') as f:
                json.dump(data, f, indent=4)

    # 清理临时目录
    import shutil
    if os.path.exists("temp_data"):
        shutil.rmtree("temp_data")

    handle_data()


if __name__ == "__main__":
    # main()
    handle_data()

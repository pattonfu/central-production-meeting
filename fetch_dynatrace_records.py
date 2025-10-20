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

#  TODO 测试
# TODAY_STR = "20251014"
TODAY_STR = datetime.now().strftime('%Y%m%d')

LAST_DAY_STR = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')

OUTPUT_DIR = f"output/{TODAY_STR}"
LAST_OUTPUT_DIR = f"output/{LAST_DAY_STR}"

os.makedirs(OUTPUT_DIR, exist_ok=True)

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
    "The socket took over .* seconds to connect .*"
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


def make_request(query, cookie, csrftoken, start_time_str, end_time_str, day_num):
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

        output_filename = f'{OUTPUT_DIR}/dql_result_for_day_{day_num}.json'

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
    output_filename_7_days = f"{OUTPUT_DIR}/dql_result_for_7_days_{TODAY_STR}.json"
    all_records = []
    last_1_day = []
    for i in range(7):
        temp_filename = f'{OUTPUT_DIR}/dql_result_for_day_{i + 1}.json'
        if os.path.exists(temp_filename):
            with open(temp_filename, 'r', encoding='utf-8') as f:
                records = json.load(f)
                all_records.extend(records)
                if i == 6:
                    last_1_day = records
    with open(output_filename_7_days, 'w', encoding='utf-8') as f:
        json.dump(all_records, f, indent=4)

    if not all_records:
        logging.error("过去 7 天没有获取到数据，程序退出。")
        return

    # 获取上一天的 dql_result_for_7_days_*.json 数据，如果不存在则使用空列表
    output_filename_7_days_yesterday_data = []
    if os.path.exists(f"{LAST_OUTPUT_DIR}/dql_result_for_7_days_{LAST_DAY_STR}.json"):
        with open(f"{LAST_OUTPUT_DIR}/dql_result_for_7_days_{LAST_DAY_STR}.json", 'r', encoding='utf-8') as f:
            output_filename_7_days_yesterday_data = json.load(f)

    output_filename_1_days_yesterday_data = []
    if os.path.exists(f"{LAST_OUTPUT_DIR}/dql_result_for_day_7.json"):
        with open(f"{LAST_OUTPUT_DIR}/dql_result_for_day_7.json", 'r', encoding='utf-8') as f:
            output_filename_1_days_yesterday_data = json.load(f)

    logging.info(f"7 天的聚合数据已保存到 {output_filename_7_days}，总记录数：{len(all_records)}")

    # 处理数据：按 span.events.exception.message 分组，合并唯一的应用和堆栈跟踪值，求和 count()
    result = {}
    for record in all_records:
        app = record.get("app", "Unknown App")
        message = record.get("span.events.exception.message", "No Exception Message") or ""
        stacktrace = record.get("span.events.exception.stack_trace", "No Exception Stacktrace") or ""
        count = int(record.get("count()", 0))

        if message == "":
            logging.warning("发现空异常消息，记录：%s", record)
            break

        if message not in result:
            result[message] = {
                "apps": set(),
                "stacktraces": set(),
                "total_count": 0,
                "pre_total_count": 0
            }

        # 从 output_filename_7_days_yesterday_data 设置 pre_total_count
        for rec in output_filename_7_days_yesterday_data:
            if rec.get("span.events.exception.message", "No Exception Message") == message and not rec.get(
                    "has_pre_total_count", False):
                result[message]["pre_total_count"] = int(rec.get("count()", 0)) + result[message].get("pre_total_count",
                                                                                                      0)
                # 设置标志以指示 pre_total_count 有值
                rec["has_pre_total_count"] = True

        result[message]["apps"].add(app)
        result[message]["stacktraces"].add(stacktrace)
        result[message]["total_count"] += count

    # 筛选 output_filename_7_days_yesterday_data 中 has_pre_total_count == False 的数据，并将其添加到 result 中，total_count = 0
    for rec in output_filename_7_days_yesterday_data:
        if not rec.get("has_pre_total_count", False):
            message = rec.get("span.events.exception.message", "No Exception Message") or ""
            if message not in result:
                result[message] = {
                    "apps": set(),
                    "stacktraces": set(),
                    "total_count": 0,
                    "pre_total_count": 0
                }
            result[message]["pre_total_count"] = int(rec.get("count()", 0)) + result[message].get("pre_total_count", 0)
            result[message]["apps"].add(rec.get("app", "Unknown App"))
            result[message]["stacktraces"].add(rec.get("span.events.exception.stack_trace", "No Exception Stacktrace"))

    for record in last_1_day:
        message = record.get("span.events.exception.message", "No Exception Message") or ""
        if message == "":
            logging.warning("发现空异常消息，记录：%s", record)
            break
        count = int(record.get("count()", 0))
        app = record.get("app", "Unknown App")
        message = record.get("span.events.exception.message", "No Exception Message") or ""
        stacktrace = record.get("span.events.exception.stack_trace", "No Exception Stacktrace")

        # 从 output_filename_1_days_yesterday_data 设置 pre_count
        is_new = True
        for rec in output_filename_1_days_yesterday_data:
            if rec.get("span.events.exception.message", "No Exception Message") == message and not rec.get(
                    "has_pre_quantity_for_previous_day", False):
                result[message]["pre_quantity_for_previous_day"] = int(rec.get("count()", 0)) + result[message].get(
                    "pre_quantity_for_previous_day", 0)
                # 设置标志以指示 pre_quantity_for_previous_day 有值
                rec["has_pre_quantity_for_previous_day"] = True
                is_new = False

        if message in result:
            if "quantity_for_previous_day" not in result[message]:
                result[message]["quantity_for_previous_day"] = 0

            result[message]["quantity_for_previous_day"] = result[message]["quantity_for_previous_day"] + count
            result[message]["apps"].add(app)
            result[message]["stacktraces"].add(stacktrace)
        else:
            result[message] = {
                "apps": {app},
                "stacktraces": {stacktrace},
                "total_count": 0,
                "quantity_for_previous_day": count
            }

        if is_new:
            result[message]["is_new"] = True

    # 聚合消息分类
    categorized_result = {}
    for message, details in result.items():
        new_message = apply_fuzzy_rules(message)
        if new_message not in categorized_result:
            categorized_result[new_message] = {
                "apps": set(),
                "stacktraces": set(),
                "raw_messages": set(),
                "total_count": 0,
                "pre_total_count": 0,
                "quantity_for_previous_day": 0,
                "pre_quantity_for_previous_day": 0
            }

        categorized_result[new_message]["raw_messages"].add(message)
        categorized_result[new_message]["apps"].update(details["apps"])
        categorized_result[new_message]["stacktraces"].update(details["stacktraces"])
        categorized_result[new_message]["total_count"] += details["total_count"]
        categorized_result[new_message]["pre_total_count"] += details.get("pre_total_count", 0)
        if "pre_quantity_for_previous_day" in details:
            if "pre_quantity_for_previous_day" not in categorized_result[new_message]:
                categorized_result[new_message]["pre_quantity_for_previous_day"] = 0
            categorized_result[new_message]["pre_quantity_for_previous_day"] += details["pre_quantity_for_previous_day"]
        if "quantity_for_previous_day" in details:
            if "quantity_for_previous_day" not in categorized_result[new_message]:
                categorized_result[new_message]["quantity_for_previous_day"] = 0
            categorized_result[new_message]["quantity_for_previous_day"] += details["quantity_for_previous_day"]
    result = categorized_result
    logging.info(f"分类后，有 {len(result)} 种异常消息类型。")

    # 将结果转换为列表并排序
    sorted_result = sorted(result.items(), key=lambda x: x[1]["stacktraces"], reverse=True)
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

        output_data.append({
            "app": ", ".join(details["apps"]),
            "exception message(exp)": message.replace(".*", "******"),
            "raw messages": "\n\n".join(details["raw_messages"]),
            "exception stacktrace": "\n\n".join(details["stacktraces"]),
            "quantity": str(details["total_count"]) + "\n" + (
                f"prev: {str(details['pre_total_count'])}" if details.get("pre_total_count") else "prev 0"),
            "quantity for the previous day": str(details.get("quantity_for_previous_day", 0)) + "\n" + (
                f"prev: {str(details['pre_quantity_for_previous_day'])}" if details.get(
                    "pre_quantity_for_previous_day") else "prev 0"),
            "is_new": "Yes" if details.get("is_new") else ""
        })
    df = pd.DataFrame(output_data)
    excel_filename = f"{OUTPUT_DIR}/summary.xlsx"
    df.to_excel(excel_filename, index=False)
    logging.info(f"处理完成，结果已保存到 {excel_filename}")


def apply_fuzzy_rules(message):
    for rule in FUZZY_RULES:
        if re.fullmatch(rule, message.strip()):
            return rule
    return message


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

    # 获取过去 7 天的数据
    end_time = datetime.now()
    start_time = end_time - timedelta(days=7)

    # 循环 7 次，处理连续的 1 天时间窗口，然后聚合
    for i in range(7):
        start_time_str = (start_time + timedelta(days=i)).strftime("%Y-%m-%dT%H:%M:%S.000")
        end_time_str = (start_time + timedelta(days=i + 1)).strftime("%Y-%m-%dT%H:%M:%S.000")

        logging.info(f"正在获取第 {i + 1} 天的数据：{start_time_str} 到 {end_time_str}")
        temp_filename = make_request(query, cookie, csrftoken, start_time_str, end_time_str, i + 1)
        logging.info(f"第 {i + 1} 天的数据已保存到 {temp_filename}")

    handle_data()


if __name__ == "__main__":
    main()
    # handle_data()

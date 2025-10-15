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

##################### Global Variables Start #####################

#  TODO test
# TODAY_STR = "20251014"
TODAY_STR = datetime.now().strftime('%Y%m%d')

LAST_DAY_STR = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')

OUTPUT_DIR = f"output/{TODAY_STR}"
LAST_OUTPUT_DIR = f"output/{LAST_DAY_STR}"

os.makedirs(OUTPUT_DIR, exist_ok=True)

# Polling interval for query results (seconds)
POLL_INTERVAL = 10

# Classification rules for exception messages, defined as regex patterns
# Rules can be extended as needed. Based on Exception Message in output/summary.xlsx
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


##################### Global Variables End #####################


def setup_logging():
    """Configure logging to file and console."""
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


# User-Agent list
agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1"
]


def make_request(query, cookie, csrftoken, start_time_str, end_time_str, day_num):
    # Execute DQL request
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
        logging.info("Sending DQL execution request...")
        response1 = requests.post(api1_url, headers=api1_headers, json=api1_body)
        response1.raise_for_status()
        api1_result = response1.json()

        output_filename = f'{OUTPUT_DIR}/dql_result_for_day_{day_num}.json'

        request_token = api1_result.get("requestToken")

        logging.info("DQL execution request succeeded, response saved, requestToken=%s", request_token)
        if not request_token:
            logging.error("No requestToken found in DQL execution response.")
            return None

        # Poll for execution result
        request_token = urlencode({'': request_token})[1:]
        api2_url = f"https://wyv31614.live.dynatrace.com/rest/v2/logmonitoring/dql/query:poll?request-token={request_token}&request-timeout-milliseconds=30000"
        start_time = datetime.now()
        timeout = timedelta(minutes=6)

        while datetime.now() - start_time < timeout:
            try:
                logging.info("Polling for DQL execution result...")
                response2 = requests.get(api2_url, headers=api1_headers)
                response2.raise_for_status()
                api2_result = response2.json()

                state = api2_result.get("state")
                if state == "SUCCEEDED":
                    logging.info("DQL execution result succeeded.")

                    records = api2_result.get("result", {}).get("records", [])
                    with open(output_filename, 'w') as f:
                        json.dump(records, f, indent=4)

                    execution_time_milliseconds = api2_result.get("result", {}).get("metadata", {}).get("grail",
                                                                                                        {}).get(
                        "executionTimeMilliseconds", {})
                    if execution_time_milliseconds:
                        # Convert executionTimeMilliseconds to minutes
                        logging.info("Analysis timeframe duration: %.2f Mins", execution_time_milliseconds / 60000)
                    scanned_bytes = api2_result.get("result", {}).get("metadata", {}).get("grail", {}).get(
                        "scannedBytes")
                    if scanned_bytes:
                        # Convert bytes to TB for logging
                        scanned_tb = scanned_bytes / (1024 ** 4)
                        logging.info("Scanned data size: %.2f TB", scanned_tb)

                    logging.info("DQL execution result saved to %s", output_filename)
                    return output_filename

                logging.info(f"DQL execution state: {state}. Retrying in {POLL_INTERVAL} seconds...")
                time.sleep(POLL_INTERVAL)

            except requests.RequestException as e:
                logging.error(f"Error polling DQL execution result: {traceback.print_exc()}")

        logging.error("Polling DQL execution result timed out after 6 minutes.")

    except requests.RequestException as e:
        logging.error(f"Error executing DQL request: {e}")
        logging.error(traceback.format_exc())

    return None


def handle_data():
    # Aggregate: combine all JSON files in the output directory
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
        logging.error("No data fetched for the past 7 days, exiting.")
        return

    # fetch the last day's dql_result_for_7_days_*.json data, if no exists, use empty list
    output_filename_7_days_yesterday_data = []
    with open(f"{LAST_OUTPUT_DIR}/dql_result_for_7_days_{LAST_DAY_STR}.json", 'r', encoding='utf-8') as f:
        output_filename_7_days_yesterday_data = json.load(f)

    output_filename_1_days_yesterday_data = []
    if os.path.exists(f"{LAST_OUTPUT_DIR}/dql_result_for_day_7.json"):
        with open(f"{LAST_OUTPUT_DIR}/dql_result_for_day_7.json", 'r', encoding='utf-8') as f:
            output_filename_1_days_yesterday_data = json.load(f)

    logging.info(f"Aggregated data for 7 days saved to {output_filename_7_days}, total records: {len(all_records)}")

    # Process data: group by span.events.exception.message, merge unique app and stacktrace values, sum count()
    result = {}
    for record in all_records:
        app = record.get("app", "Unknown App")
        message = record.get("span.events.exception.message", "No Exception Message") or ""
        stacktrace = record.get("span.events.exception.stack_trace", "No Exception Stacktrace") or ""
        count = int(record.get("count()", 0))

        if message == "":
            logging.warning("Found empty exception message, record: %s", record)
            break

        if message not in result:
            result[message] = {
                "apps": set(),
                "stacktraces": set(),
                "total_count": 0,
                "pre_total_count": 0
            }

        # set the pre_total_count from output_filename_7_days_yesterday_data
        for rec in output_filename_7_days_yesterday_data:
            if rec.get("span.events.exception.message", "No Exception Message") == message and not rec.get(
                    "has_pre_total_count", False):
                result[message]["pre_total_count"] = int(rec.get("count()", 0)) + result[message].get("pre_total_count",
                                                                                                      0)
                # make a flag to indicate that pre total_count has the value
                rec["has_pre_total_count"] = True

        result[message]["apps"].add(app)
        result[message]["stacktraces"].add(stacktrace)
        result[message]["total_count"] += count

    # filter output_filename_7_days_yesterday_data has_pre_total_count == False datas, and add them to result with total_count = 0
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
            logging.warning("Found empty exception message, record: %s", record)
            break
        count = int(record.get("count()", 0))
        app = record.get("app", "Unknown App")
        message = record.get("span.events.exception.message", "No Exception Message") or ""
        stacktrace = record.get("span.events.exception.stack_trace", "No Exception Stacktrace")

        # set the pre_count from output_filename_1_days_yesterday_data
        is_new = True
        for rec in output_filename_1_days_yesterday_data:
            if rec.get("span.events.exception.message", "No Exception Message") == message and not rec.get(
                    "has_pre_quantity_for_previous_day", False):
                result[message]["pre_quantity_for_previous_day"] = int(rec.get("count()", 0)) + result[message].get(
                    "pre_quantity_for_previous_day", 0)
                # make a flag to indicate that pre_quantity_for_previous_day has the value
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

    # Aggregating classification of messages
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
    logging.info(f"After classification, there are {len(result)} types of exception messages.")

    # Convert result to list and sort
    sorted_result = sorted(result.items(), key=lambda x: x[1]["stacktraces"], reverse=True)
    # Output to Excel file
    output_data = []
    for message, details in sorted_result:
        # consider the None/empty for exception stacktrace
        if not details["stacktraces"]:
            details["stacktraces"] = {""}
        if not details["raw_messages"]:
            details["raw_messages"] = {""}
        # replace None with string ""
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
    logging.info(f"Processing complete, result saved to {excel_filename}")


def apply_fuzzy_rules(message):
    for rule in FUZZY_RULES:
        if re.fullmatch(rule, message.strip()):
            return rule
    return message


def main():
    # Set up logging
    log_file = setup_logging()
    logging.info(f"Log file: {log_file}")

    # Read query, cookie and csrftoken from resources
    with open('resources/query.txt', 'r', encoding='utf-8') as f:
        query = f.read()
    with open('resources/cookie.txt', 'r', encoding='utf-8') as f:
        cookie = f.read().strip()
    with open('resources/csrftoken.txt', 'r', encoding='utf-8') as f:
        csrftoken = f.read().strip()
    if not query or not cookie:
        logging.error("query or cookie is empty, please check resources/query.txt and resources/cookie.txt.")
    logging.info("Read query and cookie.")

    # Get data for the past 7 days TODO
    end_time = datetime.now() - timedelta(days=1)
    start_time = end_time - timedelta(days=8)

    # Loop 7 times over consecutive 1-day windows and then aggregate
    for i in range(7):
        start_time_str = (start_time + timedelta(days=i)).strftime("%Y-%m-%dT%H:%M:%S.000")
        end_time_str = (start_time + timedelta(days=i + 1)).strftime("%Y-%m-%dT%H:%M:%S.000")

        logging.info(f"Fetching data for day {i + 1}: {start_time_str} to {end_time_str}")
        temp_filename = make_request(query, cookie, csrftoken, start_time_str, end_time_str, i + 1)
        logging.info(f"Data for day {i + 1} saved to {temp_filename}")

    handle_data()


if __name__ == "__main__":
    # main()
    handle_data()

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

OUTPUT_DIR = "output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Polling interval for query results (seconds)
POLL_INTERVAL = 5

# Classification rules for exception messages, defined as regex patterns
# Rules can be extended as needed. Based on Exception Message in output/summary.xlsx
FUZZY_RULES = [
    "Call entry with interaction_id='.*' not found",
    "Unexpected response status: 500 for post /calls-router/handoff/.*: #<OAuth2::Response:.*>",
    "undefined method `.*' for nil:NilClass",
    "undefined method `.*' for #<.*>",
    "Request waited .*ms, then ran for longer than .*ms",
    "OpenSSL::SSL::SSLError: SSL_read: unexpected eof while reading .*",
    "OpenSSL::SSL::SSLError: SSL_read: no response data.*",
    "Failed to open TCP connection to .* \(execution expired\)",
    "Errno::ETIMEDOUT: Connection timed out .*",
    "Could not find call_flow_class for .*",
    "Cannot dial sip:.*reason: add external contact only supports phone numbers",
    "CallQueueEvictor couldn't continue the waiting flow for call .*",
]


##################### Global Variables End #####################


def setup_logging():
    """Configure logging to file and console."""
    log_filename = f"log_{datetime.now().strftime('%Y%m%d')}.log"
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


def make_request(query, cookie, start_time_str, end_time_str):
    # Execute DQL request
    api1_url = "https://wyv31614.live.dynatrace.com/rest/v2/logmonitoring/dql/query:execute"
    user_agent = random.choice(agents)
    api1_headers = {
        "User-Agent": user_agent,
        "x-csrftoken": "727f7040-9c4a-4a3c-90de-a9dc98bfbdbb|11|prd-ad1e51f8-d5ac-4d71-9f2b-26d913b93775",
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

        with open(f'{OUTPUT_DIR}/dql_request_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json', 'w') as f:
            json.dump(api1_result, f, indent=4)
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
                    output_filename = f'{OUTPUT_DIR}/dql_result_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
                    with open(output_filename, 'w') as f:
                        json.dump(records, f, indent=4)
                    analysis_timeframe = api2_result.get("result", {}).get("metadata", {}).get("trail", {}).get(
                        "analysisTimeframe", {})
                    start_ts = analysis_timeframe.get("start")
                    end_ts = analysis_timeframe.get("end")
                    if start_ts and end_ts:
                        duration = float(end_ts) - float(start_ts)
                        logging.info("Analysis timeframe: start=%s, end=%s, duration=%.2f seconds", start_ts, end_ts,
                                     duration)
                    scanned_bytes = api2_result.get("result", {}).get("metadata", {}).get("trail", {}).get(
                        "scannedBytes")
                    if scanned_bytes:
                        logging.info("Scanned data: %s bytes (%.2f GB)", scanned_bytes,
                                     int(scanned_bytes) / (1024 ** 3))

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


def handle_data(output_filename_7_days, output_filename_1_day=None):
    # Load JSON data from files (list)
    with open(output_filename_7_days, 'r', encoding='utf-8') as f:
        data_7_days = json.load(f)
    with open(output_filename_1_day, 'r', encoding='utf-8') as f:
        data_1_day = json.load(f) if output_filename_1_day else []
    if not data_7_days and not data_1_day:
        logging.warning("Result files are empty, no data to process.")
        return

    logging.info(f"Read {len(data_7_days)} records for 7 days, {len(data_1_day)} records for 1 day.")
    # Process data: group by span.events.exception.message, merge unique app and stacktrace values, sum count()
    result = {}
    for record in data_7_days:
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
                "total_count": 0
            }

        result[message]["apps"].add(app)
        result[message]["stacktraces"].add(stacktrace)
        result[message]["total_count"] += count

    for record in data_1_day:
        message = record.get("span.events.exception.message", "No Exception Message") or ""
        if message == "":
            logging.warning("Found empty exception message, record: %s", record)
            break
        count = int(record.get("count()", 0))

        if message in result:
            result[message]["quantity_for_previous_day"] = count
        else:
            result[message] = {
                "apps": set(),
                "stacktraces": set(),
                "total_count": 0,
                "quantity_for_previous_day": count
            }

    # Fuzzy classification of messages
    categorized_result = {}
    for message, details in result.items():
        new_message = apply_fuzzy_rules(message)
        if new_message not in categorized_result:
            categorized_result[new_message] = {
                "apps": set(),
                "stacktraces": set(),
                "raw_messages": set(),
                "total_count": 0
            }
            if "quantity_for_previous_day" in details:
                categorized_result[new_message]["quantity_for_previous_day"] = 0
        categorized_result[new_message]["raw_messages"].add(message)
        categorized_result[new_message]["apps"].update(details["apps"])
        categorized_result[new_message]["stacktraces"].update(details["stacktraces"])
        categorized_result[new_message]["total_count"] += details["total_count"]
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
        output_data.append({
            "app": ", ".join(details["apps"]),
            "exception message(exp)": message,
            "raw messages": "\n\n".join(details["raw_messages"]),
            "exception stacktrace": "\n\n".join(details["stacktraces"]),
            "quantity": details["total_count"],
            "quantity for the previous day": details.get("quantity_for_previous_day", 0)
        })
    df = pd.DataFrame(output_data)
    excel_filename = f"{OUTPUT_DIR}/summary.xlsx"
    df.to_excel(excel_filename, index=False)
    logging.info(f"Processing complete, result saved to {excel_filename}")


def apply_fuzzy_rules(message):
    for rule in FUZZY_RULES:
        if re.fullmatch(rule, message):
            return rule
    return message


def main():
    # Set up logging
    log_file = setup_logging()
    logging.info(f"Log file: {log_file}")

    # Read query and cookie from resources
    with open('resources/query.txt', 'r', encoding='utf-8') as f:
        query = f.read()
    with open('resources/cookie.txt', 'r', encoding='utf-8') as f:
        cookie = f.read()
    if not query or not cookie:
        logging.error("query or cookie is empty, please check resources/query.txt and resources/cookie.txt.")
    logging.info("Read query and cookie.")

    # Get data for the past 7 days
    end_time = datetime.now()
    start_time = end_time - timedelta(days=7)
    end_time_str = end_time.strftime("%Y-%m-%dT%H:%M:%S.000")
    start_time_str = start_time.strftime("%Y-%m-%dT%H:%M:%S.000")
    output_filename_7_days = make_request(query, cookie, start_time_str, end_time_str)

    # Get data for the past 1 day
    start_time_1_day = end_time - timedelta(days=1)
    start_time_str_1_day = start_time_1_day.strftime("%Y-%m-%dT%H:%M:%S.000")
    output_filename_1_day = make_request(query, cookie, start_time_str_1_day, end_time_str)

    if not output_filename_7_days or not output_filename_1_day:
        logging.error("Request did not complete successfully, cannot process data.")
    else:
        logging.info("Request completed successfully, results saved.")
        handle_data(output_filename_7_days, output_filename_1_day)


if __name__ == "__main__":
    main()

    # TODO test
    # handle_data("output/dql_result_20250929_144028.json", "output/dql_result_20250929_144044.json")

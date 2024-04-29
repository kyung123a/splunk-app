import logging
from datetime import datetime, timedelta, date
import time
import requests
import sys
import json
import logging.handlers
import os
from urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)
now = datetime.now().replace(second=0, microsecond=0)

def time_convertor(dt):
    return str(int(dt) * 1000)

def get_range():

    # 현재 시간을 기준으로 어제의 날짜를 구합니다
    today = date.today()
    yesterday = today - timedelta(days=1)

    # 어제의 자정 (00:00) 시간을 구합니다
    start_of_yesterday = datetime(yesterday.year, yesterday.month, yesterday.day)

    # 다음 날의 자정 (24:00) 시간을 구합니다
    start_of_next_day = start_of_yesterday + timedelta(days=1)

    # datetime 객체를 epoch time으로 변환합니다
    start = int(time.mktime(start_of_yesterday.timetuple()))
    end = int(time.mktime(start_of_next_day.timetuple()))

    return start, end

def run(config):
    QUAXAR_API_KEY = config.get('api_key')
    QUAXAR_API_URL = config.get('base_url')

    start, end = get_range()
    since = time_convertor(start)
    until = time_convertor(end)

    start_datetime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start))
    end_datetime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end))

    event_log.info("INFO request time start ~ end: %s(%s) ~ %s(%s)\n" % (start_datetime, since, end_datetime, until))

    try:
        session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(max_retries=3)
        session.mount('https://', adapter)
        response = session.post(
            url=QUAXAR_API_URL,
            headers={
                "Content-Type": "application/json",
                "Authorization": QUAXAR_API_KEY,
            },
            json={
                "since": since,
                "until": until,
            },
            verify=False,
            timeout=60
        )

        event_log.info("INFO count: %s\n" % (response.json()["total"]))
        event_log.info("INFO response: %s\n" % (response.status_code))

        if response.status_code != 200:
            return False

        # 데이터 재구성
        for item in response.json()["data"]:
            # item["loggedAt"] = datetime.fromtimestamp(
            #     int(item["loggedAt"]) / 1000
            # ).strftime("%Y-%m-%d %H:%M:%S")

            # item["exposedAt"] = datetime.fromtimestamp(
            #     int(item["exposedAt"]) / 1000
            # ).strftime("%Y-%m-%d %H:%M:%S")
            ingest_log.info(json.dumps(item))
        return True

    except Exception as e:
        event_log.error("ERROR sending quaxar request : %s\n" % e)
        return False

    finally:
        session.close()

# quaxar.py 실행 로그
def setup_event_logger(level):
    logger = logging.getLogger('event')
    logger.propagate = False
    logger.setLevel(logging.DEBUG)
    logger_filename = os.path.join("/opt/splunk", "var", "log", "splunk", "quaxar-sigv.log")
    logger_handler = logging.handlers.TimedRotatingFileHandler(logger_filename, when="midnight", backupCount=10)
    formatter = logging.Formatter('[%(levelname)s|%(filename)s:%(lineno)s] %(asctime)s > %(message)s', datefmt='[%Y/%m/%d | %p %I:%M:%S]')
    logger_handler.setFormatter(formatter)
    logger.addHandler(logger_handler)
    return logger

# quaxar ip api call 결과 수집 로그
def setup_ingest_logger(level):
    now = datetime.now()
    # output_date = now.strftime("%Y-%m-%d")
    logger = logging.getLogger('ingest')
    logger.propagate = False
    logger.setLevel(logging.DEBUG)
    logger_handler = logging.handlers.TimedRotatingFileHandler("./export-sigv.json", when="midnight", backupCount=10)
    formatter = logging.Formatter('%(message)s')
    logger_handler.setFormatter(formatter)
    logger.addHandler(logger_handler)
    return logger

event_log = setup_event_logger(logging.INFO)
ingest_log = setup_ingest_logger(logging.INFO)

if __name__ == '__main__':
    if len(sys.argv) < 2 or sys.argv[1] != "--execute":
        event_log.error("FATAL Unsupported execution mode (expected --execute flag)\n")
        sys.exit(1)

    try:
        payload = json.loads(sys.stdin.read())
        event_log.info("INFO payload: %s\n" % payload)
        config = payload['configuration']
        success = run(config)
        if not success:
            sys.exit(2)
    except Exception as e:
        event_log.error("ERROR Unexpected error: %s\n" % e)
        sys.exit(3)
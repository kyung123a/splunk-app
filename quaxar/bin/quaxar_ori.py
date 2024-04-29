import logging
from datetime import datetime, timedelta
import requests
import sys
import json
import logging.handlers
import os
from urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)
now = datetime.now().replace(second=0, microsecond=0)

def time_convertor(dt: datetime):
    return str(int(dt.timestamp()) * 1000)

def get_range():
    start = now - timedelta(minutes=30)
    end = now - timedelta(minutes=20)
    return start, end

def run(config):
    QUAXAR_API_KEY = config.get('api_key')
    QUAXAR_API_URL = config.get('base_url')

    start, end = get_range()
    event_log.info("INFO request time start ~ end: %s ~ %s\n" % (start, end))
    since = time_convertor(start)
    until = time_convertor(end)
    typeArr = ["Voice Phishing", "Look ALike Domain"]

    try:
        for param in typeArr:
            session = requests.Session()
            adapter = requests.adapters.HTTPAdapter(max_retries=3)
            session.mount('https://', adapter)
            response = session.post(
                url=QUAXAR_API_URL + "/basm/list",
                headers={
                    "Content-Type": "application/json",
                    "Authorization": QUAXAR_API_KEY,
                },
                json={
                    "since": since,
                    "until": until,
                    "type": param,
                },
                verify=False,
                timeout=60,
            )

            if response.status_code != 200:
                return False
        
            event_log.info("INFO '%s' request response: %s\n" % (param, response.json()))

            for item in response.json()["data"]:
                item["detected"] = datetime.fromtimestamp(
                    int(item["detected"]) / 1000
                ).strftime("%Y-%m-%d %H:%M:%S")
                ingest_log.info(json.dumps(item))
        return True
    except Exception as e:
        event_log.error("ERROR sending quaxar request : %s\n" % e)

# quaxar.py 실행 로그
def setup_event_logger(level):
    logger = logging.getLogger('event')
    logger.propagate = False
    logger.setLevel(logging.DEBUG)
    logger_filename = os.path.join(os.environ["SPLUNK_HOME"], "var", "log", "splunk", "quaxar.log")
    logger_handler = logging.handlers.TimedRotatingFileHandler(logger_filename, when="midnight", backupCount=30)
    formatter = logging.Formatter('[%(levelname)s|%(filename)s:%(lineno)s] %(asctime)s > %(message)s', datefmt='[%Y/%m/%d | %p %I:%M:%S]')
    logger_handler.setFormatter(formatter)
    logger.addHandler(logger_handler)
    return logger

# quaxar ip api call 결과 수집 로그
def setup_ingest_logger(level):
    now = datetime.now()
    output_date = now.strftime("%Y-%m-%d")
    logger = logging.getLogger('ingest')
    logger.propagate = False
    logger.setLevel(logging.DEBUG)
    logger_handler = logging.handlers.TimedRotatingFileHandler("/data/logs/quaxar/result/export.json", when="midnight", backupCount=30)
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
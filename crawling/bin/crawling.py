import logging
from datetime import datetime, timedelta, date
import time
import requests
import sys
import json
import logging.handlers
import os
from bs4 import BeautifulSoup

def run():
    selector_path = config.get('selector_path')
    
    try:
        event_log.info(selector_path)
        resp = requests.get('https://www.boho.or.kr')
        html = resp.text
        soup = BeautifulSoup(html, 'html.parser')
        # news = soup.select_one("#sec01 dl.step")
        news = soup.select_one(selector_path)
        secu_step_code = news.attrs['class'][1]

        secu_code_list = {
            "s01":"정상",
            "s02":"관심",
            "s03":"주의",
            "s04":"경계",
            "s05":"심각"
        }
        event_log.info(secu_code_list[secu_step_code])
        ingest_log.info(secu_code_list[secu_step_code])
        
        return True

    except Exception as e:
        event_log.error("ERROR crawling : %s\n" % e)
        return False

    # finally:
    #     session.close()

# crawling.py 실행 로그
def setup_event_logger(level):
    logger = logging.getLogger('event')
    logger.propagate = False
    logger.setLevel(logging.DEBUG)
    logger_filename = os.path.join("history-crawling.log")
    logger_handler = logging.handlers.TimedRotatingFileHandler(logger_filename, when="midnight", backupCount=5)
    formatter = logging.Formatter('[%(levelname)s|%(filename)s:%(lineno)s] %(asctime)s > %(message)s', datefmt='[%Y/%m/%d | %p %H:%M:%S]')
    logger_handler.setFormatter(formatter)
    logger.addHandler(logger_handler)
    return logger

# crawling 결과 수집 로그
def setup_ingest_logger(level):
    now = datetime.now()
    # output_date = now.strftime("%Y-%m-%d")
    logger = logging.getLogger('ingest')
    logger.propagate = False
    logger.setLevel(logging.DEBUG)
    logger_handler = logging.handlers.TimedRotatingFileHandler("./export-crawling.log", when="midnight", backupCount=5)
    formatter = logging.Formatter('%(asctime)s | %(message)s', datefmt='[%Y/%m/%d | %p %H:%M:%S]')
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
        event_log.info("start")
        config = payload['configuration']
        success = run()
        if not success:
            sys.exit(2)
    except Exception as e:
        event_log.error("ERROR Unexpected error: %s\n" % e)
        sys.exit(3)
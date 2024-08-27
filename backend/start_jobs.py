import atexit
import logging

from jobs import scheduler

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Message-Consumer")
logger.setLevel(logging.INFO)

IS_RUN_STATE = True

def my_any_func():
    IS_RUN_STATE = False

if __name__ == "__main__":
    atexit.register(my_any_func)
    try:
        scheduler.start()
        while IS_RUN_STATE:
            pass
    finally:
        scheduler.shutdown()

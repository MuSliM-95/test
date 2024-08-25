import atexit
from jobs import scheduler


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

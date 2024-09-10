import asyncio
import atexit

from jobs.jobs import scheduler

IS_RUN_STATE = True

def my_any_func():
    scheduler.shutdown()

if __name__ == "__main__":
    atexit.register(my_any_func)
    scheduler.start()
    asyncio.get_event_loop().run_forever()
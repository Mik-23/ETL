import time
from functools import wraps


def backoff(exceptions: list, logger,
            title: str, start_sleep_time=0.1,
            factor=2, border_sleep_time=10, max_tries=100):

    def func_wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            time_out = start_sleep_time
            for try_number in range(max_tries):
                logger.info(f"Попытка номер: "
                            f"{try_number + 1} - задержка {time_out}")
                try:
                    connection = func(*args, **kwargs)
                    return connection
                except exceptions as e:
                    logger.exception(
                        f"{title} снова\n"
                        f"Тайм-аут через {time_out} секунд\n"
                        f"Ошибка backoff: {e}"
                    )
                    if time_out >= border_sleep_time:
                        time_out = border_sleep_time
                    else:
                        time_out += start_sleep_time * 2 ** factor
                    time.sleep(time_out)
        return inner
    return func_wrapper

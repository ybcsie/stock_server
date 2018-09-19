import datetime
import time


# tw_date: 107/01/01
def tw_date2int(date_str):
    date_split = date_str.split('/')
    return int("{}{}{}".format(int(date_split[0]) + 1911, date_split[1], date_split[2]))


def float_parser(float_str):
    if float_str == "X0.00":
        return 0.0

    new_str = ""

    try:
        for char in float_str:
            if char == ',':
                continue

            new_str += char

        op_float = float(new_str)
    except:
        raise ValueError("error parsing {}".format(float_str))

    return op_float


def delay(sec):
    start_datetime = datetime.datetime.now()
    while (datetime.datetime.now() - start_datetime).total_seconds() < sec:
        time.sleep(0.3)


def wait_retry(logger, sec):
    logger.logp("Retry after {}s...".format(sec))
    time.sleep(sec)


def check_smd_content_by_key(day_data, key):
    try:
        if int(tw_date2int(day_data[0]) / 100) == int(key):
            return True
    except:
        pass

    return False

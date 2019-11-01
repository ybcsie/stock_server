import crawler
import msgopt
import tools
import json
import datetime
import os
import threading


logger = msgopt.Logger("updater")


def update_listed_list(listed_path):
    content = crawler.get_listed_list()
    if content is None:
        logger.logp("cannot get listed list")
        return -1

    sid_file = open(listed_path, 'w', encoding="UTF-8")
    sid_file.write(content)
    sid_file.close()
    return 0


def get_stock_id_list(sid_path):
    if not os.path.exists(sid_path):
        raise RuntimeError("{} not exist".format(sid_path))

    sid_reader = open(sid_path, 'r', encoding="UTF-8")
    stock_id_list = sid_reader.read().split(';')
    sid_reader.close()

    for i in range(len(stock_id_list)):
        stock_info = stock_id_list[i].split(',')
        assert len(
            stock_info) == 2, "Error: stock list -- {}".format(stock_id_list[i])
        stock_id_list[i] = int(stock_info[0])

    return stock_id_list


def update_smd_in_list(stock_id_list, smd_dir, months, force_update=False):
    update_log_path = smd_dir + "/update.log"

    if is_smd_need_update(update_log_path) or force_update:
        for stock_id in stock_id_list:
            smd_path = "{}/{}.smd".format(smd_dir, stock_id)

            logger.logp("update {}".format(stock_id))
            update_smd(smd_path, stock_id, months)

    update_log_file = open(update_log_path, 'w')
    update_log_file.write(datetime.datetime.now().strftime("%Y/%m/%d/%H"))
    update_log_file.close()


def get_smd_dict(smd_path):
    opt = {}
    if not os.path.exists(smd_path):
        return opt

    try:
        smd_file = open(smd_path, 'r')

        for key, content in json.loads(smd_file.read()).items():
            if len(content) == 0 or tools.check_smd_content_by_key(content[0], key):
                opt[key] = content

        smd_file.close()

        return opt

    except:
        return opt


def is_content_need_update(content):
    if content is None:
        return True

    if len(content) == 0:
        return False

    now = datetime.datetime.now()
    if tools.check_smd_content_by_key(content[0], int(now.strftime("%Y%m"))):
        if int(now.strftime("%Y%m%d")) > tools.tw_date2int(content[len(content) - 1][0]):
            return True

    return False


def update_smd(smd_path, stock_id, months):
    exist_dict = get_smd_dict(smd_path)

    now = datetime.datetime.now()
    cur_month = now.month
    cur_year = now.year
    for i in range(months):
        if cur_month == 0:
            cur_month = 12
            cur_year -= 1

        if cur_year < 2010:
            break

        key = "{}{:02d}".format(cur_year, cur_month)
        content = exist_dict.get(key)

        if is_content_need_update(content) or i == 0:
            content = crawler.get_month_data(cur_year, cur_month, stock_id)

            if content is None:
                logger.logp("cannot get data: {} {} {}".format(
                    cur_year, cur_month, stock_id))
            else:
                exist_dict[key] = content

        cur_month -= 1

    smd_tmp_path = "{}.tmp".format(smd_path)
    smd_tmp_file = open(smd_tmp_path, 'w', encoding="UTF-8")
    smd_tmp_file.write(json.dumps(exist_dict))
    smd_tmp_file.close()

    os.replace(smd_tmp_path, smd_path)


def is_smd_need_update(update_log_path):
    if not os.path.exists(update_log_path):
        return True

    log_file = open(update_log_path, 'r', encoding="UTF-8")
    last_datetime = datetime.datetime.strptime(log_file.read(), "%Y/%m/%d/%H")
    log_file.close()
    now = datetime.datetime.now()

    update_datetime = datetime.datetime.strptime(
        "{}/{}/{}/15".format(now.year, now.month, now.day), "%Y/%m/%d/%H")
    if now.hour < 15:
        update_datetime -= datetime.timedelta(days=1)

    if last_datetime >= update_datetime:
        return False

    return True


def get_dtd_dict(dtd_path):
    if not os.path.exists(dtd_path):
        return {}

    try:
        dtd_file = open(dtd_path, 'r')

        opt = json.loads(dtd_file.read())

        dtd_file.close()

        return opt

    except:
        return {}


def update_dtd(dtd_path, yyyymm):
    if yyyymm < 201401:
        return

    now = datetime.datetime.now()
    cur_datetime = datetime.datetime.strptime("{}01".format(yyyymm), "%Y%m%d")
    one_day_delta = datetime.timedelta(days=1)

    exist_dict = get_dtd_dict(dtd_path)
    while True:
        if cur_datetime > now or int(cur_datetime.strftime("%Y%m")) > yyyymm:
            break

        if cur_datetime >= datetime.datetime.strptime("20140106", "%Y%m%d"):
            key = "{}".format(cur_datetime.strftime("%Y%m%d"))
            content = exist_dict.get(key)

            if content is None:
                content = crawler.get_day_trading_data(key)

                if content is None:
                    logger.logp("cannot get data: {}".format(key))
                else:
                    exist_dict[key] = content

        cur_datetime += one_day_delta

    dtd_tmp_path = "{}.tmp".format(dtd_path)
    dtd_tmp_file = open(dtd_tmp_path, 'w', encoding="UTF-8")
    dtd_tmp_file.write(json.dumps(exist_dict))
    dtd_tmp_file.close()

    os.replace(dtd_tmp_path, dtd_path)


def update_all_dtd(dtd_dir, months):
    now = datetime.datetime.now()
    cur_month = now.month
    cur_year = now.year
    for i in range(months):
        if cur_month == 0:
            cur_month = 12
            cur_year -= 1

        yyyymm = cur_year * 100 + cur_month
        update_dtd("{}/{}.dtd".format(dtd_dir, yyyymm), yyyymm)

        cur_month -= 1


def update_livedata_dict(stock_id_list, livedata_dict):
    now = datetime.datetime.now()
    if (7 * 60 + 30) < (now.hour * 60 + now.minute) < (8 * 60 + 30):
        tools.delay(300)
        return

    logger.logp("update_livedata : start")

    t_start = datetime.datetime.now()

    size = len(stock_id_list)
    id_list_list = []
    if size > 100:
        for i in range(0, size, 100):
            id_list_list.append(stock_id_list[i:i + 100])

    for i, cur_id_list in enumerate(id_list_list):
        logger.logp("get live data {} / {}".format(i + 1, len(id_list_list)))
        livedata_list = crawler.get_livedata_list(cur_id_list)
        if livedata_list is not None:
            read_livedata_list(livedata_list, livedata_dict)

    t_end = datetime.datetime.now()
    logger.logp("update_livedata : Total time = {} s".format(
        (t_end - t_start).total_seconds()))
    logger.logp("update_livedata : Done")


def read_livedata_list(livedata_list, livedata_dict):
    for livedata in livedata_list:
        stock_id = int(livedata["c"])

        try:
            date = int(livedata["d"])
        except:
            logger.logp(
                "Error: livedata date -- {} {}".format(stock_id, livedata))
            return

        try:
            vol = tools.float_parser(livedata["v"])
        except KeyError:
            vol = 0.0

        try:
            first = tools.float_parser(livedata["o"])
        except KeyError:
            try:
                first = tools.float_parser(livedata["pz"])
            except KeyError:
                first = 0.0

        try:
            highest = tools.float_parser(livedata["h"])
        except KeyError:
            try:
                highest = tools.float_parser(livedata["pz"])
            except KeyError:
                highest = 0.0

        try:
            lowest = tools.float_parser(livedata["l"])
        except KeyError:
            try:
                lowest = tools.float_parser(livedata["pz"])
            except KeyError:
                lowest = 0.0

        try:
            last = tools.float_parser(livedata["z"])
        except KeyError:
            try:
                last = tools.float_parser(livedata["pz"])
            except KeyError:
                last = 0.0

        try:
            delta = last - tools.float_parser(livedata["y"])
        except KeyError:
            delta = 0.0

        livedata_dict["{}".format(stock_id)] = [
            date, vol, first, highest, lowest, last, delta]


def update_sfd(stock_id, days, sfd_path, smd_path):
    now = datetime.datetime.now()

    content_dict = get_sfd_dict(sfd_path)

    trade_day_list = get_trade_day_list(smd_path)

    for i in range(days):
        yyyymmdd = int(now.strftime("%Y%m%d"))

        if yyyymmdd in trade_day_list:
            content = content_dict.get(str(yyyymmdd))

            # logger.logp("check: {} {}".format(stock_id, yyyymmdd))

            if content is None or content.get("DataPrice") is None:
                content = crawler.get_full_data(stock_id, yyyymmdd)

                if content is not None:
                    if content != {}:
                        if int(datetime.datetime.fromtimestamp(content["NowDate"] / 1000).strftime("%Y%m%d")) != yyyymmdd:
                            content = {}

                    content_dict[str(yyyymmdd)] = content

        now -= datetime.timedelta(days=1)

    tmp_path = sfd_path + ".tmp"
    tmp = open(tmp_path, 'w')
    tmp.write(json.dumps(content_dict))
    tmp.close()

    os.replace(tmp_path, sfd_path)


def get_sfd_dict(sfd_path):
    opt = {}
    if not os.path.exists(sfd_path):
        return opt

    try:
        sfd_file = open(sfd_path, 'r')
        opt = json.loads(sfd_file.read())
        sfd_file.close()

        return opt

    except:
        return opt


def update_sfd_in_list(stock_id_list, sfd_dir, smd_dir, days, force_update=False):
    now = datetime.datetime.now()
    if 8 <= now.hour <= 15:
        return

    update_log_path = sfd_dir + "/update.log"
    if is_smd_need_update(update_log_path) or force_update:
        size = len(stock_id_list)
        id_list_list = []
        if size > 200:
            for i in range(0, size, 200):
                id_list_list.append(stock_id_list[i:i + 200])

        t_list = []
        for i, cur_id_list in enumerate(id_list_list):
            t = threading.Thread(target=t_update_sfd_in_list,
                                 args=(cur_id_list, sfd_dir, smd_dir, days))
            t_list.append(t)
            t.start()

        for t in t_list:
            while t.is_alive():
                tools.delay(10)

    update_log_file = open(update_log_path, 'w')
    update_log_file.write(datetime.datetime.now().strftime("%Y/%m/%d/%H"))
    update_log_file.close()


def t_update_sfd_in_list(stock_id_list, sfd_dir, smd_dir, days):
    for stock_id in stock_id_list:
        sfd_path = "{}/{}.sfd".format(sfd_dir, stock_id)
        smd_path = "{}/{}.smd".format(smd_dir, stock_id)

        # logger.logp("update {}".format(stock_id))
        update_sfd(stock_id, days, sfd_path, smd_path)

    logger.logp("done")


def get_trade_day_list(smd_path):
    if not os.path.exists(smd_path):
        print("{} not exist".format(smd_path))
        return None

    # read
    smd_file = open(smd_path, 'r', encoding="UTF-8")
    content_dict = json.loads(smd_file.read())
    smd_file.close()

    trade_day_list = []

    for key in content_dict:
        for trade_day in content_dict[key]:
            trade_day_list.append(tools.tw_date2int(trade_day[0]))

    return trade_day_list

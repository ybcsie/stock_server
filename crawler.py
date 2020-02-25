import msgopt
import tools
import time
import urllib.request
import http.cookiejar
import json


logger = msgopt.Logger("crawler")


def get_listed_list():
    url = "http://isin.twse.com.tw/isin/class_main.jsp?market=1&issuetype=1"
    max_try = 3
    while True:
        if max_try == 0:
            return None

        if max_try != 3:
            tools.delay(3)

        max_try -= 1

        try:
            res = urllib.request.urlopen(url)
        except:
            logger.logp("Error: get listed id -- urllib")
            continue

        content = res.read().decode("cp950", errors='ignore')
        i_end = content.find("</table>")
        if i_end < 0:
            logger.logp("Error: get listed id -- source")
            continue

        i = i_end + 10

        i_end = content.find("</table>", i)
        if i_end < 0:
            logger.logp("Error: get listed id -- source")
            continue

        op_str = ""
        is_first_data = True

        while i < i_end:
            stock_id_str = ""
            ipo_date = ""

            i = content.find("<tr>", i)
            if i < 0:
                break

            for j in range(3):
                i = content.find("<td", i + 5)

            i = content.find('>', i + 5)
            i += 1

            while content[i] != '<':
                if content[i] != ' ' and content[i] != '\n':
                    stock_id_str += content[i]
                i += 1

            # ipo date
            for j in range(5):
                i = content.find("<td", i + 5)
            i = content.find('>', i + 5)
            i += 1
            while content[i] != '<':
                if content[i] != ' ' and content[i] != '\n':
                    ipo_date += content[i]
                i += 1

            op = "{},{}".format(stock_id_str, ipo_date)
            if is_first_data:
                is_first_data = False
            else:
                op = ';' + op

            op_str += op

        return op_str


def get_month_data(year, month, stock_id):
    logger.logp("Get month data: {} {}".format(year, month))

    arg = "STOCK_DAY?response=json&date={}{:02d}01&stockNo={}".format(year, month, stock_id)
    url = "http://www.twse.com.tw/exchangeReport/" + arg

    tools.delay(5)  # delay

    max_try = 3
    while True:
        logger.logp("Trying connection...")
        from socket import timeout
        try:
            res = urllib.request.urlopen(url, timeout=10)
            logger.logp("OK")

        except timeout:
            logger.logp("Error: urllib -- timeout")
            tools.wait_retry(logger, 10)
            continue

        except :
            logger.logp("Error: urllib")
            tools.wait_retry(logger, 30)
            continue

        logger.logp("Trying json decode...")
        data = ""
        try:
            data = json.loads(res.read().decode())
            if data["stat"] != "OK":
                if data["stat"] == "很抱歉，沒有符合條件的資料!":
                    return []
                logger.logp("data error: stat = {}".format(data["stat"]))

                tools.wait_retry(logger, 5)
                if max_try == 0:
                    return None

                max_try -= 1
                continue

        except:
            logger.logp("Error: json \"{}\"".format(data))
            tools.wait_retry(logger, 5)
            continue

        # check content date
        if tools.check_smd_content_by_key(data["data"][0], year * 100 + month):
            return data["data"]

        else:
            logger.logp("error content: {} {}".format(year * 100 + month, data["data"]))
            tools.wait_retry(logger, 5)
            continue


def get_day_trading_data(yyyymmdd):
    logger.logp("get_day_trading_data: {}".format(yyyymmdd))

    url = "http://www.twse.com.tw/exchangeReport/TWTB4U?response=json&date={}&selectType=All".format(yyyymmdd)

    tools.delay(3)  # delay

    max_try = 3
    while True:
        logger.logp("Trying connection...")
        from socket import timeout
        try:
            res = urllib.request.urlopen(url, timeout=10)
            logger.logp("OK")

        except timeout:
            logger.logp("Error: urllib -- timeout")
            tools.wait_retry(logger, 10)
            continue

        except :
            logger.logp("Error: urllib")
            tools.wait_retry(logger, 30)
            continue

        logger.logp("Trying json decode...")
        # check stat
        try:
            data = json.loads(res.read().decode())
            if data["stat"] != "OK":
                logger.logp("data error: stat = {}".format(data["stat"]))

                tools.wait_retry(logger, 5)
                if max_try == 0:
                    return None

                max_try -= 1
                continue

        except:
            logger.logp("Error: json when checking stat")
            tools.wait_retry(logger, 5)
            continue

        # check date
        try:
            if data["date"] != "{}".format(yyyymmdd):
                logger.logp("data error: date = {}".format(data["date"]))

                tools.wait_retry(logger, 5)
                if max_try == 0:
                    return None

                max_try -= 1
                continue

        except:
            logger.logp("Error: json when checking date")
            tools.wait_retry(logger, 5)
            continue

        # return
        return data["data"]


def get_livedata_list(stock_id_list):
    delay = 4
    max_try = 3
    while max_try > 0:
        tools.delay(delay)

        try:
            logger.logp("connecting to livedata...")
            url = "http://163.29.17.179/stock/fibest.jsp"
            cookie = http.cookiejar.CookieJar()
            handler = urllib.request.HTTPCookieProcessor(cookie)
            opener = urllib.request.build_opener(handler)
            logger.logp("opening url fibest...")
            opener.open(url)
            logger.logp("url fibest opened.")

            stock_arg = ""
            for stock_id in stock_id_list:
                stock_arg += "tse_{}.tw|".format(stock_id)

            arg = "getStockInfo.jsp?ex_ch={}&json=1&delay=0&_={}".format(stock_arg, int(time.time() * 1000))
            url = "http://163.29.17.179/stock/api/" + arg
            request = urllib.request.Request(url)
            logger.logp("opening url getStockInfo...")
            res = opener.open(request, timeout=10)
            logger.logp("url getStockInfo opened.")

        except:
            logger.logp("Error: connection")
            max_try -= 1
            continue

        try:
            livedata_list = json.loads(res.read().decode())
            if livedata_list["rtmessage"] != "OK":
                logger.logp("Error: data")
                max_try -= 1
                continue

        except:
            logger.logp("Error: json")
            max_try -= 1
            continue

        return livedata_list["msgArray"]

    tools.delay(30)
    return None


def get_full_data(stock_id, yyyymmdd):
    delay = 0.3
    max_try = 5
    while max_try > 0:
        tools.delay(delay)
        logger.logp("connect {} {}".format(stock_id, yyyymmdd))

        try:
            args = "?action=r&id={}&date={}".format(stock_id, yyyymmdd)
            url = "http://www.cmoney.tw/notice/chart/stockchart.aspx" + args
            cookie = http.cookiejar.CookieJar()
            handler = urllib.request.HTTPCookieProcessor(cookie)
            opener = urllib.request.build_opener(handler)
            res = opener.open(url, timeout=10).read().decode()

        except:
            logger.logp("Error: connection")
            max_try -= 1
            tools.delay(3)
            continue

        try:
            i = res.find("var ck")
            s = res.find('"', i) + 1
            e = res.find('"', s)

            ck = res[s: e]

        except:
            logger.logp("Error: parse ck")
            max_try -= 1
            tools.delay(3)
            continue

        try:
            args += "&ck=" + ck
            url2 = "http://www.cmoney.tw/notice/chart/stock-chart-service.ashx" + args
            request = urllib.request.Request(url2)
            request.add_header("Referer", url)
            res = opener.open(request, timeout=10)

        except:
            logger.logp("Error: connection")
            max_try -= 1
            tools.delay(3)
            continue

        try:
            content = json.loads(res.read().decode())
            if content["ErrorCode"] == 0:
                return content
            else:
                if content["ErrorCode"] == 124554:
                    return {}

                logger.logp("ErrorCode: {}".format(content["ErrorCode"]))
                max_try -= 1
                tools.delay(3)
                continue

        except:
            logger.logp("Error: json")
            max_try -= 1
            tools.delay(3)
            continue

    return None

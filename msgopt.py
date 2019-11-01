# last modify: 2019.01.16 15:36

import datetime
import os


class Logger:
    LOG_DIR = "logs"
    DT_FORMAT = "%Y%m%d"

    def del_old_log_files(self):
        for log_filename in os.listdir(Logger.LOG_DIR):
            if not log_filename.endswith(".log"):
                continue

            this_date = log_filename.split('_')[0]
            this_date = datetime.datetime.strptime(this_date, Logger.DT_FORMAT)
            if (self.log_date - this_date).days > 15:
                os.remove("{}/{}".format(Logger.LOG_DIR, log_filename))

    def __init__(self, log_name, display_func=print):
        self.log_name = log_name
        self.display = display_func
        self.log_file = None
        self.log_date = None
        self.set_log_file()

    def set_log_file(self):
        if self.log_file is not None:
            self.log_file.close()

        self.log_date = datetime.datetime.strptime(datetime.datetime.now().strftime(Logger.DT_FORMAT), Logger.DT_FORMAT)
        log_path = "{}/{}_{}.log".format(Logger.LOG_DIR, self.log_date.strftime(Logger.DT_FORMAT), self.log_name)

        if not os.path.exists(Logger.LOG_DIR):
            os.makedirs(Logger.LOG_DIR)

        self.log_file = open(log_path, 'a')

    def log(self, text, tag=""):
        if datetime.datetime.now().day != self.log_date.day:
            self.set_log_file()

        text = Logger.add_tag(text, tag)
        content = "[{}]\n{}".format(datetime.datetime.now(), text)
        self.log_file.write(content + '\n')
        self.log_file.flush()

    def logp(self, text, tag=""):
        text = Logger.add_tag(text, tag)
        self.display(text)
        self.log(text)

    @staticmethod
    def add_tag(text, tag):
        if tag != "":
            text = "[{}] {}".format(tag, text)

        return text



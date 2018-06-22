import datetime
import os


class Logger:
    log_dir = "logs"
    log_date = 0

    @staticmethod
    def del_old_log_files():
        log_filename_list = []
        for log_filename in os.listdir(Logger.log_dir):
            if log_filename.endswith(".log"):
                log_filename_list.append(log_filename)

        for log_filename in log_filename_list:
            this_date = int(log_filename.split('_')[0])
            if Logger.log_date - this_date > 15:
                os.remove("{}/{}".format(Logger.log_dir, log_filename))

    def __init__(self, log_name, display_func=print):
        self.log_name = log_name
        self.display = display_func
        self.log_file = None
        self.set_log_file()

    def set_log_file(self):
        if self.log_file is not None:
            self.log_file.close()

        Logger.log_date = int(datetime.datetime.now().strftime("%Y%m%d"))
        log_path = "{}/{}_{}.log".format(Logger.log_dir, Logger.log_date, self.log_name)

        if not os.path.exists(Logger.log_dir):
            os.makedirs(Logger.log_dir)

        self.log_file = open(log_path, 'a')

    def log(self, text):
        if int(datetime.datetime.now().strftime("%Y%m%d")) > Logger.log_date:
            self.set_log_file()

        content = "[{}]\n{}".format(datetime.datetime.now(), text)
        self.log_file.write(content + '\n')
        self.log_file.flush()

    def logp(self, text):
        self.display(text)
        self.log(text)



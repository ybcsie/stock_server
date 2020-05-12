import crawler
import json
import socket


masterip = "192.168.1.100"
slaveips = ["192.168.1.101", "192.168.1.102"]
port = 5888

is_master = True
slave_state = None

slaves = []

class Cmd:
    QSTATE = "qstate"
    GETSMD = "getsmd"
    ERR = "err"


class State:
    WAITING = "waiting"
    CRAWLING = "crawling"
    READY = "ready"


def cmd_qstate(arg_list):
    return slave_state


def cmd_getsmd(arg_list):
    content = json.dumps(crawler.get_month_data(int(arg_list[0]), int(arg_list[1]), int(arg_list[2])))
    return content


def cmd_err(arg_list):
    return "error"


cmd_dict = {Cmd.QSTATE: cmd_qstate, Cmd.GETSMD: cmd_getsmd, Cmd.ERR: cmd_err}


def encode_cmd(cmd, arg_list):
    for arg in arg_list:
        cmd = "{} {}".format(cmd, arg)

    try:
        cmd_b = cmd.encode()
    except:
        cmd_b = None

    return cmd_b


def encode_data(data):
    try:
        data_b = data.encode()
    except:
        data_b = "error".encode()

    return data_b


def msg_b_handler(msg_b):
    msg = None

    try:
        msg = msg_b.decode()
        if is_master:
            return msg

        msg = msg.split(' ')
    except:
        return None

    cmd_handler = cmd_dict.get(msg[0])
    if cmd_handler is None:
        cmd_handler = cmd_dict["err"]

    return cmd_handler(msg[1:])


def slave_start():
    print("slave_start")

    global is_master, slave_state
    is_master = False

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(("", port))
    server.listen()

    slave_state = State.WAITING

    while True:
        if slave_state == State.WAITING or slave_state == State.READY:
            print("listening")
            conn, addr = server.accept()  # waiting

            print("connection from: {}".format(addr))

            while True:
                master_msg_b = conn.recv(4096)
                print("master_msg_b = {}".format(master_msg_b))

                res_msg = msg_b_handler(master_msg_b)
                res_msg_b = encode_data(res_msg)
                conn.sendall(res_msg_b)

        elif slave_state == State.CRAWLING:
            slave_state = State.READY


def master_start():
    print("master_start")

    global slaves

    for slaveip in slaveips:
        try:
            print("connect to slave: {}".format(slaveip))
            slave = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            slave.connect((slaveip, port))
            slaves.append(slave)
            print("OK")
        except:
            print("Failed")
            continue
    
    print("{} slaves connected".format(len(slaves)))


def master_close():
    for slave in slaves:
        slave.close()


def master_send_cmd(slave, cmd, arg_list=[]):
    req_msg_b = encode_cmd(cmd, arg_list)
    if req_msg_b is None:
        return "error"

    try:
        print("master_send_cmd: {}", req_msg_b)
        slave.sendall(req_msg_b)
        slave_msg_b = slave.recv(4096)
        slave_msg = msg_b_handler(slave_msg_b)

    except:
        slave_msg = "error"

    return slave_msg


def get_month_data(slave_id, year, month, stock_id):
    return json.loads(master_send_cmd(slaves[slave_id], Cmd.GETSMD, [year, month, stock_id]))


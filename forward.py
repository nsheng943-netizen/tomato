import yaml
import socket
import threading
import time
import logging
from logging import FileHandler
from datetime import datetime
import queue

# 配置日志记录器
LOG_FILE = f"./logs/forward_{datetime.now().strftime('%Y-%m-%d-%H-%M')}.txt"
# 创建独立 logger
my_logger = logging.getLogger('my_logger')
my_logger.setLevel(logging.INFO)
# 设置格式
formatter = logging.Formatter('%(asctime)s.%(msecs)03d,[%(levelname)s],%(message)s', datefmt='%Y-%m-%d %H:%M:%S')
# 记录heartbeat消息
TO_HELLO_LIST = []
# 存储udp app
UDP_APP_LIST = []
SOCKET_BUFFER_LEN = 256
# 缓存待转发数据
forward_buffer_queue = queue.Queue()
forward_addr_list = []

def udp_app_init(port, isblocking=False, noblocking_timeout=1.0):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    # if port != 0:   # 如果是0则不绑定端口
    sock.bind(("0.0.0.0", port))
    if not isblocking:
        sock.setblocking(False) # 设置为非阻塞模式
        sock.settimeout(noblocking_timeout)    # 等待超时1秒
    else:
        sock.setblocking(True)  # 设置阻塞模式
    print(f"UDP服务器{sock.getsockname()}已启动")
    return sock


# 心跳发送函数
def connecting(listen_addr, listen_heartbeat, forward_list, idx):
    # 解析监听地址
    lip, lport = listen_addr
    lmsg = str(listen_heartbeat['msg'])
    udp_app = UDP_APP_LIST[idx]
    udp_app.sendto(lmsg.encode(), (lip, lport))
    TO_HELLO_LIST.append((idx, lip, lport, lmsg))

    for target in forward_list: 
        ip, port = target['target_addr']
        hearb = bool(target['enable_heartbeat'])
        msg = str(target['heartbeat_msg'])
        if hearb:
            udp_app.sendto(msg.encode(), (ip, port))
            TO_HELLO_LIST.append((idx, ip, port, msg))
        forward_addr_list[idx].append((ip, port))

    while True:
        buffer = None
        try:
            buffer, addr = udp_app.recvfrom(SOCKET_BUFFER_LEN)
            print(f"connecting有数据，来自{addr} 长度为:{len(buffer)}")
            log_r = f"recvfrom,{addr[0]}:{addr[1]},{len(buffer)}"
            my_logger.info(log_r)
            forward_buffer_queue.put((idx, buffer))

            # for target in forward_list:
            #     ip, port = target['target_addr']
            #     udp_app.sendto(buffer, (ip, port))
            #     log_s = f"sendto,{ip}:{port},{len(buffer)}"
            #     my_logger.info(log_s)

        # except socket.timeout:
        except Exception as e:
            print(f"exception: {e}")
            continue

def worker_forward():
    print("worker_forward start")
    while True:
        try:
            idx, buffer = forward_buffer_queue.get(timeout=1)
            udp_app = UDP_APP_LIST[idx]
            for ip,port in forward_addr_list[idx]:
                udp_app.sendto(buffer, (ip, port))

        except queue.Empty:
            continue


if __name__ == "__main__":

    file_handler = FileHandler(LOG_FILE, encoding='utf-8')
    file_handler.setFormatter(formatter)
    my_logger.addHandler(file_handler)

    # 读取YAML配置文件
    with open("pyconfig.yaml", "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    # 启动5个线程分别处理5个监听地址
    for idx, rule in enumerate(config["forward_rules"]):
        listen_addr = rule["listen_addr"]
        ip, port = listen_addr
        listen_heartbeat = rule["listen_heartbeat"]
        forward_list = rule["forward_list"]
        udp_app = udp_app_init(0, True)
        UDP_APP_LIST.append(udp_app)
        #初始化空队列
        forward_addr_list.append([])

        # 创建线程
        threading.Thread(target=connecting, args=(listen_addr, listen_heartbeat, forward_list, idx), daemon=True).start()

    threading.Thread(target=worker_forward, args=(), daemon=True).start()

    while True:
        time.sleep(5)
        for idx, ip, port, msg in TO_HELLO_LIST:
            udp_app = UDP_APP_LIST[idx]
            udp_app.sendto(msg.encode(), (ip, port))
            log_hb = f"heartb,{ip}:{port},{msg}"
            my_logger.info(log_hb)


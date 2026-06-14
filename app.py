import time
import math
import queue
import struct
import socket
import logging
import threading
from datetime import datetime
from logging import FileHandler

import yaml
from flask import Flask, render_template
from flask_socketio import SocketIO

# ====================== 应用初始化 ======================
app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*")

# ====================== 常量与配置 ======================
# 功能开关
ENABLE_LOG = True
ENABLE_HEARTBEAT = True

# 系统参数
SOCKET_BUFFER_SIZE = 256
OFFLINE_THRESHOLD = 30  # 秒
MOBILE_NODE_OFFLINE_THRESHOLD = 60  # 秒
BROADCAST_INTERVAL = 2  # 秒
LOSS_RATE_CALC_INTERVAL = 10  # 秒
HEARTBEAT_INTERVAL = 10  # 秒

# 日志配置
LOG_FILE_PATH = f"./logs/log_{datetime.now().strftime('%Y-%m-%d-%H-%M')}.csv"
logger = logging.getLogger("node_monitor")
logger.setLevel(logging.INFO)
log_formatter = logging.Formatter(
    '%(asctime)s.%(msecs)03d,[%(levelname)s],%(message)s',
    datefmt='%m-%d %H:%M:%S'
)

# 前端地图默认配置
MAP_DEFAULT_CONFIG = {
    "username": "admin",
    "center": [122.20, 29.98],
    "zoom": 16,
    "show_gram": True,
    "show_settings": False,
    "names": ["信标1", "信标2", "信标3", "信标4", "水下移动节点"]
}

# 工作状态映射
WORK_STATUS = {0: "idle", 1: "sending", 2: "receiving", 3: "offline"}

# 丢包率统计配置
PACKET_INTERVALS = [5, 5, 5, 5]
packet_counters = [0, 0, 0, 0]
loss_rate_history = [[], [], [], []]

# ====================== 全局数据存储 ======================
# 网络配置（从config.yaml加载）
udp_node_sockets = []  # 普通节点UDP套接字列表 (node_id, socket, address)
udp_locate_sockets = []  # 定位节点UDP套接字列表
UDP_ADDR_LIST = []
UDP_ADDR_LOCATE_LIST = []

# 数据队列
data_queue = queue.Queue()

# 节点状态数据
node_states = {
    "node1": [
        {"name": "信标1", "id": 0, "lng": 121.977620, "lat": 29.699264, "status": "idle",
         "warning": False, "freq": "normal", "signal_toa": "--", "signal_toe": "--",
         "loss_rate": None, "draw": True},
        {"最后信息更新": "--", "测流信息": "NaN", "声通心跳": "NaN", "面板信息": "NaN"}
    ],
    "node2": [
        {"name": "信标2", "id": 1, "lng": 121.97699, "lat": 29.698515, "status": "idle",
         "warning": False, "freq": "normal", "signal_toa": "--", "signal_toe": "--",
         "loss_rate": None, "draw": True},
        {"最后信息更新": "--", "测流信息": "NaN", "声通心跳": "NaN", "面板信息": "NaN"}
    ],
    "node3": [
        {"name": "信标3", "id": 2, "lng": 121.978142, "lat": 29.697516, "status": "idle",
         "warning": False, "freq": "normal", "signal_toa": "--", "signal_toe": "--",
         "loss_rate": None, "draw": True},
        {"最后信息更新": "--", "测流信息": "NaN", "声通心跳": "NaN", "面板信息": "NaN"}
    ],
    "node4": [
        {"name": "信标4", "id": 3, "lng": 121.978255, "lat": 29.697522, "status": "idle",
         "warning": False, "freq": "normal", "signal_toa": "--", "signal_toe": "--",
         "loss_rate": None, "draw": True},
        {"最后信息更新": "--", "测流信息": "NaN", "声通心跳": "NaN", "面板信息": "NaN"}
    ],
    "node5": [
        {"name": "水下移动节点", "id": 4, "lng": 121.977098, "lat": 29.696006, "depth": 5.0,
         "status": "idle", "warning": False, "freq": "normal", "signal_toa": "--",
         "signal_toe": "--", "loss_rate": "--", "draw": False},
        {"最后信息更新": "--", "状态信息": "NaN", "心跳信息": "NaN", "上行数据帧": "NaN",
         "距信标1": "--", "距信标2": "--", "距信标3": "--",
         "距信标4": "--", "误差": "--"}
    ]
}

# 定位解算结果
locate_results = {
    "locateNode1": [
        {"name": "定位估计点", "id": 5, "calc_lng": 0.0, "calc_lat": 0.0,
         "calc_depth": 15.1, "calc_time": "--", "confidence": 0.0},
        {"最后信息更新": "--", "timestamp": 0, "real_lng": 0.0, "real_lat": 0.0,
         "pressure_temp": 0, "pressure_humi": 0}
    ]
}

# 节点最后活动时间
last_active_time = {
    "node1": 0, "node2": 0, "node3": 0, "node4": 0, "node5": 0, "locateNode1": 0
}


# ====================== 工具函数 ======================
def wgs84_to_gcj02(lng, lat):
    """
    将WGS-84坐标转换为GCJ-02坐标（火星坐标系）
    """
    PI = 3.141592653589793
    a = 6378245.0
    ee = 0.006693421622965943

    def transform_lat(x, y):
        ret = -100.0 + 2.0 * x + 3.0 * y + 0.2 * y * y + 0.1 * x * y + 0.2 * math.sqrt(abs(x))
        ret += (20.0 * math.sin(6.0 * x * PI) + 20.0 * math.sin(2.0 * x * PI)) * 2.0 / 3.0
        ret += (20.0 * math.sin(y * PI) + 40.0 * math.sin(y / 3.0 * PI)) * 2.0 / 3.0
        ret += (160.0 * math.sin(y / 12.0 * PI) + 320.0 * math.sin(y * PI / 30.0)) * 2.0 / 3.0
        return ret

    def transform_lng(x, y):
        ret = 300.0 + x + 2.0 * y + 0.1 * x * x + 0.1 * x * y + 0.1 * math.sqrt(abs(x))
        ret += (20.0 * math.sin(6.0 * x * PI) + 20.0 * math.sin(2.0 * x * PI)) * 2.0 / 3.0
        ret += (20.0 * math.sin(x * PI) + 40.0 * math.sin(x / 3.0 * PI)) * 2.0 / 3.0
        ret += (150.0 * math.sin(x / 12.0 * PI) + 300.0 * math.sin(y / 30.0 * PI)) * 2.0 / 3.0
        return ret

    d_lat = transform_lat(lng - 105.0, lat - 35.0)
    d_lng = transform_lng(lng - 105.0, lat - 35.0)

    rad_lat = math.radians(lat)
    magic = math.sin(rad_lat)
    magic = 1 - ee * magic * magic
    sqrt_magic = math.sqrt(magic)

    d_lat = (d_lat * 180.0) / ((a * (1 - ee)) / (magic * sqrt_magic) * PI)
    d_lng = (d_lng * 180.0) / (a / sqrt_magic * math.cos(rad_lat) * PI)

    return round(lng + d_lng, 6), round(lat + d_lat, 6)


def calculate_distance(lng1, lat1, lng2, lat2):
    """
    使用Haversine公式计算两个经纬度点之间的球面距离（米）
    """
    if not all([lng1, lat1, lng2, lat2]):
        return 0.0

    lng1, lat1 = wgs84_to_gcj02(lng1, lat1)
    lng2, lat2 = wgs84_to_gcj02(lng2, lat2)

    R = 6378137  # WGS-84地球长半轴（米）
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lng2 - lng1)

    a = math.sin(delta_phi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2) ** 2
    return round(R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a)), 2)


def update_mobile_node_distances():
    """
    更新水下移动节点到各信标的距离
    """
    calc_lng = locate_results["locateNode1"][0]["calc_lng"]
    calc_lat = locate_results["locateNode1"][0]["calc_lat"]

    if not (calc_lng and calc_lat):
        for i in range(1, 5):
            node_states["node5"][1][f"dist_to_beacon{i}"] = "--"
        return

    for i in range(1, 5):
        beacon_node = f"node{i}"
        beacon_lng = node_states[beacon_node][0]["lng"]
        beacon_lat = node_states[beacon_node][0]["lat"]

        if beacon_lng and beacon_lat:
            distance = calculate_distance(calc_lng, calc_lat, beacon_lng, beacon_lat)
            node_states["node5"][1][f"dist_to_beacon{i}"] = f"{distance}m"
        else:
            node_states["node5"][1][f"dist_to_beacon{i}"] = "--"


def decode_custom_timestamp(ts):
    """
    解析云洲自定义时间戳格式
    """
    year = 2024 + ((ts >> 43) & 0x1F)
    month = (ts >> 39) & 0x0F
    day = (ts >> 34) & 0x1F
    hour = (ts >> 29) & 0x1F
    minute = (ts >> 23) & 0x3F
    second = (ts & 0x7FFFFF) / 100000

    return f"{year}-{month:02d}-{day:02d} {hour:02d}:{minute:02d}:{second:.5f}"


def get_current_time_str():
    """获取当前时间字符串"""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


# ====================== UDP通信模块 ======================
def create_udp_socket(port, blocking=False, timeout=1.0):
    """
    创建并配置UDP套接字
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("0.0.0.0", port))

    if not blocking:
        sock.setblocking(False)
        sock.settimeout(timeout)

    if ENABLE_LOG:
        logger.info(f"UDP服务器已启动: {sock.getsockname()}")
    print(f"UDP服务器已启动: {sock.getsockname()}")

    return sock


def node_data_receiver(sock, node_id, port):
    """
    普通节点数据接收线程
    """
    while True:
        try:
            buffer, addr = sock.recvfrom(SOCKET_BUFFER_SIZE)
            data_queue.put((buffer, node_id, addr[0], port))
            last_active_time[f"node{node_id}"] = time.time()

            # 移动节点数据也会通过信标节点转发
            if len(buffer) in (36, 47):
                last_active_time["node5"] = time.time()

        except (socket.timeout, TimeoutError, ConnectionResetError):
            # 检查节点是否离线
            time_since_last_active = time.time() - last_active_time.get(f"node{node_id}", time.time())
            if time_since_last_active > OFFLINE_THRESHOLD:
                node_states[f"node{node_id}"][0]["status"] = "offline"
            continue
        except Exception as e:
            if ENABLE_LOG:
                logger.error(f"节点{node_id}接收数据错误: {e}", exc_info=True)
            continue


def locate_data_receiver(sock):
    """
    定位节点数据接收与解析线程
    """
    while True:
        try:
            buffer, addr = sock.recvfrom(SOCKET_BUFFER_SIZE)
            if len(buffer) != 53:
                logger.warning(f"收到无效长度的定位数据包: {len(buffer)}")
                continue

            # 解析53字节定位数据包
            calc_time = struct.unpack("I", buffer[0:4])[0]
            confidence = struct.unpack("c", buffer[4:5])[0]
            calc_lng, calc_lat = struct.unpack("dd", buffer[5:21])
            depth = struct.unpack("f", buffer[21:25])[0]
            timestamp = struct.unpack("I", buffer[25:29])[0]
            real_lng, real_lat = struct.unpack("dd", buffer[29:45])
            temp, humi = struct.unpack("ff", buffer[45:53])

            last_active_time["node5"] = time.time()

            # 更新定位结果
            if calc_lng != 0.0 and calc_lat != 0.0:
                locate_results["locateNode1"][0]["calc_lng"] = calc_lng
                locate_results["locateNode1"][0]["calc_lat"] = calc_lat

            if depth != 0:
                locate_results["locateNode1"][0]["calc_depth"] = depth

            locate_results["locateNode1"][0].update({
                "calc_time": calc_time,
                "confidence": confidence
            })

            locate_results["locateNode1"][1].update({
                "last_update": get_current_time_str(),
                "timestamp": timestamp,
                "real_lng": real_lng,
                "real_lat": real_lat,
                "pressure_temp": temp,
                "pressure_humi": humi
            })

            # 更新移动节点状态
            node_states["node5"][0].update({
                "lng": real_lng,
                "lat": real_lat,
                "depth": depth,
                "status": "receiving"
            })

            node_states["node5"][1].update({
                "last_update": get_current_time_str(),
                "pressure_temp": temp,
                "pressure_humi": humi,
                "calc_lng": calc_lng,
                "calc_lat": calc_lat,
                "depth": depth
            })

            # 计算距离和误差
            update_mobile_node_distances()
            if all([real_lng, real_lat, calc_lng, calc_lat]):
                error = calculate_distance(real_lng, real_lat, calc_lng, calc_lat)
                locate_results["locateNode1"][1]["error"] = error

            if ENABLE_LOG:
                logger.info(f"NYC定位,{addr[0]}-{addr[1]},{calc_lng},{calc_lat},{depth},"
                            f"{calc_time},{confidence},{timestamp},{real_lng},{real_lat},{temp},{humi}")

        except (socket.timeout, TimeoutError, ConnectionResetError):
            # 检查移动节点是否离线
            time_since_last_active = time.time() - last_active_time.get("node5", time.time())
            if time_since_last_active > MOBILE_NODE_OFFLINE_THRESHOLD:
                node_states["node5"][0]["status"] = "offline"
            continue
        except Exception as e:
            if ENABLE_LOG:
                logger.error(f"定位数据解析错误: {e}", exc_info=True)
            continue


# ====================== 数据解析模块 ======================
def data_parser_worker():
    """
    数据解析工作线程
    """
    while True:
        if data_queue.empty():
            time.sleep(0.01)
            continue

        buffer, node_id, ip, port = data_queue.get()
        packet_length = len(buffer)

        try:
            if packet_length == 107:
                parse_107_byte_packet(buffer, node_id, ip, port)
            elif packet_length == 47:
                parse_47_byte_packet(buffer, node_id, ip, port)
            elif packet_length == 36:
                parse_36_byte_packet(buffer, node_id, ip, port)
            else:
                logger.warning(f"收到未知长度的数据包: {packet_length}")
        except Exception as e:
            if ENABLE_LOG:
                logger.error(f"数据包解析失败(长度{packet_length}): {e}", exc_info=True)


def parse_107_byte_packet(buffer, node_id, ip, port):
    """解析107字节USV声通数据包"""
    # 解析头部
    flag = struct.unpack('B', buffer[0:1])[0]
    node_idx = struct.unpack('B', buffer[1:2])[0]
    timestamp = struct.unpack('I', buffer[2:6])[0]
    work_status = struct.unpack('B', buffer[6:7])[0]
    ad_sample_rate = struct.unpack('H', buffer[7:9])[0]
    signal_pos = struct.unpack('I', buffer[9:13])[0]
    correlation_peak = struct.unpack('d', buffer[13:21])[0]

    # 解析传感器数据
    tt = struct.unpack('i', buffer[21:25])[0]
    sensor_data = struct.unpack('8f', buffer[25:57])
    ext_pressure, ext_temp, board_temp, int_humidity = sensor_data[:4]
    current_acom, voltage_acom, current_pa, voltage_pa = sensor_data[4:]

    # 解析电源数据
    power_data = struct.unpack('8H', buffer[57:73])
    cur24 = power_data[0] * 10 / 8191
    cur12 = power_data[1] * 10 / 8191
    volt12 = power_data[5] * 30 / 8191
    cur6 = power_data[2] * 10 / 8191
    volt6 = power_data[3] * 30 / 8191
    cur5 = power_data[4] * 10 / 8191
    volt5 = power_data[6] * 30 / 8191
    volt_bridge = power_data[7] * 10 / 8191

    # 解析警告信息
    warning = struct.unpack('B', buffer[73:74])[0]
    is_warning = (warning & 0b1000) == 0b1000

    # 解析冰面节点信息
    surface_temp = struct.unpack('f', buffer[74:78])[0]
    surface_humi = struct.unpack('f', buffer[78:82])[0]
    gps_state = struct.unpack('c', buffer[82:83])[0]
    gps_time = struct.unpack('I', buffer[83:87])[0]
    longitude = struct.unpack('d', buffer[87:95])[0]
    latitude = struct.unpack('d', buffer[95:103])[0]
    speed = struct.unpack('f', buffer[103:107])[0]

    # 格式化信息字符串
    flow_info = (f"标志位:{flag}, 节点ID:{node_idx}, 时间戳:{timestamp}, 工作状态:{work_status}, "
                 f"AD采样率:{ad_sample_rate}, 信号位置:{signal_pos}, 相关峰值:{correlation_peak:.2f}")

    heartbeat_info = (f"舱外压力:{ext_pressure:.2f}mbar, 舱外温度:{ext_temp:.2f}°C, "
                      f"舱内温度:{board_temp:.2f}°C, 舱内湿度:{int_humidity:.2f}%, "
                      f"通信机电流:{current_acom:.2f}A, 通信机电压:{voltage_acom:.2f}V, "
                      f"功放电流:{current_pa:.2f}A, 功放电压:{voltage_pa:.2f}V, "
                      f"24V电流:{cur24:.1f}A, 12V电流:{cur12:.1f}A, 12V电压:{volt12:.1f}V, "
                      f"6V电流:{cur6:.1f}A, 6V电压:{volt6:.1f}V, 5V电流:{cur5:.1f}A, "
                      f"5V电压:{volt5:.1f}V, 桥电压:{volt_bridge:.1f}V")

    surface_info = (f"转发节点温度:{surface_temp:.1f}°C, 转发节点湿度:{surface_humi:.1f}%, "
                    f"GPS时间:{gps_time}, 经度:{longitude:.6f}, 纬度:{latitude:.6f}, "
                    f"速度:{speed:.2f}knot")

    # 记录日志
    if ENABLE_LOG:
        log_msg = (f"USV声通,{ip}-{port},{node_id - 1},{flag},{node_idx},{timestamp},{work_status},"
                   f"{ad_sample_rate},{signal_pos},{correlation_peak:.2f},{tt},{ext_pressure:.2f},"
                   f"{ext_temp:.2f},{board_temp:.2f},{int_humidity:.2f},{current_acom:.2f},"
                   f"{voltage_acom:.2f},{current_pa:.2f},{voltage_pa:.2f},{cur24:.2f},{cur12:.2f},"
                   f"{volt12:.2f},{cur6:.2f},{volt6:.2f},{cur5:.2f},{volt5:.2f},{volt_bridge:.2f},"
                   f"{warning},{surface_temp:.2f},{surface_humi:.2f},{gps_state},{gps_time},"
                   f"{longitude:.6f},{latitude:.6f},{speed:.2f}")
        logger.info(log_msg)

    # 更新节点状态
    node_key = f"node{node_id}"
    node_states[node_key][0].update({
        "lat": latitude,
        "lng": longitude,
        "status": WORK_STATUS.get(work_status, "unknown"),
        "warning": is_warning
    })

    node_states[node_key][1].update({
        "last_update": get_current_time_str(),
        "flow_info": flow_info,
        "acoustic_heartbeat": heartbeat_info,
        "surface_info": surface_info
    })

    packet_counters[node_id - 1] += 1
    update_mobile_node_distances()


def parse_47_byte_packet(buffer, node_id, ip, port):
    """解析47字节AUV状态数据包"""
    auv_time = struct.unpack('I', buffer[4:8])[0]
    auv_id = struct.unpack('B', buffer[8:9])[0]
    auv_status = struct.unpack('B', buffer[9:10])[0]
    error_code = struct.unpack('I', buffer[10:14])[0]
    lat = struct.unpack('i', buffer[14:18])[0] / 10000000
    lng = struct.unpack('i', buffer[18:22])[0] / 10000000
    depth = struct.unpack('H', buffer[22:24])[0] / 10
    altitude = struct.unpack('H', buffer[24:26])[0]
    heading = struct.unpack('H', buffer[26:28])[0]
    battery = struct.unpack('B', buffer[28:29])[0]
    toa = struct.unpack('<Q', buffer[29:37])[0]
    toe = struct.unpack('<Q', buffer[37:45])[0]

    toa_str = decode_custom_timestamp(toa)
    toe_str = decode_custom_timestamp(toe)

    # 更新信标节点的信号时间
    node_states[f"node{node_id}"][0]["signal_toa"] = toa_str
    node_states[f"node{node_id}"][0]["signal_toe"] = toe_str

    # 更新移动节点状态
    node_states["node5"][0].update({
        "lng": lng,
        "lat": lat,
        "depth": depth,
        "status": "receiving"
    })

    status_info = (f"时间戳:{auv_time}, 标识号:{auv_id}, 状态机:{auv_status}, 故障码:{error_code}, "
                   f"纬度:{lat}, 经度:{lng}, 深度:{depth}, 高度:{altitude}, 航向:{heading}, 电量:{battery}")

    node_states["node5"][1].update({
        "last_update": get_current_time_str(),
        "status_info": status_info
    })

    # 更新定位结果
    if lng != 0.0 and lat != 0.0:
        locate_results["locateNode1"][0]["calc_lng"] = lng
        locate_results["locateNode1"][0]["calc_lat"] = lat

    if depth != 0:
        locate_results["locateNode1"][0]["calc_depth"] = depth

    locate_results["locateNode1"][1]["timestamp"] = auv_time

    update_mobile_node_distances()

    # 记录日志
    if ENABLE_LOG:
        log_msg = (f"AUV状态,{ip}-{port},{auv_time},{auv_id},{auv_status},{error_code},"
                   f"{lat:.2f},{lng:.2f},{depth:.2f},{altitude:.2f},{heading:.2f},{battery:.2f},"
                   f"{toa_str},{toe_str}")
        logger.info(log_msg)


def parse_36_byte_packet(buffer, node_id, ip, port):
    """解析36字节AUV心跳数据包"""
    auv_time = struct.unpack('I', buffer[4:8])[0]
    lat = struct.unpack('i', buffer[8:12])[0] / 10000000
    lng = struct.unpack('i', buffer[12:16])[0] / 10000000
    depth = struct.unpack('H', buffer[16:18])[0] / 10
    toa = struct.unpack('<Q', buffer[18:26])[0]
    toe = struct.unpack('<Q', buffer[26:34])[0]

    toa_str = decode_custom_timestamp(toa)
    toe_str = decode_custom_timestamp(toe)

    # 更新信标节点的信号时间
    node_states[f"node{node_id}"][0]["signal_toa"] = toa_str
    node_states[f"node{node_id}"][0]["signal_toe"] = toe_str

    # 更新移动节点状态
    node_states["node5"][0].update({
        "lng": lng,
        "lat": lat,
        "depth": depth,
        "status": "receiving"
    })

    heartbeat_info = f"时间戳:{auv_time}, 纬度:{lat}, 经度:{lng}, 深度:{depth}"

    node_states["node5"][1].update({
        "last_update": get_current_time_str(),
        "heartbeat_info": heartbeat_info
    })

    # 更新定位结果
    if lng != 0.0 and lat != 0.0:
        locate_results["locateNode1"][0]["calc_lng"] = lng
        locate_results["locateNode1"][0]["calc_lat"] = lat

    if depth != 0:
        locate_results["locateNode1"][0]["calc_depth"] = depth

    locate_results["locateNode1"][1]["timestamp"] = auv_time

    update_mobile_node_distances()

    # 记录日志
    if ENABLE_LOG:
        log_msg = (f"AUV心跳,{ip}-{port},{auv_time},{lat:.2f},{lng:.2f},{depth:.2f},"
                   f"{toa_str},{toe_str}")
        logger.info(log_msg)


# ====================== 定时任务模块 ======================
def heartbeat_sender():
    """
    心跳包发送线程
    """
    while True:
        # 向普通节点发送心跳
        for _, sock, addr in udp_node_sockets:
            try:
                sock.sendto(b'HEARTBEAT', addr)
            except Exception as e:
                logger.warning(f"向{addr}发送心跳失败: {e}")

        # 向定位节点发送心跳
        for i, sock in enumerate(udp_locate_sockets):
            try:
                sock.sendto(b'HEARTBEAT', UDP_ADDR_LOCATE_LIST[i])
            except Exception as e:
                logger.warning(f"向定位节点{i}发送心跳失败: {e}")

        time.sleep(HEARTBEAT_INTERVAL)


def websocket_broadcaster():
    """
    WebSocket数据广播线程
    """
    loss_rate_calc_counter = 0
    calc_cycles = LOSS_RATE_CALC_INTERVAL // BROADCAST_INTERVAL
    history_window = 60 // LOSS_RATE_CALC_INTERVAL  # 保留1分钟的历史数据

    while True:
        loss_rate_calc_counter += 1

        # 计算丢包率
        if loss_rate_calc_counter >= calc_cycles:
            for i in range(4):
                expected_packets = LOSS_RATE_CALC_INTERVAL / PACKET_INTERVALS[i]
                if expected_packets > 0:
                    loss_rate = 1 - packet_counters[i] / expected_packets
                    loss_rate = max(0.0, min(1.0, loss_rate))  # 限制在0-1之间

                    # 更新历史记录
                    while len(loss_rate_history[i]) >= history_window:
                        loss_rate_history[i].pop(0)
                    loss_rate_history[i].append(loss_rate)

                    # 计算平均丢包率
                    avg_loss_rate = sum(loss_rate_history[i]) / len(loss_rate_history[i])
                    node_states[f"node{i + 1}"][0]["loss_rate"] = round(avg_loss_rate, 2)

                packet_counters[i] = 0

            loss_rate_calc_counter = 0

        # 广播节点状态和定位结果
        socketio.emit("update_nodes", {"node_status": node_states})
        socketio.emit("update_locate_nodes", {"locate_node_status": locate_results})

        # 重置接收状态
        for node in node_states:
            if node_states[node][0]["status"] == "receiving":
                node_states[node][0]["status"] = "idle"

        time.sleep(BROADCAST_INTERVAL)


# ====================== Web路由 ======================
@app.route("/")
def index():
    """主页"""
    return render_template("index.html", START_CONFIG=MAP_DEFAULT_CONFIG)


@socketio.on("connect")
def handle_connect():
    """处理客户端连接"""
    print(f"用户 {MAP_DEFAULT_CONFIG['username']} 已连接")


@socketio.on("disconnect")
def handle_disconnect():
    """处理客户端断开"""
    print(f"用户 {MAP_DEFAULT_CONFIG['username']} 已断开")


# ====================== 配置加载 ======================
def load_configuration():
    """
    从config.yaml加载配置
    """
    global ENABLE_LOG, ENABLE_HEARTBEAT, MAP_DEFAULT_CONFIG
    global UDP_ADDR_LIST, UDP_ADDR_LOCATE_LIST, udp_node_sockets, udp_locate_sockets

    try:
        with open("./config.yaml", 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)

        # 加载普通节点网络配置
        UDP_ADDR_LIST = []
        for network in ["Network1", "Network2"]:
            if config['Connect'].get(network, {}).get('enable'):
                for i, addr in enumerate(config['Connect'][network]['addrs']):
                    if addr is not None:
                        UDP_ADDR_LIST.append((1, i + 1, tuple(addr)))

        # 加载定位节点网络配置
        UDP_ADDR_LOCATE_LIST = []
        for addr in config['Location']['Network']['addrs']:
            if addr is not None:
                UDP_ADDR_LOCATE_LIST.append(tuple(addr))

        # 加载日志配置
        ENABLE_LOG = config['Login']['enableLog']
        if ENABLE_LOG:
            file_handler = FileHandler(LOG_FILE_PATH, encoding='utf-8')
            file_handler.setFormatter(log_formatter)
            logger.addHandler(file_handler)

            # 写入日志表头
            logger.info("USV声通,网络-端口,nodeId,标志位,节点ID,时间戳,工作状态,AD采样率,到达时刻,相关峰值,"
                        "TimeStamp,ExternalPresure,ExternalTemperature,BoardTemperature,InternalHumidity,"
                        "CurrentAcom,VoltageAcom,CurrentPA,VoltagePA,警告信息,TraTemp,TraHumi,GPSState,"
                        "GPSTime,Longtitude,Latitude,Speed")
            logger.info("NYC定位,网络-端口,定位经度,定位纬度,深度,解算时间,置信度,时间戳,真实经度,真实纬度,"
                        "耐压舱温度,耐压舱湿度")
            logger.info("AUV状态,网络-端口,时间戳,标识号,状态机,故障码,纬度,经度,深度,高度,航向,电量,toa,toe")
            logger.info("AUV心跳,网络-端口,时间戳,纬度,经度,深度,toa,toe")

        # 加载节点配置
        config_points = config['Map']['configPoints']
        connect_names = config['Connect']['connectNames']
        device_freqs = config['Map']['deviceFrequence']

        for i, node_key in enumerate(node_states):
            if i < len(connect_names):
                node_states[node_key][0]['name'] = connect_names[i]
            if i < len(device_freqs):
                node_states[node_key][0]['freq'] = device_freqs[i]

            if config['Map']['useConfig'] and i < len(config_points):
                node_states[node_key][0]['lng'] = config_points[i][0]
                node_states[node_key][0]['lat'] = config_points[i][1]

        # 加载地图配置
        MAP_DEFAULT_CONFIG.update({
            "username": config['Login']['userName'],
            "center": config['Map']['center'],
            "zoom": config['Map']['zoom'],
            "names": connect_names
        })

        # 加载心跳配置
        ENABLE_HEARTBEAT = config['Connect']['SendHeartBeat']

        print("配置加载成功")

    except Exception as e:
        print(f"配置加载失败: {e}")
        print("将使用默认配置运行")


# ====================== 应用启动 ======================
def initialize_udp_sockets():
    """初始化UDP套接字"""
    global udp_node_sockets, udp_locate_sockets

    # 初始化普通节点UDP套接字
    udp_node_sockets = []
    for net_id, node_i, (ip, port) in UDP_ADDR_LIST:
        sock = create_udp_socket(0, False)  # 使用随机端口
        udp_node_sockets.append((node_i, sock, (ip, port)))

    # 初始化定位节点UDP套接字
    udp_locate_sockets = []
    for (ip, port) in UDP_ADDR_LOCATE_LIST:
        sock = create_udp_socket(0, False)  # 使用随机端口
        udp_locate_sockets.append(sock)


def start_background_threads():
    """启动所有后台线程"""
    # 数据解析线程
    threading.Thread(target=data_parser_worker, daemon=True).start()

    # 普通节点数据接收线程
    for node_id, sock, port in udp_node_sockets:
        threading.Thread(target=node_data_receiver, args=(sock, node_id, port), daemon=True).start()

    # 定位节点数据接收线程
    for sock in udp_locate_sockets:
        threading.Thread(target=locate_data_receiver, args=(sock,), daemon=True).start()

    # 心跳发送线程
    if ENABLE_HEARTBEAT:
        threading.Thread(target=heartbeat_sender, daemon=True).start()

    # WebSocket广播线程
    threading.Thread(target=websocket_broadcaster, daemon=True).start()


if __name__ == '__main__':
    # 加载配置
    load_configuration()

    # 初始化UDP套接字
    initialize_udp_sockets()

    # 初始化节点最后活动时间
    for node in last_active_time:
        last_active_time[node] = time.time()

    # 启动后台线程
    start_background_threads()

    # 启动Flask应用
    socketio.run(
        app,
        host='0.0.0.0',
        port=5001,
        debug=False,
        allow_unsafe_werkzeug=True
    )
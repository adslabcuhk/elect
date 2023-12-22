import os
import re
import sys
from collections import defaultdict

# 文件目录，假设所有文件都在这个目录下
file_dir = sys.argv[1]

# 使用 defaultdict 来存储每个时间戳的累加 CPU 负载
timestamp_to_cpu = defaultdict(float)

# 遍历目录中的每个文件
for filename in os.listdir(file_dir):
    filepath = os.path.join(file_dir, filename)
    
    # 跳过非文件类型
    if not os.path.isfile(filepath):
        continue
    
    with open(filepath, 'r') as f:
        # 跳过第一行
        f.readline()
        
        # 处理其余的行
        for line in f:
            # 使用冒号分割，以获取时间戳和 CPU 负载信息
            timestamp, cpu_info = line.split(": PID")
            
            # 使用正则表达式提取 CPU 负载（假设“%”前可能没有数字）
            match = re.search(r"CPU Load: ([\d.]+)?%", cpu_info)
            
            # 如果有匹配结果，并且匹配组中有数据，则提取 CPU 负载值
            if match and match.group(1):
                cpu_load = float(match.group(1))
            else:
                cpu_load = 0.0  # 如果没有数值，则默认为 0.0
            
            # 累加到对应的时间戳
            timestamp_to_cpu[timestamp.strip()] += cpu_load

# 找到第一个和最后一个非0的时间戳
first_non_zero = None
last_non_zero = None
for timestamp, cpu_load in timestamp_to_cpu.items():
    if cpu_load != 0.0:
        if first_non_zero is None:
            first_non_zero = timestamp
        last_non_zero = timestamp

output_flag = False
# 输出每个时间戳对应的累加 CPU 负载
for timestamp, total_cpu_load in timestamp_to_cpu.items():
    if timestamp == first_non_zero:
        output_flag = True
    if output_flag:
        # print(f"{timestamp}: Total CPU Load = {total_cpu_load}%")
        print(f"{total_cpu_load}")
    if timestamp == last_non_zero:
        break
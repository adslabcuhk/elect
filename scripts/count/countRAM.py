import os
import re
import numpy as np
from collections import OrderedDict
from scipy import stats
import sys

file_dir = sys.argv[1]

timestamp_to_memory = OrderedDict()

for filename in os.listdir(file_dir):
    filepath = os.path.join(file_dir, filename)
    
    if not os.path.isfile(filepath):
        continue
    
    with open(filepath, 'r') as f:
        f.readline()
        f.readline()
        for line in f:
            timestamp, memory_info = line.split(": Cassandra PID")
            
            match = re.search(r"is using (\d+) KiB", memory_info)
            
            if match:
                memory_usage = int(match.group(1))
            else:
                memory_usage = 0
            
            timestamp = timestamp.strip()
            if timestamp not in timestamp_to_memory:
                timestamp_to_memory[timestamp] = []
            timestamp_to_memory[timestamp].append(memory_usage)

all_values = [value for sublist in timestamp_to_memory.values() for value in sublist]
mean = np.mean(all_values)
std_dev = np.std(all_values)

confidence_level = 0.95
degrees_freedom = len(all_values) - 1
confidence_interval = stats.t.interval(confidence_level, degrees_freedom, mean, std_dev)

for timestamp, memory_usages in timestamp_to_memory.items():
    filtered_memory_usages = [x for x in memory_usages if confidence_interval[0] <= x <= confidence_interval[1]]
    if filtered_memory_usages:
        total_memory_usage = sum(filtered_memory_usages)
        print(f"{total_memory_usage}")

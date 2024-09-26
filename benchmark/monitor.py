import psutil
import time
import argparse
import os


def get_process_cpu_utilization(pid, file_name, interval=0.5):
    with open(file_name, 'w') as f:
        header = 'timestamp'
        for i in range(os.cpu_count()):
            header += f',core{i + 1}'
        header += '\n'
        f.write(header)
        while True:
            try:
                cpu_usage = psutil.cpu_percent(interval=interval, percpu=True)
                f.write(f'{int(time.time())},' + ','.join(map(str, cpu_usage)) + '\n')
                f.flush()
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                pass

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="")
    parser.add_argument('-p', help='process id of the main python script', type=int)
    parser.add_argument('-f', help='monitoring log file', type=str)
    args = parser.parse_args()
    pid = args.p
    file_name = args.f
    print('Monitoring started')
    print(f"script's process is {pid}")
    os.makedirs(os.path.dirname(file_name), exist_ok=True)
    get_process_cpu_utilization(pid, file_name)

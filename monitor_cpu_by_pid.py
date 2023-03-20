import subprocess
import time
import os
import re
import datetime


def acquire_process(command, owner):
    '''
    使用for循环，stop 所有符合条件的进程
    :param command:
    :param owner:
    :return:
    '''
    ps_result = subprocess.Popen("ps -ef | grep " + command, shell=True, stdout=subprocess.PIPE)
    ps_result_readable = ps_result.stdout.read().decode()
    # check owner & get pid & stop
    ps_result_list = ps_result_readable[:-1].split('\n')
    pids = list()
    for result in ps_result_list:
        run_process = result.split(' ')
        if "00:00:00" in str(run_process):
            continue
        if owner and owner not in str(run_process):
            continue
        process_pid = re.compile(owner + ' +(\d+) ').findall(string=str(result))[0]
        pids.append(process_pid)
    return pids


def cpu_monitor_by_process(command, pids, time_gap, cpu_monitor_folder):
    '''
    :param raw_command: shortest command, like "kvrocks"
    :param owner:
    :param cpu_save_path:
    :param monitor_target_file:
    :return:
    '''
    while True:
        for pid in pids:
            # only keep command related
            cpu_result = subprocess.Popen(f"top -n 1 -b -p {pid} | grep {command}", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            # with current server info
            # cpu_result = subprocess.Popen(f"top -n 1 -b -p {pid}", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

            cpu_str = str(int(datetime.datetime.now().timestamp())) + " " + cpu_result.stdout.read().decode()
            # print(cpu_str)
            cpu_monitor_output_path = cpu_monitor_folder + "cpu_" + str(pid) + ".log"
            with open(cpu_monitor_output_path, 'a') as f:
                f.write(cpu_str)
        time.sleep(int(time_gap))



if __name__ == '__main__':
    target_command = "tsdb"
    process_owner = "root"
    monitor_time_gap = 10

    monitor_log_save_folder = "./log/"
    if not os.path.exists(monitor_log_save_folder):
        os.makedirs(monitor_log_save_folder)

    pids = acquire_process(command=target_command, owner=process_owner)
    cpu_monitor_by_process(command=target_command, pids=pids, time_gap=monitor_time_gap, cpu_monitor_folder=monitor_log_save_folder)


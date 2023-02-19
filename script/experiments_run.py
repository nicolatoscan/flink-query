import os
import subprocess
import argparse
import time
import socket
import sys
import psutil
# from gevent import joinall

from experiments_prop import *

flink_home = "/root/flink-1.16.1" # os.environ['FLINK_LOCAL_HOME']
flink_exe = flink_home + "/bin/flink"
start_flink = flink_home + "/bin/start-cluster.sh"
stop_flink = flink_home + "/bin/stop-cluster.sh"
task_slots_flink = flink_home + "/bin/taskmanager.sh"

FNULL = open(os.devnull, 'w')

usage = "python3 <script_name.py> <jar_file>"
parser = argparse.ArgumentParser(description='to run flink experiments')
parser.add_argument("jar_name")
parser.add_argument("--Parallelism", type=int, default=1, choices=range(1, 10))

args = parser.parse_args()

def restart_flink():
    cmd = stop_flink
    print(cmd)
    process = subprocess.Popen(cmd.split(), stdout=FNULL, stderr=subprocess.STDOUT)
    output, error = process.communicate()

    time.sleep(2)

    cmd = start_flink
    print(cmd)
    process = subprocess.Popen(cmd.split(), stdout=FNULL, stderr=subprocess.STDOUT)
    output, error = process.communicate()

def add_task_slots(num=1):
    cmd = task_slots_flink + " start"
    for _ in range(num):
        process = subprocess.Popen(cmd.split(), stdout=FNULL, stderr=subprocess.STDOUT)
        output, error = process.communicate()

def kill_task_slots(num=1):
    cmd = task_slots_flink + " stop"
    for _ in range(num):
        process = subprocess.Popen(cmd.split(), stdout=FNULL, stderr=subprocess.STDOUT)
        output, error = process.communicate()

def kill_task_slots_all():
    cmd = task_slots_flink + " stop-all"
    process = subprocess.Popen(cmd.split(), stdout=FNULL, stderr=subprocess.STDOUT)
    output, error = process.communicate()

def kill_running_jobs():
    cmd = os.getcwd() + "/kill_running_jobs.sh"
    process = subprocess.Popen(cmd.split(), stdout=FNULL, stderr=subprocess.STDOUT)
    output, error = process.communicate()

def run_flink_job(isLocal, jar_path, parallel):
    restart_flink()

    flink = flink_exe
    if isLocal:
        flink = flink_exe
    flink_command = flink + " run -d " + jar_path
    if parallel > 1:
        flink_command += " -p " + str(parallel)
        add_task_slots(parallel)
    
    # flink_command += " -input /mnt/d/Knowledge_Base/flink-query/test.txt" 
    # + " -output " + "/mnt/d/Knowledge_Base/flink-query/output.csv"
    print("")
    print(flink_command)
    print("")
    print("  +++++ started running the job at: " + time.strftime("%H.%M.%S", time.localtime()))

    # time.sleep(30)

    process = subprocess.Popen(flink_command.split(), stdout=subprocess.PIPE)
    output, error = process.communicate()

def run_local(jar_name, job_alias, unique_exp_name, parallel):
    jar_path = jar_name

    run_flink_job(True, jar_path, parallel)

def main():
    job_alias = "wc"
    print("Starting the experiments")

    exp_start_time = time.strftime("%m.%d-%H.%M", time.localtime())
    unique_exp_name = job_alias + "-" + exp_start_time
    parallel = args.Parallelism

    kill_task_slots_all()
    run_local(args.jar_name, job_alias, unique_exp_name, parallel)

    # kill_task_slots(1)

if __name__ == "__main__":
    main()
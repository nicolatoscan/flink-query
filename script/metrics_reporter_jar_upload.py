import os
import subprocess
import time
from pssh.clients.native import ParallelSSHClient
from gevent import joinall

from experiments_prop import *

flink_home = "/mnt/e/DownloadFromBrowser/flink-1.16.0" # os.environ['FLINK_HOME']
flink_lib = flink_home + "/lib"

FNULL = open(os.devnull, 'w')

def package_metrics_reporter_jar():
    retval = os.getcwd()
    project_path = retval + "/metrics/src/main/java/com/flink"
    # project_path = "/mnt/d/Knowledge_Base/flink-query/src/main/java/com/toscan/metric"
    os.chdir(project_path)

    cmd = "mvn -DskipTests clean package"
    process = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE)
    output, error = process.communicate()
    os.chdir(retval)

    print("jar file packaged")

def get_metrics_reporter_jar():
    cmd = "cp /mnt/d/Knowledge_Base/flink-query/metrics/src/main/java/com/flink/target/original-flink-metrics-reporter-1.0-SNAPSHOT.jar flink-metrics-my.jar"
    process = subprocess.Popen(cmd.split(), stdout=FNULL, stderr=subprocess.STDOUT)
    output, error = process.communicate()

def upload_jar():
    cmd = "cp flink-metrics-my.jar " + flink_lib
    process = subprocess.Popen(cmd.split(), stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    time.sleep(1)
    output, error = process.communicate()
    process.stdin.close()

    # cmds = client.scp_send("flink-metrics-my.jar", "/usr/local/etc/flink-remote/lib/flink-metrics-my.jar")
    # joinall(cmds, raise_error=True)

def main():
    package_metrics_reporter_jar()
    get_metrics_reporter_jar()
    upload_jar()

if __name__ == "__main__":
    main()
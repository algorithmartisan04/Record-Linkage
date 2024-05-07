# pylint: disable=E0401
import paramiko # Since this is only installed in apptainer, this import gives me an error when working

import subprocess as sp
import time

# NOTE:
#
# * You must be currently in the master node when starting this program
# * This code will be executed insde apptainer with the following command
"""
apptainer shell --bind /gscratch:/gscratch,/mmfs1/home/seunguk/spark/work:/spark/spark-3.4.0-bin-hadoop3/work,/mmfs1/home/seunguk/spark/conf:/spark/spark-3.4.0-bin-hadoop3/conf /gscratch/stf/seunguk/spark.sif
apptainer shell --bind /mmfs1/home/seunguk/spark/work:/spark/spark-3.4.0-bin-hadoop3/work,/mmfs1/home/seunguk/spark/conf:/spark/spark-3.4.0-bin-hadoop3/conf /mmfs1/home/seunguk/apptainer/def_sif_files/python.sif

"""
# * First node in nodes.txt is the master node

# Paths
SPARK_HOME = "/spark/spark-3.4.0-bin-hadoop3"

# TODO: Change this back when done testing
JOB_FILE = "/mmfs1/home/seunguk/int_jobs/spark.py"


# Spark util paths
KNOWN_HOST_DIR = "/mmfs1/home/seunguk/.ssh/known_hosts"
SPARK_WORK_DIR = "/mmfs1/home/seunguk/spark/work"
SPARK_LOG_DIR = "/mmfs1/home/seunguk/spark/logs"
SPARK_CONF_DIR = "/mmfs1/home/seunguk/spark/conf"
APPTAINER_DIR = "/gscratch/stf/seunguk/spark.sif"

CORE_COUNT = 1
CORE_MEMORY = "50g"
PORT_NUMBER = 7077

# Get the allocated nodes in nodes.tx
nodes = []
with open ("nodes.txt", "r") as f:
    for line in f:
        nodes.append(line.strip())
print(nodes)


# Update submit.sh file to correctly define the master node
spark_submit_command = f"{SPARK_HOME}/bin/spark-submit {JOB_FILE} --master spark://{nodes[0]}:7077"
with open("/mmfs1/home/seunguk/int_jobs/submit.sh", "w") as f:
    f.write(spark_submit_command)


# Update the spark-env.sh file to correctly define the config for the Spark Cluster
with open("/mmfs1/home/seunguk/spark/conf/spark-env.sh", "w") as f:
    f.write("#!/usr/bin/env bash\n")
    f.write("export SPARK_MASTER_HOST=" + nodes[0] + "\n")
    f.write("export SPARK_WORKER_CORES=" + str(CORE_COUNT) + "\n")
    f.write("export SPARK_WORKER_MEMORY=" + CORE_MEMORY + "\n")
    f.write("export SPARK_CONF_DIR=" + SPARK_CONF_DIR + "\n")
    f.write("export SPARK_LOG_DIR=" + SPARK_LOG_DIR + "\n")
    # f.write("export SPARK_WORK_DIR=" + SPARK_WORK_DIR + "\n")
    f.write("export SPARK_MASTER_PORT=" + str(PORT_NUMBER) + "\n")
    # f.write("export SPARK_HOME=/gscratch/stf/seunguk/spark-3.5.0-bin-hadoop3" + "\n") # TODO: Spark installation outside of apptainer

# Starts the Spark master on the master node
start_master_command = "/spark/spark-3.4.0-bin-hadoop3/sbin/start-master.sh; sleep 2"
result = sp.run(start_master_command, shell=True, capture_output=True, text=True)
print("Master STDOUT: ", result.stdout)
print("Master STDERR: ", result.stderr)
print("\n")


# Connect to each worker node and start Spark worker in each of them
for node in nodes[1:-1]:
    print(f"worker {node}")
    worker = paramiko.SSHClient()
    worker.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    worker.load_system_host_keys(KNOWN_HOST_DIR)
    worker.connect(node)
    start_worker_command = f"apptainer exec --bind /gscratch:/gscratch,{SPARK_WORK_DIR}:/spark/spark-3.4.0-bin-hadoop3/work,{SPARK_CONF_DIR}:/spark/spark-3.4.0-bin-hadoop3/conf {APPTAINER_DIR} bash -c \"/spark/spark-3.4.0-bin-hadoop3/sbin/start-worker.sh spark://{nodes[0]}:7077; sleep 2 \""

    stdin, stdout, stderr = worker.exec_command(start_worker_command)
    stdin.close()

    output = stdout.read().decode()
    error = stderr.read().decode()
    worker.close()
    del worker, stdin, stdout, stderr
    print("Worker STDOUT: ", output)
    print("Worker STDERR: ", error)
    print("\n")


# Connect to the client to submit the work
client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.load_system_host_keys(KNOWN_HOST_DIR)
client.connect(nodes[-1])

client_command = f"apptainer exec --bind /gscratch:/gscratch,{SPARK_WORK_DIR}:/spark/spark-3.4.0-bin-hadoop3/work,{SPARK_CONF_DIR}:/spark/spark-3.4.0-bin-hadoop3/conf {APPTAINER_DIR} bash /mmfs1/home/seunguk/int_jobs/submit.sh; sleep 2"
stdin_, stdout_, stderr_ = client.exec_command(client_command)

# Prints at least a part of the output without hanging

# What I think is happening:
# I am using paramikoto to ssh into different spark nodes and waiting for the
# response. Normally what should happen is that paramiko reads all stdout and
# stderr to buffer and flushes them when the stream ends. However, I have found
# many online posts discussing a long unresolved issue where if the stdout
# stream is too long to fit inside the preallocated buffer size allocated by
# paramiko, the entire read process freezes and cannot continue to stream
# stderr and basically crashes. In my case, the Spark log outputs written to
# stdout looks to be too long and so I am waiting a minute and just ending the
# channel to continue with the process.

# IMPORTANT: interactive.py program will finish early since the timeout will be
# faster than the actual program. As long as the cluster is active, the cluster
# will run the comparison and print out the results after some time.
print("started\n")

client.close()



# Change this value depending on how many runs are in spark.py
test_case_count = 1

original_file_line = None

# Reads how many lines the original output file has
with open("/mmfs1/home/seunguk/int_jobs/spark.txt", "r") as f:
    original_file_line = sum(1 for line in f)
    print(f"Original_file_line: {original_file_line}\n\n")

# Checks how many lines the current output file has
def check_file():
    with open("/mmfs1/home/seunguk/int_jobs/spark.txt", "r") as file:
        line_count = sum(1 for line in file)
        print(f"Current_file_line: {line_count}\n")

        return line_count == original_file_line + test_case_count

while not check_file():
    time.sleep(10)

print("done")
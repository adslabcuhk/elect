# Instructions to reproduce the evaluations of the paper

Here are the detailed instructions to perform the same experiments in our paper.

## Artifact claims

We claim that the resultant numbers might differ from those in our paper due to various factors (e.g., cluster sizes, machines, OS, software packages, etc.). Nevertheless, we expect ELECT to still achieve similar performance (in normal operations) with Cassandra while significantly reducing storage overhead (i.e., our main results). In addition, to reduce the influence of cloud storage location, hardware requirement, complexity, and running time of the evaluation, we made some changes to the evaluation configurations.

* We require a single client node, six server nodes, and one storage node (as the cold tier) in AE.
* We replaced the Alibaba OSS with a server node within the same cluster as the cold tier to store the cold data.

## Testbed access

We provide two testbeds for the evaluation with a properly setup environment. Since the repo will be made public, in order to ensure data security, we will provide the connection key and specific connection method of the two on the HotCRP website. Both testbeds contain 8 machines (6 ELECT server nodes, 1 object storage server node, and 1 client node). **When using both testbeds, please skip the next section, "Environment setup."**

## Environment setup (5 human-minutes + ~ 40 compute-minutes)

We provide scripts to set up the environment for the evaluation. The scripts are tested on Ubuntu 22.04 LTS. Note that the running time of the scripts depends on the node number, network bandwidth, and the performance of the cluster nodes.

**Step 1:** Set up and check the user account and sudo password on each node. We assume all the nodes have the same username and password. We use the user name and password to set up the running environment automatically. In addition, please also check whether each node has the same network interface name (for the given IP address). If not, please modify the `networkInterface` variable in `scripts/settings.sh` on each node.

**Step 2:** Set up the cluster node info in `scripts/settings.sh` on each node. Please fill in the following variables in the script. Note that we assume all the nodes have the same configurations (e.g., same user name, same path to the artifact folder, same network interface name, etc.).

```shell
NodesList=(10.31.0.185 10.31.0.181 10.31.0.182 10.31.0.184 10.31.0.188 10.31.0.180) # The IP addresses of the ELECT cluster nodes
OSSServerNode="10.31.0.190" # The IP address of the OSS server node
OSSServerPort=8000 # The port number of the OSS server node
ClientNode="10.31.0.187" # The IP address of the client node (it can be the local node running the scripts)
UserName="cc" # The user name of all the previous nodes
sudoPasswd="" # The sudo password of all the previous nodes; we use this to automatically install the required packages; we assume all the nodes have the same user name. For the Chameleon cloud, please keep this as empty.
PathToArtifact="/home/${UserName}/ELECT" # The path to the artifact folder; we assume all the nodes have the same path.
PathToELECTExpDBBackup="/home/${UserName}/ELECTExpDBBackup" # The path to the backup folder for storing the loaded DB content; we assume all the nodes have the same path.
PathToELECTLog="/home/${UserName}/ELECTLog" # The path to the log folder for storing the experiment logs; we assume all the nodes have the same path.
PathToELECTResultSummary="/home/${UserName}/ELECTResules" # The path to the result summary folder for storing the final experiment results; we assume all the nodes have the same path. 
```

**Step 3:** Run the following script on one of the nodes (we suggest running on the client node). This script will install the required packages, set up the environment variables, and set up the SSH connection between the nodes.

```shell
bash scripts/setup.sh full
```

## Evaluations

This section describes how to reproduce the evaluations in our paper. The total running time to reproduce all evaluation results is about 7~8 days.

To simplify the reproduction process, we provide an `Ansible`-based script to run all the experiments. The script will automatically run the experiments and generate the result logs. The scripts will take about 9~10 days to finish all the experiments. We suggest **running the scripts of Exp#2 first**, which can reproduce the main results (i.e., achieve controllable storage saving compared with Cassandra; provide similar performance of different types of KV operations such as read, write, scan, and update) of our paper.

### Note on the evaluation results

The summarized result of each evaluation will be stored in the `scripts/exp/` folder and named as `${ExpName}.log`. 

For the performance evaluation, the result will be summarized in different ways according to the running round number of the experiment (defined in each of the experiment scripts by the variable `RunningRoundNumber`). 

* If the running round number is 1, the result will be directly output as in the example shown below.

```shell
[Exp info] scheme: elect, workload: Read, KVNumber: 10000000, KeySize: 24, ValueSize: 1000, ClientNumber: 16, read consistency level: ONE
Throughput (unit: op/s): 
Only one round: 9992.38
[READ] Average operation latency (unit: us):
Only one round: 1369.53
[READ] 99th percentile latency (unit: us):
Only one round: 1883.00
```

* If the running round number is between 1 to 5, the result will be output with the average, maximum, and minimum as the example shown below.

```shell
[Exp info] scheme: elect, workload: Read, KVNumber: 10000000, KeySize: 24, ValueSize: 1000, ClientNumber: 16, read consistency level: ONE
Throughput (unit: op/s): 
Average: 9992.38, Min:  9753.433208489389, Max:  10231.330379889298
[READ] Average operation latency (unit: us):
Average: 1369.53, Min: 1355.774261, Max: 1383.292994
[READ] 99th percentile latency (unit: us):
Average: 1883.00, Min: 1823, Max: 1943
```

* If the running round number is more than or equal to 5, the result will be output with the average and 95% student-t distribution confidence interval, as shown in the example below.

```shell
[Exp info] scheme: elect, workload: Read, KVNumber: 1000000, KeySize: 24, ValueSize: 1000, ClientNumber: 16, read consistency level: ONE
Throughput (unit: op/s): 
Average: 31219.24; The 95% confidence interval: (30463.56, 31974.92)
[READ] Average operation latency (unit: us):
Average: 465.35; The 95% confidence interval: (452.85, 477.85)
[READ] 99th percentile latency (unit: us):
Average: 1623.00; The 95% confidence interval: (1553.32, 1692.68)
```

For the storage overhead evaluation, the result will be summarized based on the total, hot-tier, and cold-tier storage overhead. For example:

```shell
[Exp info] scheme: elect, workload: Read, KVNumber: 100000000, KeySize: 24, ValueSize: 1000
Total storage overhead (unit: GB): 185.9
Hot-tier storage overhead (unit: GB): 134.1
Cold-tier storage overhead (unit: GB): 51.8
```

### Overall system analysis (Exp#1~5 in our paper)

#### Exp#1: Performance with YCSB core workloads (1 human minutes + ~ 20 compute-hours)

* Running:

```shell
bash scripts/exp/Exp1-ycsb.sh
```

#### Exp#2: Micro-benchmarks on KV operations (1 human-minutes + ~ 5 compute-hours)

* Running:

```shell
bash scripts/exp/Exp2-operations.sh
```

#### Exp#3: Performance breakdown (1 human-minutes + ~ 5 compute-hours)

* Running:

```shell
bash scripts/exp/Exp3-breakdown.sh
```

#### Exp#4: Full-node recovery (1 human-minutes + ~ 8 compute-hours)

* Running:

```shell
bash scripts/exp/Exp4-recovery.sh
```

#### Exp#5: Resource usage (1 human-minutes + ~ 5 compute-hours)

* Running:

```shell
bash scripts/exp/Exp5-resource.sh
```

### Parameter analysis (Exp#6~8 in our paper)

#### Exp#6: Impact of key and value sizes (1 human-minutes + ~ 40 compute-hours)

* Running:

```shell
bash scripts/exp/Exp6-kvSize.sh
```

#### Exp#7: Impact of storage-saving target (1 human-minutes + ~ 45 compute-hours)

* Running:

```shell
bash scripts/exp/Exp7-balanceParam.sh
```

#### Exp#8: Impact of erasure coding parameters (1 human-minutes + ~ 12 compute-hours)

The original experiment requires at least 12 nodes (1 client node, 10 server nodes, and 1 storage node). For the provided testbeds, limited by the number of available nodes, we adapt the changing range of erasure code K from 4~8 to 2~4. This result is only used to verify ELECT's adaptability to different K values.

* Running:

```shell
bash scripts/exp/Exp8-ecParam.sh
```

### System setting analysis (Exp#9,10 in our paper)

#### Exp#9: Impact of read consistency level (1 human-minutes + ~ 5 compute-hours)

* Running:

```shell
bash scripts/exp/Exp9-consistency.sh
```

#### Exp#10: Impact of number of clients (1 human minutes + ~ 5 compute-hours)

* Running:

```shell
bash scripts/exp/Exp10-clients.sh
```

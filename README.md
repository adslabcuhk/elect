# ELECT

## Introduction

ELECT is a distributed tiered KV store that enables replication and erasure coding tiering. This repo contains the implementation of the ELECT prototype, YCSB benchmark tool, and evaluation scripts used in our USENIX FAST 2024 paper.

* `src/`: includes the implementation of the ELECT prototype and a simple object storage backend that can be deployed within a local cluster.
* `scripts/`: includes the setup and evaluation scripts and our modified version of YCSB, which supports user-defined key and value sizes.

## Artifact Evaluation Instructions

Please refer to the [AE_INSTRUCTION.md](AE_INSTRUCTION.md) for details. We provide two testbeds with a settled system environment to run our evaluation scripts. Note that to ensure security, we will provide the connection key and specific connection method of the two testbeds on the HotCRP website.

## Prerequisites

### Testbed

As a distributed KV store, ELECT requires a cluster of machines to run. With the default erasure coding parameters (i.e., [n,k]==[6,4]), ELECT requires a minimum of 6 machines as the storage nodes. In addition, to avoid unstable access to Alibaba OSS, we use a server node within the same cluster as the cold tier to store the cold data. We also need a client node to run the YCSB benchmark tool. Therefore, we need at least 8 machines to run the prototype. 

### Dependencies

* For Java project build: openjdk-11-jdk, openjdk-11-jre, ant, ant-optional Maven.
* For erasure-coding library build: clang, llvm, libisal-dev.
* For scripts: python3, ansible, python3-pip, cassandra-driver, bc, numpy, scipy.

The packages above can be directly installed via `apt-get` and `pip` package managers:

```shell 
sudo apt-get install -y openjdk-11-jdk openjdk-11-jre ant ant-optional maven clang llvm libisal-dev python3 ansible python3-pip bc
pip install cassandra-driver numpy scipy
```

Note that the dependencies for both ELECT and YCSB will be automatically installed via Maven during compilation.

## Build

### Environment setup (5 human minutes)

The build procedure of both the ELECT prototype and YCSB requires an internet connection to download the dependencies via Maven. In case the internet connection requires a proxy, we provide an example maven setting file `./scripts/env/settings.xml`. Please modify the file according to your proxy settings and then put it into the local Maven directory, as shown below.

```shell
mkdir -p ~/.m2
cp ./scripts/env/settings.xml ~/.m2/
```

### Step 1: ELECT prototype (5 human-minutes + ~ 40 compute-minutes)

Since the prototype utilizes the Intel Isa-L library to achieve high-performance erasure coding, we need to build the EC library first:

```shell
# Set Java Home for isa-l library 
export JAVA_HOME=/usr/lib/jvm/java-1.11.0-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
# Build the JNI-based erasure coding library
cd src/elect
cd src/native/src/org/apache/cassandra/io/erasurecode/
chmod +x genlib.sh 
./genlib.sh
cd ../../../../../../../../
cp src/native/src/org/apache/cassandra/io/erasurecode/libec.so lib/sigar-bin
```

Then, we can build the ELECT prototype:

```shell
# Build with java 11
cd src/elect
mkdir build lib
ant realclean
ant -Duse.jdk11=true
```

### Step 2: The object storage backend (1 human-minutes + ~ 1 compute-minutes)

To avoid unstable access to Alibaba OSS (if cross-country), we use a server node within the same cluster as the cold tier to store the cold data. We use the `OSSServer` tool to provide the file-based storage service. The `OSSServer` tool is a simple object storage server that supports the following operations: `read` and `write`. We provide the source code of the `OSSServer` tool in `src/coldTier/`. To build the `OSSServer` tool, please run the following command:

```shell
cd src/coldTier
make clean 
make
```

### Step 3: YCSB benchmark tool (2 human-minutes + ~ 30 compute-minutes)

We build the modified version of YCSB, which supports user-defined key and value sizes. The build procedure is similar to the original YCSB.

```shell
cd scripts/ycsb
mvn clean package
```

## Configuration

### Cluster setup (~20 human-minutes)

SSH key-free access is required between all nodes in the ELECT cluster.

**Step 1:** Generate the SSH key pair on each node.

```shell 
ssh-keygen -q -t rsa -b 2048 -N "" -f ~/.ssh/id_rsa
```

**Step 2:** Create an SSH configuration file. You can run the following command with the specific node IP, Port, and User to generate the configuration file. Note that you can run the command multiple times to add all the nodes to the configuration file.

```shell
# Replace xxx with the correct IP, Port, and User information, and replace ${i} to the correct node ID.
cat <<EOT >> ~/.ssh/config
Host node${i}
    StrictHostKeyChecking no
    HostName xxx.xxx.xxx.xxx
    Port xx
    User xxx
EOT
```

**Step 3:** Copy the SSH public key to all the nodes in the cluster.

```shell
ssh-copy-id node${i}
```

### Configuring ELECT (~20 human-minutes)

The ELECT prototype requires to configure the cluster information before running. We provide an example configuration file `src/elect/conf/elect.yaml`. Please modify the file according to your cluster settings and the instructions shown below (lines 11-34).

```shell
cluster_name: 'ELECT Cluster'

# ELECT settings
ec_data_nodes: 4 # The erasure coding parameter (k)
parity_nodes: 2 # The erasure coding parameter (n - k)
target_storage_saving: 0.6 # The balance parameter (storage saving target) controls the approximate storage saving ratio of the cold tier.
enable_migration: true # Enable the migration module to migrate cold data to the cold tier.
enable_erasure_coding: true # Enable redundancy transitioning module to encode the cold data.
# Manual settings to achieve a balanced workload across different nodes.
initial_token: -9223372036854775808 # The initial token of the current node.
token_ranges: -9223372036854775808,-6148914691236517376,-3074457345618258944,0,3074457345618257920,6148914691236515840 # The initial tokens of all nodes in the cluster.
# Current node settings
listen_address: 192.168.10.21 # IP address of the current node.
rpc_address: 192.168.10.21 # IP address of the current node.
cold_tier_ip: 192.168.10.21 # The IP address of the object storage server (cold tier).
cold_tier_port: 8080 # The port of the object storage server (cold tier).
seed_provider:
  - class_name: org.apache.cassandra.locator.SimpleSeedProvider
    parameters:
      # ELECT: Put all the server nodes' IPs here. Make sure these IPs are sorted from small to large. Example: "<ip1>,<ip2>,<ip3>"
      - seeds: "192.168.10.21,192.168.10.22,192.168.10.23,192.168.10.25,192.168.10.26,192.168.10.28"
```

**Note that you can configure the prototype to run the raw Cassandra by setting `enable_migration` and `enable_erasure_coding` to `false`.**

To simplify the configuration of `initial_token` and `token_ranges`, which is important for ELECT to achieve optimal storage saving. We provide a script `./scripts/genToken.sh` to generate the token ranges for all the nodes in the cluster with the given node number. 

```shell
cd ./scripts
python3 genToken.py ${n} # Replace ${n} to the number of nodes in the cluster.
# Sample output:
[Node 1]
initial_token: -9223372036854775808
[Node 2]
initial_token: -6148914691236517376
[Node 3]
initial_token: -3074457345618258944
[Node 4]
initial_token: 0
[Node 5]
initial_token: 3074457345618257920
[Node 6]
initial_token: 6148914691236515840
```

After getting the initial token for each node, please fill the generated number into the `initial_token` and `token_ranges` fields in the configuration file.

## Running 

To test the ELECT prototype, we need to run the following steps:

1. Run the object storage server as the cold tier.
2. Run the ELECT cluster.
3. Run the YCSB benchmark.

We describe the detailed steps below.

### Run the object storage server as the cold tier (~1 human minutes + ~1 compute-minutes)

```shell
cd src/coldTier
mkdir data # Create the data directories.
nohup java OSSServer ${port} >coldStorage.log 2>&1 & # ${port} should be replaced with the port number of the object storage server.
```

Note that the port of OSSServer is the same as the `cold_tier_port` in the ELECT configuration file.

If the object storage server is running correctly, you can see the following output in the log file (`coldStorage.log`):

```shell
Server started on port: 8080
```

### Run the ELECT cluster (~5 human-minutes + ~3 compute-minutes)

After the `OSSServer` is running correctly and configuring the cluster information in the `elect.yaml` file on each of the server nodes, we can run the ELECT cluster with the following command (on each server node):

```shell
cd src/elect
rm -rf data logs # Clean up the data and log directories.
# Create the data directories.
mkdir -p data/receivedParityHashes/
mkdir -p data/localParityHashes/
mkdir -p data/ECMetadata/
mkdir -p data/tmp/
mkdir -p logs
# Run the cluster 
nohup bin/elect >logs/debug.log 2>&1 &
```

```shell
DEBUG [OptionalTasks:1] 2023-12-13 18:45:05,396 CassandraDaemon.java:404 - Completed submission of build tasks for any materialized views defined at startup
DEBUG [ScheduledTasks:1] 2023-12-13 18:45:25,030 MigrationCoordinator.java:264 - Pulling unreceived schema versions...
```

It will take about 1~2 minutes to fully set up the cluster. You can check the cluster status via the following command:

```shell
cd src/elect
bin/nodetool ring
```

Once the cluster is ready, you can see the information of all nodes in the cluster on each of the server nodes. Note that each node in the cluster should own the same percentage of the consistent hashing ring. For example:

```shell
Datacenter: datacenter1
==========
Address             Rack        Status State   Load            Owns                Token
192.168.10.21       rack1       Up     Normal  75.76 KiB       33.33%              -9223372036854775808
192.168.10.22       rack1       Up     Normal  97 KiB          33.33%              -6148914691236517376
192.168.10.23       rack1       Up     Normal  70.53 KiB       33.33%              -3074457345618258944
192.168.10.25       rack1       Up     Normal  93.88 KiB       33.33%              0
192.168.10.26       rack1       Up     Normal  96.4 KiB        33.33%              3074457345618257920
192.168.10.28       rack1       Up     Normal  75.75 KiB       33.33%              6148914691236515840
```


### Run YCSB benchmark (~1 human-minutes + ~70 compute-minutes)

After the ELECT cluster is set up, we can run the YCSB benchmark tool on the client node to evaluate the performance of ELECT. We show the steps as follows:

**Step 1:** Create the keyspace and set the replication factor for the YCSB benchmark.

```shell
cd src/elect
bin/cqlsh ${coordinator} -e "create keyspace ycsb WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 3 };
        USE ycsb;
        create table usertable0 (y_id varchar primary key, field0 varchar);
        ALTER TABLE usertable0 WITH compaction = { 'class': 'LeveledCompactionStrategy', 'sstable_size_in_mb': $sstable_size, 'fanout_size': $fanout_size};
        ALTER TABLE usertable1 WITH compaction = { 'class': 'LeveledCompactionStrategy', 'sstable_size_in_mb': $sstable_size, 'fanout_size': $fanout_size};
        ALTER TABLE usertable2 WITH compaction = { 'class': 'LeveledCompactionStrategy', 'sstable_size_in_mb': $sstable_size, 'fanout_size': $fanout_size};
        ALTER TABLE ycsb.usertable0 WITH compression = {'enabled':'false'};
        ALTER TABLE ycsb.usertable1 WITH compression = {'enabled':'false'};
        ALTER TABLE ycsb.usertable2 WITH compression = {'enabled':'false'};
        consistency all;"
# Parameters:
# ${coordinator}: The IP address of any ELECT server node.
# ${sstable_size}: The maximum SSTable size. Default 4.
# ${fanout_size}: The size ratio between adjacent levels. Default 10.
```

**Step 2:** Load data into the ELECT cluster.

```shell
cd scripts/ycsb
bin/ycsb.sh load cassandra-cql -p hosts=${NodesList} -p cassandra.keyspace=${keyspace} -p cassandra.tracing="false" -threads ${threads} -s -P workloads/${workload}
# The parameters:
# ${NodesList}: the list of server nodes in the cluster. E.g., 192.168.0.1,192.168.0.2,192.168.0.3
# ${keyspace}: the keyspace name of the YCSB benchmark. E.g., ycsb for ELECT and ycsbraw for raw Cassandra.
# ${threads}: the number of threads (number of simulated clients) of the YCSB benchmark. E.g., 1, 2, 4, 8, 16, 32, 64
# ${workload}: the workload file of the YCSB benchmark. E.g., workloads/workloada, workloads/workloadb, workloads/workloadc
```

**Step 3:** Run benchmark with specific workloads.

```shell
cd scripts/ycsb
bin/ycsb.sh run cassandra-cql -p hosts=${NodesList} -p cassandra.readconsistencylevel=${consistency} -p cassandra.keyspace=${keyspace} -p cassandra.tracing="false" -threads $threads -s -P workloads/${workload}
# The parameters:
# ${NodesList}: the list of server nodes in the cluster. E.g., 192.168.0.1,192.168.0.2,192.168.0.3
# ${consistency}: the read consistency level of the YCSB benchmark. E.g., ONE, TWO, ALL
# ${keyspace}: the keyspace name of the YCSB benchmark. E.g., ycsb for ELECT and ycsbraw for raw Cassandra.
# ${threads}: the number of threads (number of simulated clients) of the YCSB benchmark. E.g., 1, 2, 4, 8, 16, 32, 64
# ${workload}: the workload file of the YCSB benchmark. E.g., workloads/workloada, workloads/workloadb, workloads/workloadc
```

## Common problems

### The ELECT cluster cannot be booted correctly

Please check the system clock of all the nodes in the cluster. The system clock of all the nodes should be synchronized. You can use the following command to synchronize the system clock of all the nodes in the cluster.

```shell
sudo date 122015002023 # set the date to  2023-12-20-15:00
```

### The read operation of ELECT cannot work correctly

Please check the `seeds:` field in the configuration file. The current version of ELECT requires the seed nodes' IP in the configuration file to be sorted from small to large. For example:


```shell
# Wrong version:
- seeds: "192.168.10.28,192.168.10.21,192.168.10.25"
# Correct version:
- seeds: "192.168.10.21,192.168.10.25,192.168.10.28"
```

# Geode Redis Module

## Contents
1. [Overview](#overview)
2. [Performance Test Scripts](#performance-test)

## <a name="overview"></a>Overview

The [Geode Redis Module](https://geode.apache.org/docs/guide/12/tools_modules/redis_adapter.html) allows 
Geode to function as a drop-in replacement for a Redis data store, letting Redis applications 
take advantage of Geode’s scaling capabilities without changing their client code. Redis clients 
connect to a Geode server in the same way they connect to a Redis server, using an IP address and a 
port number.

## <a name="performance-test"></a>Performance Test Scripts

###The `benchmark.sh` Script
To run the performance tests, use the `benchmark.sh` script located in the 
`geode-redis/src/performanceTest` directory.  This script uses the `redis-benchmark` command to
measure performance of various Redis commands in requests per second. It runs the commands in two
different ways. First it runs several clients in parallel, all testing the same command. Then it
runs all the commands together in parallel, crudely simulating a variety of simultaneous operations.

Optional command line arguments for `benchmark.sh`:
- `-h` indicates the host to connect to (default: `localhost`)
- `-p` indicates the port to connect to (default: `6379`)
- `-t` indicates the number of times the `redis-benchmark` command will run (default: `10`)
- `-c` indicates the number of times the individual commands will run (default: `100000`)
- `-f` indicates an optional prefix for the summary file name

The script will output a CSV file called `[optional prefix_]benchmark_summary.csv` which can be
easily loaded into any spreadsheet program for analysis.

Sample output:
```csv
Command,Fastest Response Time (Msec),95th-99th Percentile (Msec),Slowest Response Time (Msec),Avg Requests Per Second
SET,1,8,43,13329
GET,1,4,15,17842
INCR,1,5,29,16069
MSET,1,7,732,11183
Parallel-SET,1,4,20,18460
Parallel-GET,1,4,21,18291
Parallel-INCR,1,4,20,18271
Parallel-MSET,1,4,21,17771
```

###Benchmark Helper Scripts
The `benchmark.sh` script uses several helper scripts. The `execute-operation.sh` script runs a
particular instance of `redis-benchmark`. The `summarize-operation-results.sh` script processes the
output of `execute-operation.sh`, and the `summarize-batch-results.sh` script processes the output
of multiple runs of `summarize-operation-results.sh`.

###The `environment-setup.sh` Script
The `environment-setup.sh` is optional. It can start a local Geode Redis Adapter, or confirm that a
local Redis server is running, then call `benchmark.sh`.

Mandatory command line arguments for `environment-setup.sh`:
- either `-g` or `-r`

`-g` will:
- Start a local Geode Redis Server for you
- Shut the server down once the benchmark finishes

`-r` will:
- Connect to a Redis server that is already running

Optional command line arguments for `environment-setup.sh`:
- `-f` indicates an optional prefix for the summary file name that will be passed to `benchmark.sh`

###The `shacompare.sh` Script
The `shacompare.sh` script is used to compare performance between different commits. This script
takes in two different commit hashes, checks them out, uses `environment-setup.sh` script to start
up the Geode Redis server, and runs the benchmarks for each commit. It generates two summary CSV
files, labeled with the short commit hash, that can be compared for performance changes.

Mandatory command line arguments for `shacompare.sh`:
- `-b` is the commit hash for the first commit you would like to compare
- `-c` is the commit hash for the second commit you would like to compare
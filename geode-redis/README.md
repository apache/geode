# Geode Redis Module

## Contents
1. [Overview](#overview)
2. [Performance Test](#performance-test)

## <a name="overview"></a>Overview

The [Geode Redis Module](https://geode.apache.org/docs/guide/12/tools_modules/redis_adapter.html) allows 
Geode to function as a drop-in replacement for a Redis data store, letting Redis applications 
take advantage of Geode’s scaling capabilities without changing their client code. Redis clients 
connect to a Geode server in the same way they connect to a Redis server, using an IP address and a 
port number.

## <a name="performance-test"></a>Performance Test

To run the performance tests, use the `benchmark.sh` script located in the 
`geode-redis/src/performanceTest` directory.  This script uses the `redis-benchmark` command to
measure performance of various Redis commands in requests per second.

Mandatory command line arguments for `benchmark.sh`:
- either `-g` or `-r`

`-g` will:
- Start a local Geode Redis Server for you
- Shut the server down once the benchmark finishes

`-r` will:
- Connect to a Redis server that is already running

Optional command line arguments for `benchmark.sh`:
- `-h` indicates the host to connect to (default: `localhost`)
- `-p` indicates the port to connect to (default: `6379`)
- `-t` indicates the number of times the `redis-benchmark` command will run (default: `10`)
- `-c` indicates the number of times the individual commands will run (default: `100000`)

The script will output a CSV file called `[current git short SHA]-aggregate.csv` which can be easily
loaded into any spreadsheet program for analysis.

Sample output:
```csv
Command, Average Requests Per Second
SET, 78561.4
GET, 86181.4
INCR, 81021.9
LPUSH, 32934.9
RPUSH, 32095.3
LPOP, 11715.8
RPOP, 12054.4
SADD, 74603.7
SPOP, 2853.92
```

The `benchmark.sh` script calls the `aggregator.sh` script, which handles running the 
`redis-benchmark` command and aggregating the results after `benchmark.sh` has validated the
environment.  The aggregator script can be run on its own if you already have an environment set up.

Optional command line arguments for `aggregator.sh`:
- `-h` indicates the host to connect to (default: `localhost`)
- `-p` indicates the port to connect to (default: `6379`)
- `-t` indicates the number of times the `redis-benchmark` command will run (default: `10`)
- `-c` indicates the number of times the individual commands will run (default: `100000`)
- `-n` indicates the output file prefix you would like to use (default: current git short SHA)

The `shacompare.sh` script takes in two different commit hashes, checks them out, and calls the 
`benchmark.sh` script for each.  It generates two aggregate CSV files that can later be compared
for performance changes.

Mandatory command line arguments for `shacompare.sh`:
- `-b` is the commit hash for the first commit you would like to compare
- `-c` is the commit hash for the second commit you would like to compare
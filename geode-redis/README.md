# Geode Redis Module

## Contents
1. [Introduction](#introduction)
2. [How To Try It](#how-to-try-it)

## <a name="introduction"></a>Introduction

The Geode Redis APIs allow an application to send Redis commands to Geode. This will allow users to 
switch seamlessly between using native Redis or Geode as a data store/caching solution. 

The API listens for incoming Redis commands on a designated port and then translates the commands 
into Geode commands.  The current set of supported Redis commands is listed below. 

## <a name="how-to-try-it"></a>How To Try It

We’ll build the develop branch of Apache Geode and then connect the Redis-CLI to that instance.  
Once we have established that the Redis APIs are available, we’ll connect a Spring Session Data 
Redis application.

### Building Apache Geode
The Apache Geode source code can be found here

1. In a terminal, git clone the Geode repo:
    ```commandline
    $ git clone https://github.com/apache/geode.git
    ```

2. Change the working directory to the Geode directory you cloned
	```commandline
	$ cd geode
    ```

3. Build the Geode application without running the test (REQUIRES JAVA 8)
    ```commandline
    $ ./gradlew build -x test
   ```

4. Once the build has completed, navigate to the geode-assembly directory which contains the Apache 
    Geode Shell - also referred to as GFSH:
    ```commandline
    $ cd geode-assembly/build/install/apache-geode/bin
   ```

5. Once in that folder run the following command:
    ```commandline
   $ ./gfsh
   ```

You should now see GFSH starting up with a version of 1.14.x.-build.x

![screenshot of GFSH running in the terminal](gfsh.png)

### Starting a Geode Server with Redis Enabled
Using GFSH enter the following commands:

1. Start a locator. The locator tracks servers and server load. When a client requests a server 
connection, the locator directs the client to one of the least loaded servers. Learn more. 
   ```commandline
    gfsh> start locator
    ``` 

2. After the locator has started, start a server that will be able to handle incoming Redis commands. 

    For example:
    ```commandline
    gfsh> start server --name=redisServer1 --locators=localhost[10334] --server-port=0 --redis-port=6379 --redis-bind-address=127.0.0.1
    ```
    * --name: A name you create for your server.
    * --locators: This is the location of the locator you started in step 1. 
    * --server-port: The server port (what the Java client ultimately connects to) 
    * --redis-port: The port that your redis application will connect to
    * --redis-bind-address: This will be the address your client uses to connect. 


Your Geode instance should now be up and running (1 locator and 1 server) and ready to accept Redis 
commands.  

Keep this terminal open and running so that you can easily shutdown the Geode instance when you are 
done working locally.

To confirm that things are running correctly, in a separate terminal run:
  ```commandline
$ redis-cli
  ```
If working correctly you should now be in the redis-cli and see `127.0.0.1:6379>`.  If you run the 
`PING` command you should receive a response of `PONG`. 

#### Adding an additional Geode Redis server 
If you’re interested in testing Geode scalability, in GFSH run the start server command again BUT 
make sure you change the `--name=` and `--redis-port=` parameters. 

For example: 
   ```commandLine
   $ start server --name=redisServer2 --locators=localhost[10334] --server-port=0 --redis-port=6380 --redis-bind-address=127.0.0.1
   ```

#### Stopping the Geode Redis server
To shutdown the Geode instance you started, in the terminal with GFSH running type the following command

   ```commandLine
$ shutdown --include-locators=true
   ```
	
As this command will shut down the entire Geode instance/cluster, you will be prompted with the following choice: 

```commandline
As a lot of data in memory will be lost, including possibly events in queues, do you really want to shutdown the entire distributed system? (Y/n)
```

To confirm that everything shutdown correctly, if you try and execute a Redis command in the redis-cli you should see the following message:

```commandline
Could not connect to Redis at 127.0.0.1:6379: Connection refused 
not connected>
```

| Supported Commands     | Unsupported Commands (Implemented - not tested)  | Unimplemented Commands  |
|------------------------|---------------------------------------------------|--------------------------|
| Server commands:       | Server commands:                 | ACL  |
| AUTH                    |  DBSIZE                         | CAT  |
| PING (only returns PONG)|  ECHO                           | ACL DELUSER |
| QUIT                    |  TIME                           | ACL GENPASS |
| Key commands:           |  SHUTDOWN                       | ACL GETUSER |
| DEL                     |   Key commands:                 | ACL HELP |      
| EXISTS                  |   FLUSHALL (no ASYNC option)    | ACL LIST |      
| KEYS                    |   FLUSHDB  (no ASYNC option)    | ACL LOAD |      
| RENAME                  |   SCAN                          | ACL LOG  |      
| TYPE                    |   String commands:              | ACL SAVE |      
| String commands:        |   BITCOUNT                      | ACL SETUSER |   
| APPEND                  |   BITOP                         | ACL USERS |     
| GET                     |   BITPOS                        | ACL WHOAMI |    
| SET                     | DECR                            | BGREWRITEAOF |
| Hash commands:          | DECRBY                          | BGSAVE  |       
| HGETALL                 | GETBIT                          | BITFIELD  |
| HMSET                   | GETRANGE                        | BLPOP  |
| HSET                    | GETSET                          | BRPOP  |
| Set commands:           | INCR                            | BRPOPLPUSH  |
| SADD                    | INCRBY                          | BZPOPMAX  |
| SMEMBERS                | INCRBYFLOAT                     | BZPOPMIN  |
| SREM                    | MGET                            | CLIENT CACHING |
| Pub/sub commands:       | MSET                            | CLIENT GETNAME  |
| SUBSCRIBE               | MSETNX                          | CLIENT GETREDIR  |
| UNSUBSCRIBE             | PSETEX                          | CLIENT ID  |
| PSUBSCRIBE              | SETEX                           | CLIENT KILL  |
| PUBLISH                 | SETBIT                          | CLIENT LIST  |
| PUNSUBSCRIBE            | SETNX                           | CLIENT PAUSE  |
| Expiration commands:    | SETRANGE                        | CLIENT REPLY  |
| EXPIRE                  | STRLEN                          | CLIENT SETNAME  |
| EXPIREAT                | Hash commands:                  | CLIENT TRACKING  |
| PEXPIRE                 | HDEL                            | CLIENT UNBLOCK  |
| PEXPIREAT               | HEXISTS                         | CLUSTER ADDSLOTS  |
| PERSIST                 | HGET                            | CLUSTER BUMPEPOCH  |
| TTL                     | HINCRBY                         | CLUSTER COUNT-FAILURE-REPORTS  |
| PTTL                    | HINCRBYFLOAT                    | CLUSTER COUNTKEYSINSLOT |
|                         | HKEYS                           | CLUSTER DELSLOTS  |
|                         | HLEN                            | CLUSTER FAILOVER   |
|                         | HMGET                           | CLUSTER FLUSHSLOTS  |
|                         | HSCAN                           | CLUSTER FORGET  |
|                         | HSETNX                          | CLUSTER GETKEYSINSLOT  |
|                         | HVALS                           | CLUSTER INFO  |
|                         | Set commands:                   | CLUSTER KEYSLOT  |
|                         | SCARD                           | CLUSTER MEET  |
|                         | SDIFF                           | CLUSTER MYID  |
|                         | SDIFFSTORE                      | CLUSTER NODES  |
|                         | SISMEMBER                       | CLUSTER REPLICAS  |
|                         | SINTER                          | CLUSTER REPLICATE  |
|                         | SINTERSTORE                     | CLUSTER RESET  |
|                         | SMOVE                           | CLUSTER SAVECONFIG  |
|                         | SPOP                            | CLUSTER SET-CONFIG-EPOCH  |
|                         | SRANDMEMBER                     | CLUSTER SETSLOT  |
|                         | SUNION                          | CLUSTER SLAVES  |
|                         | SUNIONSTORE                     | CLUSTER SLOTS  |
|                         | SSCAN                           | COMMAND  |
|                         |                                 | COMMAND GETKEYS |
|                         |                                 | COMMAND INFO  |
|                         |                                 | CONFIG GET |
|                         |                                 | CONFIG RESETSTAT |
|                         |                                 | CONFIG REWRITE  |
|                         |                                 | DEBUG OBJECT  |
|                         |                                 | DEBUG SEGFAULT  |
|                         |                                 | DISCARD  |
|                         |                                 | DUMP  |
|                         |                                 | EVAL  |
|                         |                                 | EVALSHA  |
|                         |                                 | EXEC  |
|                         |                                 | GEOADD  |
|                         |                                 | GEODIST  |
|                         |                                 | GEOHASH  |
|                         |                                 | GEOPOS  |
|                         |                                 | GEORADIUS  |
|                         |                                 | GEORADIUSBYMEMBER  |
|                         |                                 | HELLO  |
|                         |                                 | HSTRLEN  |
|                         |                                 | INFO  |
|                         |                                 | LASTSAVE  |
|                         |                                 | LATENCY DOCTOR  |
|                         |                                 | LATENCY GRAPH  |
|                         |                                 | LATENCY HELP  |
|                         |                                 | LATENCY HISTORY  |
|                         |                                 | LATENCY LATEST  |
|                         |                                 | LATENCY RESET  |
|                         |                                 | LINDEX  |
|                         |                                 | LINSERT  |
|                         |                                 | LLEN  |
|                         |                                 | LOLWUT  |
|                         |                                 | LPOP  |
|                         |                                 | LPOS  |
|                         |                                 | LPUSH  |
|                         |                                 | LPUSHX  |
|                         |                                 | LRANGE  |
|                         |                                 | LREM  |
|                         |                                 | LSET  |
|                         |                                 | LTRIM  |
|                         |                                 | MEMORY DOCTOR  |
|                         |                                 | MEMORY HELP  |
|                         |                                 | MEMORY MALLOC-STATS  |
|                         |                                 | MEMORY PURGE  |
|                         |                                 | MEMORY STATS  |
|                         |                                 | MEMORY USAGE  |
|                         |                                 | MIGRATE  |
|                         |                                 | MODULE LIST  |
|                         |                                 | MODULE LOAD  |
|                         |                                 | MODULE UNLOAD  |
|                         |                                 | MONITOR  |
|                         |                                 | MOVE  |
|                         |                                 | MULTI  |
|                         |                                 | OBJECT  |
|                         |                                 | PFADD  |
|                         |                                 | PFCOUNT  |
|                         |                                 | PFMERGE  |
|                         |                                 | PSYNC  |
|                         |                                 | PUBSUB  |
|                         |                                 | RANDOMKEY  |
|                         |                                 | READONLY  |
|                         |                                 | READWRITE  |
|                         |                                 | RENAMENX  |
|                         |                                 | REPLICAOF  |
|                         |                                 | RESTORE  |
|                         |                                 | ROLE  |
|                         |                                 | RPOP  |
|                         |                                 | RPOPLPUSH  |
|                         |                                 | RPUSH  |
|                         |                                 | RPUSHX  |
|                         |                                 | SAVE  |
|                         |                                 | SCRIPT DEBUG |
|                         |                                 | SCRIPT EXISTS  |
|                         |                                 | SCRIPT FLUSH |
|                         |                                 | SCRIPT KILL  |
|                         |                                 | SCRIPT LOAD  |
|                         |                                 | SELECT  |
|                         |                                 | SLAVEOF  |
|                         |                                 | SLOWLOG  |
|                         |                                 | SORT  |
|                         |                                 | STRALGO LCS  |
|                         |                                 | SWAPDB  |
|                         |                                 | SYNC  |
|                         |                                 | TOUCH  |
|                         |                                 | UNLINK  |
|                         |                                 | UNWATCH  |
|                         |                                 | WAIT  |
|                         |                                 | WATCH  |
|                         |                                 | XACK  |
|                         |                                 | XADD  |
|                         |                                 | XCLAIM  |
|                         |                                 | XDEL  |
|                         |                                 | XGROUP  |
|                         |                                 | XINFO  |
|                         |                                 | XLEN  |
|                         |                                 | XPENDING  |
|                         |                                 | XRANGE  |
|                         |                                 | XREAD  |
|                         |                                 | XREADGROUP GROUP |
|                         |                                 | XREVRANGE  |
|                         |                                 | XTRIM  |
|                         |                                 | ZADD  |
|                         |                                 | ZCARD  |
|                         |                                 | ZCOUNT  |
|                         |                                 | ZINCRBY  |
|                         |                                 | ZINTERSTORE  |
|                         |                                 | ZLEXCOUNT  |
|                         |                                 | ZPOPMAX  |
|                         |                                 | ZPOPMIN  |
|                         |                                 | ZRANGE  |
|                         |                                 | ZRANGEBYLEX  |
|                         |                                 | ZRANGEBYSCORE  |
|                         |                                 | ZRANK  |            
|                         |                                 | ZREM  |
|                         |                                 | ZREMRANGEBYLEX  |
|                         |                                 | ZREMRANGEBYRANK  |
|                         |                                 | ZREMRANGEBYSCORE  |  
|                         |                                 | ZREVRANGE  |  
|                         |                                 | ZREVRANGEBYSCORE  |  
|                         |                                 | ZREVRANK  |  
|                         |                                 | ZSCAN  |  
|                         |                                 | ZSCORE  |  
|                         |                                 | ZUNIONSTORE  |  
|                         |                                 | UNWATCH  |  

# Apache Geode APIs Compatible with Redis

Note: This feature is experimental and is subject to change in future releases of Apache Geode.

[Introduction](#introduction)  
[How To Try It](#how-to-try-it)  
[Add an Additional Geode Server Compatible with Redis](#adding-a-server)  
[Shutting Down](#shutting-down)  
[Security](#security)  
[Redis Commands](#redis-commands)

## <a name="introduction"></a>Introduction

The Geode APIs compatible with Redis allows Redis clients to use Geode as a highly-available Redis data store. Redis applications can take advantage of Geode’s scaling capabilities with minimal changes to the client code (Redis clients must be compatible with Redis CLUSTER commands).

Redis clients connect to a Geode server in the same way they connect to a Redis server, using a hostname and a port number.

## <a name="redis-commands"></a>Supported Redis Commands

Not all Redis commands are supported at this time. See [Supported Redis Commands](#supported-redis-commands) for the implemented subset.

## <a name="how-to-try-it"></a>How To Try It

Install and configure Geode v1.15 or later.

In a terminal, start Geode using the `gfsh` command.

With `gfsh` running, in the same terminal, start one [locator](https://geode.apache.org/docs/guide/12/configuring/running/running_the_locator.html) 

```
gfsh> start locator
```

Once the locator has started, start at least one server with a command of the form:
```console
gfsh> start server \
  --name=<serverName> \
  --locators=localhost[<locatorPort>] \
  --compatible-with-redis-port=<compatibleWithRedisPort> \
  --compatible-with-redis-bind-address=<compatibleWithRedisBindAddress>
```

If any of the options `compatible-with-redis-bind-address`, `compatible-with-redis-username`, or `compatible-with-redis-port` are included, a Geode server with APIs compatible with Redis will be started.

- Replace `<serverName>` with the name of your server.

- Replace `<locatorPort>` with your locator port.

- Replace `<compatibleWithRedisPort>` with the port that the Geode server listens on for Redis commands. The typical port used with a cluster compatible with Redis is 6379.

- Replace `<compatibleWithRedisBindAddress>` with the address of the server host.

Your Geode instance should now be up and running (1 locator and 1 server) and ready to accept Redis 
    commands.  

**Keep this terminal open and running so that you can easily shutdown the Geode instance when you are 
    done working locally.**

To confirm the server is listening, in a separate terminal run:

```console
$> redis-cli -c -h <compatibleWithRedisBindAddress> -p <compatibleWithRedisPort> ping
```

- Replace `<compatibleWithRedisBindAddress>`, `<compatibleWithRedisPort>`, and `<compatibleWithRedisPassword>` with the same values as the server.

If the server is functioning properly, you should see a response of `PONG`.

### <a name="adding-a-server"></a> Add an additional Geode server compatible with Redis APIs
If you’re interested in testing Geode scalability, in gfsh run the `start server` command again **BUT**
make sure you change the `--name=` and `--redis-port=` parameters.

For example:
```commandLine
gfsh> start server --name=redisServer2 --locators=localhost[10334] --server-port=0 --compatible-with-redis-port=6380
```

### <a name="shutting-down"></a>Shutting Down
To shut down the Geode instance you started, in the terminal with gfsh running type the following command

```commandLine
gfsh> shutdown --include-locators=true
```

This command shuts down the entire Geode instance/cluster. You are prompted with the following choice:

```commandline
As a lot of data in memory will be lost, including possibly events in queues, do you really want to shutdown the entire distributed system? (Y/n)
```

To confirm that everything shut down correctly, if you execute a Redis command in the redis-cli you should see the following message:

```commandline
Could not connect to Redis at 127.0.0.1:6379: Connection refused 
not connected>
```

## <a name="security"></a>Security

Security is implemented slightly differently to OSS Redis. Redis stores password information in plain text in the redis.conf file.     

When using Apache Geode, to enable security, a Security Manager needs to be configured on the server(s). This Security Manager will authenticate `AUTH <password>` commands and `AUTH <username> <password>` commands. Users can set a custom `default` username using the `geode-compatible-with-redis-username` parameter. This username will be used when `AUTH <password>` commands are sent without a `<username>`. 

The following gfsh command will configure a `SimpleSecurityManager`:

```console
gfsh> start server \
  --name=<serverName> \
  --locators=<locatorPort> \
  --compatible-with-redis-port=<compatibleWithRedisPort> \
  --compatible-with-redis-bind-address=<compatibleWithRedisBindAddress> \
  --compatible-with-redis-username=<compatibleWithRedisUsername> \
  --J=-Dgemfire.security-manager=org.apache.geode.examples.SimpleSecurityManager
```

To confirm that the server is working, in a separate terminal run:

```console
$> redis-cli -c -h <compatibleWithRedisBindAddress> -p <compatibleWithRedisPort> \
  --user <compatibleWithRedisUsername> -a <compatibleWithRedisUsername> ping
```

The `SimpleSecurityManager` is only to be used **for demonstration purposes**. It will authenticate successfully when the `password` and `username` are the same.

Note that the `compatible-with-redis-username` property is only needed if `AUTH` commands are issued without a username. In this case, the Security Manager will need to respond to authentication requests using this username.

## <a name="application-developement"></a>Application Developement

### Things to know before you begin
- Apache Geode for Redis currently implements a subset of the full Redis set of commands
- Applications must be using a redis client that supports Redis Cluster mode.
- If your application is using Spring Session Data Redis you will need to add the following code to disable Spring Session from calling CONFIG (CONFIG is not supported).

```java
@Bean
public static ConfigureRedisAction configureRedisAction() {
      return ConfigureRedisAction.NO_OP;
}
```
This is a known solution for many Managed Redis products (ElastiCache, Azure Cache for Redis, etc) that disable the CONFIG command for security reasons.  You can read more about why this is done [here](https://github.com/spring-projects/spring-session/issues/124).

## <a name="redis-commands"></a>Redis Commands

The Geode APIs compatible with Redis implement a subset of the full Redis command set.

#### <a name="supported-redis-commands"></a> Supported Redis Commands [Return to top](#introduction)

- APPEND  
- AUTH  
- CLUSTER INFO  
- CLUSTER NODES  
- CLUSTER SLOTS  
- DECR  
- DECRBY  
- DEL  
- ECHO  
- EXISTS  
- EXPIRE  
- EXPIREAT  
- GET  
- GETRANGE  
- GETSET
- HDEL  
- HEXISTS  
- HGET  
- HGETALL  
- HINCRBY  
- HINCRBYFLOAT  
- HKEYS  
- HLEN  
- HMGET  
- HMSET  
- HSCAN <sup>1</sup>  
- HSET  
- HSETNX  
- HSTRLEN  
- HVALS  
- INCR  
- INCRBY  
- INCRBYFLOAT  
- INFO <sup>2</sup>  
- KEYS  
- MGET  
- PERSIST  
- PEXPIRE  
- PEXPIREAT  
- PING  
- PSETEX
- PSUBSCRIBE  
- PTTL  
- PUBLISH  
- PUBSUB  
- PUNSUBSCRIBE  
- QUIT  
- RENAME  
- SADD  
- SET  
- SETEX
- SETNX  
- SETRANGE  
- SLOWLOG <sup>3</sup>  
- SMEMBERS  
- SREM  
- STRLEN  
- SUBSCRIBE  
- TTL  
- TYPE  
- UNSUBSCRIBE  
- ZADD  
- ZCARD  
- ZCOUNT  
- ZINCRBY  
- ZLEXCOUNT  
- ZPOPMAX  
- ZPOPMIN  
- ZRANGE  
- ZRANGEBYLEX  
- ZRANGEBYSCORE  
- ZRANK  
- ZREM  
- ZREMRANGEBYLEX  
- ZREMRANGEBYRANK  
- ZREMRANGEBYSCORE  
- ZREVRANGE  
- ZREVRANGEBYLEX  
- ZREVRANGEBYSCORE  
- ZREVRANK  
- ZSCORE  

**NOTES:**

<sup>1</sup>Redis accepts 64-bit signed integers for the HSCAN cursor and COUNT parameters. The Geode APIs compatible with Redis are limited to 32-bit integer values for these parameters.
<br/>  
<sup>2</sup> INFO is implemented for the sections and fields listed below:

 - clients

    - connected_clients

    - blocked_clients (always returns 0)

 - cluster

    - cluster_enables (always returns 0)

 - keyspace

    - returns stats for db: 0

 - memory

    - maxmemory

    - used_memory

    - mem_fragmentation_ratio (always reports 1.00) 

 - persistence

    - loading (always returns 0)

    - rdb_changes_since_last_save (always returns 0)

    - rdb_last_save_time (always returns 0)

 - replication

    - role

    - connected_slaves (always returns 0)

 - server

   - redis_version

   - redis_mode

   - tcp_port

   - uptime_in_seconds

   - uptime_in_days

 - stats

    - total_commands_processed

    - instantaneous_ops_per_sec

    - total_net_input_bytes

    - instantaneous_input_kbps

    - total_connections_received

    - keyspace_hits

    - keyspace_misses

    - evicted_keys (always returns 0)

    - rejected_connections (always returns 0)

<br/>  
<sup>3</sup>  SLOWLOG is implemented as a NoOp.


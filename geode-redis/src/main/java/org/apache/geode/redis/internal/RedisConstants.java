/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.redis.internal;


public class RedisConstants {

  public static final int NUM_DEFAULT_KEYS = 3;

  /*
   * Responses
   */
  public static final String QUIT_RESPONSE = "OK";
  public static final String COMMAND_QUEUED = "QUEUED";


  /*
   * Error responses
   */
  static final String PARSING_EXCEPTION_MESSAGE =
      "The command received by GeodeRedisServer was improperly formatted";
  public static final String SERVER_ERROR_MESSAGE =
      "The server had an internal error please try again";
  static final String SERVER_ERROR_UNKNOWN_RESPONSE = "Unkown response";
  static final String SERVER_ERROR_SHUTDOWN = "The server is shutting down";
  static final String ERROR_UNSUPPORTED_OPERATION_IN_TRANSACTION =
      "This command is not supported within a transaction";
  static final String ERROR_TRANSACTION_EXCEPTION =
      "This transcation cannot be initiated, make sure the command is executed against a replicate region or your data is collocated. If you are using persistent regions, make sure transactions are enabled";
  public static final String ERROR_NOT_NUMERIC = "Illegal non numeric argument";
  public static final String ERROR_INVALID_ARGUMENT_UNIT_NUM =
      "Either illegal non numeric argument or invalid unit" +
          "(please use either km/m/ft/mi)";
  public static final String ERROR_UNKOWN_COMMAND = "Unable to process unknown command";
  public static final String ERROR_COMMIT_CONFLICT =
      "There has been a conflict with another transaction";
  public static final String ERROR_REGION_CREATION =
      "This key could not be created. Gemfire does not allow certain characters to used in keys";
  public static final String ERROR_UNWATCH =
      "Keys cannot be watched or unwatched because GemFire watches all keys by default for transactions";
  public static final String ERROR_WATCH =
      "Keys cannot be watched or unwatched because GemFire watches all keys by default for transactions";
  public static final String ERROR_ILLEGAL_GLOB = "Incorrect syntax for given glob regex";
  public static final String ERROR_OUT_OF_RANGE = "The number provided is out of range";
  public static final String ERROR_INVALID_LATLONG = "Invalid longitude-latitude pair";
  public static final String ERROR_NESTED_MULTI = "The MULTI command cannot be nested";
  public static final String ERROR_NAN_INF_INCR = "increment would produce NaN or Infinity";
  public static final String ERROR_NO_PASS =
      "Attempting to authenticate when no password has been set";
  public static final String ERROR_INVALID_PWD =
      "Attemping to authenticate with an invalid password";
  public static final String ERROR_NOT_AUTH = "Must authenticate before sending any requests";
  public static final String ERROR_ZSET_MEMBER_NOT_FOUND = "could not decode requested zset member";
  public static final String ERROR_WRONG_TYPE =
      "WRONGTYPE Operation against a key holding the wrong kind of value";
  public static final String ERROR_NOT_INTEGER = "value is not an integer or out of range";
  public static final String ERROR_OVERFLOW = "increment or decrement would overflow";
  public static final String ERROR_NO_SUCH_KEY = "no such key";
  public static final String ERROR_SYNTAX = "syntax error";
  public static final String ERROR_INVALID_EXPIRE_TIME = "invalid expire time in set";

  public static class ArityDef {

    /*
     * General
     */
    public static final int DBSIZE_ARITY = 0;
    public static final String AUTH =
        "The wrong number of arguments or syntax was provided, the format for the AUTH command is \"AUTH password\"";
    public static final String DBSIZE = null;
    public static final String DEL =
        "The wrong number of arguments or syntax was provided, the format for the DEL command is \"DEL key [key ...]\"";
    public static final String ECHO =
        "The wrong number of arguments or syntax was provided, the format for the ECHO command is \"ECHO message\"";
    public static final String EXISTS =
        "The wrong number of arguments or syntax was provided, the format for the EXISTS command is \"EXISTS key\"";
    public static final String EXPIREAT =
        "The wrong number of arguments or syntax was provided, the format for the EXPIREAT command is \"EXPIREAT key timestamp\"";
    public static final String EXPIRE =
        "The wrong number of arguments or syntax was provided, the format for the EXPIRE command is \"EXPIRE key seconds\"";
    public static final String FLUSHALL = null;
    public static final String KEYS =
        "The wrong number of arguments or syntax was provided, the format for the KEYS command is \"KEYS pattern\"";
    public static final String PERSIST =
        "The wrong number of arguments or syntax was provided, the format for the PERSIST command is \"PERSIST key\"";
    public static final String PEXPIREAT =
        "The wrong number of arguments or syntax was provided, the format for the PEXPIREAT command is \"PEXPIREAT key milliseconds-timestamp\"";
    public static final String PEXPIRE =
        "The wrong number of arguments or syntax was provided, the format for the PEXPIRE command is \"PEXPIRE key milliseconds\"";
    public static final String PING = null;
    public static final String PTTL =
        "The wrong number of arguments or syntax was provided, the format for the PTTL command is \"PTTL key\"";
    public static final String QUIT = null;
    public static final String SCAN =
        "The wrong number of arguments or syntax was provided, the format for the SCAN command is \"SCAN cursor [MATCH pattern] [COUNT count]\"";
    public static final String SHUTDOWN = null;
    public static final String TIME = null;
    public static final String TTL =
        "The wrong number of arguments or syntax was provided, the format for the TTL command is \"TTL key\"";
    public static final String TYPE =
        "The wrong number of arguments or syntax was provided, the format for the TYPE command is \"TYPE key\"";
    public static final String UNKNOWN = null;

    /*
     * Hash
     */
    public static final String HDEL =
        "The wrong number of arguments or syntax was provided, the format for the HDEL command is \"HDEL key field [field ...]\"";
    public static final String HEXISTS =
        "The wrong number of arguments or syntax was provided, the format for the HEXISTS command is \"HEXISTS key field\"";
    public static final String HGETALL =
        "The wrong number of arguments or syntax was provided, the format for the HGETALL command is \"HGETALL key\"";
    public static final String HGET =
        "The wrong number of arguments or syntax was provided, the format for the HGET command is \"HGET key field\"";
    public static final String HINCRBY =
        "The wrong number of arguments or syntax was provided, the format for the HINCRBY command is \"HINCRBY key field increment\"";
    public static final String HINCRBYFLOAT =
        "The wrong number of arguments or syntax was provided, the format for the HINCRBYFLOAT command is \"HINCRBYFLOAT key field increment\"";
    public static final String HKEYS =
        "The wrong number of arguments or syntax was provided, the format for the HKEYS command is \"HKEYS key\"";
    public static final String HLEN =
        "The wrong number of arguments or syntax was provided, the format for the HLEN command is \"HLEN key\"";
    public static final String HMGET =
        "The wrong number of arguments or syntax was provided, the format for the HMGET command is \"HMGET key field [field ...]\"";
    public static final String HMSET =
        "The wrong number of arguments or syntax was provided, the format for the HMSET command is \"HMSET key field value [field value ...]\", or not every field is associated with a value";
    public static final String HSCAN =
        "The wrong number of arguments or syntax was provided, the format for the SSCAN command is \"SSCAN key cursor [MATCH pattern] [COUNT count]\"";
    public static final String HSET =
        "The wrong number of arguments or syntax was provided, the format for the HSET command is \"HSET key field value\"";
    public static final String HSETNX =
        "The wrong number of arguments or syntax was provided, the format for the HSETNX command is \"HSETNX key field value\"";
    public static final String HVALS =
        "The wrong number of arguments or syntax was provided, the format for the HVALS command is \"HVALS key\"";

    /*
     * Hll
     */
    public static final String PFADD =
        "The wrong number of arguments or syntax was provided, the format for the PFADD command is \"PFADD key element [element ...]\"";
    public static final String PFCOUNT =
        "The wrong number of arguments or syntax was provided, the format for the PFCOUNT command is \"PFCOUNT key [key ...]\"";
    public static final String PFMERGE =
        "The wrong number of arguments or syntax was provided, the format for the PFMERGE command is \"PFMERGE destkey sourcekey [sourcekey ...]\"";

    /*
     * List
     */
    public static final String LINDEX =
        "The wrong number of arguments or syntax was provided, the format for the LINDEX command is \"LINDEX key index";
    public static final String LINSERT = null;
    public static final String LLEN =
        "The wrong number of arguments or syntax was provided, the format for the LLEN command is \"LLEN key";
    public static final String LPOP =
        "The wrong number of arguments or syntax was provided, the format for the LPOP command is \"LPOP key";
    public static final String LPUSH =
        "The wrong number of arguments or syntax was provided, the format for the LPUSH command is \"LPUSH key value [value ...]";
    public static final String LPUSHX =
        "The wrong number of arguments or syntax was provided, the format for the LPUSHX command is \"LPUSHX key value";
    public static final String LRANGE =
        "The wrong number of arguments or syntax was provided, the format for the LRANGE command is \"LRANGE key start stop\"";
    public static final String LREM =
        "The wrong number of arguments or syntax was provided, the format for the LREM command is \"LREM key count value\"";
    public static final String LSET =
        "The wrong number of arguments or syntax was provided, the format for the LSET command is \"LSET key index value\"";
    public static final String LTRIM =
        "The wrong number of arguments or syntax was provided, the format for the LTRIM command is \"LTRIM key start stop\"";
    public static final String RPOP =
        "The wrong number of arguments or syntax was provided, the format for the RPOP command is \"RPOP key";
    public static final String RPUSH =
        "The wrong number of arguments or syntax was provided, the format for the RPUSH command is \"RPUSH key value [value ...]";
    public static final String RPUSHX =
        "The wrong number of arguments or syntax was provided, the format for the RPUSHX command is \"RPUSHX key value";

    /*
     * Set
     */
    public static final String SADD =
        "The wrong number of arguments or syntax was provided, the format for the SADD command is \"SADD key member [member ...]\"";
    public static final String SCARD =
        "The wrong number of arguments or syntax was provided, the format for the SCARD command is \"SCARD key\"";
    public static final String SDIFF =
        "The wrong number of arguments or syntax was provided, the format for the SDIFF command is \"SDIFF key [key ...]\"";
    public static final String SDIFFSTORE =
        "The wrong number of arguments or syntax was provided, the format for the SDIFF command is \"SDIFFSTORE destination key [key ...]\"";
    public static final String SINTER =
        "The wrong number of arguments or syntax was provided, the format for the SINTER command is \"SINTER key [key ...]\"";
    public static final String SINTERSTORE =
        "The wrong number of arguments or syntax was provided, the format for the SINTERSTORE command is \"SINTERSTORE destination key [key ...]\"";
    public static final String SISMEMBER =
        "The wrong number of arguments or syntax was provided, the format for the SISMEMBER command is \"SISMEMBER key member\"";
    public static final String SMEMBERS =
        "The wrong number of arguments or syntax was provided, the format for the SMEMBERS command is \"SMEMBERS key\"";
    public static final String SMOVE =
        "The wrong number of arguments or syntax was provided, the format for the SMOVE command is \"SMOVE source destination member\"";
    public static final String SPOP =
        "The wrong number of arguments or syntax was provided, the format for the SPOP command is \"SPOP key [count]\"";
    public static final String SRANDMEMBER =
        "The wrong number of arguments or syntax was provided, the format for the SRANDMEMBER command is \"SRANDMEMBER key [count]\"";
    public static final String SREM =
        "The wrong number of arguments or syntax was provided, the format for the SREM command is \"SREM key member [member ...]\"";
    public static final String SSCAN =
        "The wrong number of arguments or syntax was provided, the format for the SSCAN command is \"SSCAN key cursor [MATCH pattern] [COUNT count]\"";
    public static final String SUNION =
        "The wrong number of arguments or syntax was provided, the format for the SUNION command is \"SUNION key [key ...]\"";
    public static final String SUNIONSTORE =
        "The wrong number of arguments or syntax was provided, the format for the SUNIONSTORE command is \"SUNIONSTORE destination key [key ...]\"";

    /*
     * Sorted set
     */
    public static final String ZADD =
        "The wrong number of arguments or syntax was provided, the format for the ZADD command is \"ZADD key score member [score member ...]\", or not every score matches to a member";
    public static final String ZCARD =
        "The wrong number of arguments or syntax was provided, the format for the ZCARD command is \"ZCARD key\"";
    public static final String ZCOUNT =
        "The wrong number of arguments or syntax was provided, the format for the ZCOUNT command is \"ZCOUNT key min max\"";
    public static final String ZINCRBY =
        "The wrong number of arguments or syntax was provided, the format for the ZINCRBY command is \"ZINCRBY key increment member\"";
    public static final String ZLEXCOUNT =
        "The wrong number of arguments or syntax was provided, the format for the ZLEXCOUNT command is \"ZLEXCOUNT key min max\"";
    public static final String ZRANGEBYLEX =
        "The wrong number of arguments or syntax was provided, the format for the ZRANGEBYLEX command is \"ZRANGEBYLEX key min max [LIMIT offset count]\"";
    public static final String ZRANGEBYSCORE =
        "The wrong number of arguments or syntax was provided, the format for the ZRANGEBYSCORE command is \"ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]\"";
    public static final String ZRANGE =
        "The wrong number of arguments or syntax was provided, the format for the ZRANGE command is \"ZRANGE key start stop [WITHSCORES]\"";
    public static final String ZRANK =
        "The wrong number of arguments or syntax was provided, the format for the ZRANK command is \"ZRANK key member\"";
    public static final String ZREM =
        "The wrong number of arguments or syntax was provided, the format for the ZREM command is \"ZREM key member [member ...]\"";
    public static final String ZREMRANGEBYLEX =
        "The wrong number of arguments or syntax was provided, the format for the ZREMRANGEBYLEX command is \"ZREMRANGEBYLEX key min max\"";
    public static final String ZREMRANGEBYRANK =
        "The wrong number of arguments or syntax was provided, the format for the ZREMRANGEBYRANK command is \"ZREMRANGEBYRANK key start stop\"";
    public static final String ZREMRANGEBYSCORE =
        "The wrong number of arguments or syntax was provided, the format for the ZREMRANGEBYSCORE command is \"ZREMRANGEBYSCORE key min max\"";
    public static final String ZREVRANGEBYSCORE =
        "The wrong number of arguments or syntax was provided, the format for the ZREVRANGEBYSCORE command is \"ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count]\"";
    public static final String ZREVRANGE =
        "The wrong number of arguments or syntax was provided, the format for the ZREVRANGE command is \"ZREVRANGE key start stop [WITHSCORES]\"";
    public static final String ZREVRANK =
        "The wrong number of arguments or syntax was provided, the format for the ZREVRANK command is \"ZREVRANK key member\"";
    public static final String ZSCAN =
        "The wrong number of arguments or syntax was provided, the format for the SSCAN command is \"SSCAN key cursor [MATCH pattern] [COUNT count]\"";
    public static final String ZSCORE =
        "The wrong number of arguments or syntax was provided, the format for the ZSCORE command is \"ZSCORE key member\"";

    /*
     * Geospatial
     */
    public static final String GEOADD =
        "The wrong number of arguments or syntax was provided, the format for the GEOADD command is \"GEOADD key longitude latitude member [longitude latitude member ...]\", or not every latitude/longitude pair matches to a member";
    public static final String GEOHASH =
        "The wrong number of arguments or syntax was provided, the format for the GEOHASH command is \"GEOHASH key member [member...]\"";
    public static final String GEOPOS =
        "The wrong number of arguments or syntax was provided, the format for the GEOPOS command is \"GEOPOS key member [member...]\"";
    public static final String GEODIST =
        "The wrong number of arguments or syntax was provided, the format for the GEODIST command is \"GEODIST key member member [unit]\"";
    public static final String GEORADIUS =
        "The wrong number of arguments or syntax was provided, the format for the GEORADIUS command is \"GEORADIUS key longitude latitude radius m|km|ft|mi [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count] [ASC|DESC]\"";
    public static final String GEORADIUSBYMEMBER =
        "The wrong number of arguments or syntax was provided, the format for the GEORADIUSBYMEMBER command is \"GEORADIUSBYMEMBER key member radius m|km|ft|mi [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count] [ASC|DESC]\"";

    /*
     * String
     */
    public static final String APPEND =
        "The wrong number of arguments or syntax was provided, the format for the APPEND command is \"APPEND key value\"";
    public static final String BITCOUNT =
        "The wrong number of arguments or syntax was provided, the format for the BITCOUNT command is \"BITCOUNT key [start end]\"";
    public static final String BITOP =
        "The wrong number of arguments or syntax was provided, the format for the BITOP command is \"BITOP operation destkey key [key ...]\"";
    public static final String BITPOS =
        "The wrong number of arguments or syntax was provided, the format for the BITOPS command is \"BITPOS key bit [start] [end]\"";
    public static final String DECRBY =
        "The wrong number of arguments or syntax was provided, the format for the DECRBY command is \"DECRRBY key decrement\"";
    public static final String DECR =
        "The wrong number of arguments or syntax was provided, the format for the DECR command is \"DECR key\"";
    public static final String GETBIT =
        "The wrong number of arguments or syntax was provided, the format for the GETBIT command is \"GETBIT key offset\"";
    public static final String GETEXECUTOR =
        "The wrong number of arguments or syntax was provided, the format for the GET command is \"GET key\"";
    public static final String GETRANGE =
        "The wrong number of arguments or syntax was provided, the format for the GETRANGE command is \"GETRANGE key start end\"";
    public static final String GETSET =
        "The wrong number of arguments or syntax was provided, the format for the GETSET command is \"GETSET key value\"";
    public static final String INCRBY =
        "The wrong number of arguments or syntax was provided, the format for the INCRBY command is \"INCRBY key increment\"";
    public static final String INCRBYFLOAT =
        "The wrong number of arguments or syntax was provided, the format for the INCRBY command is \"INCRBY key increment\"";
    public static final String INCR =
        "The wrong number of arguments or syntax was provided, the format for the INCR command is \"INCR key\"";
    public static final String MGET =
        "The wrong number of arguments or syntax was provided, the format for the MGET command is \"MGET key [key ...]\"";
    public static final String MSET =
        "The wrong number of arguments or syntax was provided, the format for the MSET command is \"MSET key value [key value ...]\", or not every key matches a value";
    public static final String MSETNX =
        "The wrong number of arguments or syntax was provided, the format for the MSETNX command is \"MSETNX key value [key value ...]\", or not every key matches a value";
    public static final String PSETEX =
        "The wrong number of arguments or syntax was provided, the format for the PSETEX command is \"PSETEX key milliseconds value\"";
    public static final String PUBLISH =
        "The wrong number of arguments or syntax was provided, the format for the PUBLISH command is \"PUBLISH channel message\"";
    public static final String SETBIT =
        "The wrong number of arguments or syntax was provided, the format for the SETBIT command is \"SETBIT key offset value\"";
    public static final String SET =
        "The wrong number of arguments or syntax was provided, the format for the SET command is \"SET key value [EX seconds] [PX milliseconds] [NX|XX]\"";
    public static final String SETEX =
        "The wrong number of arguments or syntax was provided, the format for the SETEX command is \"SETEX key seconds value\"";
    public static final String SETNX =
        "The wrong number of arguments or syntax was provided, the format for the SETNX command is \"SETNX key value\"";
    public static final String SETRANGE =
        "The wrong number of arguments or syntax was provided, the format for the SETRANGE command is \"SETRANGE key offset value\"";
    public static final String STRLEN =
        "The wrong number of arguments or syntax was provided, the format for the STRELEN command is \"STRLEN key\"";

    /*
     * Transaction
     */
    public static final String DISCARD = null;
    public static final String EXEC = null;
    public static final String MULTI = null;
    public static final String UNWATCH = null;
    public static final String WATCH = null;
  }

}

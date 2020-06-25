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

import static org.apache.geode.redis.internal.RedisCommandSupportLevel.SUPPORTED;
import static org.apache.geode.redis.internal.RedisCommandSupportLevel.UNIMPLEMENTED;
import static org.apache.geode.redis.internal.RedisCommandSupportLevel.UNSUPPORTED;

import org.apache.geode.redis.internal.ParameterRequirements.EvenParameterRequirements;
import org.apache.geode.redis.internal.ParameterRequirements.ExactParameterRequirements;
import org.apache.geode.redis.internal.ParameterRequirements.MaximumParameterRequirements;
import org.apache.geode.redis.internal.ParameterRequirements.MinimumParameterRequirements;
import org.apache.geode.redis.internal.ParameterRequirements.ParameterRequirements;
import org.apache.geode.redis.internal.ParameterRequirements.SpopParameterRequirements;
import org.apache.geode.redis.internal.ParameterRequirements.UnspecifiedParameterRequirements;
import org.apache.geode.redis.internal.executor.Executor;
import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.executor.UnknownExecutor;
import org.apache.geode.redis.internal.executor.connection.AuthExecutor;
import org.apache.geode.redis.internal.executor.connection.EchoExecutor;
import org.apache.geode.redis.internal.executor.connection.PingExecutor;
import org.apache.geode.redis.internal.executor.connection.QuitExecutor;
import org.apache.geode.redis.internal.executor.hash.HDelExecutor;
import org.apache.geode.redis.internal.executor.hash.HExistsExecutor;
import org.apache.geode.redis.internal.executor.hash.HGetAllExecutor;
import org.apache.geode.redis.internal.executor.hash.HGetExecutor;
import org.apache.geode.redis.internal.executor.hash.HIncrByExecutor;
import org.apache.geode.redis.internal.executor.hash.HIncrByFloatExecutor;
import org.apache.geode.redis.internal.executor.hash.HKeysExecutor;
import org.apache.geode.redis.internal.executor.hash.HLenExecutor;
import org.apache.geode.redis.internal.executor.hash.HMGetExecutor;
import org.apache.geode.redis.internal.executor.hash.HMSetExecutor;
import org.apache.geode.redis.internal.executor.hash.HScanExecutor;
import org.apache.geode.redis.internal.executor.hash.HSetExecutor;
import org.apache.geode.redis.internal.executor.hash.HSetNXExecutor;
import org.apache.geode.redis.internal.executor.hash.HStrLenExecutor;
import org.apache.geode.redis.internal.executor.hash.HValsExecutor;
import org.apache.geode.redis.internal.executor.key.DelExecutor;
import org.apache.geode.redis.internal.executor.key.ExistsExecutor;
import org.apache.geode.redis.internal.executor.key.ExpireAtExecutor;
import org.apache.geode.redis.internal.executor.key.ExpireExecutor;
import org.apache.geode.redis.internal.executor.key.KeysExecutor;
import org.apache.geode.redis.internal.executor.key.PExpireAtExecutor;
import org.apache.geode.redis.internal.executor.key.PExpireExecutor;
import org.apache.geode.redis.internal.executor.key.PTTLExecutor;
import org.apache.geode.redis.internal.executor.key.PersistExecutor;
import org.apache.geode.redis.internal.executor.key.RenameExecutor;
import org.apache.geode.redis.internal.executor.key.ScanExecutor;
import org.apache.geode.redis.internal.executor.key.TTLExecutor;
import org.apache.geode.redis.internal.executor.key.TypeExecutor;
import org.apache.geode.redis.internal.executor.pubsub.PsubscribeExecutor;
import org.apache.geode.redis.internal.executor.pubsub.PublishExecutor;
import org.apache.geode.redis.internal.executor.pubsub.PunsubscribeExecutor;
import org.apache.geode.redis.internal.executor.pubsub.SubscribeExecutor;
import org.apache.geode.redis.internal.executor.pubsub.UnsubscribeExecutor;
import org.apache.geode.redis.internal.executor.server.DBSizeExecutor;
import org.apache.geode.redis.internal.executor.server.FlushAllExecutor;
import org.apache.geode.redis.internal.executor.server.ShutDownExecutor;
import org.apache.geode.redis.internal.executor.server.TimeExecutor;
import org.apache.geode.redis.internal.executor.set.SAddExecutor;
import org.apache.geode.redis.internal.executor.set.SCardExecutor;
import org.apache.geode.redis.internal.executor.set.SDiffExecutor;
import org.apache.geode.redis.internal.executor.set.SDiffStoreExecutor;
import org.apache.geode.redis.internal.executor.set.SInterExecutor;
import org.apache.geode.redis.internal.executor.set.SInterStoreExecutor;
import org.apache.geode.redis.internal.executor.set.SIsMemberExecutor;
import org.apache.geode.redis.internal.executor.set.SMembersExecutor;
import org.apache.geode.redis.internal.executor.set.SMoveExecutor;
import org.apache.geode.redis.internal.executor.set.SPopExecutor;
import org.apache.geode.redis.internal.executor.set.SRandMemberExecutor;
import org.apache.geode.redis.internal.executor.set.SRemExecutor;
import org.apache.geode.redis.internal.executor.set.SScanExecutor;
import org.apache.geode.redis.internal.executor.set.SUnionExecutor;
import org.apache.geode.redis.internal.executor.set.SUnionStoreExecutor;
import org.apache.geode.redis.internal.executor.string.AppendExecutor;
import org.apache.geode.redis.internal.executor.string.BitCountExecutor;
import org.apache.geode.redis.internal.executor.string.BitOpExecutor;
import org.apache.geode.redis.internal.executor.string.BitPosExecutor;
import org.apache.geode.redis.internal.executor.string.DecrByExecutor;
import org.apache.geode.redis.internal.executor.string.DecrExecutor;
import org.apache.geode.redis.internal.executor.string.GetBitExecutor;
import org.apache.geode.redis.internal.executor.string.GetExecutor;
import org.apache.geode.redis.internal.executor.string.GetRangeExecutor;
import org.apache.geode.redis.internal.executor.string.GetSetExecutor;
import org.apache.geode.redis.internal.executor.string.IncrByExecutor;
import org.apache.geode.redis.internal.executor.string.IncrByFloatExecutor;
import org.apache.geode.redis.internal.executor.string.IncrExecutor;
import org.apache.geode.redis.internal.executor.string.MGetExecutor;
import org.apache.geode.redis.internal.executor.string.MSetExecutor;
import org.apache.geode.redis.internal.executor.string.MSetNXExecutor;
import org.apache.geode.redis.internal.executor.string.PSetEXExecutor;
import org.apache.geode.redis.internal.executor.string.SetBitExecutor;
import org.apache.geode.redis.internal.executor.string.SetEXExecutor;
import org.apache.geode.redis.internal.executor.string.SetExecutor;
import org.apache.geode.redis.internal.executor.string.SetNXExecutor;
import org.apache.geode.redis.internal.executor.string.SetRangeExecutor;
import org.apache.geode.redis.internal.executor.string.StrlenExecutor;
import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

/**
 * The redis command type used by the server. Each command is directly from the redis protocol.
 */
public enum RedisCommandType {

  /***************************************
   *** Supported Commands ***
   ***************************************/

  /*************** Connection ****************/
  AUTH(new AuthExecutor(), SUPPORTED, new ExactParameterRequirements(2)),
  PING(new PingExecutor(), SUPPORTED, new MaximumParameterRequirements(2)),
  QUIT(new QuitExecutor(), SUPPORTED),

  /*************** Keys ******************/

  DEL(new DelExecutor(), SUPPORTED, new MinimumParameterRequirements(2)),
  EXISTS(new ExistsExecutor(), SUPPORTED, new MinimumParameterRequirements(2)),
  EXPIRE(new ExpireExecutor(), SUPPORTED, new ExactParameterRequirements(3)),
  EXPIREAT(new ExpireAtExecutor(), SUPPORTED, new ExactParameterRequirements(3)),
  KEYS(new KeysExecutor(), SUPPORTED, new ExactParameterRequirements(2)),
  PERSIST(new PersistExecutor(), SUPPORTED, new ExactParameterRequirements(2)),
  PEXPIRE(new PExpireExecutor(), SUPPORTED, new ExactParameterRequirements(3)),
  PEXPIREAT(new PExpireAtExecutor(), SUPPORTED, new ExactParameterRequirements(3)),
  PTTL(new PTTLExecutor(), SUPPORTED, new ExactParameterRequirements(2)),
  RENAME(new RenameExecutor(), SUPPORTED, new ExactParameterRequirements(3)),
  TTL(new TTLExecutor(), SUPPORTED, new ExactParameterRequirements(2)),
  TYPE(new TypeExecutor(), SUPPORTED, new ExactParameterRequirements(2)),

  /************* Strings *****************/

  APPEND(new AppendExecutor(), SUPPORTED, new ExactParameterRequirements(3)),
  GET(new GetExecutor(), SUPPORTED, new ExactParameterRequirements(2)),
  SET(new SetExecutor(), SUPPORTED, new MinimumParameterRequirements(3)),

  /************* Hashes *****************/

  HGETALL(new HGetAllExecutor(), SUPPORTED, new ExactParameterRequirements(2)),
  HMSET(new HMSetExecutor(), SUPPORTED,
      new MinimumParameterRequirements(4).and(new EvenParameterRequirements())),
  HSET(new HSetExecutor(), SUPPORTED,
      new MinimumParameterRequirements(4).and(new EvenParameterRequirements())),

  /************* Sets *****************/

  SADD(new SAddExecutor(), SUPPORTED, new MinimumParameterRequirements(3)),
  SMEMBERS(new SMembersExecutor(), SUPPORTED, new ExactParameterRequirements(2)),
  SREM(new SRemExecutor(), SUPPORTED, new MinimumParameterRequirements(3)),

  /********** Publish Subscribe **********/

  SUBSCRIBE(new SubscribeExecutor(), SUPPORTED, new MinimumParameterRequirements(2)),
  PUBLISH(new PublishExecutor(), SUPPORTED, new ExactParameterRequirements(3)),
  UNSUBSCRIBE(new UnsubscribeExecutor(), SUPPORTED, new MinimumParameterRequirements(2)),
  PSUBSCRIBE(new PsubscribeExecutor(), SUPPORTED, new MinimumParameterRequirements(2)),
  PUNSUBSCRIBE(new PunsubscribeExecutor(), SUPPORTED, new MinimumParameterRequirements(2)),

  UNKNOWN(new UnknownExecutor(), SUPPORTED),


  /***************************************
   *** Unsupported Commands ***
   ***************************************/

  /***************************************
   *************** Connection *************
   ***************************************/

  ECHO(new EchoExecutor(), UNSUPPORTED),

  /***************************************
   *************** Keys ******************
   ***************************************/

  SCAN(new ScanExecutor(), UNSUPPORTED),

  /***************************************
   ************** Strings ****************
   ***************************************/

  BITCOUNT(new BitCountExecutor(), UNSUPPORTED),
  BITOP(new BitOpExecutor(), UNSUPPORTED),
  BITPOS(new BitPosExecutor(), UNSUPPORTED),
  DECR(new DecrExecutor(), UNSUPPORTED),
  DECRBY(new DecrByExecutor(), UNSUPPORTED),
  GETBIT(new GetBitExecutor(), UNSUPPORTED),
  GETRANGE(new GetRangeExecutor(), UNSUPPORTED),
  GETSET(new GetSetExecutor(), UNSUPPORTED),
  INCR(new IncrExecutor(), UNSUPPORTED),
  INCRBY(new IncrByExecutor(), UNSUPPORTED),
  INCRBYFLOAT(new IncrByFloatExecutor(), UNSUPPORTED),
  MGET(new MGetExecutor(), UNSUPPORTED),
  MSET(new MSetExecutor(), UNSUPPORTED),
  MSETNX(new MSetNXExecutor(), UNSUPPORTED),
  PSETEX(new PSetEXExecutor(), UNSUPPORTED),
  SETEX(new SetEXExecutor(), UNSUPPORTED),
  SETBIT(new SetBitExecutor(), UNSUPPORTED),
  SETNX(new SetNXExecutor(), UNSUPPORTED),
  SETRANGE(new SetRangeExecutor(), UNSUPPORTED),
  STRLEN(new StrlenExecutor(), UNSUPPORTED),

  /***************************************
   **************** Hashes ***************
   ***************************************/

  HDEL(new HDelExecutor(), UNSUPPORTED, new MinimumParameterRequirements(3)),
  HEXISTS(new HExistsExecutor(), UNSUPPORTED, new ExactParameterRequirements(3)),
  HGET(new HGetExecutor(), UNSUPPORTED, new ExactParameterRequirements(3)),
  HINCRBY(new HIncrByExecutor(), UNSUPPORTED, new ExactParameterRequirements(4)),
  HINCRBYFLOAT(new HIncrByFloatExecutor(), UNSUPPORTED, new ExactParameterRequirements(4)),
  HKEYS(new HKeysExecutor(), UNSUPPORTED, new ExactParameterRequirements(2)),
  HLEN(new HLenExecutor(), UNSUPPORTED, new ExactParameterRequirements(2)),
  HMGET(new HMGetExecutor(), UNSUPPORTED, new MinimumParameterRequirements(3)),
  HSCAN(new HScanExecutor(), UNSUPPORTED, new MinimumParameterRequirements(3)),
  HSETNX(new HSetNXExecutor(), UNSUPPORTED, new ExactParameterRequirements(4)),
  HSTRLEN(new HStrLenExecutor(), UNSUPPORTED, new ExactParameterRequirements(3)),
  HVALS(new HValsExecutor(), UNSUPPORTED, new ExactParameterRequirements(2)),

  /***************************************
   **************** Sets *****************
   ***************************************/

  SCARD(new SCardExecutor(), UNSUPPORTED, new ExactParameterRequirements(2)),
  SDIFF(new SDiffExecutor(), UNSUPPORTED, new MinimumParameterRequirements(2)),
  SDIFFSTORE(new SDiffStoreExecutor(), UNSUPPORTED, new MinimumParameterRequirements(3)),
  SISMEMBER(new SIsMemberExecutor(), UNSUPPORTED, new ExactParameterRequirements(3)),
  SINTER(new SInterExecutor(), UNSUPPORTED, new MinimumParameterRequirements(2)),
  SINTERSTORE(new SInterStoreExecutor(), UNSUPPORTED, new MinimumParameterRequirements(3)),
  SMOVE(new SMoveExecutor(), UNSUPPORTED, new ExactParameterRequirements(4)),
  SPOP(new SPopExecutor(), UNSUPPORTED,
      new MinimumParameterRequirements(2).and(new MaximumParameterRequirements(3))
          .and(new SpopParameterRequirements())),
  SRANDMEMBER(new SRandMemberExecutor(), UNSUPPORTED, new MinimumParameterRequirements(2)),
  SUNION(new SUnionExecutor(), UNSUPPORTED, new MinimumParameterRequirements(2)),
  SUNIONSTORE(new SUnionStoreExecutor(), UNSUPPORTED, new MinimumParameterRequirements(3)),
  SSCAN(new SScanExecutor(), UNSUPPORTED, new MinimumParameterRequirements(3)),

  /***************************************
   *************** Server ****************
   ***************************************/

  DBSIZE(new DBSizeExecutor(), UNSUPPORTED),
  FLUSHALL(new FlushAllExecutor(), UNSUPPORTED),
  FLUSHDB(new FlushAllExecutor(), UNSUPPORTED),
  SHUTDOWN(new ShutDownExecutor(), UNSUPPORTED),
  TIME(new TimeExecutor(), UNSUPPORTED),

  /////////// UNIMPLEMENTED /////////////////////

  ACL(null, UNIMPLEMENTED),
  BGREWRITEAOF(null, UNIMPLEMENTED),
  BGSAVE(null, UNIMPLEMENTED),
  BITFIELD(null, UNIMPLEMENTED),
  BLPOP(null, UNIMPLEMENTED),
  BRPOP(null, UNIMPLEMENTED),
  BRPOPLPUSH(null, UNIMPLEMENTED),
  BZPOPMIN(null, UNIMPLEMENTED),
  BZPOPMAX(null, UNIMPLEMENTED),
  CLIENT(null, UNIMPLEMENTED),
  CLUSTER(null, UNIMPLEMENTED),
  COMMAND(null, UNIMPLEMENTED),
  CONFIG(null, UNIMPLEMENTED),
  DEBUG(null, UNIMPLEMENTED),
  DISCARD(null, UNIMPLEMENTED),
  DUMP(null, UNIMPLEMENTED),
  EVAL(null, UNIMPLEMENTED),
  EVALSHA(null, UNIMPLEMENTED),
  EXEC(null, UNIMPLEMENTED),
  GEOADD(null, UNIMPLEMENTED),
  GEOHASH(null, UNIMPLEMENTED),
  GEOPOS(null, UNIMPLEMENTED),
  GEODIST(null, UNIMPLEMENTED),
  GEORADIUS(null, UNIMPLEMENTED),
  GEORADIUSBYMEMBER(null, UNIMPLEMENTED),
  HELLO(null, UNIMPLEMENTED),
  INFO(null, UNIMPLEMENTED),
  LATENCY(null, UNIMPLEMENTED),
  LASTSAVE(null, UNIMPLEMENTED),
  LINDEX(null, UNIMPLEMENTED),
  LINSERT(null, UNIMPLEMENTED),
  LLEN(null, UNIMPLEMENTED),
  LOLWUT(null, UNIMPLEMENTED),
  LPOP(null, UNIMPLEMENTED),
  LPUSH(null, UNIMPLEMENTED),
  LPUSHX(null, UNIMPLEMENTED),
  LRANGE(null, UNIMPLEMENTED),
  LREM(null, UNIMPLEMENTED),
  LSET(null, UNIMPLEMENTED),
  LTRIM(null, UNIMPLEMENTED),
  MEMORY(null, UNIMPLEMENTED),
  MIGRATE(null, UNIMPLEMENTED),
  MODULE(null, UNIMPLEMENTED),
  MONITOR(null, UNIMPLEMENTED),
  MOVE(null, UNIMPLEMENTED),
  MULTI(null, UNIMPLEMENTED),
  OBJECT(null, UNIMPLEMENTED),
  PFADD(null, UNIMPLEMENTED),
  PFCOUNT(null, UNIMPLEMENTED),
  PFMERGE(null, UNIMPLEMENTED),
  PSYNC(null, UNIMPLEMENTED),
  PUBSUB(null, UNIMPLEMENTED),
  RANDOMKEY(null, UNIMPLEMENTED),
  READONLY(null, UNIMPLEMENTED),
  READWRITE(null, UNIMPLEMENTED),
  RENAMENX(null, UNIMPLEMENTED),
  RESTORE(null, UNIMPLEMENTED),
  ROLE(null, UNIMPLEMENTED),
  RPOP(null, UNIMPLEMENTED),
  RPOPLPUSH(null, UNIMPLEMENTED),
  RPUSH(null, UNIMPLEMENTED),
  RPUSHX(null, UNIMPLEMENTED),
  SAVE(null, UNIMPLEMENTED),
  SCRIPT(null, UNIMPLEMENTED),
  SELECT(null, UNIMPLEMENTED),
  SLAVEOF(null, UNIMPLEMENTED),
  REPLICAOF(null, UNIMPLEMENTED),
  SLOWLOG(null, UNIMPLEMENTED),
  SORT(null, UNIMPLEMENTED),
  STRALGO(null, UNIMPLEMENTED),
  SWAPDB(null, UNIMPLEMENTED),
  SYNC(null, UNIMPLEMENTED),
  TOUCH(null, UNIMPLEMENTED),
  UNLINK(null, UNIMPLEMENTED),
  UNWATCH(null, UNIMPLEMENTED),
  WAIT(null, UNIMPLEMENTED),
  WATCH(null, UNIMPLEMENTED),
  XINFO(null, UNIMPLEMENTED),
  XADD(null, UNIMPLEMENTED),
  XTRIM(null, UNIMPLEMENTED),
  XDEL(null, UNIMPLEMENTED),
  XRANGE(null, UNIMPLEMENTED),
  XREVRANGE(null, UNIMPLEMENTED),
  XLEN(null, UNIMPLEMENTED),
  XREAD(null, UNIMPLEMENTED),
  XGROUP(null, UNIMPLEMENTED),
  XREADGROUP(null, UNIMPLEMENTED),
  XACK(null, UNIMPLEMENTED),
  XCLAIM(null, UNIMPLEMENTED),
  XPENDING(null, UNIMPLEMENTED),
  ZADD(null, UNIMPLEMENTED),
  ZCARD(null, UNIMPLEMENTED),
  ZCOUNT(null, UNIMPLEMENTED),
  ZINCRBY(null, UNIMPLEMENTED),
  ZINTERSTORE(null, UNIMPLEMENTED),
  ZLEXCOUNT(null, UNIMPLEMENTED),
  ZPOPMAX(null, UNIMPLEMENTED),
  ZPOPMIN(null, UNIMPLEMENTED),
  ZRANGE(null, UNIMPLEMENTED),
  ZRANGEBYLEX(null, UNIMPLEMENTED),
  ZREVRANGEBYLEX(null, UNIMPLEMENTED),
  ZRANGEBYSCORE(null, UNIMPLEMENTED),
  ZRANK(null, UNIMPLEMENTED),
  ZREM(null, UNIMPLEMENTED),
  ZREMRANGEBYLEX(null, UNIMPLEMENTED),
  ZREMRANGEBYRANK(null, UNIMPLEMENTED),
  ZREMRANGEBYSCORE(null, UNIMPLEMENTED),
  ZREVRANGE(null, UNIMPLEMENTED),
  ZREVRANGEBYSCORE(null, UNIMPLEMENTED),
  ZREVRANK(null, UNIMPLEMENTED),
  ZSCORE(null, UNIMPLEMENTED),
  ZUNIONSCORE(null, UNIMPLEMENTED),
  ZSCAN(null, UNIMPLEMENTED);

  private final Executor executor;
  private final ParameterRequirements parameterRequirements;
  private final RedisCommandSupportLevel supportLevel;

  RedisCommandType(Executor executor, RedisCommandSupportLevel supportLevel) {
    this(executor, supportLevel, new UnspecifiedParameterRequirements());
  }

  RedisCommandType(Executor executor, RedisCommandSupportLevel supportLevel,
      ParameterRequirements parameterRequirements) {
    this.executor = executor;
    this.supportLevel = supportLevel;
    this.parameterRequirements = parameterRequirements;
  }

  public boolean isSupported() {
    return supportLevel == SUPPORTED;
  }

  public boolean isUnsupported() {
    return supportLevel == UNSUPPORTED;
  }

  public boolean isUnimplemented() {
    return supportLevel == UNIMPLEMENTED;
  }

  public RedisResponse executeCommand(Command command,
      ExecutionHandlerContext executionHandlerContext) {
    parameterRequirements.checkParameters(command, executionHandlerContext);

    return executor.executeCommand(command, executionHandlerContext);
  }
}

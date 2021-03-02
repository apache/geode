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

import static org.apache.geode.redis.internal.RedisCommandSupportLevel.INTERNAL;
import static org.apache.geode.redis.internal.RedisCommandSupportLevel.SUPPORTED;
import static org.apache.geode.redis.internal.RedisCommandSupportLevel.UNIMPLEMENTED;
import static org.apache.geode.redis.internal.RedisCommandSupportLevel.UNSUPPORTED;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_SYNTAX;

import org.apache.geode.redis.internal.ParameterRequirements.EvenParameterRequirements;
import org.apache.geode.redis.internal.ParameterRequirements.ExactParameterRequirements;
import org.apache.geode.redis.internal.ParameterRequirements.MaximumParameterRequirements;
import org.apache.geode.redis.internal.ParameterRequirements.MinimumParameterRequirements;
import org.apache.geode.redis.internal.ParameterRequirements.OddParameterRequirements;
import org.apache.geode.redis.internal.ParameterRequirements.ParameterRequirements;
import org.apache.geode.redis.internal.ParameterRequirements.SlowlogParameterRequirements;
import org.apache.geode.redis.internal.ParameterRequirements.SpopParameterRequirements;
import org.apache.geode.redis.internal.ParameterRequirements.UnspecifiedParameterRequirements;
import org.apache.geode.redis.internal.executor.Executor;
import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.executor.UnknownExecutor;
import org.apache.geode.redis.internal.executor.client.ClientExecutor;
import org.apache.geode.redis.internal.executor.cluster.ClusterExecutor;
import org.apache.geode.redis.internal.executor.connection.AuthExecutor;
import org.apache.geode.redis.internal.executor.connection.EchoExecutor;
import org.apache.geode.redis.internal.executor.connection.PingExecutor;
import org.apache.geode.redis.internal.executor.connection.QuitExecutor;
import org.apache.geode.redis.internal.executor.connection.SelectExecutor;
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
import org.apache.geode.redis.internal.executor.server.CommandExecutor;
import org.apache.geode.redis.internal.executor.server.DBSizeExecutor;
import org.apache.geode.redis.internal.executor.server.FlushAllExecutor;
import org.apache.geode.redis.internal.executor.server.InfoExecutor;
import org.apache.geode.redis.internal.executor.server.ReadonlyExecutor;
import org.apache.geode.redis.internal.executor.server.ShutDownExecutor;
import org.apache.geode.redis.internal.executor.server.SlowlogExecutor;
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
  DECR(new DecrExecutor(), SUPPORTED, new ExactParameterRequirements(2)),
  GET(new GetExecutor(), SUPPORTED, new ExactParameterRequirements(2)),
  INCRBY(new IncrByExecutor(), SUPPORTED, new ExactParameterRequirements(3)),
  INCR(new IncrExecutor(), SUPPORTED, new ExactParameterRequirements(2)),
  GETRANGE(new GetRangeExecutor(), SUPPORTED, new ExactParameterRequirements(4)),
  INCRBYFLOAT(new IncrByFloatExecutor(), SUPPORTED, new ExactParameterRequirements(3)),
  MGET(new MGetExecutor(), SUPPORTED, new MinimumParameterRequirements(2)),
  SET(new SetExecutor(), SUPPORTED, new MinimumParameterRequirements(3)),
  SETNX(new SetNXExecutor(), SUPPORTED, new ExactParameterRequirements(3)),
  STRLEN(new StrlenExecutor(), SUPPORTED, new ExactParameterRequirements(2)),

  /************* Hashes *****************/

  HDEL(new HDelExecutor(), SUPPORTED, new MinimumParameterRequirements(3)),
  HGET(new HGetExecutor(), SUPPORTED, new ExactParameterRequirements(3)),
  HGETALL(new HGetAllExecutor(), SUPPORTED, new ExactParameterRequirements(2)),
  HINCRBYFLOAT(new HIncrByFloatExecutor(), SUPPORTED, new ExactParameterRequirements(4)),
  HLEN(new HLenExecutor(), SUPPORTED, new ExactParameterRequirements(2)),
  HMGET(new HMGetExecutor(), SUPPORTED, new MinimumParameterRequirements(3)),
  HMSET(new HMSetExecutor(), SUPPORTED,
      new MinimumParameterRequirements(4).and(new EvenParameterRequirements())),
  HSET(new HSetExecutor(), SUPPORTED,
      new MinimumParameterRequirements(4).and(new EvenParameterRequirements())),
  HSETNX(new HSetNXExecutor(), SUPPORTED, new ExactParameterRequirements(4)),
  HSTRLEN(new HStrLenExecutor(), SUPPORTED, new ExactParameterRequirements(3)),
  HINCRBY(new HIncrByExecutor(), SUPPORTED, new ExactParameterRequirements(4)),
  HVALS(new HValsExecutor(), SUPPORTED, new ExactParameterRequirements(2)),
  HEXISTS(new HExistsExecutor(), SUPPORTED, new ExactParameterRequirements(3)),
  HKEYS(new HKeysExecutor(), SUPPORTED, new ExactParameterRequirements(2)),

  /************* Sets *****************/

  SADD(new SAddExecutor(), SUPPORTED, new MinimumParameterRequirements(3)),
  SMEMBERS(new SMembersExecutor(), SUPPORTED, new ExactParameterRequirements(2)),
  SREM(new SRemExecutor(), SUPPORTED, new MinimumParameterRequirements(3)),

  /********** Publish Subscribe **********/
  SUBSCRIBE(new SubscribeExecutor(), SUPPORTED, new MinimumParameterRequirements(2)),
  PUBLISH(new PublishExecutor(), SUPPORTED, new ExactParameterRequirements(3)),
  PSUBSCRIBE(new PsubscribeExecutor(), SUPPORTED, new MinimumParameterRequirements(2)),
  PUNSUBSCRIBE(new PunsubscribeExecutor(), SUPPORTED, new MinimumParameterRequirements(1)),
  UNSUBSCRIBE(new UnsubscribeExecutor(), SUPPORTED, new MinimumParameterRequirements(1)),

  /***************************************
   ********* Internal Commands ***********
   ***************************************/
  // do not call these directly, only to be used in other commands
  INTERNALPTTL(new UnknownExecutor(), INTERNAL, new ExactParameterRequirements(2)),
  INTERNALTYPE(new UnknownExecutor(), INTERNAL, new ExactParameterRequirements(2)),
  INTERNALSMEMBERS(new UnknownExecutor(), INTERNAL, new ExactParameterRequirements(3)),

  /***************************************
   *** Unsupported Commands ***
   ***************************************/

  /***************************************
   *************** Connection *************
   ***************************************/

  ECHO(new EchoExecutor(), UNSUPPORTED, new ExactParameterRequirements(2)),
  SELECT(new SelectExecutor(), UNSUPPORTED, new ExactParameterRequirements(2)),

  /***************************************
   *************** Keys ******************
   ***************************************/

  SCAN(new ScanExecutor(), UNSUPPORTED, new EvenParameterRequirements(ERROR_SYNTAX).and(new MinimumParameterRequirements(2))),
  UNLINK(new DelExecutor(), UNSUPPORTED, new MinimumParameterRequirements(2)),

  /***************************************
   ************** Strings ****************
   ***************************************/

  BITCOUNT(new BitCountExecutor(), UNSUPPORTED, new MinimumParameterRequirements(2)),
  BITOP(new BitOpExecutor(), UNSUPPORTED, new MinimumParameterRequirements(4)),
  BITPOS(new BitPosExecutor(), UNSUPPORTED, new MinimumParameterRequirements(3)),
  DECRBY(new DecrByExecutor(), UNSUPPORTED, new ExactParameterRequirements(3)),
  GETBIT(new GetBitExecutor(), UNSUPPORTED, new ExactParameterRequirements(3)),
  GETSET(new GetSetExecutor(), UNSUPPORTED, new ExactParameterRequirements(3)),
  MSET(new MSetExecutor(), UNSUPPORTED,
      new MinimumParameterRequirements(3).and(new OddParameterRequirements())),
  MSETNX(new MSetNXExecutor(), UNSUPPORTED,
      new MinimumParameterRequirements(3).and(new OddParameterRequirements())),
  PSETEX(new PSetEXExecutor(), UNSUPPORTED, new ExactParameterRequirements(4)),
  SETBIT(new SetBitExecutor(), UNSUPPORTED, new ExactParameterRequirements(4)),
  SETEX(new SetEXExecutor(), UNSUPPORTED, new ExactParameterRequirements(4)),
  SETRANGE(new SetRangeExecutor(), UNSUPPORTED, new ExactParameterRequirements(4)),

  /***************************************
   **************** Hashes ***************
   ***************************************/

  HSCAN(new HScanExecutor(), UNSUPPORTED, new MinimumParameterRequirements(3),
      new OddParameterRequirements(ERROR_SYNTAX)),

  /***************************************
   **************** Sets *****************
   ***************************************/

  SCARD(new SCardExecutor(), UNSUPPORTED, new ExactParameterRequirements(2)),
  SDIFF(new SDiffExecutor(), UNSUPPORTED, new MinimumParameterRequirements(2)),
  SDIFFSTORE(new SDiffStoreExecutor(), UNSUPPORTED, new MinimumParameterRequirements(3)),
  SINTER(new SInterExecutor(), UNSUPPORTED, new MinimumParameterRequirements(2)),
  SINTERSTORE(new SInterStoreExecutor(), UNSUPPORTED, new MinimumParameterRequirements(3)),
  SISMEMBER(new SIsMemberExecutor(), UNSUPPORTED, new ExactParameterRequirements(3)),
  SMOVE(new SMoveExecutor(), UNSUPPORTED, new ExactParameterRequirements(4)),
  SPOP(new SPopExecutor(), UNSUPPORTED, new MinimumParameterRequirements(2)
      .and(new MaximumParameterRequirements(3, ERROR_SYNTAX)).and(new SpopParameterRequirements())),
  SRANDMEMBER(new SRandMemberExecutor(), UNSUPPORTED, new MinimumParameterRequirements(2)),
  SSCAN(new SScanExecutor(), UNSUPPORTED, new MinimumParameterRequirements(3),
      new OddParameterRequirements(ERROR_SYNTAX)),
  SUNION(new SUnionExecutor(), UNSUPPORTED, new MinimumParameterRequirements(2)),
  SUNIONSTORE(new SUnionStoreExecutor(), UNSUPPORTED, new MinimumParameterRequirements(3)),

  /***************************************
   *************** Server ****************
   ***************************************/

  COMMAND(new CommandExecutor(), UNSUPPORTED, new ExactParameterRequirements(1)),
  DBSIZE(new DBSizeExecutor(), UNSUPPORTED, new ExactParameterRequirements(1)),
  FLUSHALL(new FlushAllExecutor(), UNSUPPORTED, new MaximumParameterRequirements(2, ERROR_SYNTAX)),
  FLUSHDB(new FlushAllExecutor(), UNSUPPORTED, new MaximumParameterRequirements(2, ERROR_SYNTAX)),
  INFO(new InfoExecutor(), UNSUPPORTED, new MaximumParameterRequirements(2, ERROR_SYNTAX)),
  SHUTDOWN(new ShutDownExecutor(), UNSUPPORTED, new MaximumParameterRequirements(2, ERROR_SYNTAX)),
  SLOWLOG(new SlowlogExecutor(), UNSUPPORTED, new SlowlogParameterRequirements()),
  TIME(new TimeExecutor(), UNSUPPORTED, new ExactParameterRequirements(1)),

  /*********** CLUSTER **********/
  CLUSTER(new ClusterExecutor(), UNSUPPORTED, new MinimumParameterRequirements(1)),
  READONLY(new ReadonlyExecutor(), UNSUPPORTED, new MinimumParameterRequirements(1)),

  /*********** Sorted Sets **********/
  ZADD(new ZaddExecutor(), SUPPORTED, new MinimumParameterRequirements(1)),

  /************ CLIENT *************/
  CLIENT(new ClientExecutor(), UNSUPPORTED, new MinimumParameterRequirements(1)),

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
  SLAVEOF(null, UNIMPLEMENTED),
  REPLICAOF(null, UNIMPLEMENTED),
  SORT(null, UNIMPLEMENTED),
  STRALGO(null, UNIMPLEMENTED),
  SWAPDB(null, UNIMPLEMENTED),
  SYNC(null, UNIMPLEMENTED),
  TOUCH(null, UNIMPLEMENTED),
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
  ZSCAN(null, UNIMPLEMENTED),

  /***************************************
   *** Unknown Commands ***
   ***************************************/
  UNKNOWN(new UnknownExecutor(), RedisCommandSupportLevel.UNKNOWN);

  private final Executor executor;
  private final ParameterRequirements parameterRequirements;
  private final ParameterRequirements deferredParameterRequirements;
  private final RedisCommandSupportLevel supportLevel;

  RedisCommandType(Executor executor, RedisCommandSupportLevel supportLevel) {
    this(executor, supportLevel, new UnspecifiedParameterRequirements());
  }

  RedisCommandType(Executor executor, RedisCommandSupportLevel supportLevel,
      ParameterRequirements parameterRequirements) {
    this(executor, supportLevel, parameterRequirements, new UnspecifiedParameterRequirements());
  }

  RedisCommandType(Executor executor, RedisCommandSupportLevel supportLevel,
      ParameterRequirements parameterRequirements,
      ParameterRequirements deferredParameterRequirements) {
    this.executor = executor;
    this.supportLevel = supportLevel;
    this.parameterRequirements = parameterRequirements;
    this.deferredParameterRequirements = deferredParameterRequirements;
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

  public boolean isInternal() {
    return supportLevel == INTERNAL;
  }

  public boolean isUnknown() {
    return supportLevel == RedisCommandSupportLevel.UNKNOWN;
  }

  public boolean isAllowedWhileSubscribed() {
    switch (this) {
      case SUBSCRIBE:
      case PSUBSCRIBE:
      case UNSUBSCRIBE:
      case PUNSUBSCRIBE:
      case PING:
      case QUIT:
        return true;
      default:
        return false;
    }
  }

  public void checkDeferredParameters(Command command,
      ExecutionHandlerContext executionHandlerContext) {
    deferredParameterRequirements.checkParameters(command, executionHandlerContext);
  }

  public RedisResponse executeCommand(Command command,
      ExecutionHandlerContext executionHandlerContext) {

    parameterRequirements.checkParameters(command, executionHandlerContext);

    return executor.executeCommand(command, executionHandlerContext);
  }
}

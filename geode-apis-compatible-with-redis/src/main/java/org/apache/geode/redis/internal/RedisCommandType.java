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
import static org.apache.geode.redis.internal.RedisCommandSupportLevel.UNSUPPORTED;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_SYNTAX;

import org.apache.geode.redis.internal.executor.Executor;
import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.executor.UnknownExecutor;
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
import org.apache.geode.redis.internal.executor.key.DumpExecutor;
import org.apache.geode.redis.internal.executor.key.ExistsExecutor;
import org.apache.geode.redis.internal.executor.key.ExpireAtExecutor;
import org.apache.geode.redis.internal.executor.key.ExpireExecutor;
import org.apache.geode.redis.internal.executor.key.KeysExecutor;
import org.apache.geode.redis.internal.executor.key.PExpireAtExecutor;
import org.apache.geode.redis.internal.executor.key.PExpireExecutor;
import org.apache.geode.redis.internal.executor.key.PTTLExecutor;
import org.apache.geode.redis.internal.executor.key.PersistExecutor;
import org.apache.geode.redis.internal.executor.key.RenameExecutor;
import org.apache.geode.redis.internal.executor.key.RestoreExecutor;
import org.apache.geode.redis.internal.executor.key.ScanExecutor;
import org.apache.geode.redis.internal.executor.key.TTLExecutor;
import org.apache.geode.redis.internal.executor.key.TypeExecutor;
import org.apache.geode.redis.internal.executor.pubsub.PsubscribeExecutor;
import org.apache.geode.redis.internal.executor.pubsub.PubSubExecutor;
import org.apache.geode.redis.internal.executor.pubsub.PublishExecutor;
import org.apache.geode.redis.internal.executor.pubsub.PunsubscribeExecutor;
import org.apache.geode.redis.internal.executor.pubsub.SubscribeExecutor;
import org.apache.geode.redis.internal.executor.pubsub.UnsubscribeExecutor;
import org.apache.geode.redis.internal.executor.server.CommandExecutor;
import org.apache.geode.redis.internal.executor.server.DBSizeExecutor;
import org.apache.geode.redis.internal.executor.server.FlushAllExecutor;
import org.apache.geode.redis.internal.executor.server.InfoExecutor;
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
import org.apache.geode.redis.internal.executor.sortedset.ZAddExecutor;
import org.apache.geode.redis.internal.executor.sortedset.ZCardExecutor;
import org.apache.geode.redis.internal.executor.sortedset.ZCountExecutor;
import org.apache.geode.redis.internal.executor.sortedset.ZIncrByExecutor;
import org.apache.geode.redis.internal.executor.sortedset.ZInterStoreExecutor;
import org.apache.geode.redis.internal.executor.sortedset.ZLexCountExecutor;
import org.apache.geode.redis.internal.executor.sortedset.ZPopMaxExecutor;
import org.apache.geode.redis.internal.executor.sortedset.ZPopMinExecutor;
import org.apache.geode.redis.internal.executor.sortedset.ZRangeByLexExecutor;
import org.apache.geode.redis.internal.executor.sortedset.ZRangeByScoreExecutor;
import org.apache.geode.redis.internal.executor.sortedset.ZRangeExecutor;
import org.apache.geode.redis.internal.executor.sortedset.ZRankExecutor;
import org.apache.geode.redis.internal.executor.sortedset.ZRemExecutor;
import org.apache.geode.redis.internal.executor.sortedset.ZRemRangeByLexExecutor;
import org.apache.geode.redis.internal.executor.sortedset.ZRemRangeByRankExecutor;
import org.apache.geode.redis.internal.executor.sortedset.ZRemRangeByScoreExecutor;
import org.apache.geode.redis.internal.executor.sortedset.ZRevRangeByLexExecutor;
import org.apache.geode.redis.internal.executor.sortedset.ZRevRangeByScoreExecutor;
import org.apache.geode.redis.internal.executor.sortedset.ZRevRangeExecutor;
import org.apache.geode.redis.internal.executor.sortedset.ZRevRankExecutor;
import org.apache.geode.redis.internal.executor.sortedset.ZScanExecutor;
import org.apache.geode.redis.internal.executor.sortedset.ZScoreExecutor;
import org.apache.geode.redis.internal.executor.sortedset.ZUnionStoreExecutor;
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
import org.apache.geode.redis.internal.parameters.ClusterParameterRequirements;
import org.apache.geode.redis.internal.parameters.Parameter;
import org.apache.geode.redis.internal.parameters.SlowlogParameterRequirements;
import org.apache.geode.redis.internal.parameters.SpopParameterRequirements;

/**
 * The redis command type used by the server. Each command is directly from the redis protocol.
 */
public enum RedisCommandType {

  /***************************************
   *** Supported Commands ***
   ***************************************/

  /*************** Connection ****************/
  AUTH(new AuthExecutor(), SUPPORTED, new Parameter().min(2).max(3, ERROR_SYNTAX).firstKey(0)),
  ECHO(new EchoExecutor(), SUPPORTED, new Parameter().exact(2).firstKey(0)),
  PING(new PingExecutor(), SUPPORTED, new Parameter().min(1).max(2).firstKey(0)),
  QUIT(new QuitExecutor(), SUPPORTED, new Parameter().firstKey(0)),

  /*************** Keys ******************/

  DEL(new DelExecutor(), SUPPORTED, new Parameter().min(2).lastKey(-1)),
  DUMP(new DumpExecutor(), SUPPORTED, new Parameter().exact(2)),
  EXISTS(new ExistsExecutor(), SUPPORTED, new Parameter().min(2).lastKey(-1)),
  EXPIRE(new ExpireExecutor(), SUPPORTED, new Parameter().exact(3)),
  EXPIREAT(new ExpireAtExecutor(), SUPPORTED, new Parameter().exact(3)),
  KEYS(new KeysExecutor(), SUPPORTED, new Parameter().exact(2).firstKey(0)),
  PERSIST(new PersistExecutor(), SUPPORTED, new Parameter().exact(2)),
  PEXPIRE(new PExpireExecutor(), SUPPORTED, new Parameter().exact(3)),
  PEXPIREAT(new PExpireAtExecutor(), SUPPORTED, new Parameter().exact(3)),
  PTTL(new PTTLExecutor(), SUPPORTED, new Parameter().exact(2)),
  RENAME(new RenameExecutor(), SUPPORTED, new Parameter().exact(3).lastKey(2)),
  RESTORE(new RestoreExecutor(), SUPPORTED, new Parameter().min(4)),
  TTL(new TTLExecutor(), SUPPORTED, new Parameter().exact(2)),
  TYPE(new TypeExecutor(), SUPPORTED, new Parameter().exact(2)),

  /************* Strings *****************/

  APPEND(new AppendExecutor(), SUPPORTED, new Parameter().exact(3)),
  DECR(new DecrExecutor(), SUPPORTED, new Parameter().exact(2)),
  DECRBY(new DecrByExecutor(), SUPPORTED, new Parameter().exact(3)),
  GET(new GetExecutor(), SUPPORTED, new Parameter().exact(2)),
  GETSET(new GetSetExecutor(), SUPPORTED, new Parameter().exact(3)),
  INCRBY(new IncrByExecutor(), SUPPORTED, new Parameter().exact(3)),
  INCR(new IncrExecutor(), SUPPORTED, new Parameter().exact(2)),
  GETRANGE(new GetRangeExecutor(), SUPPORTED, new Parameter().exact(4)),
  INCRBYFLOAT(new IncrByFloatExecutor(), SUPPORTED, new Parameter().exact(3)),
  MGET(new MGetExecutor(), SUPPORTED, new Parameter().min(2).lastKey(-1)),
  MSET(new MSetExecutor(), UNSUPPORTED, new Parameter().min(3).odd().lastKey(-1).step(2)),
  PSETEX(new PSetEXExecutor(), SUPPORTED, new Parameter().exact(4)),
  SET(new SetExecutor(), SUPPORTED, new Parameter().min(3)),
  SETEX(new SetEXExecutor(), SUPPORTED, new Parameter().exact(4)),
  SETNX(new SetNXExecutor(), SUPPORTED, new Parameter().exact(3)),
  SETRANGE(new SetRangeExecutor(), SUPPORTED, new Parameter().exact(4)),
  STRLEN(new StrlenExecutor(), SUPPORTED, new Parameter().exact(2)),

  /************* Hashes *****************/

  HDEL(new HDelExecutor(), SUPPORTED, new Parameter().min(3)),
  HGET(new HGetExecutor(), SUPPORTED, new Parameter().exact(3)),
  HGETALL(new HGetAllExecutor(), SUPPORTED, new Parameter().exact(2)),
  HINCRBYFLOAT(new HIncrByFloatExecutor(), SUPPORTED, new Parameter().exact(4)),
  HLEN(new HLenExecutor(), SUPPORTED, new Parameter().exact(2)),
  HMGET(new HMGetExecutor(), SUPPORTED, new Parameter().min(3)),
  HMSET(new HMSetExecutor(), SUPPORTED, new Parameter().min(4).even()),
  HSET(new HSetExecutor(), SUPPORTED, new Parameter().min(4).even()),
  HSETNX(new HSetNXExecutor(), SUPPORTED, new Parameter().exact(4)),
  HSTRLEN(new HStrLenExecutor(), SUPPORTED, new Parameter().exact(3)),
  HINCRBY(new HIncrByExecutor(), SUPPORTED, new Parameter().exact(4)),
  HVALS(new HValsExecutor(), SUPPORTED, new Parameter().exact(2)),
  HSCAN(new HScanExecutor(), SUPPORTED, new Parameter().min(3).odd()),
  HEXISTS(new HExistsExecutor(), SUPPORTED, new Parameter().exact(3)),
  HKEYS(new HKeysExecutor(), SUPPORTED, new Parameter().exact(2)),

  /************* Sets *****************/

  SADD(new SAddExecutor(), SUPPORTED, new Parameter().min(3)),
  SMEMBERS(new SMembersExecutor(), SUPPORTED, new Parameter().exact(2)),
  SREM(new SRemExecutor(), SUPPORTED, new Parameter().min(3)),

  /************ Sorted Sets **************/

  ZADD(new ZAddExecutor(), SUPPORTED, new Parameter().min(4)),
  ZCARD(new ZCardExecutor(), SUPPORTED, new Parameter().exact(2)),
  ZCOUNT(new ZCountExecutor(), SUPPORTED, new Parameter().exact(4)),
  ZINCRBY(new ZIncrByExecutor(), SUPPORTED, new Parameter().exact(4)),
  ZINTERSTORE(new ZInterStoreExecutor(), SUPPORTED, new Parameter().min(4)),
  ZLEXCOUNT(new ZLexCountExecutor(), SUPPORTED, new Parameter().exact(4)),
  ZPOPMAX(new ZPopMaxExecutor(), SUPPORTED, new Parameter().min(2).max(3, ERROR_SYNTAX)),
  ZPOPMIN(new ZPopMinExecutor(), SUPPORTED, new Parameter().min(2).max(3, ERROR_SYNTAX)),
  ZRANGE(new ZRangeExecutor(), SUPPORTED, new Parameter().min(4).max(5, ERROR_SYNTAX)),
  ZRANGEBYLEX(new ZRangeByLexExecutor(), SUPPORTED, new Parameter().min(4)),
  ZRANGEBYSCORE(new ZRangeByScoreExecutor(), SUPPORTED, new Parameter().min(4)),
  ZRANK(new ZRankExecutor(), SUPPORTED, new Parameter().exact(3)),
  ZREM(new ZRemExecutor(), SUPPORTED, new Parameter().min(3)),
  ZREMRANGEBYLEX(new ZRemRangeByLexExecutor(), SUPPORTED, new Parameter().exact(4)),
  ZREMRANGEBYRANK(new ZRemRangeByRankExecutor(), SUPPORTED, new Parameter().exact(4)),
  ZREMRANGEBYSCORE(new ZRemRangeByScoreExecutor(), SUPPORTED, new Parameter().exact(4)),
  ZREVRANGE(new ZRevRangeExecutor(), SUPPORTED, new Parameter().min(4).max(5, ERROR_SYNTAX)),
  ZREVRANGEBYLEX(new ZRevRangeByLexExecutor(), SUPPORTED, new Parameter().min(4)),
  ZREVRANGEBYSCORE(new ZRevRangeByScoreExecutor(), SUPPORTED, new Parameter().min(4)),
  ZREVRANK(new ZRevRankExecutor(), SUPPORTED, new Parameter().exact(3)),
  ZSCAN(new ZScanExecutor(), SUPPORTED, new Parameter().min(3).odd(ERROR_SYNTAX)),
  ZSCORE(new ZScoreExecutor(), SUPPORTED, new Parameter().exact(3)),
  ZUNIONSTORE(new ZUnionStoreExecutor(), SUPPORTED, new Parameter().min(4)),

  /************* Server *****************/
  COMMAND(new CommandExecutor(), SUPPORTED, new Parameter().min(1).firstKey(0)),
  SLOWLOG(new SlowlogExecutor(), SUPPORTED, new Parameter().min(2)
      .custom(SlowlogParameterRequirements.checkParameters()).firstKey(0)),
  INFO(new InfoExecutor(), SUPPORTED, new Parameter().min(1).max(2, ERROR_SYNTAX).firstKey(0)),

  /********** Publish Subscribe **********/
  SUBSCRIBE(new SubscribeExecutor(), SUPPORTED, new Parameter().min(2).firstKey(0)),
  PUBLISH(new PublishExecutor(), SUPPORTED, new Parameter().exact(3).firstKey(0)),
  PSUBSCRIBE(new PsubscribeExecutor(), SUPPORTED, new Parameter().min(2).firstKey(0)),
  PUNSUBSCRIBE(new PunsubscribeExecutor(), SUPPORTED, new Parameter().min(1).firstKey(0)),
  UNSUBSCRIBE(new UnsubscribeExecutor(), SUPPORTED, new Parameter().min(1).firstKey(0)),
  PUBSUB(new PubSubExecutor(), SUPPORTED, new Parameter().min(2).firstKey(0)),

  /************* Cluster *****************/
  CLUSTER(new ClusterExecutor(), SUPPORTED, new Parameter().min(2)
      .custom(ClusterParameterRequirements.checkParameters()).firstKey(0)),

  /***************************************
   ********* Internal Commands ***********
   ***************************************/
  // do not call these directly, only to be used in other commands
  INTERNALPTTL(new UnknownExecutor(), INTERNAL, new Parameter().exact(2)),
  INTERNALTYPE(new UnknownExecutor(), INTERNAL, new Parameter().exact(2)),
  INTERNALSMEMBERS(new UnknownExecutor(), INTERNAL, new Parameter().exact(3)),

  /***************************************
   ******** Unsupported Commands *********
   ***************************************/

  /*************** Connection *************/

  SELECT(new SelectExecutor(), UNSUPPORTED, new Parameter().exact(2).firstKey(0)),

  /*************** Keys ******************/

  SCAN(new ScanExecutor(), UNSUPPORTED, new Parameter().min(2).even(ERROR_SYNTAX).firstKey(0)),
  UNLINK(new DelExecutor(), UNSUPPORTED, new Parameter().min(2).lastKey(-1)),

  /************** Strings ****************/

  BITCOUNT(new BitCountExecutor(), UNSUPPORTED, new Parameter().min(2)),
  BITOP(new BitOpExecutor(), UNSUPPORTED, new Parameter().min(4).firstKey(2).lastKey(-1)),
  BITPOS(new BitPosExecutor(), UNSUPPORTED, new Parameter().min(3)),
  GETBIT(new GetBitExecutor(), UNSUPPORTED, new Parameter().exact(3)),
  MSETNX(new MSetNXExecutor(), UNSUPPORTED, new Parameter().min(3).odd().lastKey(-1).step(2)),
  SETBIT(new SetBitExecutor(), UNSUPPORTED, new Parameter().exact(4)),

  /**************** Sets *****************/

  SCARD(new SCardExecutor(), UNSUPPORTED, new Parameter().exact(2)),
  SDIFF(new SDiffExecutor(), UNSUPPORTED, new Parameter().min(2).lastKey(-1)),
  SDIFFSTORE(new SDiffStoreExecutor(), UNSUPPORTED, new Parameter().min(3).lastKey(-1)),
  SINTER(new SInterExecutor(), UNSUPPORTED, new Parameter().min(2).lastKey(-1)),
  SINTERSTORE(new SInterStoreExecutor(), UNSUPPORTED, new Parameter().min(3).lastKey(-1)),
  SISMEMBER(new SIsMemberExecutor(), UNSUPPORTED, new Parameter().exact(3)),
  SMOVE(new SMoveExecutor(), UNSUPPORTED, new Parameter().exact(4).lastKey(2)),
  SPOP(new SPopExecutor(), UNSUPPORTED, new Parameter().min(2).max(3)
      .custom(SpopParameterRequirements.checkParameters())),
  SRANDMEMBER(new SRandMemberExecutor(), UNSUPPORTED, new Parameter().min(2)),
  SSCAN(new SScanExecutor(), UNSUPPORTED, new Parameter().min(3),
      new Parameter().odd(ERROR_SYNTAX).firstKey(0)),
  SUNION(new SUnionExecutor(), UNSUPPORTED, new Parameter().min(2).lastKey(-1)),
  SUNIONSTORE(new SUnionStoreExecutor(), UNSUPPORTED, new Parameter().min(3).lastKey(-1)),

  /*************** Server ****************/

  DBSIZE(new DBSizeExecutor(), UNSUPPORTED, new Parameter().exact(1).firstKey(0)),
  FLUSHALL(new FlushAllExecutor(), UNSUPPORTED,
      new Parameter().min(1).max(2, ERROR_SYNTAX).firstKey(0)),
  FLUSHDB(new FlushAllExecutor(), UNSUPPORTED,
      new Parameter().min(1).max(2, ERROR_SYNTAX).firstKey(0)),
  SHUTDOWN(new ShutDownExecutor(), UNSUPPORTED,
      new Parameter().min(1).max(2, ERROR_SYNTAX).firstKey(0)),
  TIME(new TimeExecutor(), UNSUPPORTED, new Parameter().exact(1).firstKey(0)),

  /***************************************
   *********** Unknown Commands **********
   ***************************************/
  UNKNOWN(new UnknownExecutor(), RedisCommandSupportLevel.UNKNOWN);

  private final Executor executor;
  private final Parameter parameterRequirements;
  private final Parameter deferredParameterRequirements;
  private final RedisCommandSupportLevel supportLevel;

  RedisCommandType(Executor executor, RedisCommandSupportLevel supportLevel) {
    this(executor, supportLevel, new Parameter().custom((c, e) -> {
    }));
  }

  RedisCommandType(Executor executor, RedisCommandSupportLevel supportLevel,
      Parameter parameterRequirements) {
    this(executor, supportLevel, parameterRequirements, new Parameter().custom((c, e) -> {
    }));
  }

  RedisCommandType(Executor executor, RedisCommandSupportLevel supportLevel,
      Parameter parameterRequirements,
      Parameter deferredParameterRequirements) {
    this.executor = executor;
    this.supportLevel = supportLevel;
    this.parameterRequirements = parameterRequirements;
    this.deferredParameterRequirements = deferredParameterRequirements;
  }

  public int arity() {
    return parameterRequirements.getArity();
  }

  public int firstKey() {
    return parameterRequirements.getFirstKey();
  }

  public int lastKey() {
    return parameterRequirements.getLastKey();
  }

  public int step() {
    return parameterRequirements.getStep();
  }

  public boolean isSupported() {
    return supportLevel == SUPPORTED;
  }

  public boolean isUnsupported() {
    return supportLevel == UNSUPPORTED;
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
      ExecutionHandlerContext executionHandlerContext) throws Exception {

    parameterRequirements.checkParameters(command, executionHandlerContext);

    return executor.executeCommand(command, executionHandlerContext);
  }
}

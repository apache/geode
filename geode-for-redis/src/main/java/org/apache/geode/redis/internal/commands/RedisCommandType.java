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

package org.apache.geode.redis.internal.commands;

import static org.apache.geode.redis.internal.RedisConstants.ERROR_SYNTAX;
import static org.apache.geode.redis.internal.commands.RedisCommandSupportLevel.INTERNAL;
import static org.apache.geode.redis.internal.commands.RedisCommandSupportLevel.SUPPORTED;
import static org.apache.geode.redis.internal.commands.RedisCommandSupportLevel.UNSUPPORTED;
import static org.apache.geode.redis.internal.commands.RedisCommandType.Flag.ADMIN;
import static org.apache.geode.redis.internal.commands.RedisCommandType.Flag.DENYOOM;
import static org.apache.geode.redis.internal.commands.RedisCommandType.Flag.FAST;
import static org.apache.geode.redis.internal.commands.RedisCommandType.Flag.LOADING;
import static org.apache.geode.redis.internal.commands.RedisCommandType.Flag.MAY_REPLICATE;
import static org.apache.geode.redis.internal.commands.RedisCommandType.Flag.MOVABLEKEYS;
import static org.apache.geode.redis.internal.commands.RedisCommandType.Flag.NOSCRIPT;
import static org.apache.geode.redis.internal.commands.RedisCommandType.Flag.NO_AUTH;
import static org.apache.geode.redis.internal.commands.RedisCommandType.Flag.RANDOM;
import static org.apache.geode.redis.internal.commands.RedisCommandType.Flag.READONLY;
import static org.apache.geode.redis.internal.commands.RedisCommandType.Flag.SORT_FOR_SCRIPT;
import static org.apache.geode.redis.internal.commands.RedisCommandType.Flag.STALE;
import static org.apache.geode.redis.internal.commands.RedisCommandType.Flag.WRITE;

import java.util.Set;

import org.apache.geode.redis.internal.commands.executor.CommandExecutor;
import org.apache.geode.redis.internal.commands.executor.RedisResponse;
import org.apache.geode.redis.internal.commands.executor.UnknownExecutor;
import org.apache.geode.redis.internal.commands.executor.cluster.ClusterExecutor;
import org.apache.geode.redis.internal.commands.executor.connection.AuthExecutor;
import org.apache.geode.redis.internal.commands.executor.connection.ClientExecutor;
import org.apache.geode.redis.internal.commands.executor.connection.EchoExecutor;
import org.apache.geode.redis.internal.commands.executor.connection.PingExecutor;
import org.apache.geode.redis.internal.commands.executor.connection.QuitExecutor;
import org.apache.geode.redis.internal.commands.executor.connection.SelectExecutor;
import org.apache.geode.redis.internal.commands.executor.hash.HDelExecutor;
import org.apache.geode.redis.internal.commands.executor.hash.HExistsExecutor;
import org.apache.geode.redis.internal.commands.executor.hash.HGetAllExecutor;
import org.apache.geode.redis.internal.commands.executor.hash.HGetExecutor;
import org.apache.geode.redis.internal.commands.executor.hash.HIncrByExecutor;
import org.apache.geode.redis.internal.commands.executor.hash.HIncrByFloatExecutor;
import org.apache.geode.redis.internal.commands.executor.hash.HKeysExecutor;
import org.apache.geode.redis.internal.commands.executor.hash.HLenExecutor;
import org.apache.geode.redis.internal.commands.executor.hash.HMGetExecutor;
import org.apache.geode.redis.internal.commands.executor.hash.HMSetExecutor;
import org.apache.geode.redis.internal.commands.executor.hash.HScanExecutor;
import org.apache.geode.redis.internal.commands.executor.hash.HSetExecutor;
import org.apache.geode.redis.internal.commands.executor.hash.HSetNXExecutor;
import org.apache.geode.redis.internal.commands.executor.hash.HStrLenExecutor;
import org.apache.geode.redis.internal.commands.executor.hash.HValsExecutor;
import org.apache.geode.redis.internal.commands.executor.key.DelExecutor;
import org.apache.geode.redis.internal.commands.executor.key.DumpExecutor;
import org.apache.geode.redis.internal.commands.executor.key.ExistsExecutor;
import org.apache.geode.redis.internal.commands.executor.key.ExpireAtExecutor;
import org.apache.geode.redis.internal.commands.executor.key.ExpireExecutor;
import org.apache.geode.redis.internal.commands.executor.key.KeysExecutor;
import org.apache.geode.redis.internal.commands.executor.key.PExpireAtExecutor;
import org.apache.geode.redis.internal.commands.executor.key.PExpireExecutor;
import org.apache.geode.redis.internal.commands.executor.key.PTTLExecutor;
import org.apache.geode.redis.internal.commands.executor.key.PersistExecutor;
import org.apache.geode.redis.internal.commands.executor.key.RenameExecutor;
import org.apache.geode.redis.internal.commands.executor.key.RenameNXExecutor;
import org.apache.geode.redis.internal.commands.executor.key.RestoreExecutor;
import org.apache.geode.redis.internal.commands.executor.key.ScanExecutor;
import org.apache.geode.redis.internal.commands.executor.key.TTLExecutor;
import org.apache.geode.redis.internal.commands.executor.key.TypeExecutor;
import org.apache.geode.redis.internal.commands.executor.pubsub.PsubscribeExecutor;
import org.apache.geode.redis.internal.commands.executor.pubsub.PubSubExecutor;
import org.apache.geode.redis.internal.commands.executor.pubsub.PublishExecutor;
import org.apache.geode.redis.internal.commands.executor.pubsub.PunsubscribeExecutor;
import org.apache.geode.redis.internal.commands.executor.pubsub.SubscribeExecutor;
import org.apache.geode.redis.internal.commands.executor.pubsub.UnsubscribeExecutor;
import org.apache.geode.redis.internal.commands.executor.server.COMMANDCommandExecutor;
import org.apache.geode.redis.internal.commands.executor.server.DBSizeExecutor;
import org.apache.geode.redis.internal.commands.executor.server.FlushAllExecutor;
import org.apache.geode.redis.internal.commands.executor.server.InfoExecutor;
import org.apache.geode.redis.internal.commands.executor.server.LolWutExecutor;
import org.apache.geode.redis.internal.commands.executor.server.SlowlogExecutor;
import org.apache.geode.redis.internal.commands.executor.server.TimeExecutor;
import org.apache.geode.redis.internal.commands.executor.set.SAddExecutor;
import org.apache.geode.redis.internal.commands.executor.set.SCardExecutor;
import org.apache.geode.redis.internal.commands.executor.set.SDiffExecutor;
import org.apache.geode.redis.internal.commands.executor.set.SDiffStoreExecutor;
import org.apache.geode.redis.internal.commands.executor.set.SInterExecutor;
import org.apache.geode.redis.internal.commands.executor.set.SInterStoreExecutor;
import org.apache.geode.redis.internal.commands.executor.set.SIsMemberExecutor;
import org.apache.geode.redis.internal.commands.executor.set.SMembersExecutor;
import org.apache.geode.redis.internal.commands.executor.set.SMoveExecutor;
import org.apache.geode.redis.internal.commands.executor.set.SPopExecutor;
import org.apache.geode.redis.internal.commands.executor.set.SRandMemberExecutor;
import org.apache.geode.redis.internal.commands.executor.set.SRemExecutor;
import org.apache.geode.redis.internal.commands.executor.set.SScanExecutor;
import org.apache.geode.redis.internal.commands.executor.set.SUnionExecutor;
import org.apache.geode.redis.internal.commands.executor.set.SUnionStoreExecutor;
import org.apache.geode.redis.internal.commands.executor.sortedset.ZAddExecutor;
import org.apache.geode.redis.internal.commands.executor.sortedset.ZCardExecutor;
import org.apache.geode.redis.internal.commands.executor.sortedset.ZCountExecutor;
import org.apache.geode.redis.internal.commands.executor.sortedset.ZIncrByExecutor;
import org.apache.geode.redis.internal.commands.executor.sortedset.ZInterStoreExecutor;
import org.apache.geode.redis.internal.commands.executor.sortedset.ZLexCountExecutor;
import org.apache.geode.redis.internal.commands.executor.sortedset.ZPopMaxExecutor;
import org.apache.geode.redis.internal.commands.executor.sortedset.ZPopMinExecutor;
import org.apache.geode.redis.internal.commands.executor.sortedset.ZRangeByLexExecutor;
import org.apache.geode.redis.internal.commands.executor.sortedset.ZRangeByScoreExecutor;
import org.apache.geode.redis.internal.commands.executor.sortedset.ZRangeExecutor;
import org.apache.geode.redis.internal.commands.executor.sortedset.ZRankExecutor;
import org.apache.geode.redis.internal.commands.executor.sortedset.ZRemExecutor;
import org.apache.geode.redis.internal.commands.executor.sortedset.ZRemRangeByLexExecutor;
import org.apache.geode.redis.internal.commands.executor.sortedset.ZRemRangeByRankExecutor;
import org.apache.geode.redis.internal.commands.executor.sortedset.ZRemRangeByScoreExecutor;
import org.apache.geode.redis.internal.commands.executor.sortedset.ZRevRangeByLexExecutor;
import org.apache.geode.redis.internal.commands.executor.sortedset.ZRevRangeByScoreExecutor;
import org.apache.geode.redis.internal.commands.executor.sortedset.ZRevRangeExecutor;
import org.apache.geode.redis.internal.commands.executor.sortedset.ZRevRankExecutor;
import org.apache.geode.redis.internal.commands.executor.sortedset.ZScanExecutor;
import org.apache.geode.redis.internal.commands.executor.sortedset.ZScoreExecutor;
import org.apache.geode.redis.internal.commands.executor.sortedset.ZUnionStoreExecutor;
import org.apache.geode.redis.internal.commands.executor.string.AppendExecutor;
import org.apache.geode.redis.internal.commands.executor.string.BitCountExecutor;
import org.apache.geode.redis.internal.commands.executor.string.BitOpExecutor;
import org.apache.geode.redis.internal.commands.executor.string.BitPosExecutor;
import org.apache.geode.redis.internal.commands.executor.string.DecrByExecutor;
import org.apache.geode.redis.internal.commands.executor.string.DecrExecutor;
import org.apache.geode.redis.internal.commands.executor.string.GetBitExecutor;
import org.apache.geode.redis.internal.commands.executor.string.GetExecutor;
import org.apache.geode.redis.internal.commands.executor.string.GetRangeExecutor;
import org.apache.geode.redis.internal.commands.executor.string.GetSetExecutor;
import org.apache.geode.redis.internal.commands.executor.string.IncrByExecutor;
import org.apache.geode.redis.internal.commands.executor.string.IncrByFloatExecutor;
import org.apache.geode.redis.internal.commands.executor.string.IncrExecutor;
import org.apache.geode.redis.internal.commands.executor.string.MGetExecutor;
import org.apache.geode.redis.internal.commands.executor.string.MSetExecutor;
import org.apache.geode.redis.internal.commands.executor.string.MSetNXExecutor;
import org.apache.geode.redis.internal.commands.executor.string.PSetEXExecutor;
import org.apache.geode.redis.internal.commands.executor.string.SetBitExecutor;
import org.apache.geode.redis.internal.commands.executor.string.SetEXExecutor;
import org.apache.geode.redis.internal.commands.executor.string.SetExecutor;
import org.apache.geode.redis.internal.commands.executor.string.SetNXExecutor;
import org.apache.geode.redis.internal.commands.executor.string.SetRangeExecutor;
import org.apache.geode.redis.internal.commands.executor.string.StrlenExecutor;
import org.apache.geode.redis.internal.commands.parameters.ClusterParameterRequirements;
import org.apache.geode.redis.internal.commands.parameters.Parameter;
import org.apache.geode.redis.internal.commands.parameters.SlowlogParameterRequirements;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

/**
 * The redis command type used by the server. Each command is directly from the redis protocol.
 */
public enum RedisCommandType {

  /***************************************
   *** Supported Commands ***
   ***************************************/

  /*************** Connection ****************/
  AUTH(new AuthExecutor(), SUPPORTED, new Parameter().min(2).max(3, ERROR_SYNTAX).firstKey(0)
      .flags(NOSCRIPT, LOADING, STALE, FAST, NO_AUTH)),
  CLIENT(new ClientExecutor(), SUPPORTED,
      new Parameter().min(2).firstKey(0).flags(ADMIN, NOSCRIPT, RANDOM, LOADING, STALE)),
  ECHO(new EchoExecutor(), SUPPORTED, new Parameter().exact(2).firstKey(0).flags(FAST)),
  PING(new PingExecutor(), SUPPORTED, new Parameter().min(1).max(2).firstKey(0).flags(STALE, FAST)),
  QUIT(new QuitExecutor(), SUPPORTED, new Parameter().firstKey(0)),

  /*************** Keys ******************/

  DEL(new DelExecutor(), SUPPORTED, new Parameter().min(2).lastKey(-1).flags(WRITE)),
  DUMP(new DumpExecutor(), SUPPORTED, new Parameter().exact(2).flags(READONLY, RANDOM)),
  EXISTS(new ExistsExecutor(), SUPPORTED, new Parameter().min(2).lastKey(-1).flags(READONLY, FAST)),
  EXPIRE(new ExpireExecutor(), SUPPORTED, new Parameter().exact(3).flags(WRITE, FAST)),
  EXPIREAT(new ExpireAtExecutor(), SUPPORTED, new Parameter().exact(3).flags(WRITE, FAST)),
  KEYS(new KeysExecutor(), SUPPORTED,
      new Parameter().exact(2).firstKey(0).flags(READONLY, SORT_FOR_SCRIPT)),
  PERSIST(new PersistExecutor(), SUPPORTED, new Parameter().exact(2).flags(WRITE, FAST)),
  PEXPIRE(new PExpireExecutor(), SUPPORTED, new Parameter().exact(3).flags(WRITE, FAST)),
  PEXPIREAT(new PExpireAtExecutor(), SUPPORTED, new Parameter().exact(3).flags(WRITE, FAST)),
  PTTL(new PTTLExecutor(), SUPPORTED, new Parameter().exact(2).flags(READONLY, RANDOM, FAST)),
  RENAME(new RenameExecutor(), SUPPORTED, new Parameter().exact(3).lastKey(2).flags(WRITE)),
  RENAMENX(new RenameNXExecutor(), SUPPORTED,
      new Parameter().exact(3).lastKey(2).flags(WRITE, FAST)),
  RESTORE(new RestoreExecutor(), SUPPORTED, new Parameter().min(4).flags(WRITE, DENYOOM)),
  TTL(new TTLExecutor(), SUPPORTED, new Parameter().exact(2).flags(READONLY, RANDOM, FAST)),
  TYPE(new TypeExecutor(), SUPPORTED, new Parameter().exact(2).flags(READONLY, FAST)),

  /************* Strings *****************/

  APPEND(new AppendExecutor(), SUPPORTED, new Parameter().exact(3).flags(WRITE, DENYOOM, FAST)),
  DECR(new DecrExecutor(), SUPPORTED, new Parameter().exact(2).flags(WRITE, DENYOOM, FAST)),
  DECRBY(new DecrByExecutor(), SUPPORTED, new Parameter().exact(3).flags(WRITE, DENYOOM, FAST)),
  GET(new GetExecutor(), SUPPORTED, new Parameter().exact(2).flags(READONLY, FAST)),
  GETSET(new GetSetExecutor(), SUPPORTED, new Parameter().exact(3).flags(WRITE, DENYOOM, FAST)),
  INCRBY(new IncrByExecutor(), SUPPORTED, new Parameter().exact(3).flags(WRITE, DENYOOM, FAST)),
  INCR(new IncrExecutor(), SUPPORTED, new Parameter().exact(2).flags(WRITE, DENYOOM, FAST)),
  GETRANGE(new GetRangeExecutor(), SUPPORTED, new Parameter().exact(4).flags(READONLY)),
  INCRBYFLOAT(new IncrByFloatExecutor(), SUPPORTED,
      new Parameter().exact(3).flags(WRITE, DENYOOM, FAST)),
  MGET(new MGetExecutor(), SUPPORTED, new Parameter().min(2).lastKey(-1).flags(READONLY, FAST)),
  MSET(new MSetExecutor(), SUPPORTED,
      new Parameter().min(3).odd().lastKey(-1).step(2).flags(WRITE, DENYOOM)),
  MSETNX(new MSetNXExecutor(), SUPPORTED,
      new Parameter().min(3).odd().lastKey(-1).step(2).flags(WRITE, DENYOOM)),
  PSETEX(new PSetEXExecutor(), SUPPORTED, new Parameter().exact(4).flags(WRITE, DENYOOM)),
  SET(new SetExecutor(), SUPPORTED, new Parameter().min(3).flags(WRITE, DENYOOM)),
  SETEX(new SetEXExecutor(), SUPPORTED, new Parameter().exact(4).flags(WRITE, DENYOOM)),
  SETNX(new SetNXExecutor(), SUPPORTED, new Parameter().exact(3).flags(WRITE, DENYOOM, FAST)),
  SETRANGE(new SetRangeExecutor(), SUPPORTED, new Parameter().exact(4).flags(WRITE, DENYOOM)),
  STRLEN(new StrlenExecutor(), SUPPORTED, new Parameter().exact(2).flags(READONLY, FAST)),

  /************* Hashes *****************/

  HDEL(new HDelExecutor(), SUPPORTED, new Parameter().min(3).flags(WRITE, FAST)),
  HGET(new HGetExecutor(), SUPPORTED, new Parameter().exact(3).flags(READONLY, FAST)),
  HGETALL(new HGetAllExecutor(), SUPPORTED, new Parameter().exact(2).flags(READONLY, RANDOM)),
  HINCRBYFLOAT(new HIncrByFloatExecutor(), SUPPORTED,
      new Parameter().exact(4).flags(WRITE, DENYOOM, FAST)),
  HLEN(new HLenExecutor(), SUPPORTED, new Parameter().exact(2).flags(READONLY, FAST)),
  HMGET(new HMGetExecutor(), SUPPORTED, new Parameter().min(3).flags(READONLY, FAST)),
  HMSET(new HMSetExecutor(), SUPPORTED, new Parameter().min(4).even().flags(WRITE, DENYOOM, FAST)),
  HSET(new HSetExecutor(), SUPPORTED, new Parameter().min(4).even().flags(WRITE, DENYOOM, FAST)),
  HSETNX(new HSetNXExecutor(), SUPPORTED, new Parameter().exact(4).flags(WRITE, DENYOOM, FAST)),
  HSTRLEN(new HStrLenExecutor(), SUPPORTED, new Parameter().exact(3).flags(READONLY, FAST)),
  HINCRBY(new HIncrByExecutor(), SUPPORTED, new Parameter().exact(4).flags(WRITE, DENYOOM, FAST)),
  HVALS(new HValsExecutor(), SUPPORTED, new Parameter().exact(2).flags(READONLY, SORT_FOR_SCRIPT)),
  HSCAN(new HScanExecutor(), SUPPORTED, new Parameter().min(3).flags(READONLY, RANDOM),
      new Parameter().odd(ERROR_SYNTAX)),
  HEXISTS(new HExistsExecutor(), SUPPORTED, new Parameter().exact(3).flags(READONLY, FAST)),
  HKEYS(new HKeysExecutor(), SUPPORTED, new Parameter().exact(2).flags(READONLY, SORT_FOR_SCRIPT)),

  /************* Sets *****************/

  SADD(new SAddExecutor(), SUPPORTED, new Parameter().min(3).flags(WRITE, DENYOOM, FAST)),
  SCARD(new SCardExecutor(), SUPPORTED, new Parameter().exact(2).flags(READONLY, FAST)),
  SDIFF(new SDiffExecutor(), SUPPORTED,
      new Parameter().min(2).lastKey(-1).flags(READONLY, SORT_FOR_SCRIPT)),
  SISMEMBER(new SIsMemberExecutor(), SUPPORTED, new Parameter().exact(3).flags(READONLY, FAST)),
  SMEMBERS(new SMembersExecutor(), SUPPORTED,
      new Parameter().exact(2).flags(READONLY, SORT_FOR_SCRIPT)),
  SREM(new SRemExecutor(), SUPPORTED, new Parameter().min(3).flags(WRITE, FAST)),

  /************ Sorted Sets **************/

  ZADD(new ZAddExecutor(), SUPPORTED, new Parameter().min(4).flags(WRITE, DENYOOM, FAST)),
  ZCARD(new ZCardExecutor(), SUPPORTED, new Parameter().exact(2).flags(READONLY, FAST)),
  ZCOUNT(new ZCountExecutor(), SUPPORTED, new Parameter().exact(4).flags(READONLY, FAST)),
  ZINCRBY(new ZIncrByExecutor(), SUPPORTED, new Parameter().exact(4).flags(WRITE, DENYOOM, FAST)),
  ZINTERSTORE(new ZInterStoreExecutor(), SUPPORTED,
      new Parameter().min(4).flags(WRITE, DENYOOM, MOVABLEKEYS)),
  ZLEXCOUNT(new ZLexCountExecutor(), SUPPORTED, new Parameter().exact(4).flags(READONLY, FAST)),
  ZPOPMAX(new ZPopMaxExecutor(), SUPPORTED,
      new Parameter().min(2).max(3, ERROR_SYNTAX).flags(WRITE, FAST)),
  ZPOPMIN(new ZPopMinExecutor(), SUPPORTED,
      new Parameter().min(2).max(3, ERROR_SYNTAX).flags(WRITE, FAST)),
  ZRANGE(new ZRangeExecutor(), SUPPORTED,
      new Parameter().min(4).max(5, ERROR_SYNTAX).flags(READONLY)),
  ZRANGEBYLEX(new ZRangeByLexExecutor(), SUPPORTED, new Parameter().min(4).flags(READONLY)),
  ZRANGEBYSCORE(new ZRangeByScoreExecutor(), SUPPORTED, new Parameter().min(4).flags(READONLY)),
  ZRANK(new ZRankExecutor(), SUPPORTED, new Parameter().exact(3).flags(READONLY, FAST)),
  ZREM(new ZRemExecutor(), SUPPORTED, new Parameter().min(3).flags(WRITE, FAST)),
  ZREMRANGEBYLEX(new ZRemRangeByLexExecutor(), SUPPORTED, new Parameter().exact(4).flags(WRITE)),
  ZREMRANGEBYRANK(new ZRemRangeByRankExecutor(), SUPPORTED, new Parameter().exact(4).flags(WRITE)),
  ZREMRANGEBYSCORE(new ZRemRangeByScoreExecutor(), SUPPORTED,
      new Parameter().exact(4).flags(WRITE)),
  ZREVRANGE(new ZRevRangeExecutor(), SUPPORTED,
      new Parameter().min(4).max(5, ERROR_SYNTAX).flags(READONLY)),
  ZREVRANGEBYLEX(new ZRevRangeByLexExecutor(), SUPPORTED, new Parameter().min(4).flags(READONLY)),
  ZREVRANGEBYSCORE(new ZRevRangeByScoreExecutor(), SUPPORTED,
      new Parameter().min(4).flags(READONLY)),
  ZREVRANK(new ZRevRankExecutor(), SUPPORTED, new Parameter().exact(3).flags(READONLY, FAST)),
  ZSCAN(new ZScanExecutor(), SUPPORTED, new Parameter().min(3).flags(READONLY, RANDOM),
      new Parameter().odd(ERROR_SYNTAX)),
  ZSCORE(new ZScoreExecutor(), SUPPORTED, new Parameter().exact(3).flags(READONLY, FAST)),
  ZUNIONSTORE(new ZUnionStoreExecutor(), SUPPORTED,
      new Parameter().min(4).flags(WRITE, DENYOOM, MOVABLEKEYS)),

  /************* Server *****************/
  COMMAND(new COMMANDCommandExecutor(), SUPPORTED, new Parameter().min(1).firstKey(0).flags(RANDOM,
      LOADING, STALE)),
  SLOWLOG(new SlowlogExecutor(), SUPPORTED, new Parameter().min(2)
      .custom(SlowlogParameterRequirements.checkParameters()).firstKey(0)
      .flags(ADMIN, RANDOM, LOADING, STALE)),
  INFO(new InfoExecutor(), SUPPORTED, new Parameter().min(1).max(2, ERROR_SYNTAX).firstKey(0)
      .flags(RANDOM, LOADING, STALE)),
  LOLWUT(new LolWutExecutor(), SUPPORTED, new Parameter().min(1).firstKey(0).flags(READONLY, FAST)),


  /********** Publish Subscribe **********/
  SUBSCRIBE(new SubscribeExecutor(), SUPPORTED, new Parameter().min(2).firstKey(0).flags(
      Flag.PUBSUB,
      NOSCRIPT,
      LOADING, STALE)),
  PUBLISH(new PublishExecutor(), SUPPORTED,
      new Parameter().exact(3).firstKey(0).flags(Flag.PUBSUB, LOADING, STALE, FAST, MAY_REPLICATE)),
  PSUBSCRIBE(new PsubscribeExecutor(), SUPPORTED,
      new Parameter().min(2).firstKey(0).flags(Flag.PUBSUB, NOSCRIPT, LOADING, STALE)),
  PUNSUBSCRIBE(new PunsubscribeExecutor(), SUPPORTED,
      new Parameter().min(1).firstKey(0).flags(Flag.PUBSUB, NOSCRIPT, LOADING, STALE)),
  UNSUBSCRIBE(new UnsubscribeExecutor(), SUPPORTED,
      new Parameter().min(1).firstKey(0).flags(Flag.PUBSUB, NOSCRIPT, LOADING, STALE)),
  PUBSUB(new PubSubExecutor(), SUPPORTED,
      new Parameter().min(2).firstKey(0).flags(Flag.PUBSUB, RANDOM, LOADING, STALE)),

  /************* Cluster *****************/
  CLUSTER(new ClusterExecutor(), SUPPORTED, new Parameter().min(2)
      .custom(ClusterParameterRequirements.checkParameters()).firstKey(0)
      .flags(ADMIN, RANDOM, STALE)),

  /***************************************
   ******** Unsupported Commands *********
   ***************************************/

  /*************** Connection *************/

  SELECT(new SelectExecutor(), UNSUPPORTED, new Parameter().exact(2).firstKey(0).flags(LOADING,
      STALE,
      FAST)),

  /*************** Keys ******************/

  SCAN(new ScanExecutor(), UNSUPPORTED, new Parameter().min(2).even(ERROR_SYNTAX).firstKey(0).flags(
      READONLY,
      RANDOM)),
  UNLINK(new DelExecutor(), UNSUPPORTED, new Parameter().min(2).lastKey(-1).flags(WRITE, FAST)),

  /************** Strings ****************/

  BITCOUNT(new BitCountExecutor(), UNSUPPORTED, new Parameter().min(2).flags(READONLY)),
  BITOP(new BitOpExecutor(), UNSUPPORTED,
      new Parameter().min(4).firstKey(2).lastKey(-1).flags(WRITE, DENYOOM)),
  BITPOS(new BitPosExecutor(), UNSUPPORTED, new Parameter().min(3).flags(READONLY)),
  GETBIT(new GetBitExecutor(), UNSUPPORTED, new Parameter().exact(3).flags(READONLY, FAST)),
  SETBIT(new SetBitExecutor(), UNSUPPORTED, new Parameter().exact(4).flags(WRITE, DENYOOM)),

  /**************** Sets *****************/

  SDIFFSTORE(new SDiffStoreExecutor(), UNSUPPORTED,
      new Parameter().min(3).lastKey(-1).flags(WRITE, DENYOOM)),
  SINTER(new SInterExecutor(), UNSUPPORTED,
      new Parameter().min(2).lastKey(-1).flags(READONLY, SORT_FOR_SCRIPT)),
  SINTERSTORE(new SInterStoreExecutor(), UNSUPPORTED,
      new Parameter().min(3).lastKey(-1).flags(WRITE, DENYOOM)),
  SMOVE(new SMoveExecutor(), UNSUPPORTED, new Parameter().exact(4).lastKey(2).flags(WRITE, FAST)),
  SPOP(new SPopExecutor(), UNSUPPORTED,
      new Parameter().min(2).max(3, ERROR_SYNTAX).flags(WRITE, RANDOM, FAST)),
  SRANDMEMBER(new SRandMemberExecutor(), UNSUPPORTED,
      new Parameter().min(2).flags(READONLY, RANDOM)),
  SSCAN(new SScanExecutor(), UNSUPPORTED, new Parameter().min(3).flags(READONLY, RANDOM),
      new Parameter().odd(ERROR_SYNTAX).firstKey(0)),
  SUNION(new SUnionExecutor(), UNSUPPORTED,
      new Parameter().min(2).lastKey(-1).flags(READONLY, SORT_FOR_SCRIPT)),
  SUNIONSTORE(new SUnionStoreExecutor(), UNSUPPORTED,
      new Parameter().min(3).lastKey(-1).flags(WRITE, DENYOOM)),

  /*************** Server ****************/

  DBSIZE(new DBSizeExecutor(), UNSUPPORTED, new Parameter().exact(1).firstKey(0).flags(READONLY,
      FAST)),
  FLUSHALL(new FlushAllExecutor(), UNSUPPORTED,
      new Parameter().min(1).max(2, ERROR_SYNTAX).firstKey(0).flags(WRITE)),
  FLUSHDB(new FlushAllExecutor(), UNSUPPORTED,
      new Parameter().min(1).max(2, ERROR_SYNTAX).firstKey(0).flags(WRITE)),
  TIME(new TimeExecutor(), UNSUPPORTED,
      new Parameter().exact(1).firstKey(0).flags(RANDOM, LOADING, STALE, FAST)),

  /***************************************
   *********** Unknown Commands **********
   ***************************************/
  UNKNOWN(new UnknownExecutor(), RedisCommandSupportLevel.UNKNOWN);

  public enum Flag {
    ADMIN,
    DENYOOM,
    FAST,
    LOADING,
    MAY_REPLICATE,
    MOVABLEKEYS,
    NO_AUTH,
    NOSCRIPT,
    PUBSUB,
    RANDOM,
    READONLY,
    SORT_FOR_SCRIPT,
    STALE,
    WRITE;
  }

  private final CommandExecutor commandExecutor;
  private final Parameter parameterRequirements;
  private final Parameter deferredParameterRequirements;
  private final RedisCommandSupportLevel supportLevel;

  RedisCommandType(CommandExecutor commandExecutor, RedisCommandSupportLevel supportLevel) {
    this(commandExecutor, supportLevel, new Parameter().custom(c -> {
    }));
  }

  RedisCommandType(CommandExecutor commandExecutor, RedisCommandSupportLevel supportLevel,
      Parameter parameterRequirements) {
    this(commandExecutor, supportLevel, parameterRequirements, new Parameter().custom(c -> {
    }));
  }

  RedisCommandType(CommandExecutor commandExecutor, RedisCommandSupportLevel supportLevel,
      Parameter parameterRequirements,
      Parameter deferredParameterRequirements) {
    this.commandExecutor = commandExecutor;
    this.supportLevel = supportLevel;
    this.parameterRequirements = parameterRequirements;
    this.deferredParameterRequirements = deferredParameterRequirements;
  }

  public int arity() {
    return parameterRequirements.getArity();
  }

  public Set<Flag> flags() {
    return parameterRequirements.getFlags();
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

  public boolean getRequiresWritePermission() {
    return parameterRequirements.getFlags().contains(WRITE) || (this == PUBLISH);
  }

  public void checkDeferredParameters(Command command,
      ExecutionHandlerContext executionHandlerContext) {
    deferredParameterRequirements.checkParameters(command, executionHandlerContext);
  }

  public RedisResponse executeCommand(Command command,
      ExecutionHandlerContext executionHandlerContext) throws Exception {

    parameterRequirements.checkParameters(command, executionHandlerContext);

    return commandExecutor.executeCommand(command, executionHandlerContext);
  }
}

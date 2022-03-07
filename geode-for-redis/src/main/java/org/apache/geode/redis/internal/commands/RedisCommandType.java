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
import static org.apache.geode.redis.internal.RedisConstants.WRONG_NUMBER_OF_ARGUMENTS_FOR_MSET;
import static org.apache.geode.redis.internal.commands.RedisCommandSupportLevel.SUPPORTED;
import static org.apache.geode.redis.internal.commands.RedisCommandSupportLevel.UNSUPPORTED;
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

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

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
import org.apache.geode.redis.internal.commands.executor.list.LIndexExecutor;
import org.apache.geode.redis.internal.commands.executor.list.LLenExecutor;
import org.apache.geode.redis.internal.commands.executor.list.LPopExecutor;
import org.apache.geode.redis.internal.commands.executor.list.LPushExecutor;
import org.apache.geode.redis.internal.commands.executor.list.LPushXExecutor;
import org.apache.geode.redis.internal.commands.executor.list.LRangeExecutor;
import org.apache.geode.redis.internal.commands.executor.pubsub.PsubscribeExecutor;
import org.apache.geode.redis.internal.commands.executor.pubsub.PubSubExecutor;
import org.apache.geode.redis.internal.commands.executor.pubsub.PublishExecutor;
import org.apache.geode.redis.internal.commands.executor.pubsub.PunsubscribeExecutor;
import org.apache.geode.redis.internal.commands.executor.pubsub.SubscribeExecutor;
import org.apache.geode.redis.internal.commands.executor.pubsub.UnsubscribeExecutor;
import org.apache.geode.redis.internal.commands.executor.server.CommandCommandExecutor;
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

  /***************** Admin *******************/
  CLUSTER(new ClusterExecutor(), Category.ADMIN, SUPPORTED,
      new Parameter().min(2).custom(ClusterParameterRequirements.checkParameters()).firstKey(0)
          .flags(Flag.ADMIN, RANDOM, STALE)),
  QUIT(new QuitExecutor(), Category.ADMIN, SUPPORTED, new Parameter().firstKey(0)),
  SLOWLOG(new SlowlogExecutor(), Category.ADMIN, SUPPORTED, new Parameter().min(2)
      .custom(SlowlogParameterRequirements.checkParameters()).firstKey(0)
      .flags(Flag.ADMIN, RANDOM, LOADING, STALE)),

  /*************** Connection ****************/
  AUTH(new AuthExecutor(), Category.CONNECTION, SUPPORTED,
      new Parameter().min(2).max(3, ERROR_SYNTAX).firstKey(0)
          .flags(NOSCRIPT, LOADING, STALE, FAST, NO_AUTH)),
  CLIENT(new ClientExecutor(), Category.CONNECTION, SUPPORTED,
      new Parameter().min(2).firstKey(0).flags(Flag.ADMIN, NOSCRIPT, RANDOM, LOADING, STALE)),
  ECHO(new EchoExecutor(), Category.CONNECTION, SUPPORTED,
      new Parameter().exact(2).firstKey(0).flags(FAST)),
  PING(new PingExecutor(), Category.CONNECTION, SUPPORTED,
      new Parameter().min(1).max(2).firstKey(0).flags(STALE, FAST)),
  COMMAND(new CommandCommandExecutor(), Category.CONNECTION, SUPPORTED,
      new Parameter().min(1).firstKey(0).flags(RANDOM, LOADING, STALE)),

  /*************** Keyspace ******************/

  DEL(new DelExecutor(), Category.KEYSPACE, SUPPORTED,
      new Parameter().min(2).lastKey(-1).flags(WRITE)),
  DUMP(new DumpExecutor(), Category.KEYSPACE, SUPPORTED,
      new Parameter().exact(2).flags(READONLY, RANDOM)),
  EXISTS(new ExistsExecutor(), Category.KEYSPACE, SUPPORTED,
      new Parameter().min(2).lastKey(-1).flags(READONLY, FAST)),
  EXPIRE(new ExpireExecutor(), Category.KEYSPACE, SUPPORTED,
      new Parameter().exact(3).flags(WRITE, FAST)),
  EXPIREAT(new ExpireAtExecutor(), Category.KEYSPACE, SUPPORTED,
      new Parameter().exact(3).flags(WRITE, FAST)),
  KEYS(new KeysExecutor(), Category.KEYSPACE, SUPPORTED,
      new Parameter().exact(2).firstKey(0).flags(READONLY, SORT_FOR_SCRIPT)),
  PERSIST(new PersistExecutor(), Category.KEYSPACE, SUPPORTED,
      new Parameter().exact(2).flags(WRITE, FAST)),
  PEXPIRE(new PExpireExecutor(), Category.KEYSPACE, SUPPORTED,
      new Parameter().exact(3).flags(WRITE, FAST)),
  PEXPIREAT(new PExpireAtExecutor(), Category.KEYSPACE, SUPPORTED,
      new Parameter().exact(3).flags(WRITE, FAST)),
  PTTL(new PTTLExecutor(), Category.KEYSPACE, SUPPORTED,
      new Parameter().exact(2).flags(READONLY, RANDOM, FAST)),
  RENAME(new RenameExecutor(), Category.KEYSPACE, SUPPORTED,
      new Parameter().exact(3).lastKey(2).flags(WRITE)),
  RENAMENX(new RenameNXExecutor(), Category.KEYSPACE, SUPPORTED,
      new Parameter().exact(3).lastKey(2).flags(WRITE, FAST)),
  RESTORE(new RestoreExecutor(), Category.KEYSPACE, SUPPORTED,
      new Parameter().min(4).flags(WRITE, DENYOOM)),
  TTL(new TTLExecutor(), Category.KEYSPACE, SUPPORTED,
      new Parameter().exact(2).flags(READONLY, RANDOM, FAST)),
  TYPE(new TypeExecutor(), Category.KEYSPACE, SUPPORTED,
      new Parameter().exact(2).flags(READONLY, FAST)),

  /************* Strings *****************/

  APPEND(new AppendExecutor(), Category.STRING, SUPPORTED,
      new Parameter().exact(3).flags(WRITE, DENYOOM, FAST)),
  DECR(new DecrExecutor(), Category.STRING, SUPPORTED,
      new Parameter().exact(2).flags(WRITE, DENYOOM, FAST)),
  DECRBY(new DecrByExecutor(), Category.STRING, SUPPORTED,
      new Parameter().exact(3).flags(WRITE, DENYOOM, FAST)),
  GET(new GetExecutor(), Category.STRING, SUPPORTED,
      new Parameter().exact(2).flags(READONLY, FAST)),
  GETSET(new GetSetExecutor(), Category.STRING, SUPPORTED,
      new Parameter().exact(3).flags(WRITE, DENYOOM, FAST)),
  INCRBY(new IncrByExecutor(), Category.STRING, SUPPORTED,
      new Parameter().exact(3).flags(WRITE, DENYOOM, FAST)),
  INCR(new IncrExecutor(), Category.STRING, SUPPORTED,
      new Parameter().exact(2).flags(WRITE, DENYOOM, FAST)),
  GETRANGE(new GetRangeExecutor(), Category.STRING, SUPPORTED,
      new Parameter().exact(4).flags(READONLY)),
  INCRBYFLOAT(new IncrByFloatExecutor(), Category.STRING, SUPPORTED,
      new Parameter().exact(3).flags(WRITE, DENYOOM, FAST)),
  MGET(new MGetExecutor(), Category.STRING, SUPPORTED,
      new Parameter().min(2).lastKey(-1).flags(READONLY, FAST)),
  MSET(new MSetExecutor(), Category.STRING, SUPPORTED, new Parameter().min(3)
      .odd(WRONG_NUMBER_OF_ARGUMENTS_FOR_MSET).lastKey(-1).step(2).flags(WRITE, DENYOOM)),
  MSETNX(new MSetNXExecutor(), Category.STRING, SUPPORTED, new Parameter().min(3)
      .odd(WRONG_NUMBER_OF_ARGUMENTS_FOR_MSET).odd().lastKey(-1).step(2).flags(WRITE, DENYOOM)),
  PSETEX(new PSetEXExecutor(), Category.STRING, SUPPORTED,
      new Parameter().exact(4).flags(WRITE, DENYOOM)),
  SET(new SetExecutor(), Category.STRING, SUPPORTED,
      new Parameter().min(3).flags(WRITE, DENYOOM)),
  SETEX(new SetEXExecutor(), Category.STRING, SUPPORTED,
      new Parameter().exact(4).flags(WRITE, DENYOOM)),
  SETNX(new SetNXExecutor(), Category.STRING, SUPPORTED,
      new Parameter().exact(3).flags(WRITE, DENYOOM, FAST)),
  SETRANGE(new SetRangeExecutor(), Category.STRING, SUPPORTED,
      new Parameter().exact(4).flags(WRITE, DENYOOM)),
  STRLEN(new StrlenExecutor(), Category.STRING, SUPPORTED,
      new Parameter().exact(2).flags(READONLY, FAST)),

  /************* Hashes *****************/

  HDEL(new HDelExecutor(), Category.HASH, SUPPORTED, new Parameter().min(3).flags(WRITE, FAST)),
  HGET(new HGetExecutor(), Category.HASH, SUPPORTED,
      new Parameter().exact(3).flags(READONLY, FAST)),
  HGETALL(new HGetAllExecutor(), Category.HASH, SUPPORTED,
      new Parameter().exact(2).flags(READONLY, RANDOM)),
  HINCRBYFLOAT(new HIncrByFloatExecutor(), Category.HASH, SUPPORTED,
      new Parameter().exact(4).flags(WRITE, DENYOOM, FAST)),
  HLEN(new HLenExecutor(), Category.HASH, SUPPORTED,
      new Parameter().exact(2).flags(READONLY, FAST)),
  HMGET(new HMGetExecutor(), Category.HASH, SUPPORTED,
      new Parameter().min(3).flags(READONLY, FAST)),
  HMSET(new HMSetExecutor(), Category.HASH, SUPPORTED,
      new Parameter().min(4).even().flags(WRITE, DENYOOM, FAST)),
  HSET(new HSetExecutor(), Category.HASH, SUPPORTED,
      new Parameter().min(4).even().flags(WRITE, DENYOOM, FAST)),
  HSETNX(new HSetNXExecutor(), Category.HASH, SUPPORTED,
      new Parameter().exact(4).flags(WRITE, DENYOOM, FAST)),
  HSTRLEN(new HStrLenExecutor(), Category.HASH, SUPPORTED,
      new Parameter().exact(3).flags(READONLY, FAST)),
  HINCRBY(new HIncrByExecutor(), Category.HASH, SUPPORTED,
      new Parameter().exact(4).flags(WRITE, DENYOOM, FAST)),
  HVALS(new HValsExecutor(), Category.HASH, SUPPORTED,
      new Parameter().exact(2).flags(READONLY, SORT_FOR_SCRIPT)),
  HSCAN(new HScanExecutor(), Category.HASH, SUPPORTED,
      new Parameter().min(3).flags(READONLY, RANDOM),
      new Parameter().odd(ERROR_SYNTAX)),
  HEXISTS(new HExistsExecutor(), Category.HASH, SUPPORTED,
      new Parameter().exact(3).flags(READONLY, FAST)),
  HKEYS(new HKeysExecutor(), Category.HASH, SUPPORTED,
      new Parameter().exact(2).flags(READONLY, SORT_FOR_SCRIPT)),

  /************* Sets *****************/

  SADD(new SAddExecutor(), Category.SET, SUPPORTED,
      new Parameter().min(3).flags(WRITE, DENYOOM, FAST)),
  SCARD(new SCardExecutor(), Category.SET, SUPPORTED,
      new Parameter().exact(2).flags(READONLY, FAST)),
  SDIFF(new SDiffExecutor(), Category.SET, SUPPORTED,
      new Parameter().min(2).lastKey(-1).flags(READONLY, SORT_FOR_SCRIPT)),
  SDIFFSTORE(new SDiffStoreExecutor(), Category.SET, SUPPORTED,
      new Parameter().min(3).lastKey(-1).flags(WRITE, DENYOOM)),
  SINTER(new SInterExecutor(), Category.SET, SUPPORTED,
      new Parameter().min(2).lastKey(-1).flags(READONLY, SORT_FOR_SCRIPT)),
  SINTERSTORE(new SInterStoreExecutor(), Category.SET, SUPPORTED,
      new Parameter().min(3).lastKey(-1).flags(WRITE, DENYOOM)),
  SISMEMBER(new SIsMemberExecutor(), Category.SET, SUPPORTED,
      new Parameter().exact(3).flags(READONLY, FAST)),
  SMEMBERS(new SMembersExecutor(), Category.SET, SUPPORTED,
      new Parameter().exact(2).flags(READONLY, SORT_FOR_SCRIPT)),
  SMOVE(new SMoveExecutor(), Category.SET, SUPPORTED,
      new Parameter().exact(4).lastKey(2).flags(WRITE, FAST)),
  SPOP(new SPopExecutor(), Category.SET, SUPPORTED,
      new Parameter().min(2).max(3, ERROR_SYNTAX).flags(WRITE, RANDOM, FAST)),
  SRANDMEMBER(new SRandMemberExecutor(), Category.SET, SUPPORTED,
      new Parameter().min(2).max(3, ERROR_SYNTAX).flags(READONLY, RANDOM)),
  SREM(new SRemExecutor(), Category.SET, SUPPORTED, new Parameter().min(3).flags(WRITE, FAST)),
  SSCAN(new SScanExecutor(), Category.SET, SUPPORTED,
      new Parameter().min(3).flags(READONLY, RANDOM),
      new Parameter().odd(ERROR_SYNTAX)),
  SUNION(new SUnionExecutor(), Category.SET, SUPPORTED,
      new Parameter().min(2).lastKey(-1).flags(READONLY, SORT_FOR_SCRIPT)),
  SUNIONSTORE(new SUnionStoreExecutor(), Category.SET, SUPPORTED,
      new Parameter().min(3).lastKey(-1).flags(WRITE, DENYOOM)),

  /************ Sorted Sets **************/

  ZADD(new ZAddExecutor(), Category.SORTEDSET, SUPPORTED,
      new Parameter().min(4).flags(WRITE, DENYOOM, FAST)),
  ZCARD(new ZCardExecutor(), Category.SORTEDSET, SUPPORTED,
      new Parameter().exact(2).flags(READONLY, FAST)),
  ZCOUNT(new ZCountExecutor(), Category.SORTEDSET, SUPPORTED,
      new Parameter().exact(4).flags(READONLY, FAST)),
  ZINCRBY(new ZIncrByExecutor(), Category.SORTEDSET, SUPPORTED,
      new Parameter().exact(4).flags(WRITE, DENYOOM, FAST)),
  ZINTERSTORE(new ZInterStoreExecutor(), Category.SORTEDSET, SUPPORTED,
      new Parameter().min(4).flags(WRITE, DENYOOM, MOVABLEKEYS)),
  ZLEXCOUNT(new ZLexCountExecutor(), Category.SORTEDSET, SUPPORTED,
      new Parameter().exact(4).flags(READONLY, FAST)),
  ZPOPMAX(new ZPopMaxExecutor(), Category.SORTEDSET, SUPPORTED,
      new Parameter().min(2).max(3, ERROR_SYNTAX).flags(WRITE, FAST)),
  ZPOPMIN(new ZPopMinExecutor(), Category.SORTEDSET, SUPPORTED,
      new Parameter().min(2).max(3, ERROR_SYNTAX).flags(WRITE, FAST)),
  ZRANGE(new ZRangeExecutor(), Category.SORTEDSET, SUPPORTED,
      new Parameter().min(4).max(5, ERROR_SYNTAX).flags(READONLY)),
  ZRANGEBYLEX(new ZRangeByLexExecutor(), Category.SORTEDSET, SUPPORTED,
      new Parameter().min(4).flags(READONLY)),
  ZRANGEBYSCORE(new ZRangeByScoreExecutor(), Category.SORTEDSET, SUPPORTED,
      new Parameter().min(4).flags(READONLY)),
  ZRANK(new ZRankExecutor(), Category.SORTEDSET, SUPPORTED,
      new Parameter().exact(3).flags(READONLY, FAST)),
  ZREM(new ZRemExecutor(), Category.SORTEDSET, SUPPORTED,
      new Parameter().min(3).flags(WRITE, FAST)),
  ZREMRANGEBYLEX(new ZRemRangeByLexExecutor(), Category.SORTEDSET, SUPPORTED,
      new Parameter().exact(4).flags(WRITE)),
  ZREMRANGEBYRANK(new ZRemRangeByRankExecutor(), Category.SORTEDSET, SUPPORTED,
      new Parameter().exact(4).flags(WRITE)),
  ZREMRANGEBYSCORE(new ZRemRangeByScoreExecutor(), Category.SORTEDSET, SUPPORTED,
      new Parameter().exact(4).flags(WRITE)),
  ZREVRANGE(new ZRevRangeExecutor(), Category.SORTEDSET, SUPPORTED,
      new Parameter().min(4).max(5, ERROR_SYNTAX).flags(READONLY)),
  ZREVRANGEBYLEX(new ZRevRangeByLexExecutor(), Category.SORTEDSET, SUPPORTED,
      new Parameter().min(4).flags(READONLY)),
  ZREVRANGEBYSCORE(new ZRevRangeByScoreExecutor(), Category.SORTEDSET, SUPPORTED,
      new Parameter().min(4).flags(READONLY)),
  ZREVRANK(new ZRevRankExecutor(), Category.SORTEDSET, SUPPORTED,
      new Parameter().exact(3).flags(READONLY, FAST)),
  ZSCAN(new ZScanExecutor(), Category.SORTEDSET, SUPPORTED,
      new Parameter().min(3).flags(READONLY, RANDOM),
      new Parameter().odd(ERROR_SYNTAX)),
  ZSCORE(new ZScoreExecutor(), Category.SORTEDSET, SUPPORTED,
      new Parameter().exact(3).flags(READONLY, FAST)),
  ZUNIONSTORE(new ZUnionStoreExecutor(), Category.SORTEDSET, SUPPORTED,
      new Parameter().min(4).flags(WRITE, DENYOOM, MOVABLEKEYS)),

  /************** Lists *****************/

  LINDEX(new LIndexExecutor(), Category.LIST, SUPPORTED, new Parameter().exact(3).flags(READONLY)),
  LLEN(new LLenExecutor(), Category.LIST, SUPPORTED,
      new Parameter().exact(2).flags(READONLY, FAST)),
  LPOP(new LPopExecutor(), Category.LIST, SUPPORTED, new Parameter().exact(2).flags(WRITE, FAST)),
  LPUSH(new LPushExecutor(), Category.LIST, SUPPORTED,
      new Parameter().min(3).flags(WRITE, DENYOOM, FAST)),
  LPUSHX(new LPushXExecutor(), Category.LIST, SUPPORTED,
      new Parameter().min(3).flags(WRITE, DENYOOM, FAST)),
  LRANGE(new LRangeExecutor(), Category.LIST, SUPPORTED, new Parameter().exact(4).flags(READONLY)),

  /********** Publish Subscribe **********/

  SUBSCRIBE(new SubscribeExecutor(), Category.PUBSUB, SUPPORTED,
      new Parameter().min(2).firstKey(0).flags(Flag.PUBSUB, NOSCRIPT, LOADING, STALE)),
  PUBLISH(new PublishExecutor(), Category.PUBSUB, SUPPORTED,
      new Parameter().exact(3).firstKey(0).flags(Flag.PUBSUB, LOADING, STALE, FAST, MAY_REPLICATE)),
  PSUBSCRIBE(new PsubscribeExecutor(), Category.PUBSUB, SUPPORTED,
      new Parameter().min(2).firstKey(0).flags(Flag.PUBSUB, NOSCRIPT, LOADING, STALE)),
  PUNSUBSCRIBE(new PunsubscribeExecutor(), Category.PUBSUB, SUPPORTED,
      new Parameter().min(1).firstKey(0).flags(Flag.PUBSUB, NOSCRIPT, LOADING, STALE)),
  UNSUBSCRIBE(new UnsubscribeExecutor(), Category.PUBSUB, SUPPORTED,
      new Parameter().min(1).firstKey(0).flags(Flag.PUBSUB, NOSCRIPT, LOADING, STALE)),
  PUBSUB(new PubSubExecutor(), Category.PUBSUB, SUPPORTED,
      new Parameter().min(2).firstKey(0).flags(Flag.PUBSUB, RANDOM, LOADING, STALE)),

  /************* Uncategorized *****************/

  INFO(new InfoExecutor(), Category.UNCATEGORIZED, SUPPORTED,
      new Parameter().min(1).max(2, ERROR_SYNTAX).firstKey(0)
          .flags(RANDOM, LOADING, STALE)),
  LOLWUT(new LolWutExecutor(), Category.UNCATEGORIZED, SUPPORTED,
      new Parameter().min(1).firstKey(0).flags(READONLY, FAST)),


  /***************************************
   ******** Unsupported Commands *********
   ***************************************/

  /*************** Keyspace ******************/

  DBSIZE(new DBSizeExecutor(), Category.KEYSPACE, UNSUPPORTED,
      new Parameter().exact(1).firstKey(0).flags(READONLY, FAST)),
  FLUSHALL(new FlushAllExecutor(), Category.KEYSPACE, UNSUPPORTED,
      new Parameter().min(1).max(2, ERROR_SYNTAX).firstKey(0).flags(WRITE)),
  FLUSHDB(new FlushAllExecutor(), Category.KEYSPACE, UNSUPPORTED,
      new Parameter().min(1).max(2, ERROR_SYNTAX).firstKey(0).flags(WRITE)),
  SCAN(new ScanExecutor(), Category.KEYSPACE, UNSUPPORTED,
      new Parameter().min(2).even(ERROR_SYNTAX).firstKey(0).flags(READONLY, RANDOM)),
  SELECT(new SelectExecutor(), Category.KEYSPACE, UNSUPPORTED,
      new Parameter().exact(2).firstKey(0).flags(LOADING, STALE, FAST)),
  UNLINK(new DelExecutor(), Category.KEYSPACE, UNSUPPORTED,
      new Parameter().min(2).lastKey(-1).flags(WRITE, FAST)),

  /************** Bitmap ****************/

  BITCOUNT(new BitCountExecutor(), Category.BITMAP, UNSUPPORTED,
      new Parameter().min(2).flags(READONLY)),
  BITOP(new BitOpExecutor(), Category.BITMAP, UNSUPPORTED,
      new Parameter().min(4).firstKey(2).lastKey(-1).flags(WRITE, DENYOOM)),
  BITPOS(new BitPosExecutor(), Category.BITMAP, UNSUPPORTED,
      new Parameter().min(3).flags(READONLY)),
  GETBIT(new GetBitExecutor(), Category.BITMAP, UNSUPPORTED,
      new Parameter().exact(3).flags(READONLY, FAST)),
  SETBIT(new SetBitExecutor(), Category.BITMAP, UNSUPPORTED,
      new Parameter().exact(4).flags(WRITE, DENYOOM)),

  /*************** Uncategorized ****************/

  TIME(new TimeExecutor(), Category.UNCATEGORIZED, UNSUPPORTED,
      new Parameter().exact(1).firstKey(0).flags(RANDOM, LOADING, STALE, FAST)),

  /***************************************
   *********** Unknown Commands **********
   ***************************************/
  UNKNOWN(new UnknownExecutor(), Category.UNCATEGORIZED, RedisCommandSupportLevel.UNKNOWN);

  public enum Category {
    ADMIN,
    BITMAP,
    CONNECTION,
    HASH,
    KEYSPACE,
    LIST,
    PUBSUB,
    SET,
    SORTEDSET,
    STRING,
    UNCATEGORIZED,
  }

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
    WRITE
  }

  private final CommandExecutor commandExecutor;
  private final Parameter parameterRequirements;
  private final Parameter deferredParameterRequirements;
  private final RedisCommandSupportLevel supportLevel;
  private final Category category;

  RedisCommandType(CommandExecutor commandExecutor, Category category,
      RedisCommandSupportLevel supportLevel) {
    this(commandExecutor, category, supportLevel, new Parameter());
  }

  RedisCommandType(CommandExecutor commandExecutor, Category category,
      RedisCommandSupportLevel supportLevel,
      Parameter parameterRequirements) {
    this(commandExecutor, category, supportLevel, parameterRequirements, new Parameter());
  }

  RedisCommandType(CommandExecutor commandExecutor, Category category,
      RedisCommandSupportLevel supportLevel,
      Parameter parameterRequirements,
      Parameter deferredParameterRequirements) {
    this.commandExecutor = commandExecutor;
    this.category = category;
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

  public Category category() {
    return category;
  }

  public boolean isSupported() {
    return supportLevel == SUPPORTED;
  }

  public boolean isUnsupported() {
    return supportLevel == UNSUPPORTED;
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

  public static List<RedisCommandType> getCommandsForCategory(Category type) {
    return Arrays.stream(RedisCommandType.values())
        .filter(c -> c.category == type)
        .collect(Collectors.toList());
  }
}

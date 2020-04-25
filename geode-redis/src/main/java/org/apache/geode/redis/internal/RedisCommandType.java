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

import static org.apache.geode.redis.internal.RedisDataType.NONE;
import static org.apache.geode.redis.internal.RedisDataType.REDIS_HASH;
import static org.apache.geode.redis.internal.RedisDataType.REDIS_HLL;
import static org.apache.geode.redis.internal.RedisDataType.REDIS_LIST;
import static org.apache.geode.redis.internal.RedisDataType.REDIS_PUBSUB;
import static org.apache.geode.redis.internal.RedisDataType.REDIS_SET;
import static org.apache.geode.redis.internal.RedisDataType.REDIS_SORTEDSET;
import static org.apache.geode.redis.internal.RedisDataType.REDIS_STRING;

import org.apache.geode.redis.internal.executor.AuthExecutor;
import org.apache.geode.redis.internal.executor.DBSizeExecutor;
import org.apache.geode.redis.internal.executor.DelExecutor;
import org.apache.geode.redis.internal.executor.EchoExecutor;
import org.apache.geode.redis.internal.executor.ExistsExecutor;
import org.apache.geode.redis.internal.executor.ExpireAtExecutor;
import org.apache.geode.redis.internal.executor.ExpireExecutor;
import org.apache.geode.redis.internal.executor.FlushAllExecutor;
import org.apache.geode.redis.internal.executor.KeysExecutor;
import org.apache.geode.redis.internal.executor.PExpireAtExecutor;
import org.apache.geode.redis.internal.executor.PExpireExecutor;
import org.apache.geode.redis.internal.executor.PTTLExecutor;
import org.apache.geode.redis.internal.executor.PersistExecutor;
import org.apache.geode.redis.internal.executor.PingExecutor;
import org.apache.geode.redis.internal.executor.QuitExecutor;
import org.apache.geode.redis.internal.executor.RenameExecutor;
import org.apache.geode.redis.internal.executor.ScanExecutor;
import org.apache.geode.redis.internal.executor.ShutDownExecutor;
import org.apache.geode.redis.internal.executor.TTLExecutor;
import org.apache.geode.redis.internal.executor.TimeExecutor;
import org.apache.geode.redis.internal.executor.TypeExecutor;
import org.apache.geode.redis.internal.executor.UnkownExecutor;
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
import org.apache.geode.redis.internal.executor.hash.HValsExecutor;
import org.apache.geode.redis.internal.executor.hll.PFAddExecutor;
import org.apache.geode.redis.internal.executor.hll.PFCountExecutor;
import org.apache.geode.redis.internal.executor.hll.PFMergeExecutor;
import org.apache.geode.redis.internal.executor.list.LIndexExecutor;
import org.apache.geode.redis.internal.executor.list.LInsertExecutor;
import org.apache.geode.redis.internal.executor.list.LLenExecutor;
import org.apache.geode.redis.internal.executor.list.LPopExecutor;
import org.apache.geode.redis.internal.executor.list.LPushExecutor;
import org.apache.geode.redis.internal.executor.list.LPushXExecutor;
import org.apache.geode.redis.internal.executor.list.LRangeExecutor;
import org.apache.geode.redis.internal.executor.list.LRemExecutor;
import org.apache.geode.redis.internal.executor.list.LSetExecutor;
import org.apache.geode.redis.internal.executor.list.LTrimExecutor;
import org.apache.geode.redis.internal.executor.list.RPopExecutor;
import org.apache.geode.redis.internal.executor.list.RPushExecutor;
import org.apache.geode.redis.internal.executor.list.RPushXExecutor;
import org.apache.geode.redis.internal.executor.pubsub.PsubscribeExecutor;
import org.apache.geode.redis.internal.executor.pubsub.PublishExecutor;
import org.apache.geode.redis.internal.executor.pubsub.PunsubscribeExecutor;
import org.apache.geode.redis.internal.executor.pubsub.SubscribeExecutor;
import org.apache.geode.redis.internal.executor.pubsub.UnsubscribeExecutor;
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
import org.apache.geode.redis.internal.executor.sortedset.GeoAddExecutor;
import org.apache.geode.redis.internal.executor.sortedset.GeoDistExecutor;
import org.apache.geode.redis.internal.executor.sortedset.GeoHashExecutor;
import org.apache.geode.redis.internal.executor.sortedset.GeoPosExecutor;
import org.apache.geode.redis.internal.executor.sortedset.GeoRadiusByMemberExecutor;
import org.apache.geode.redis.internal.executor.sortedset.GeoRadiusExecutor;
import org.apache.geode.redis.internal.executor.sortedset.ZAddExecutor;
import org.apache.geode.redis.internal.executor.sortedset.ZCardExecutor;
import org.apache.geode.redis.internal.executor.sortedset.ZCountExecutor;
import org.apache.geode.redis.internal.executor.sortedset.ZIncrByExecutor;
import org.apache.geode.redis.internal.executor.sortedset.ZLexCountExecutor;
import org.apache.geode.redis.internal.executor.sortedset.ZRangeByLexExecutor;
import org.apache.geode.redis.internal.executor.sortedset.ZRangeByScoreExecutor;
import org.apache.geode.redis.internal.executor.sortedset.ZRangeExecutor;
import org.apache.geode.redis.internal.executor.sortedset.ZRankExecutor;
import org.apache.geode.redis.internal.executor.sortedset.ZRemExecutor;
import org.apache.geode.redis.internal.executor.sortedset.ZRemRangeByLexExecutor;
import org.apache.geode.redis.internal.executor.sortedset.ZRemRangeByRankExecutor;
import org.apache.geode.redis.internal.executor.sortedset.ZRemRangeByScoreExecutor;
import org.apache.geode.redis.internal.executor.sortedset.ZRevRangeByScoreExecutor;
import org.apache.geode.redis.internal.executor.sortedset.ZRevRangeExecutor;
import org.apache.geode.redis.internal.executor.sortedset.ZRevRankExecutor;
import org.apache.geode.redis.internal.executor.sortedset.ZScanExecutor;
import org.apache.geode.redis.internal.executor.sortedset.ZScoreExecutor;
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
import org.apache.geode.redis.internal.executor.transactions.DiscardExecutor;
import org.apache.geode.redis.internal.executor.transactions.ExecExecutor;
import org.apache.geode.redis.internal.executor.transactions.MultiExecutor;
import org.apache.geode.redis.internal.executor.transactions.UnwatchExecutor;
import org.apache.geode.redis.internal.executor.transactions.WatchExecutor;

/**
 * The redis command type used by the server. Each command is directly from the redis protocol and
 * calling {@link #getExecutor()} on a type returns the executor class for that command.
 */
public enum RedisCommandType {

  /***************************************
   *************** Keys ******************
   ***************************************/

  AUTH(new AuthExecutor()),
  DEL(new DelExecutor()),
  EXISTS(new ExistsExecutor()),
  EXPIRE(new ExpireExecutor()),
  EXPIREAT(new ExpireAtExecutor()),
  FLUSHALL(new FlushAllExecutor()),
  FLUSHDB(new FlushAllExecutor()),
  KEYS(new KeysExecutor()),
  PERSIST(new PersistExecutor()),
  PEXPIRE(new PExpireExecutor()),
  PEXPIREAT(new PExpireAtExecutor()),
  PTTL(new PTTLExecutor()),
  RENAME(new RenameExecutor()),
  SCAN(new ScanExecutor()),
  TTL(new TTLExecutor()),
  TYPE(new TypeExecutor()),

  /***************************************
   ************** Strings ****************
   ***************************************/

  APPEND(new AppendExecutor(), REDIS_STRING),
  BITCOUNT(new BitCountExecutor(), REDIS_STRING),
  BITOP(new BitOpExecutor(), REDIS_STRING),
  BITPOS(new BitPosExecutor(), REDIS_STRING),
  DECR(new DecrExecutor(), REDIS_STRING),
  DECRBY(new DecrByExecutor(), REDIS_STRING),
  GET(new GetExecutor(), REDIS_STRING),
  GETBIT(new GetBitExecutor(), REDIS_STRING),
  GETRANGE(new GetRangeExecutor(), REDIS_STRING),
  GETSET(new GetSetExecutor(), REDIS_STRING),
  INCR(new IncrExecutor(), REDIS_STRING),
  INCRBY(new IncrByExecutor(), REDIS_STRING),
  INCRBYFLOAT(new IncrByFloatExecutor(), REDIS_STRING),
  MGET(new MGetExecutor(), REDIS_STRING),
  MSET(new MSetExecutor(), REDIS_STRING),
  MSETNX(new MSetNXExecutor(), REDIS_STRING),
  PSETEX(new PSetEXExecutor(), REDIS_STRING),
  SETEX(new SetEXExecutor(), REDIS_STRING),
  SET(new SetExecutor(), REDIS_STRING),
  SETBIT(new SetBitExecutor(), REDIS_STRING),
  SETNX(new SetNXExecutor(), REDIS_STRING),
  SETRANGE(new SetRangeExecutor(), REDIS_STRING),
  STRLEN(new StrlenExecutor(), REDIS_STRING),

  /***************************************
   **************** Hashes ***************
   ***************************************/

  HDEL(new HDelExecutor(), REDIS_HASH),
  HEXISTS(new HExistsExecutor(), REDIS_HASH),
  HGET(new HGetExecutor(), REDIS_HASH),
  HGETALL(new HGetAllExecutor(), REDIS_HASH),
  HINCRBY(new HIncrByExecutor(), REDIS_HASH),
  HINCRBYFLOAT(new HIncrByFloatExecutor(), REDIS_HASH),
  HKEYS(new HKeysExecutor(), REDIS_HASH),
  HLEN(new HLenExecutor(), REDIS_HASH),
  HMGET(new HMGetExecutor(), REDIS_HASH),
  HMSET(new HMSetExecutor(), REDIS_HASH),
  HSCAN(new HScanExecutor(), REDIS_HASH),
  HSET(new HSetExecutor(), REDIS_HASH),
  HSETNX(new HSetNXExecutor(), REDIS_HASH),
  HVALS(new HValsExecutor(), REDIS_HASH),

  /***************************************
   *********** HyperLogLogs **************
   ***************************************/

  PFADD(new PFAddExecutor(), REDIS_HLL),
  PFCOUNT(new PFCountExecutor(), REDIS_HLL),
  PFMERGE(new PFMergeExecutor(), REDIS_HLL),

  /***************************************
   *************** Lists *****************
   ***************************************/

  LINDEX(new LIndexExecutor(), REDIS_LIST),
  LINSERT(new LInsertExecutor(), REDIS_LIST),
  LLEN(new LLenExecutor(), REDIS_LIST),
  LPOP(new LPopExecutor(), REDIS_LIST),
  LPUSH(new LPushExecutor(), REDIS_LIST),
  LPUSHX(new LPushXExecutor(), REDIS_LIST),
  LRANGE(new LRangeExecutor(), REDIS_LIST),
  LREM(new LRemExecutor(), REDIS_LIST),
  LSET(new LSetExecutor(), REDIS_LIST),
  LTRIM(new LTrimExecutor(), REDIS_LIST),
  RPOP(new RPopExecutor(), REDIS_LIST),
  RPUSH(new RPushExecutor(), REDIS_LIST),
  RPUSHX(new RPushXExecutor(), REDIS_LIST),

  /***************************************
   **************** Sets *****************
   ***************************************/

  SADD(new SAddExecutor(), REDIS_SET),
  SCARD(new SCardExecutor(), REDIS_SET),
  SDIFF(new SDiffExecutor(), REDIS_SET),
  SDIFFSTORE(new SDiffStoreExecutor(), REDIS_SET),
  SISMEMBER(new SIsMemberExecutor(), REDIS_SET),
  SINTER(new SInterExecutor(), REDIS_SET),
  SINTERSTORE(new SInterStoreExecutor(), REDIS_SET),
  SMEMBERS(new SMembersExecutor(), REDIS_SET),
  SMOVE(new SMoveExecutor(), REDIS_SET),
  SPOP(new SPopExecutor(), REDIS_SET),
  SRANDMEMBER(new SRandMemberExecutor(), REDIS_SET),
  SUNION(new SUnionExecutor(), REDIS_SET),
  SUNIONSTORE(new SUnionStoreExecutor(), REDIS_SET),
  SSCAN(new SScanExecutor(), REDIS_SET),
  SREM(new SRemExecutor(), REDIS_SET),

  /***************************************
   ************* Sorted Sets *************
   ***************************************/

  ZADD(new ZAddExecutor(), REDIS_SORTEDSET),
  ZCARD(new ZCardExecutor(), REDIS_SORTEDSET),
  ZCOUNT(new ZCountExecutor(), REDIS_SORTEDSET),
  ZINCRBY(new ZIncrByExecutor(), REDIS_SORTEDSET),
  ZLEXCOUNT(new ZLexCountExecutor(), REDIS_SORTEDSET),
  ZRANGE(new ZRangeExecutor(), REDIS_SORTEDSET),
  ZRANGEBYLEX(new ZRangeByLexExecutor(), REDIS_SORTEDSET),
  ZRANGEBYSCORE(new ZRangeByScoreExecutor(), REDIS_SORTEDSET),
  ZREVRANGE(new ZRevRangeExecutor(), REDIS_SORTEDSET),
  ZRANK(new ZRankExecutor(), REDIS_SORTEDSET),
  ZREM(new ZRemExecutor(), REDIS_SORTEDSET),
  ZREMRANGEBYLEX(new ZRemRangeByLexExecutor(), REDIS_SORTEDSET),
  ZREMRANGEBYRANK(new ZRemRangeByRankExecutor(), REDIS_SORTEDSET),
  ZREMRANGEBYSCORE(new ZRemRangeByScoreExecutor(), REDIS_SORTEDSET),
  ZREVRANGEBYSCORE(new ZRevRangeByScoreExecutor(), REDIS_SORTEDSET),
  ZREVRANK(new ZRevRankExecutor(), REDIS_SORTEDSET),
  ZSCAN(new ZScanExecutor(), REDIS_SORTEDSET),
  ZSCORE(new ZScoreExecutor(), REDIS_SORTEDSET),

  /***************************************
   ********** Publish Subscribe **********
   ***************************************/

  SUBSCRIBE(new SubscribeExecutor(), REDIS_PUBSUB),
  PUBLISH(new PublishExecutor(), REDIS_PUBSUB),
  UNSUBSCRIBE(new UnsubscribeExecutor(), REDIS_PUBSUB),
  PSUBSCRIBE(new PsubscribeExecutor(), REDIS_PUBSUB),
  PUNSUBSCRIBE(new PunsubscribeExecutor(), REDIS_PUBSUB),

  /**************************************
   * Geospatial commands ****************
   **************************************/

  GEOADD(new GeoAddExecutor(), REDIS_SORTEDSET),
  GEOHASH(new GeoHashExecutor(), REDIS_SORTEDSET),
  GEOPOS(new GeoPosExecutor(), REDIS_SORTEDSET),
  GEODIST(new GeoDistExecutor(), REDIS_SORTEDSET),
  GEORADIUS(new GeoRadiusExecutor(), REDIS_SORTEDSET),
  GEORADIUSBYMEMBER(new GeoRadiusByMemberExecutor(), REDIS_SORTEDSET),

  /***************************************
   ************ Transactions *************
   ***************************************/

  DISCARD(new DiscardExecutor()),
  EXEC(new ExecExecutor()),
  MULTI(new MultiExecutor()),
  UNWATCH(new UnwatchExecutor()),
  WATCH(new WatchExecutor()),

  /***************************************
   *************** Server ****************
   ***************************************/

  DBSIZE(new DBSizeExecutor()),
  ECHO(new EchoExecutor()),
  TIME(new TimeExecutor()),
  PING(new PingExecutor()),
  QUIT(new QuitExecutor()),
  SHUTDOWN(new ShutDownExecutor()),
  UNKNOWN(new UnkownExecutor());

  private final Executor executor;
  private final RedisDataType dataType;

  /**
   * @return {@link Executor} for the command type
   */
  public Executor getExecutor() {
    return executor;
  };

  public RedisDataType getDataType() {
    return dataType;
  }

  private RedisCommandType(Executor executor) {
    this(executor, NONE);
  }

  private RedisCommandType(Executor executor, RedisDataType dataType) {
    this.executor = executor;
    this.dataType = dataType;
  }
}

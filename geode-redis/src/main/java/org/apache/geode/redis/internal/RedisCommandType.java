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

import org.apache.geode.redis.internal.ParameterRequirements.EvenParameterRequirements;
import org.apache.geode.redis.internal.ParameterRequirements.ExactParameterRequirements;
import org.apache.geode.redis.internal.ParameterRequirements.MaximumParameterRequirements;
import org.apache.geode.redis.internal.ParameterRequirements.MinimumParameterRequirements;
import org.apache.geode.redis.internal.ParameterRequirements.ParameterRequirements;
import org.apache.geode.redis.internal.ParameterRequirements.SpopParameterRequirements;
import org.apache.geode.redis.internal.ParameterRequirements.UnspecifiedParameterRequirements;
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
import org.apache.geode.redis.internal.executor.UnknownExecutor;
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

/**
 * The redis command type used by the server. Each command is directly from the redis protocol.
 */
public enum RedisCommandType {

  /***************************************
   *** Supported Commands ***
   ***************************************/

  /*************** Keys ******************/

  AUTH(new AuthExecutor(), true),
  DEL(new DelExecutor(), true, new MinimumParameterRequirements(2)),
  EXISTS(new ExistsExecutor(), true, new MinimumParameterRequirements(2)),
  EXPIRE(new ExpireExecutor(), true),
  EXPIREAT(new ExpireAtExecutor(), true),
  FLUSHALL(new FlushAllExecutor(), true),
  FLUSHDB(new FlushAllExecutor(), true),
  KEYS(new KeysExecutor(), true),
  PERSIST(new PersistExecutor(), true),
  PEXPIRE(new PExpireExecutor(), true),
  PEXPIREAT(new PExpireAtExecutor(), true),
  PTTL(new PTTLExecutor(), true),
  RENAME(new RenameExecutor(), true),
  TTL(new TTLExecutor(), true),
  TYPE(new TypeExecutor(), true),

  /************* Strings *****************/

  APPEND(new AppendExecutor(), true, new ExactParameterRequirements(3)),
  GET(new GetExecutor(), true, new ExactParameterRequirements(2)),
  SET(new SetExecutor(), true, new MinimumParameterRequirements(3)),

  /************* Hashes *****************/

  HGETALL(new HGetAllExecutor(), true, new ExactParameterRequirements(2)),
  HMSET(new HMSetExecutor(), true,
      new MinimumParameterRequirements(4).and(new EvenParameterRequirements())),
  HSET(new HSetExecutor(), true,
      new MinimumParameterRequirements(4).and(new EvenParameterRequirements())),

  /************* Sets *****************/

  SADD(new SAddExecutor(), true, new MinimumParameterRequirements(3)),
  SMEMBERS(new SMembersExecutor(), true, new ExactParameterRequirements(2)),
  SREM(new SRemExecutor(), true, new MinimumParameterRequirements(3)),

  /********** Publish Subscribe **********/

  SUBSCRIBE(new SubscribeExecutor(), true),
  PUBLISH(new PublishExecutor(), true),
  UNSUBSCRIBE(new UnsubscribeExecutor(), true),
  PSUBSCRIBE(new PsubscribeExecutor(), true),
  PUNSUBSCRIBE(new PunsubscribeExecutor(), true),

  /*************** Server ****************/

  PING(new PingExecutor(), true),
  QUIT(new QuitExecutor(), true),
  UNKNOWN(new UnknownExecutor(), true),


  /***************************************
   *** Unsupported Commands ***
   ***************************************/

  /***************************************
   *************** Keys ******************
   ***************************************/

  SCAN(new ScanExecutor(), false),

  /***************************************
   ************** Strings ****************
   ***************************************/

  BITCOUNT(new BitCountExecutor(), false),
  BITOP(new BitOpExecutor(), false),
  BITPOS(new BitPosExecutor(), false),
  DECR(new DecrExecutor(), false),
  DECRBY(new DecrByExecutor(), false),
  GETBIT(new GetBitExecutor(), false),
  GETRANGE(new GetRangeExecutor(), false),
  GETSET(new GetSetExecutor(), false),
  INCR(new IncrExecutor(), false),
  INCRBY(new IncrByExecutor(), false),
  INCRBYFLOAT(new IncrByFloatExecutor(), false),
  MGET(new MGetExecutor(), false),
  MSET(new MSetExecutor(), false),
  MSETNX(new MSetNXExecutor(), false),
  PSETEX(new PSetEXExecutor(), false),
  SETEX(new SetEXExecutor(), false),
  SETBIT(new SetBitExecutor(), false),
  SETNX(new SetNXExecutor(), false),
  SETRANGE(new SetRangeExecutor(), false),
  STRLEN(new StrlenExecutor(), false),

  /***************************************
   **************** Hashes ***************
   ***************************************/

  HDEL(new HDelExecutor(), false, new MinimumParameterRequirements(3)),
  HEXISTS(new HExistsExecutor(), false, new ExactParameterRequirements(3)),
  HGET(new HGetExecutor(), false, new ExactParameterRequirements(3)),
  HINCRBY(new HIncrByExecutor(), false, new ExactParameterRequirements(4)),
  HINCRBYFLOAT(new HIncrByFloatExecutor(), false, new ExactParameterRequirements(4)),
  HKEYS(new HKeysExecutor(), false, new ExactParameterRequirements(2)),
  HLEN(new HLenExecutor(), false, new ExactParameterRequirements(2)),
  HMGET(new HMGetExecutor(), false, new MinimumParameterRequirements(3)),
  HSCAN(new HScanExecutor(), false, new MinimumParameterRequirements(3)),
  HSETNX(new HSetNXExecutor(), false, new ExactParameterRequirements(4)),
  HVALS(new HValsExecutor(), false, new ExactParameterRequirements(2)),

  /***************************************
   **************** Sets *****************
   ***************************************/

  SCARD(new SCardExecutor(), false, new ExactParameterRequirements(2)),
  SDIFF(new SDiffExecutor(), false, new MinimumParameterRequirements(2)),
  SDIFFSTORE(new SDiffStoreExecutor(), false, new MinimumParameterRequirements(3)),
  SISMEMBER(new SIsMemberExecutor(), false, new ExactParameterRequirements(3)),
  SINTER(new SInterExecutor(), false, new MinimumParameterRequirements(2)),
  SINTERSTORE(new SInterStoreExecutor(), false, new MinimumParameterRequirements(3)),
  SMOVE(new SMoveExecutor(), false, new ExactParameterRequirements(4)),
  SPOP(new SPopExecutor(), false,
      new MinimumParameterRequirements(2).and(new MaximumParameterRequirements(3))
          .and(new SpopParameterRequirements())),
  SRANDMEMBER(new SRandMemberExecutor(), false, new MinimumParameterRequirements(2)),
  SUNION(new SUnionExecutor(), false, new MinimumParameterRequirements(2)),
  SUNIONSTORE(new SUnionStoreExecutor(), false, new MinimumParameterRequirements(3)),
  SSCAN(new SScanExecutor(), false, new MinimumParameterRequirements(3)),

  /***************************************
   *************** Server ****************
   ***************************************/

  DBSIZE(new DBSizeExecutor(), false),
  ECHO(new EchoExecutor(), false),
  TIME(new TimeExecutor(), false),
  SHUTDOWN(new ShutDownExecutor(), false);

  private final Executor executor;
  private final ParameterRequirements parameterRequirements;
  private final boolean supported;

  RedisCommandType(Executor executor, boolean supported) {
    this(executor, supported, new UnspecifiedParameterRequirements());
  }

  RedisCommandType(Executor executor, boolean supported,
      ParameterRequirements parameterRequirements) {
    this.executor = executor;
    this.supported = supported;
    this.parameterRequirements = parameterRequirements;
  }

  public boolean isSupported() {
    return supported;
  }

  public RedisResponse executeCommand(Command command,
      ExecutionHandlerContext executionHandlerContext) {
    parameterRequirements.checkParameters(command, executionHandlerContext);

    return executor.executeCommandWithResponse(command, executionHandlerContext);
  }
}

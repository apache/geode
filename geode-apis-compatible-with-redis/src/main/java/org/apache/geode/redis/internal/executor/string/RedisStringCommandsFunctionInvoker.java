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
package org.apache.geode.redis.internal.executor.string;

import static org.apache.geode.redis.internal.RedisCommandType.APPEND;
import static org.apache.geode.redis.internal.RedisCommandType.BITCOUNT;
import static org.apache.geode.redis.internal.RedisCommandType.BITOP;
import static org.apache.geode.redis.internal.RedisCommandType.BITPOS;
import static org.apache.geode.redis.internal.RedisCommandType.DECR;
import static org.apache.geode.redis.internal.RedisCommandType.DECRBY;
import static org.apache.geode.redis.internal.RedisCommandType.GET;
import static org.apache.geode.redis.internal.RedisCommandType.GETBIT;
import static org.apache.geode.redis.internal.RedisCommandType.GETRANGE;
import static org.apache.geode.redis.internal.RedisCommandType.GETSET;
import static org.apache.geode.redis.internal.RedisCommandType.INCR;
import static org.apache.geode.redis.internal.RedisCommandType.INCRBY;
import static org.apache.geode.redis.internal.RedisCommandType.INCRBYFLOAT;
import static org.apache.geode.redis.internal.RedisCommandType.MGET;
import static org.apache.geode.redis.internal.RedisCommandType.SET;
import static org.apache.geode.redis.internal.RedisCommandType.SETBIT;
import static org.apache.geode.redis.internal.RedisCommandType.SETRANGE;
import static org.apache.geode.redis.internal.RedisCommandType.STRLEN;

import java.math.BigDecimal;
import java.util.List;

import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.data.RedisData;
import org.apache.geode.redis.internal.data.RedisKey;
import org.apache.geode.redis.internal.executor.RedisCommandsFunctionInvoker;

/**
 * This class is used by netty redis string command executors
 * to invoke a geode function that will run on a
 * particular server to do the redis command.
 */
public class RedisStringCommandsFunctionInvoker extends RedisCommandsFunctionInvoker
    implements RedisStringCommands {

  public RedisStringCommandsFunctionInvoker(Region<RedisKey, RedisData> region) {
    super(region);
  }

  @Override
  public long append(RedisKey key, byte[] valueToAppend) {
    return invokeCommandFunction(key, APPEND, valueToAppend);
  }

  @Override
  public byte[] get(RedisKey key) {
    return invokeCommandFunction(key, GET);
  }

  @Override
  public boolean set(RedisKey key, byte[] value, SetOptions options) {
    return invokeCommandFunction(key, SET, value, options);
  }

  @Override
  public long incr(RedisKey key) {
    return invokeCommandFunction(key, INCR);
  }

  @Override
  public long decr(RedisKey key) {
    return invokeCommandFunction(key, DECR);
  }

  @Override
  public byte[] getset(RedisKey key, byte[] value) {
    return invokeCommandFunction(key, GETSET, value);
  }

  @Override
  public long incrby(RedisKey key, long increment) {
    return invokeCommandFunction(key, INCRBY, increment);
  }

  @Override
  public long decrby(RedisKey key, long decrement) {
    return invokeCommandFunction(key, DECRBY, decrement);
  }

  @Override
  public byte[] getrange(RedisKey key, long start, long end) {
    return invokeCommandFunction(key, GETRANGE, start, end);
  }

  @Override
  public long bitcount(RedisKey key, int start, int end) {
    return invokeCommandFunction(key, BITCOUNT, start, end);
  }

  @Override
  public long bitcount(RedisKey key) {
    return invokeCommandFunction(key, BITCOUNT);
  }

  @Override
  public int strlen(RedisKey key) {
    return invokeCommandFunction(key, STRLEN);
  }

  @Override
  public int getbit(RedisKey key, int offset) {
    return invokeCommandFunction(key, GETBIT, offset);
  }

  @Override
  public int setbit(RedisKey key, long offset, int value) {
    return invokeCommandFunction(key, SETBIT, offset, value);
  }

  @Override
  public BigDecimal incrbyfloat(RedisKey key, BigDecimal increment) {
    return invokeCommandFunction(key, INCRBYFLOAT, increment);
  }

  @Override
  public int bitop(String operation, RedisKey destKey, List<RedisKey> sources) {
    return invokeCommandFunction(destKey, BITOP, operation, sources);
  }

  @Override
  public int bitpos(RedisKey key, int bit, int start, Integer end) {
    return invokeCommandFunction(key, BITPOS, bit, start, end);
  }

  @Override
  public int setrange(RedisKey key, int offset, byte[] value) {
    return invokeCommandFunction(key, SETRANGE, offset, value);
  }

  @Override
  public byte[] mget(RedisKey key) {
    return invokeCommandFunction(key, MGET);
  }
}

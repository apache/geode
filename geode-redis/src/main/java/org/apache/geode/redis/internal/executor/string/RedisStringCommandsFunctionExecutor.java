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
import static org.apache.geode.redis.internal.RedisCommandType.DECR;
import static org.apache.geode.redis.internal.RedisCommandType.DECRBY;
import static org.apache.geode.redis.internal.RedisCommandType.GET;
import static org.apache.geode.redis.internal.RedisCommandType.GETSET;
import static org.apache.geode.redis.internal.RedisCommandType.INCR;
import static org.apache.geode.redis.internal.RedisCommandType.INCRBY;
import static org.apache.geode.redis.internal.RedisCommandType.SET;

import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.RedisData;
import org.apache.geode.redis.internal.executor.CommandFunction;

public class RedisStringCommandsFunctionExecutor implements RedisStringCommands {
  private final Region<ByteArrayWrapper, RedisData> region;

  public RedisStringCommandsFunctionExecutor(Region<ByteArrayWrapper, RedisData> region) {
    this.region = region;
  }

  @Override
  public long append(ByteArrayWrapper key, ByteArrayWrapper valueToAppend) {
    return CommandFunction.execute(APPEND, key, valueToAppend, region);
  }

  @Override
  public ByteArrayWrapper get(ByteArrayWrapper key) {
    return CommandFunction.execute(GET, key, null, region);
  }

  @Override
  public boolean set(ByteArrayWrapper key, ByteArrayWrapper value, SetOptions options) {
    return CommandFunction.execute(SET, key, new Object[] {value, options}, region);
  }

  @Override
  public long incr(ByteArrayWrapper key) {
    return CommandFunction.execute(INCR, key, null, region);
  }

  @Override
  public long decr(ByteArrayWrapper key) {
    return CommandFunction.execute(DECR, key, null, region);
  }

  @Override
  public ByteArrayWrapper getset(ByteArrayWrapper key, ByteArrayWrapper value) {
    return CommandFunction.execute(GETSET, key, value, region);
  }

  @Override
  public long incrby(ByteArrayWrapper key, long increment) {
    return CommandFunction.execute(INCRBY, key, increment, region);
  }

  @Override
  public long decrby(ByteArrayWrapper key, long decrement) {
    return CommandFunction.execute(DECRBY, key, decrement, region);
  }
}

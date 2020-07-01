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
 *
 */

package org.apache.geode.redis.internal.data;

import org.apache.geode.redis.internal.executor.key.RedisKeyCommands;

public class RedisKeyCommandsFunctionExecutor extends RedisDataCommandsFunctionExecutor implements
    RedisKeyCommands {

  public RedisKeyCommandsFunctionExecutor(
      CommandHelper helper) {
    super(helper);
  }

  @Override
  public boolean del(ByteArrayWrapper key) {
    return stripedExecute(key, () -> getRegion().remove(key) != null);
  }

  @Override
  public boolean exists(ByteArrayWrapper key) {
    return stripedExecute(key, () -> getRedisData(key).exists());
  }

  @Override
  public long pttl(ByteArrayWrapper key) {
    return stripedExecute(key, () -> getRedisData(key).pttl(getRegion(), key));
  }

  @Override
  public int pexpireat(ByteArrayWrapper key, long timestamp) {
    return stripedExecute(key,
        () -> getRedisData(key).pexpireat(helper, key, timestamp));
  }

  @Override
  public int persist(ByteArrayWrapper key) {
    return stripedExecute(key, () -> getRedisData(key).persist(getRegion(), key));
  }

  @Override
  public String type(ByteArrayWrapper key) {
    return stripedExecute(key, () -> getRedisData(key).type());
  }

  @Override
  public boolean rename(ByteArrayWrapper oldKey, ByteArrayWrapper newKey) {
    // caller has already done all the stripedExecutor locking
    return getRedisData(oldKey).rename(getRegion(), oldKey, newKey);
  }
}

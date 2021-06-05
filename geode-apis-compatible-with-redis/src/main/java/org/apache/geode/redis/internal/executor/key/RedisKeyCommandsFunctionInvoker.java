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
package org.apache.geode.redis.internal.executor.key;

import static org.apache.geode.redis.internal.RedisCommandType.DEL;
import static org.apache.geode.redis.internal.RedisCommandType.EXISTS;
import static org.apache.geode.redis.internal.RedisCommandType.INTERNALPTTL;
import static org.apache.geode.redis.internal.RedisCommandType.INTERNALTYPE;
import static org.apache.geode.redis.internal.RedisCommandType.PERSIST;
import static org.apache.geode.redis.internal.RedisCommandType.PEXPIREAT;
import static org.apache.geode.redis.internal.RedisCommandType.PTTL;
import static org.apache.geode.redis.internal.RedisCommandType.TYPE;

import java.util.ArrayList;
import java.util.List;

import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.data.RedisData;
import org.apache.geode.redis.internal.data.RedisKey;
import org.apache.geode.redis.internal.executor.RedisCommandsFunctionInvoker;

/**
 * This class is used by netty redis key command executors to invoke a geode function that will run
 * on a particular server to do the redis command.
 */
public class RedisKeyCommandsFunctionInvoker extends RedisCommandsFunctionInvoker
    implements RedisKeyCommands {
  public RedisKeyCommandsFunctionInvoker(
      Region<RedisKey, RedisData> region) {
    super(region);
  }

  @Override
  public boolean del(RedisKey key) {
    return invokeCommandFunction(key, DEL);
  }

  @Override
  public boolean exists(RedisKey key) {
    return invokeCommandFunction(key, EXISTS);
  }

  @Override
  public long pttl(RedisKey key) {
    return invokeCommandFunction(key, PTTL);
  }

  @Override
  public long internalPttl(RedisKey key) {
    return invokeCommandFunction(key, INTERNALPTTL);
  }

  @Override
  public int pexpireat(RedisKey key, long timestamp) {
    return invokeCommandFunction(key, PEXPIREAT, timestamp);
  }

  @Override
  public int persist(RedisKey key) {
    return invokeCommandFunction(key, PERSIST);
  }

  @Override
  public String type(RedisKey key) {
    return invokeCommandFunction(key, TYPE);
  }

  @Override
  public String internalType(RedisKey key) {
    return invokeCommandFunction(key, INTERNALTYPE);
  }

  @Override
  public boolean rename(RedisKey oldKey, RedisKey newKey) {
    if (!region.containsKey(oldKey)) {
      return false;
    }

    List<RedisKey> keysToOperateOn = new ArrayList<>();
    keysToOperateOn.add(oldKey);
    keysToOperateOn.add(newKey);

    return invoke(RenameFunction.ID, oldKey, oldKey, newKey, keysToOperateOn, new ArrayList<>(),
        new ArrayList<>());
  }
}

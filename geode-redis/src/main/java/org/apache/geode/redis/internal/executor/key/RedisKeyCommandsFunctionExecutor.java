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
import static org.apache.geode.redis.internal.RedisCommandType.PERSIST;
import static org.apache.geode.redis.internal.RedisCommandType.PEXPIREAT;
import static org.apache.geode.redis.internal.RedisCommandType.PTTL;
import static org.apache.geode.redis.internal.RedisCommandType.TYPE;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.redis.internal.data.ByteArrayWrapper;
import org.apache.geode.redis.internal.data.RedisData;
import org.apache.geode.redis.internal.executor.CommandFunction;
import org.apache.geode.redis.internal.executor.SingleResultCollector;

public class RedisKeyCommandsFunctionExecutor implements RedisKeyCommands {
  private final Region<ByteArrayWrapper, RedisData> region;

  public RedisKeyCommandsFunctionExecutor(
      Region<ByteArrayWrapper, RedisData> region) {
    this.region = region;
  }

  @Override
  public boolean del(ByteArrayWrapper key) {
    return CommandFunction.execute(DEL, key, null, region);
  }

  @Override
  public boolean exists(ByteArrayWrapper key) {
    return CommandFunction.execute(EXISTS, key, null, region);
  }

  @Override
  public long pttl(ByteArrayWrapper key) {
    return CommandFunction.execute(PTTL, key, null, region);
  }

  @Override
  public int pexpireat(ByteArrayWrapper key, long timestamp) {
    return CommandFunction.execute(PEXPIREAT, key, timestamp, region);
  }

  @Override
  public int persist(ByteArrayWrapper key) {
    return CommandFunction.execute(PERSIST, key, null, region);
  }

  @Override
  public String type(ByteArrayWrapper key) {
    return CommandFunction.execute(TYPE, key, null, region);
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean rename(ByteArrayWrapper oldKey, ByteArrayWrapper newKey) {

    if (!region.containsKey(oldKey)) {
      return false;
    }

    List<ByteArrayWrapper> keysToOperateOn = new ArrayList<>();
    keysToOperateOn.add(oldKey);
    keysToOperateOn.add(newKey);

    SingleResultCollector<Boolean> rc = new SingleResultCollector<>();



    FunctionService
        .onRegion(region)
        .withFilter(Collections.singleton(keysToOperateOn.get(0)))
        .setArguments(
            new Object[] {oldKey, newKey, keysToOperateOn, new ArrayList<>(), new ArrayList<>()})
        .withCollector(rc)
        .execute(RenameFunction.ID)
        .getResult();

    return rc.getResult();
  }
}

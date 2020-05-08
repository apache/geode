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

package org.apache.geode.redis.internal.executor.hash;

import static org.apache.geode.redis.internal.RedisCommandType.DEL;
import static org.apache.geode.redis.internal.RedisCommandType.HDEL;
import static org.apache.geode.redis.internal.RedisCommandType.HGETALL;
import static org.apache.geode.redis.internal.RedisCommandType.HSET;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.RedisCommandType;
import org.apache.geode.redis.internal.RedisDataType;
import org.apache.geode.redis.internal.executor.CommandFunction;

@SuppressWarnings("unchecked")
public class RedisHashCommandsFunctionExecutor implements RedisHashCommands {

  private final Region<ByteArrayWrapper, RedisHash> region;

  public RedisHashCommandsFunctionExecutor(Region region) {
    this.region = region;
  }

  @Override
  public int hset(ByteArrayWrapper key, List<ByteArrayWrapper> fieldsToSet, boolean NX) {
    ResultCollector<Object[], List<Integer>> results =
        executeFunction(HSET, key, new Object[] {fieldsToSet, NX});
    return results.getResult().get(0);
  }

  @Override
  public int hdel(ByteArrayWrapper key, List<ByteArrayWrapper> fieldsToRemove) {
    ResultCollector<Object[], List<Integer>> results = executeFunction(HDEL, key, fieldsToRemove);
    return results.getResult().get(0);
  }

  @Override
  public boolean del(ByteArrayWrapper key) {
    ResultCollector<Object[], List<Boolean>> results = executeFunction(DEL, key,
        RedisDataType.REDIS_HASH);
    return results.getResult().get(0);
  }

  @Override
  public Collection<Map.Entry<ByteArrayWrapper, ByteArrayWrapper>> hgetall(ByteArrayWrapper key) {
    ResultCollector<Object[], List<Collection<Map.Entry<ByteArrayWrapper, ByteArrayWrapper>>>> results =
        executeFunction(HGETALL, key, null);
    return results.getResult().get(0);
  }

  private ResultCollector executeFunction(RedisCommandType command,
      ByteArrayWrapper key,
      Object commandArguments) {
    return CommandFunction.execute(region, command, key, commandArguments);
  }
}

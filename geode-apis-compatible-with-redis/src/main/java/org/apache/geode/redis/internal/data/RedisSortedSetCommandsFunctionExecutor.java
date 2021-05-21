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


import java.util.List;

import org.apache.geode.redis.internal.executor.sortedset.RedisSortedSetCommands;
import org.apache.geode.redis.internal.executor.sortedset.SortedSetOptions;

public class RedisSortedSetCommandsFunctionExecutor extends RedisDataCommandsFunctionExecutor
    implements RedisSortedSetCommands {

  public RedisSortedSetCommandsFunctionExecutor(CommandHelper helper) {
    super(helper);
  }

  private RedisSortedSet getRedisSortedSet(RedisKey key, boolean updateStats) {
    return helper.getRedisSortedSet(key, updateStats);
  }

  @Override
  public long zadd(RedisKey key, List<byte[]> scoresAndMembersToAdd, SortedSetOptions options) {
    return stripedExecute(key,
        () -> getRedisSortedSet(key, false).zadd(getRegion(), key, scoresAndMembersToAdd, options));
  }

  @Override
  public byte[] zscore(RedisKey key, byte[] member) {
    return stripedExecute(key, () -> getRedisSortedSet(key, true).zscore(member));
  }
}

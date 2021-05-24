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

package org.apache.geode.redis.internal.executor.sortedset;


import static org.apache.geode.redis.internal.RedisCommandType.ZADD;
import static org.apache.geode.redis.internal.RedisCommandType.ZSCORE;

import java.util.List;

import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.data.RedisData;
import org.apache.geode.redis.internal.data.RedisKey;
import org.apache.geode.redis.internal.executor.RedisCommandsFunctionInvoker;

/**
 * This class is used by netty redis set command executors
 * to invoke a geode function that will run on a
 * particular server to do the redis command.
 */
public class RedisSortedSetCommandsFunctionInvoker extends RedisCommandsFunctionInvoker
    implements RedisSortedSetCommands {

  public RedisSortedSetCommandsFunctionInvoker(Region<RedisKey, RedisData> region) {
    super(region);
  }

  @Override
  public long zadd(RedisKey key, List<byte[]> scoresAndMembersToAdd,
      ZAddOptions options) {
    return invokeCommandFunction(key, ZADD, scoresAndMembersToAdd, options);
  }

  @Override
  public byte[] zscore(RedisKey key, byte[] member) {
    return invokeCommandFunction(key, ZSCORE, member);
  }
}

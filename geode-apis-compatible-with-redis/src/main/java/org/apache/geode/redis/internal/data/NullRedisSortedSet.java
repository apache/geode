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


import java.util.ArrayList;
import java.util.List;

import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.executor.sortedset.SortedSetOptions;

class NullRedisSortedSet extends RedisSortedSet {

  NullRedisSortedSet() {
    super(new ArrayList<>());
  }

  @Override
  public boolean isNull() {
    return true;
  }

  @Override
  long zadd(Region<RedisKey, RedisData> region, RedisKey key, List<byte[]> membersToAdd,
      SortedSetOptions options) {
    if (options.isXX()) {
      return 0;
    }
    region.create(key, new RedisSortedSet(membersToAdd, options));
    return membersToAdd.size() / 2;
  }

  @Override
  byte[] zscore(byte[] member) {
    return null;
  }
}

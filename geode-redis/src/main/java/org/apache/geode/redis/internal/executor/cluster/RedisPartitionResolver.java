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

package org.apache.geode.redis.internal.executor.cluster;

import static org.apache.geode.redis.internal.RegionProvider.REDIS_SLOTS;
import static org.apache.geode.redis.internal.RegionProvider.REDIS_SLOTS_PER_BUCKET;

import org.apache.geode.cache.EntryOperation;
import org.apache.geode.cache.PartitionResolver;
import org.apache.geode.redis.internal.data.RedisData;
import org.apache.geode.redis.internal.data.RedisKey;

public class RedisPartitionResolver implements PartitionResolver<RedisKey, RedisData> {

  @Override
  public Object getRoutingObject(EntryOperation<RedisKey, RedisData> opDetails) {
    // & (REDIS_SLOTS - 1) is equivalent to % REDIS_SLOTS but supposedly faster
    return opDetails.getKey().getCrc16() & (REDIS_SLOTS - 1) / REDIS_SLOTS_PER_BUCKET;
  }

  @Override
  public String getName() {
    return RedisPartitionResolver.class.getName();
  }

}

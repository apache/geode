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
package org.apache.geode.redis.internal;

import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalRegionFactory;
import org.apache.geode.management.ManagementException;
import org.apache.geode.redis.internal.data.RedisData;
import org.apache.geode.redis.internal.data.RedisKey;
import org.apache.geode.redis.internal.executor.cluster.RedisPartitionResolver;
import org.apache.geode.redis.internal.executor.hash.RedisHashCommands;
import org.apache.geode.redis.internal.executor.hash.RedisHashCommandsFunctionInvoker;
import org.apache.geode.redis.internal.executor.sortedset.RedisSortedSetCommands;
import org.apache.geode.redis.internal.executor.sortedset.RedisSortedSetCommandsFunctionInvoker;

public class RegionProvider {
  /**
   * The name of the region that holds data stored in redis.
   */
  public static final String REDIS_DATA_REGION = "REDIS_DATA";
  public static final String REDIS_REGION_BUCKETS_PARAM = "redis.region.buckets";

  // Ideally the bucket count should be a power of 2, but technically it is not required.
  public static final int REDIS_REGION_BUCKETS =
      Integer.getInteger(REDIS_REGION_BUCKETS_PARAM, 128);

  public static final int REDIS_SLOTS = 16384;

  public static final int REDIS_SLOTS_PER_BUCKET = REDIS_SLOTS / REDIS_REGION_BUCKETS;

  private final Region<RedisKey, RedisData> dataRegion;
  private final RedisHashCommandsFunctionInvoker hashCommands;
  private final RedisSortedSetCommandsFunctionInvoker sortedSetCommands;
  private final SlotAdvisor slotAdvisor;

  public RegionProvider(InternalCache cache) {
    validateBucketCount(REDIS_REGION_BUCKETS);

    InternalRegionFactory<RedisKey, RedisData> redisDataRegionFactory =
        cache.createInternalRegionFactory(RegionShortcut.PARTITION_REDUNDANT);

    PartitionAttributesFactory<RedisKey, RedisData> attributesFactory =
        new PartitionAttributesFactory<>();
    attributesFactory.setPartitionResolver(new RedisPartitionResolver());
    attributesFactory.setTotalNumBuckets(REDIS_REGION_BUCKETS);
    redisDataRegionFactory.setPartitionAttributes(attributesFactory.create());

    dataRegion = redisDataRegionFactory.create(REDIS_DATA_REGION);

    hashCommands = new RedisHashCommandsFunctionInvoker(dataRegion);
    sortedSetCommands = new RedisSortedSetCommandsFunctionInvoker(dataRegion);

    slotAdvisor = new SlotAdvisor(dataRegion);
  }

  public Region<RedisKey, RedisData> getDataRegion() {
    return dataRegion;
  }

  public SlotAdvisor getSlotAdvisor() {
    return slotAdvisor;
  }

  /**
   * Validates that the value passed in is not greater than {@link #REDIS_SLOTS}.
   *
   * @throws ManagementException if there is a problem with the value
   */
  protected static void validateBucketCount(int buckets) {
    if (buckets > REDIS_SLOTS) {
      throw new ManagementException(String.format(
          "Could not start server compatible with Redis - System property '%s' must be <= %d",
          REDIS_REGION_BUCKETS_PARAM, REDIS_SLOTS));
    }
  }

  public RedisHashCommands getHashCommands() {
    return hashCommands;
  }

  public RedisSortedSetCommands getSortedSetCommands() {
    return sortedSetCommands;
  }
}

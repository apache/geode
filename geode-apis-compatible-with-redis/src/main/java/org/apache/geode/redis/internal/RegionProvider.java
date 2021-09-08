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

import static org.apache.geode.redis.internal.data.NullRedisDataStructures.NULL_REDIS_DATA;
import static org.apache.geode.redis.internal.data.NullRedisDataStructures.NULL_REDIS_HASH;
import static org.apache.geode.redis.internal.data.NullRedisDataStructures.NULL_REDIS_SET;
import static org.apache.geode.redis.internal.data.NullRedisDataStructures.NULL_REDIS_SORTED_SET;
import static org.apache.geode.redis.internal.data.NullRedisDataStructures.NULL_REDIS_STRING;
import static org.apache.geode.redis.internal.data.RedisDataType.REDIS_DATA;
import static org.apache.geode.redis.internal.data.RedisDataType.REDIS_HASH;
import static org.apache.geode.redis.internal.data.RedisDataType.REDIS_SET;
import static org.apache.geode.redis.internal.data.RedisDataType.REDIS_SORTED_SET;
import static org.apache.geode.redis.internal.data.RedisDataType.REDIS_STRING;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import org.apache.geode.cache.CacheTransactionManager;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalRegionFactory;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PrimaryBucketLockException;
import org.apache.geode.internal.cache.execute.BucketMovedException;
import org.apache.geode.management.ManagementException;
import org.apache.geode.redis.internal.cluster.RedisMemberInfo;
import org.apache.geode.redis.internal.data.NullRedisDataStructures;
import org.apache.geode.redis.internal.data.RedisData;
import org.apache.geode.redis.internal.data.RedisDataMovedException;
import org.apache.geode.redis.internal.data.RedisDataType;
import org.apache.geode.redis.internal.data.RedisDataTypeMismatchException;
import org.apache.geode.redis.internal.data.RedisHashCommandsFunctionExecutor;
import org.apache.geode.redis.internal.data.RedisKey;
import org.apache.geode.redis.internal.data.RedisKeyCommandsFunctionExecutor;
import org.apache.geode.redis.internal.data.RedisSetCommandsFunctionExecutor;
import org.apache.geode.redis.internal.data.RedisSortedSetCommandsFunctionExecutor;
import org.apache.geode.redis.internal.data.RedisString;
import org.apache.geode.redis.internal.data.RedisStringCommandsFunctionExecutor;
import org.apache.geode.redis.internal.executor.cluster.RedisPartitionResolver;
import org.apache.geode.redis.internal.executor.hash.RedisHashCommands;
import org.apache.geode.redis.internal.executor.key.RedisKeyCommands;
import org.apache.geode.redis.internal.executor.set.RedisSetCommands;
import org.apache.geode.redis.internal.executor.sortedset.RedisSortedSetCommands;
import org.apache.geode.redis.internal.executor.string.RedisStringCommands;
import org.apache.geode.redis.internal.services.StripedCoordinator;
import org.apache.geode.redis.internal.statistics.RedisStats;
import org.apache.geode.util.internal.UncheckedUtils;

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

  private static final Map<RedisDataType, RedisData> NULL_TYPES = new HashMap<>();

  private final Region<RedisKey, RedisData> dataRegion;
  private final PartitionedRegion partitionedRegion;
  private final RedisHashCommandsFunctionExecutor hashCommands;
  private final RedisSetCommandsFunctionExecutor setCommands;
  private final RedisStringCommandsFunctionExecutor stringCommands;
  private final RedisSortedSetCommandsFunctionExecutor sortedSetCommands;
  private final RedisKeyCommandsFunctionExecutor keyCommands;
  private final SlotAdvisor slotAdvisor;
  private final StripedCoordinator stripedCoordinator;
  private final RedisStats redisStats;

  static {
    NULL_TYPES.put(REDIS_STRING, NULL_REDIS_STRING);
    NULL_TYPES.put(REDIS_HASH, NULL_REDIS_HASH);
    NULL_TYPES.put(REDIS_SET, NULL_REDIS_SET);
    NULL_TYPES.put(REDIS_SORTED_SET, NULL_REDIS_SORTED_SET);
    NULL_TYPES.put(REDIS_DATA, NULL_REDIS_DATA);
  }

  public RegionProvider(InternalCache cache, StripedCoordinator stripedCoordinator,
      RedisStats redisStats) {
    validateBucketCount(REDIS_REGION_BUCKETS);

    this.stripedCoordinator = stripedCoordinator;
    this.redisStats = redisStats;

    InternalRegionFactory<RedisKey, RedisData> redisDataRegionFactory =
        cache.createInternalRegionFactory(RegionShortcut.PARTITION_REDUNDANT);

    PartitionAttributesFactory<RedisKey, RedisData> attributesFactory =
        new PartitionAttributesFactory<>();
    attributesFactory.setPartitionResolver(new RedisPartitionResolver());
    attributesFactory.setTotalNumBuckets(REDIS_REGION_BUCKETS);
    redisDataRegionFactory.setPartitionAttributes(attributesFactory.create());

    dataRegion = redisDataRegionFactory.create(REDIS_DATA_REGION);
    partitionedRegion = (PartitionedRegion) dataRegion;

    CacheTransactionManager txManager = cache.getCacheTransactionManager();

    stringCommands = new RedisStringCommandsFunctionExecutor(this, txManager);
    setCommands = new RedisSetCommandsFunctionExecutor(this, txManager);
    hashCommands = new RedisHashCommandsFunctionExecutor(this, txManager);
    sortedSetCommands = new RedisSortedSetCommandsFunctionExecutor(this, txManager);
    keyCommands = new RedisKeyCommandsFunctionExecutor(this, txManager);

    slotAdvisor = new SlotAdvisor(dataRegion, cache.getMyId());
  }

  public Region<RedisKey, RedisData> getLocalDataRegion() {
    return PartitionRegionHelper.getLocalPrimaryData(dataRegion);
  }

  public Region<RedisKey, RedisData> getDataRegion() {
    return dataRegion;
  }

  public SlotAdvisor getSlotAdvisor() {
    return slotAdvisor;
  }

  public RedisStats getRedisStats() {
    return redisStats;
  }

  public <T> T execute(RedisKey key, Callable<T> callable) {
    try {
      return partitionedRegion.computeWithPrimaryLocked(key,
          () -> stripedCoordinator.execute(key, callable));
    } catch (PrimaryBucketLockException | BucketMovedException | RegionDestroyedException ex) {
      throw createRedisDataMovedException(key);
    } catch (RedisException bex) {
      throw bex;
    } catch (Exception ex) {
      throw new RedisException(ex);
    }
  }

  public <T> T execute(RedisKey key, List<RedisKey> keysToLock, Callable<T> callable) {
    try {
      return partitionedRegion.computeWithPrimaryLocked(key,
          () -> stripedCoordinator.execute(keysToLock, callable));
    } catch (PrimaryBucketLockException | BucketMovedException | RegionDestroyedException ex) {
      throw createRedisDataMovedException(key);
    } catch (RedisException bex) {
      throw bex;
    } catch (Exception ex) {
      throw new RedisException(ex);
    }
  }

  public RedisData getRedisData(RedisKey key) {
    return getRedisData(key, NullRedisDataStructures.NULL_REDIS_DATA);
  }

  public RedisData getRedisData(RedisKey key, RedisData notFoundValue) {
    RedisData result;
    try {
      result = getLocalDataRegion().get(key);
    } catch (RegionDestroyedException rex) {
      throw createRedisDataMovedException(key);
    }

    if (result != null) {
      if (result.hasExpired()) {
        result.doExpiration(this, key);
        result = null;
      }
    } else {
      if (!getSlotAdvisor().isLocal(key)) {
        throw createRedisDataMovedException(key);
      }
    }
    if (result == null) {
      return notFoundValue;
    } else {
      return result;
    }
  }

  private RedisDataMovedException createRedisDataMovedException(RedisKey key) {
    RedisMemberInfo memberInfo = getRedisMemberInfo(key);
    int slot = key.getSlot();
    return new RedisDataMovedException(slot, memberInfo.getHostAddress(),
        memberInfo.getRedisPort());
  }

  private RedisMemberInfo getRedisMemberInfo(RedisKey key) {
    try {
      return getSlotAdvisor().getMemberInfo(key);
    } catch (InterruptedException ex) {
      throw new RuntimeException("Unable to determine location for key: " + key + " - " +
          ex.getMessage());
    }
  }

  public <T extends RedisData> T getTypedRedisData(RedisDataType type, RedisKey key,
      boolean updateStats) {
    RedisData redisData = getRedisData(key, NULL_TYPES.get(type));
    if (updateStats) {
      if (redisData == NULL_TYPES.get(type)) {
        redisStats.incKeyspaceMisses();
      } else {
        redisStats.incKeyspaceHits();
      }
    }

    return checkType(redisData, type);
  }

  private <T extends RedisData> T checkType(RedisData redisData, RedisDataType type) {
    if (redisData == null) {
      return null;
    }
    if (redisData.getType() != type) {
      throw new RedisDataTypeMismatchException(RedisConstants.ERROR_WRONG_TYPE);
    }
    return UncheckedUtils.uncheckedCast(redisData);
  }

  private RedisString checkStringTypeIgnoringMismatch(RedisData redisData) {
    if (redisData == null) {
      return null;
    }
    if (redisData.getType() != REDIS_STRING) {
      return NULL_REDIS_STRING;
    }
    return (RedisString) redisData;
  }

  public RedisString getRedisStringIgnoringType(RedisKey key, boolean updateStats) {
    RedisData redisData = getRedisData(key, NULL_REDIS_STRING);
    if (updateStats) {
      if (redisData == NULL_REDIS_STRING) {
        redisStats.incKeyspaceMisses();
      } else {
        redisStats.incKeyspaceHits();
      }
    }

    return checkStringTypeIgnoringMismatch(redisData);
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

  public RedisStringCommands getStringCommands() {
    return stringCommands;
  }

  public RedisSetCommands getSetCommands() {
    return setCommands;
  }

  public RedisKeyCommands getKeyCommands() {
    return keyCommands;
  }

  @SuppressWarnings("unchecked")
  public Set<DistributedMember> getRemoteRegionMembers() {
    return (Set<DistributedMember>) (Set<?>) partitionedRegion.getRegionAdvisor().adviseDataStore();
  }

  /**
   * Check if a key would be stored locally (in a primary bucket on this server). Otherwise throw a
   * {@link RedisDataMovedException}. Note that this will not check for the actual existence of the
   * key.
   */
  public void ensureKeyIsLocal(RedisKey key) {
    if (!slotAdvisor.isLocal(key)) {
      throw createRedisDataMovedException(key);
    }
  }

}

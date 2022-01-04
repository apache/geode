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
package org.apache.geode.redis.internal.services;

import static org.apache.geode.redis.internal.RedisProperties.REDIS_REGION_NAME_PROPERTY;
import static org.apache.geode.redis.internal.RedisProperties.REGION_BUCKETS;
import static org.apache.geode.redis.internal.RedisProperties.getIntegerSystemProperty;
import static org.apache.geode.redis.internal.RedisProperties.getStringSystemProperty;
import static org.apache.geode.redis.internal.data.NullRedisDataStructures.NULL_REDIS_STRING;
import static org.apache.geode.redis.internal.data.RedisDataType.REDIS_DATA;
import static org.apache.geode.redis.internal.data.RedisDataType.REDIS_STRING;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

import org.apache.geode.cache.CacheTransactionManager;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalRegionFactory;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PrimaryBucketLockException;
import org.apache.geode.internal.cache.control.HeapMemoryMonitor;
import org.apache.geode.internal.cache.control.InternalResourceManager;
import org.apache.geode.internal.cache.control.MemoryThresholds;
import org.apache.geode.internal.cache.execute.BucketMovedException;
import org.apache.geode.redis.internal.RedisConstants;
import org.apache.geode.redis.internal.RedisException;
import org.apache.geode.redis.internal.commands.executor.cluster.RedisPartitionResolver;
import org.apache.geode.redis.internal.data.RedisData;
import org.apache.geode.redis.internal.data.RedisDataMovedException;
import org.apache.geode.redis.internal.data.RedisDataType;
import org.apache.geode.redis.internal.data.RedisDataTypeMismatchException;
import org.apache.geode.redis.internal.data.RedisKey;
import org.apache.geode.redis.internal.data.RedisString;
import org.apache.geode.redis.internal.services.cluster.RedisMemberInfo;
import org.apache.geode.redis.internal.services.cluster.SlotAdvisor;
import org.apache.geode.redis.internal.services.locking.StripedCoordinator;
import org.apache.geode.redis.internal.statistics.RedisStats;
import org.apache.geode.util.internal.UncheckedUtils;

public class RegionProvider {
  /**
   * The name of the region that holds data stored in redis.
   */
  public static final String DEFAULT_REDIS_REGION_NAME = "GEODE_FOR_REDIS";
  public static final String REDIS_REGION_BUCKETS_PARAM = REGION_BUCKETS;

  public static final int REDIS_SLOTS = 16384;

  // Ideally the bucket count should be a power of 2, but technically it is not required.
  public static final int REDIS_REGION_BUCKETS =
      getIntegerSystemProperty(REDIS_REGION_BUCKETS_PARAM, 128, 1, REDIS_SLOTS);

  public static final int REDIS_SLOTS_PER_BUCKET = REDIS_SLOTS / REDIS_REGION_BUCKETS;

  private final Region<RedisKey, RedisData> dataRegion;
  private final PartitionedRegion partitionedRegion;
  private final SlotAdvisor slotAdvisor;
  private final StripedCoordinator stripedCoordinator;
  private final RedisStats redisStats;
  private final CacheTransactionManager txManager;
  private final String redisRegionName;

  public RegionProvider(InternalCache cache, StripedCoordinator stripedCoordinator,
      RedisStats redisStats) {
    this.stripedCoordinator = stripedCoordinator;
    this.redisStats = redisStats;

    InternalRegionFactory<RedisKey, RedisData> redisDataRegionFactory =
        cache.createInternalRegionFactory(RegionShortcut.PARTITION_REDUNDANT);

    PartitionAttributesFactory<RedisKey, RedisData> attributesFactory =
        new PartitionAttributesFactory<>();
    DistributionConfig config = cache.getInternalDistributedSystem().getConfig();
    attributesFactory.setRedundantCopies(config.getRedisRedundantCopies());
    attributesFactory.setPartitionResolver(new RedisPartitionResolver());
    attributesFactory.setTotalNumBuckets(REDIS_REGION_BUCKETS);
    redisDataRegionFactory.setPartitionAttributes(attributesFactory.create());

    redisRegionName =
        getStringSystemProperty(REDIS_REGION_NAME_PROPERTY, DEFAULT_REDIS_REGION_NAME);

    dataRegion = redisDataRegionFactory.create(redisRegionName);

    partitionedRegion = (PartitionedRegion) dataRegion;

    txManager = cache.getCacheTransactionManager();

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

  public String getRedisRegionName() {
    return redisRegionName;
  }

  public <T> T lockedExecute(RedisKey key, Callable<T> callable) {
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

  public <T> T lockedExecute(RedisKey key, List<RedisKey> keysToLock, Callable<T> callable) {
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

  /**
   * Execute the given Callable in the context of a GemFire transaction. On failure there is no
   * attempt to retry.
   */
  public <T> T lockedExecuteInTransaction(RedisKey key, Callable<T> callable) {
    Callable<T> txWrappedCallable = getTxWrappedCallable(callable);
    return lockedExecute(key, txWrappedCallable);
  }

  /**
   * Execute the given Callable in the context of a GemFire transaction. On failure there is no
   * attempt to retry.
   */
  public <T> T lockedExecuteInTransaction(RedisKey key, List<RedisKey> keysToLock,
      Callable<T> callable) {
    Callable<T> txWrappedCallable = getTxWrappedCallable(callable);
    return lockedExecute(key, keysToLock, txWrappedCallable);
  }

  private <T> Callable<T> getTxWrappedCallable(Callable<T> callable) {
    return () -> {
      T result;
      boolean success = false;
      txManager.begin();
      try {
        result = callable.call();
        success = true;
      } finally {
        if (success) {
          txManager.commit();
        } else {
          txManager.rollback();
        }
      }
      return result;
    };
  }

  public RedisData getRedisData(RedisKey key) {
    return getRedisData(key, false);
  }

  public RedisData getRedisData(RedisKey key, boolean updateStats) {
    return getRedisData(key, REDIS_DATA.getNullType(), updateStats);
  }

  private RedisData getRedisData(RedisKey key, RedisData notFoundValue, boolean updateStats) {
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
      if (updateStats) {
        redisStats.incKeyspaceMisses();
      }
      return notFoundValue;
    } else {
      if (updateStats) {
        redisStats.incKeyspaceHits();
      }
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

  public <T extends RedisData> T getTypedRedisDataElseRemove(RedisDataType type, RedisKey key,
      boolean updateStats) {
    RedisData redisData = getRedisData(key, null, updateStats);
    if (redisData == null) {
      return null;
    }

    if (redisData.getType() != type) {
      getDataRegion().remove(key);
      return null;
    }
    return UncheckedUtils.uncheckedCast(redisData);
  }

  public <T extends RedisData> T getTypedRedisData(RedisDataType type, RedisKey key,
      boolean updateStats) {
    RedisData redisData = getRedisData(key, type.getNullType(), updateStats);
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
    RedisData redisData = getRedisData(key, NULL_REDIS_STRING, updateStats);
    return checkStringTypeIgnoringMismatch(redisData);
  }

  @SuppressWarnings("unchecked")
  public Set<DistributedMember> getRemoteRegionMembers() {
    return (Set<DistributedMember>) (Set<?>) partitionedRegion.getRegionAdvisor().adviseDataStore();
  }

  @SuppressWarnings("unchecked")
  public Set<DistributedMember> getRegionMembers() {
    Set<DistributedMember> result = (Set<DistributedMember>) (Set<?>) partitionedRegion
        .getRegionAdvisor().adviseDataStore(true);
    result.add(partitionedRegion.getCache().getMyId());
    return result;
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

  private HeapMemoryMonitor getHeapMemoryMonitor() {
    return InternalResourceManager.getInternalResourceManager(partitionedRegion.getCache())
        .getHeapMonitor();
  }

  public Set<DistributedMember> getCriticalMembers() {
    if (MemoryThresholds.isLowMemoryExceptionDisabled()) {
      return Collections.emptySet();
    }
    return getHeapMemoryMonitor().getHeapCriticalMembersFrom(getRegionMembers());
  }
}

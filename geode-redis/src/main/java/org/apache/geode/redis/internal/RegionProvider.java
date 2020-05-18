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

import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheTransactionManager;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.TransactionId;
import org.apache.geode.cache.query.IndexExistsException;
import org.apache.geode.cache.query.IndexInvalidException;
import org.apache.geode.cache.query.IndexNameConflictException;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryInvalidException;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.RegionNotFoundException;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.hll.HyperLogLogPlus;
import org.apache.geode.management.cli.Result.Status;
import org.apache.geode.management.internal.cli.commands.CreateRegionCommand;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.redis.internal.executor.ExpirationExecutor;
import org.apache.geode.redis.internal.executor.ListQuery;
import org.apache.geode.redis.internal.executor.SortedSetQuery;

/**
 * This class stands between {@link Executor} and {@link Cache#getRegion(String)}. This is needed
 * because some keys for Redis represented as a {@link Region} in {@link GeodeRedisServer} come with
 * additional state. Therefore getting, creating, or destroying a {@link Region} needs to be
 * synchronized, which is done away with and abstracted by this class.
 */
public class RegionProvider implements Closeable {
  private final ConcurrentHashMap<ByteArrayWrapper, Region<Object, Object>> dynamicRegions;

  /**
   * This is the Redis meta data {@link Region} that holds the {@link RedisDataType} information for
   * all Regions created. The mapping is a {@link String} key which is the name of the {@link
   * Region} created to hold the data to the RedisDataType it contains.
   */
  private final KeyRegistrar keyRegistrar;

  /**
   * This is the {@link RedisDataType#REDIS_STRING} {@link Region}. This is the Region that stores
   * all string contents
   */
  private final Region<ByteArrayWrapper, ByteArrayWrapper> stringsRegion;

  /**
   * This is the {@link RedisDataType#REDIS_HLL} {@link Region}. This is the Region that stores all
   * HyperLogLog contents
   */
  private final Region<ByteArrayWrapper, HyperLogLogPlus> hLLRegion;

  private final Region<ByteArrayWrapper, RedisData> dataRegion;

  private final Cache cache;
  private final QueryService queryService;
  private final ConcurrentMap<ByteArrayWrapper, Map<Enum<?>, Query>> preparedQueries =
      new ConcurrentHashMap<>();
  private final ConcurrentMap<ByteArrayWrapper, ScheduledFuture<?>> expirationsMap;
  private final ScheduledExecutorService expirationExecutor;
  private final RegionShortcut defaultRegionType;
  @Immutable
  private static final CreateRegionCommand createRegionCmd = new CreateRegionCommand();
  private final ConcurrentHashMap<ByteArrayWrapper, Lock> dynamicRegionLocks;

  @SuppressWarnings("deprecation")
  public RegionProvider(Region<ByteArrayWrapper, ByteArrayWrapper> stringsRegion,
      Region<ByteArrayWrapper, HyperLogLogPlus> hLLRegion,
      KeyRegistrar redisMetaRegion,
      ConcurrentMap<ByteArrayWrapper, ScheduledFuture<?>> expirationsMap,
      ScheduledExecutorService expirationExecutor, RegionShortcut defaultShortcut,
      Region<ByteArrayWrapper, RedisData> dataRegion) {

    this(stringsRegion, hLLRegion, redisMetaRegion, expirationsMap, expirationExecutor,
        defaultShortcut, dataRegion, GemFireCacheImpl.getInstance());
  }

  public RegionProvider(Region<ByteArrayWrapper, ByteArrayWrapper> stringsRegion,
      Region<ByteArrayWrapper, HyperLogLogPlus> hLLRegion,
      KeyRegistrar redisMetaRegion,
      ConcurrentMap<ByteArrayWrapper, ScheduledFuture<?>> expirationsMap,
      ScheduledExecutorService expirationExecutor, RegionShortcut defaultShortcut,
      Region<ByteArrayWrapper, RedisData> dataRegion, Cache cache) {
    if (stringsRegion == null || hLLRegion == null || redisMetaRegion == null) {
      throw new NullPointerException();
    }
    this.dataRegion = dataRegion;
    dynamicRegions = new ConcurrentHashMap<>();
    this.stringsRegion = stringsRegion;
    this.hLLRegion = hLLRegion;
    keyRegistrar = redisMetaRegion;
    this.cache = cache;
    queryService = cache.getQueryService();
    this.expirationsMap = expirationsMap;
    this.expirationExecutor = expirationExecutor;
    defaultRegionType = defaultShortcut;
    dynamicRegionLocks = new ConcurrentHashMap<>();
  }

  public Region<?, ?> getRegion(ByteArrayWrapper key) {
    if (key == null) {
      return null;
    }

    return dynamicRegions.get(key);

  }

  public Region<ByteArrayWrapper, ?> getRegionForType(RedisDataType redisDataType) {
    if (redisDataType == null) {
      return null;
    }

    switch (redisDataType) {
      case REDIS_STRING:
        return stringsRegion;

      case REDIS_HASH:
      case REDIS_SET:
        return dataRegion;

      case REDIS_HLL:
        return hLLRegion;

      case REDIS_PROTECTED:
      case REDIS_PUBSUB:
      case NONE:
      case REDIS_LIST:
      case REDIS_SORTEDSET:
      default:
        return null;
    }
  }

  public void removeRegionReferenceLocally(ByteArrayWrapper key, RedisDataType type) {
    if (!typeUsesDynamicRegions(type)) {
      return;
    }
    if (getRegion(key) == null) {
      return;
    }
    Lock lock = dynamicRegionLocks.get(key);
    if (lock == null) {
      return;
    }
    boolean locked = lock.tryLock();
    if (!locked) {
      // If we cannot get the lock we ignore this remote event, this key has local event
      // that started independently, ignore this event to prevent deadlock
      return;
    }
    try {
      cancelKeyExpiration(key);
      removeRegionState(key, type);
    } finally {
      lock.unlock();
    }
  }

  public boolean removeKey(ByteArrayWrapper key) {
    RedisDataType type = keyRegistrar.getType(key);
    return removeKey(key, type);
  }

  public boolean removeKey(ByteArrayWrapper key, RedisDataType type) {
    return removeKey(key, type, true);
  }

  private boolean typeStoresDataInKeyRegistrar(RedisDataType type) {
    if (type == RedisDataType.REDIS_SET) {
      return true;
    }
    if (type == RedisDataType.REDIS_HASH) {
      return true;
    }
    return false;
  }

  public boolean removeKey(ByteArrayWrapper key, RedisDataType type, boolean cancelExpiration) {
    if (type == RedisDataType.REDIS_PROTECTED) {
      return false;
    }
    Lock lock = dynamicRegionLocks.get(key);
    try {
      if (lock != null) { // only typeUsesDynamicRegions will have a lock
        lock.lock();
      }
      if (!typeStoresDataInKeyRegistrar(type)) {
        keyRegistrar.unregister(key);
      }
      try {
        if (type == RedisDataType.REDIS_STRING) {
          return stringsRegion.remove(key) != null;
        } else if (type == RedisDataType.REDIS_HLL) {
          return hLLRegion.remove(key) != null;
        } else if (type == RedisDataType.REDIS_LIST || type == RedisDataType.REDIS_SORTEDSET) {
          return destroyRegion(key, type);
        } else {
          return false;
        }
      } catch (Exception exc) {
        return false;
      } finally {
        if (cancelExpiration) {
          cancelKeyExpiration(key);
        } else {
          removeKeyExpiration(key);
        }
        if (lock != null) {
          dynamicRegionLocks.remove(key);
        }
      }
    } finally {
      if (lock != null) {
        lock.unlock();
      }
    }
  }

  public Region<?, ?> getOrCreateRegion(ByteArrayWrapper key, RedisDataType type,
      ExecutionHandlerContext context) {
    return getOrCreateRegion0(key, type, context, true);
  }

  public boolean typeUsesDynamicRegions(RedisDataType type) {
    return type == RedisDataType.REDIS_LIST || type == RedisDataType.REDIS_SORTEDSET;
  }

  public void createRemoteRegionReferenceLocally(ByteArrayWrapper key, RedisDataType type) {
    if (!typeUsesDynamicRegions(type)) {
      return;
    }
    Region<Object, Object> r = dynamicRegions.get(key);
    if (r != null) {
      return;
    }
    Lock lock = dynamicRegionLocks.get(key);
    if (lock == null) {
      Lock newLock = new ReentrantLock();
      lock = dynamicRegionLocks.putIfAbsent(key, newLock);
      if (lock == null) {
        lock = newLock;
      }
    }
    boolean locked = lock.tryLock();
    // If we cannot get the lock then this remote event may have been initialized
    // independently on this machine, so if we wait on the lock it is more than
    // likely we will deadlock just to do the same task. This event can be ignored
    if (locked) {
      try {
        r = cache.getRegion(key.toString());
        // If r is null, this implies that we are after a create/destroy
        // simply ignore. Calls to getRegion or getOrCreate will work correctly
        if (r == null) {
          // TODO: one caller of this method only calls it if getRegion returned null. It was
          // expecting us to create it locally. If someone else will create it locally then this
          // method does not need to be called.
          return;
        }

        if (type == RedisDataType.REDIS_LIST) {
          doInitializeList(key, r);
        } else if (type == RedisDataType.REDIS_SORTEDSET) {
          try {
            doInitializeSortedSet(key, r);
          } catch (RegionNotFoundException | IndexInvalidException e) {
            // ignore
          }
        }
        dynamicRegions.put(key, r);
      } finally {
        lock.unlock();
      }
    }
  }

  private Region<?, ?> getOrCreateRegion0(ByteArrayWrapper key, RedisDataType type,
      ExecutionHandlerContext context, boolean addToMeta) {

    keyRegistrar.validate(key, type);
    Region<Object, Object> r = dynamicRegions.get(key);
    if (r != null && r.isDestroyed()) {
      removeKey(key, type);
      r = null;
    }
    if (r == null) {
      Lock lock = dynamicRegionLocks.get(key);
      if (lock == null) {
        Lock newLock = new ReentrantLock();
        lock = dynamicRegionLocks.putIfAbsent(key, newLock);
        if (lock == null) {
          lock = newLock;
        }
      }

      lock.lock();
      try {
        r = dynamicRegions.get(key);
        if (r == null) {
          boolean hasTransaction = context != null && context.hasTransaction(); // Can create
          // without context
          CacheTransactionManager txm = null;
          TransactionId transactionId = null;
          try {
            if (hasTransaction) {
              txm = cache.getCacheTransactionManager();
              transactionId = txm.suspend();
            }
            Exception concurrentCreateDestroyException;
            do {
              concurrentCreateDestroyException = null;

              r = createRegionGlobally(key.toString());

              try {
                if (type == RedisDataType.REDIS_LIST) {
                  doInitializeList(key, r);
                } else if (type == RedisDataType.REDIS_SORTEDSET) {
                  try {
                    doInitializeSortedSet(key, r);
                  } catch (RegionNotFoundException | IndexInvalidException e) {
                    concurrentCreateDestroyException = e;
                  }
                }
              } catch (QueryInvalidException e) {
                if (e.getCause() instanceof RegionNotFoundException) {
                  concurrentCreateDestroyException = e;
                }
              }
            } while (concurrentCreateDestroyException != null);
            dynamicRegions.put(key, r);
            if (addToMeta) {
              keyRegistrar.register(key, type);
            }
          } finally {
            if (hasTransaction) {
              txm.resume(transactionId);
            }
          }
        }
      } finally {
        lock.unlock();
      }
    }
    return r;
  }

  /**
   * SYNCHRONIZE EXTERNALLY OF this.locks.get(key)!!!!!
   *
   * @param key Key of region to destroy
   * @param type Type of region to destroyu
   * @return Flag if destroyed
   */
  private boolean destroyRegion(ByteArrayWrapper key, RedisDataType type) {
    Region<?, ?> r = dynamicRegions.get(key);
    if (r != null) {
      try {
        r.destroyRegion();
      } catch (Exception e) {
        return false;
      } finally {
        removeRegionState(key, type);
      }
    }
    return true;
  }

  /**
   * Do not call this method if you are not synchronized on the lock associated with this key
   *
   * @param key Key of region to remove
   * @param type Type of key to remove all state
   */
  private void removeRegionState(ByteArrayWrapper key, RedisDataType type) {
    preparedQueries.remove(key);
    dynamicRegions.remove(key);
  }

  private void doInitializeSortedSet(ByteArrayWrapper key, Region<?, ?> r)
      throws RegionNotFoundException, IndexInvalidException {
    String fullpath = r.getFullPath();
    try {
      queryService.createIndex("scoreIndex", "entry.value.score",
          r.getFullPath() + ".entrySet entry");
      queryService.createIndex("scoreIndex2", "value.score", r.getFullPath() + ".values value");
    } catch (IndexNameConflictException | IndexExistsException | UnsupportedOperationException e) {
      // ignore, these indexes already exist or unsupported but make sure prepared queries are made
    }
    HashMap<Enum<?>, Query> queryList = new HashMap<>();
    for (SortedSetQuery lq : SortedSetQuery.values()) {
      String queryString = lq.getQueryString(fullpath);
      Query query = queryService.newQuery(queryString);
      queryList.put(lq, query);
    }
    preparedQueries.put(key, queryList);
  }

  private void doInitializeList(ByteArrayWrapper key, Region<Object, Object> r) {
    r.put("head", 0);
    r.put("tail", 0);
    String fullpath = r.getFullPath();
    HashMap<Enum<?>, Query> queryList = new HashMap<>();
    for (ListQuery lq : ListQuery.values()) {
      String queryString = lq.getQueryString(fullpath);
      Query query = queryService.newQuery(queryString);
      queryList.put(lq, query);
    }
    preparedQueries.put(key, queryList);
  }

  /**
   * This method creates a Region globally with the given name. If there is an error in the
   * creation, a runtime exception will be thrown.
   *
   * @param regionPath Name of Region to create
   * @return Region Region created globally
   */
  private Region<Object, Object> createRegionGlobally(String regionPath) {
    Region<Object, Object> r;
    r = cache.getRegion(regionPath);
    if (r != null) {
      return r;
    }
    do {
      createRegionCmd.setCache(cache);
      ResultModel resultModel =
          createRegionCmd.createRegion(regionPath, defaultRegionType, null, null, true,
              null, null, null, null, null, null, null, null, false, false, true, false, false,
              false,
              true, null, null, null, null, null, null, null, null, null, null, null, null, null,
              false,
              null, null, null, null, null, null, null, null, null, null, null);

      r = cache.getRegion(regionPath);
      if (resultModel.getStatus() == Status.ERROR && r == null) {
        String err = "Unable to create region named \"" + regionPath + "\":\n";
        // TODO: check this
        throw new RegionCreationException(err + resultModel.toJson());
      }
    } while (r == null); // The region can be null in the case that it is concurrently destroyed by
    // a remote even triggered internally by Geode
    return r;
  }

  public Query getQuery(ByteArrayWrapper key, Enum<?> query) {
    return preparedQueries.get(key).get(query);
  }

  public boolean regionExists(ByteArrayWrapper key) {
    return dynamicRegions.containsKey(key);
  }

  public Region<ByteArrayWrapper, ByteArrayWrapper> getStringsRegion() {
    return stringsRegion;
  }

  public Region<ByteArrayWrapper, RedisData> getDataRegion() {
    return dataRegion;
  }

  public Region<ByteArrayWrapper, HyperLogLogPlus> gethLLRegion() {
    return hLLRegion;
  }

  /**
   * Sets the expiration for a key. The setting and modifying of a key expiration can only be set by
   * a delay, which means that both expiring after a time and at a time can be done but the delay to
   * expire at a time must be calculated before these calls. It is also important to note that the
   * delay is always handled in milliseconds
   *
   * @param key The key to set the expiration for
   * @param delay The delay in milliseconds of the expiration
   * @return True is expiration set, false otherwise
   */
  public boolean setExpiration(ByteArrayWrapper key, long delay) {
    RedisDataType type = keyRegistrar.getType(key);
    if (type == null) {
      return false;
    }
    ScheduledFuture<?> future = expirationExecutor
        .schedule(new ExpirationExecutor(key, type, this), delay, TimeUnit.MILLISECONDS);
    expirationsMap.put(key, future);
    return true;
  }

  /**
   * Modifies an expiration on a key
   *
   * @param key String key to modify expiration on
   * @param delay Delay in milliseconds to reset the expiration to
   * @return True if reset, false if not
   */
  public boolean modifyExpiration(ByteArrayWrapper key, long delay) {
    /*
     * Attempt to cancel future task
     */
    boolean canceled = cancelKeyExpiration(key);

    if (!canceled) {
      return false;
    }

    RedisDataType type = keyRegistrar.getType(key);
    if (type == null) {
      return false;
    }

    ScheduledFuture<?> future = expirationExecutor
        .schedule(new ExpirationExecutor(key, type, this), delay, TimeUnit.MILLISECONDS);
    expirationsMap.put(key, future);
    return true;
  }

  /**
   * Removes an expiration from a key
   *
   * @param key Key
   * @return True is expiration cancelled on the key, false otherwise
   */
  public boolean cancelKeyExpiration(ByteArrayWrapper key) {
    ScheduledFuture<?> future = expirationsMap.remove(key);
    if (future == null) {
      return false;
    }
    return future.cancel(false);
  }

  private boolean removeKeyExpiration(ByteArrayWrapper key) {
    return expirationsMap.remove(key) != null;
  }

  /**
   * Check method if key has expiration
   *
   * @param key Key
   * @return True if key has expiration, false otherwise
   */
  public boolean hasExpiration(ByteArrayWrapper key) {
    return expirationsMap.containsKey(key);
  }

  /**
   * Get remaining expiration time
   *
   * @param key Key
   * @return Remaining time in milliseconds or 0 if no delay or key doesn't exist
   */
  public long getExpirationDelayMillis(ByteArrayWrapper key) {
    ScheduledFuture<?> future = expirationsMap.get(key);
    return future != null ? future.getDelay(TimeUnit.MILLISECONDS) : 0L;
  }

  @Override
  public void close() {
    preparedQueries.clear();
  }

  public String dumpRegionsCache() {
    StringBuilder builder = new StringBuilder();
    for (Entry<ByteArrayWrapper, Region<Object, Object>> e : dynamicRegions.entrySet()) {
      builder.append(e.getKey()).append(" --> {").append(e.getValue()).append("}\n");
    }
    return builder.toString();
  }
}

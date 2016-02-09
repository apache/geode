/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.internal.redis;

import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheTransactionManager;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.TransactionId;
import com.gemstone.gemfire.cache.query.IndexExistsException;
import com.gemstone.gemfire.cache.query.IndexInvalidException;
import com.gemstone.gemfire.cache.query.IndexNameConflictException;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryInvalidException;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.RegionNotFoundException;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.redis.executor.ExpirationExecutor;
import com.gemstone.gemfire.internal.redis.executor.ListQuery;
import com.gemstone.gemfire.internal.redis.executor.SortedSetQuery;
import com.gemstone.gemfire.internal.redis.executor.hll.HyperLogLogPlus;
import com.gemstone.gemfire.management.cli.Result;
import com.gemstone.gemfire.management.cli.Result.Status;
import com.gemstone.gemfire.management.internal.cli.commands.CreateAlterDestroyRegionCommands;
import com.gemstone.gemfire.redis.GemFireRedisServer;

/**
 * This class stands between {@link Executor} and {@link Cache#getRegion(String)}.
 * This is needed because some keys for Redis represented as a {@link Region} in
 * {@link GemFireRedisServer} come with additional state. Therefore getting, creating,
 * or destroying a {@link Region} needs to be synchronized, which is done away with
 * and abstracted by this class.
 * 
 * @author Vitaly Gavrilov
 *
 */
public class RegionProvider implements Closeable {

  private final ConcurrentHashMap<ByteArrayWrapper, Region<?, ?>> regions;

  /**
   * This is the Redis meta data {@link Region} that holds the {@link RedisDataType}
   * information for all Regions created. The mapping is a {@link String} key which is the name
   * of the {@link Region} created to hold the data to the RedisDataType it contains.
   */
  private final Region<String, RedisDataType> redisMetaRegion;

  /**
   * This is the {@link RedisDataType#REDIS_STRING} {@link Region}. This is the Region
   * that stores all string contents
   */
  private final Region<ByteArrayWrapper, ByteArrayWrapper> stringsRegion;

  /**
   * This is the {@link RedisDataType#REDIS_HLL} {@link Region}. This is the Region
   * that stores all HyperLogLog contents
   */
  private final Region<ByteArrayWrapper, HyperLogLogPlus> hLLRegion;

  private final Cache cache;
  private final QueryService queryService;
  private final ConcurrentMap<ByteArrayWrapper, Map<Enum<?>, Query>> preparedQueries = new ConcurrentHashMap<ByteArrayWrapper, Map<Enum<?>, Query>>();
  private final ConcurrentMap<ByteArrayWrapper, ScheduledFuture<?>> expirationsMap;
  private final ScheduledExecutorService expirationExecutor;
  private final RegionShortcut defaultRegionType;
  private static final CreateAlterDestroyRegionCommands cliCmds = new CreateAlterDestroyRegionCommands();
  private final ConcurrentHashMap<String, Lock> locks;

  public RegionProvider(Region<ByteArrayWrapper, ByteArrayWrapper> stringsRegion, Region<ByteArrayWrapper, HyperLogLogPlus> hLLRegion, Region<String, RedisDataType> redisMetaRegion, ConcurrentMap<ByteArrayWrapper, ScheduledFuture<?>> expirationsMap, ScheduledExecutorService expirationExecutor, RegionShortcut defaultShortcut) {
    if (stringsRegion == null || hLLRegion == null || redisMetaRegion == null)
      throw new NullPointerException();
    this.regions = new ConcurrentHashMap<ByteArrayWrapper, Region<?, ?>>();
    this.stringsRegion = stringsRegion;
    this.hLLRegion = hLLRegion;
    this.redisMetaRegion = redisMetaRegion;
    this.cache = GemFireCacheImpl.getInstance();
    this.queryService = cache.getQueryService();
    this.expirationsMap = expirationsMap;
    this.expirationExecutor = expirationExecutor;
    this.defaultRegionType = defaultShortcut;
    this.locks = new ConcurrentHashMap<String, Lock>();
  }

  public boolean existsKey(ByteArrayWrapper key) {
    return this.redisMetaRegion.containsKey(key.toString());
  }

  public Set<String> metaKeySet() {
    return this.redisMetaRegion.keySet();
  }

  public Set<Map.Entry<String, RedisDataType>> metaEntrySet() {
    return this.redisMetaRegion.entrySet();
  }

  public int getMetaSize() {
    return this.redisMetaRegion.size() - RedisConstants.NUM_DEFAULT_KEYS;
  }

  private boolean metaRemoveEntry(ByteArrayWrapper key) {
    return this.redisMetaRegion.remove(key.toString()) != null;
  }

  public RedisDataType metaPutIfAbsent(ByteArrayWrapper key, RedisDataType value) {
    return this.redisMetaRegion.putIfAbsent(key.toString(), value);
  }

  public RedisDataType metaPut(ByteArrayWrapper key, RedisDataType value) {
    return this.redisMetaRegion.put(key.toString(), value);
  }

  public RedisDataType metaGet(ByteArrayWrapper key) {
    return this.redisMetaRegion.get(key.toString());
  }

  public Region<?, ?> getRegion(ByteArrayWrapper key) {
    return this.regions.get(key);
  }

  public void removeRegionReferenceLocally(ByteArrayWrapper key, RedisDataType type) {
    Lock lock = this.locks.get(key.toString());
    boolean locked = false;
    try {
      locked = lock.tryLock();
      // If we cannot get the lock we ignore this remote event, this key has local event
      // that started independently, ignore this event to prevent deadlock
      if (locked) {
        cancelKeyExpiration(key);
        removeRegionState(key, type);
      }
    } finally {
      if (locked) {
        lock.unlock();
      }
    }
  }

  public boolean removeKey(ByteArrayWrapper key) {
    RedisDataType type = getRedisDataType(key);
    return removeKey(key, type);
  }

  public boolean removeKey(ByteArrayWrapper key, RedisDataType type) {
    return removeKey(key, type, true);
  }

  public boolean removeKey(ByteArrayWrapper key, RedisDataType type, boolean cancelExpiration) {
    if (type == null || type == RedisDataType.REDIS_PROTECTED)
      return false;
    Lock lock = this.locks.get(key.toString());
    try {
      if (lock != null)  {// Strings/hlls will not have locks
        lock.lock();
      }
      metaRemoveEntry(key);
      try {
        if (type == RedisDataType.REDIS_STRING) {
          return this.stringsRegion.remove(key) != null;
        } else if (type == RedisDataType.REDIS_HLL) {
          return this.hLLRegion.remove(key) != null;
        } else {
          return destroyRegion(key, type);
        }
      } catch (Exception exc) {
        return false;
      } finally {
        if (cancelExpiration)
          cancelKeyExpiration(key);
        else
          removeKeyExpiration(key);
        if (lock != null)
          this.locks.remove(key.toString());
      }
    } finally {
      if (lock != null) {
        lock.unlock();
      }
    }
  }

  public Region<?, ?> getOrCreateRegion(ByteArrayWrapper key, RedisDataType type, ExecutionHandlerContext context) {
    return getOrCreateRegion0(key, type, context, true);
  }

  public void createRemoteRegionReferenceLocally(ByteArrayWrapper key, RedisDataType type) {
    if (type == null || type == RedisDataType.REDIS_STRING || type == RedisDataType.REDIS_HLL)
      return;
    Region<?, ?> r = this.regions.get(key);
    if (r != null)
      return;
    if (!this.regions.contains(key)) {
      String stringKey = key.toString();
      Lock lock = this.locks.get(stringKey);
      if (lock == null) {
        this.locks.putIfAbsent(stringKey, new ReentrantLock());
        lock = this.locks.get(stringKey);
      }
      boolean locked = false;
      try {
        locked = lock.tryLock();
        // If we cannot get the lock then this remote event may have been initialized
        // independently on this machine, so if we wait on the lock it is more than
        // likely we will deadlock just to do the same task. This event can be ignored
        if (locked) {
          r = cache.getRegion(key.toString());
          // If r is null, this implies that we are after a create/destroy
          // simply ignore. Calls to getRegion or getOrCreate will work correctly
          if (r == null)
            return;

          if (type == RedisDataType.REDIS_LIST) {
            doInitializeList(key, r);
          } else if (type == RedisDataType.REDIS_SORTEDSET) {
            try {
              doInitializeSortedSet(key, r);
            } catch (RegionNotFoundException | IndexInvalidException e) {
              //ignore
            }
          }
          this.regions.put(key, r);
        }
      } finally {
        if (locked) {
          lock.unlock();
        }
      }
    }
  }

  private Region<?, ?> getOrCreateRegion0(ByteArrayWrapper key, RedisDataType type, ExecutionHandlerContext context, boolean addToMeta) {
    checkDataType(key, type);
    Region<?, ?> r = this.regions.get(key);
    if (r != null && r.isDestroyed()) {
      removeKey(key, type);
      r = null;
    }
    if (r == null) {
      String stringKey = key.toString();
      Lock lock = this.locks.get(stringKey);
      if (lock == null) {
        this.locks.putIfAbsent(stringKey, new ReentrantLock());
        lock = this.locks.get(stringKey);
      }

      try {
        lock.lock();
        r = regions.get(key);
        if (r == null) {
          boolean hasTransaction = context != null && context.hasTransaction(); // Can create without context
          CacheTransactionManager txm = null;
          TransactionId transactionId = null;
          try {
            if (hasTransaction) {
              txm = cache.getCacheTransactionManager();
              transactionId = txm.suspend();
            }
            Exception concurrentCreateDestroyException = null;
            do {
              concurrentCreateDestroyException = null;
              r = createRegionGlobally(stringKey);
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
            } while(concurrentCreateDestroyException != null);
            this.regions.put(key, r);            
            if (addToMeta) {
              RedisDataType existingType = metaPutIfAbsent(key, type);
              if (existingType != null && existingType != type)
                throw new RedisDataTypeMismatchException("The key name \"" + key + "\" is already used by a " + existingType.toString());
            }
          } finally {
            if (hasTransaction)
              txm.resume(transactionId);
          }
        }
      } finally {
        lock.unlock();
      }
    }
    return r;
  }

  /**
   * SYNCHRONIZE EXTERNALLY OF this.locks.get(key.toString())!!!!!
   * 
   * @param key Key of region to destroy
   * @param type Type of region to destroyu
   * @return Flag if destroyed
   */
  private boolean destroyRegion(ByteArrayWrapper key, RedisDataType type) {
    Region<?, ?> r = this.regions.get(key);
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
    this.preparedQueries.remove(key);
    this.regions.remove(key);
  }

  private void doInitializeSortedSet(ByteArrayWrapper key, Region<?, ?> r) throws RegionNotFoundException, IndexInvalidException {
    String fullpath = r.getFullPath();
    try {
      queryService.createIndex("scoreIndex", "entry.value.score", r.getFullPath() + ".entrySet entry");
      queryService.createIndex("scoreIndex2", "value.score", r.getFullPath() + ".values value");
    } catch (IndexNameConflictException | IndexExistsException | UnsupportedOperationException e) {
      // ignore, these indexes already exist or unsupported but make sure prepared queries are made
    }
    HashMap<Enum<?>, Query> queryList = new HashMap<Enum<?>, Query>();
    for (SortedSetQuery lq: SortedSetQuery.values()) {
      String queryString = lq.getQueryString(fullpath);
      Query query = this.queryService.newQuery(queryString);
      queryList.put(lq, query);
    }
    this.preparedQueries.put(key, queryList);
  }

  private void doInitializeList(ByteArrayWrapper key, Region r) {
    r.put("head", Integer.valueOf(0));
    r.put("tail", Integer.valueOf(0));
    String fullpath = r.getFullPath();
    HashMap<Enum<?>, Query> queryList = new HashMap<Enum<?>, Query>();
    for (ListQuery lq: ListQuery.values()) {
      String queryString = lq.getQueryString(fullpath);
      Query query = this.queryService.newQuery(queryString);
      queryList.put(lq, query);
    }
    this.preparedQueries.put(key, queryList);
  }

  /**
   * This method creates a Region globally with the given name. If
   * there is an error in the creation, a runtime exception will
   * be thrown.
   * 
   * @param key Name of Region to create
   * @return Region Region created globally
   */
  private Region<?, ?> createRegionGlobally(String key) {
    Region<?, ?> r = null;
    r = cache.getRegion(key);
    if (r != null) return r;
    do {
      Result result = cliCmds.createRegion(key, defaultRegionType, null, null, true, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null);
      r = cache.getRegion(key);
      if (result.getStatus() == Status.ERROR && r == null) {
        String err = "";
        while(result.hasNextLine())
          err += result.nextLine();
        throw new RegionCreationException(err);
      }
    } while(r == null); // The region can be null in the case that it is concurrently destroyed by
    // a remote even triggered internally by Geode
    return r;
  }

  public Query getQuery(ByteArrayWrapper key, Enum<?> query) {
    return this.preparedQueries.get(key).get(query);
    /*
    if (query instanceof ListQuery) {
      return this.queryService.newQuery(((ListQuery)query).getQueryString(this.regions.get(key).getFullPath()));
    } else {
      return this.queryService.newQuery(((SortedSetQuery)query).getQueryString(this.regions.get(key).getFullPath()));
    }
     */
  }

  /**
   * Checks if the given key is associated with the passed data type.
   * If there is a mismatch, a {@link RuntimeException} is thrown
   * 
   * @param key Key to check
   * @param type Type to check to
   */
  protected void checkDataType(ByteArrayWrapper key, RedisDataType type) {
    RedisDataType currentType = redisMetaRegion.get(key.toString());
    if (currentType == null)
      return;
    if (currentType == RedisDataType.REDIS_PROTECTED)
      throw new RedisDataTypeMismatchException("The key name \"" + key + "\" is protected");
    if (currentType != type)
      throw new RedisDataTypeMismatchException("The key name \"" + key + "\" is already used by a " + currentType.toString());
  }

  public boolean regionExists(ByteArrayWrapper key) {
    return this.regions.containsKey(key);
  }

  public Region<ByteArrayWrapper, ByteArrayWrapper> getStringsRegion() {
    return this.stringsRegion;
  }

  public Region<ByteArrayWrapper, HyperLogLogPlus> gethLLRegion() {
    return this.hLLRegion;
  }

  private RedisDataType getRedisDataType(String key) {
    return this.redisMetaRegion.get(key);
  }

  public RedisDataType getRedisDataType(ByteArrayWrapper key) {
    return getRedisDataType(key.toString());
  }

  /**
   * Sets the expiration for a key. The setting and modifying of a key expiration can only be set by a delay,
   * which means that both expiring after a time and at a time can be done but
   * the delay to expire at a time must be calculated before these calls. It is
   * also important to note that the delay is always handled in milliseconds
   * 
   * @param key The key to set the expiration for
   * @param delay The delay in milliseconds of the expiration
   * @return True is expiration set, false otherwise
   */
  public final boolean setExpiration(ByteArrayWrapper key, long delay) {
    RedisDataType type = getRedisDataType(key);
    if (type == null)
      return false;
    ScheduledFuture<?> future = this.expirationExecutor.schedule(new ExpirationExecutor(key, type, this), delay, TimeUnit.MILLISECONDS);
    this.expirationsMap.put(key, future);
    return true;
  }

  /**
   * Modifies an expiration on a key
   * 
   * @param key String key to modify expiration on
   * @param delay Delay in milliseconds to reset the expiration to
   * @return True if reset, false if not
   */
  public final boolean modifyExpiration(ByteArrayWrapper key, long delay) {
    /*
     * Attempt to cancel future task
     */
    boolean canceled = cancelKeyExpiration(key);

    if (!canceled)
      return false;

    RedisDataType type = getRedisDataType(key);
    if (type == null)
      return false;

    ScheduledFuture<?> future = this.expirationExecutor.schedule(new ExpirationExecutor(key, type, this), delay, TimeUnit.MILLISECONDS);
    this.expirationsMap.put(key, future);
    return true;
  }

  /**
   * Removes an expiration from a key
   * 
   * @param key Key
   * @return True is expiration cancelled on the key, false otherwise
   */
  public final boolean cancelKeyExpiration(ByteArrayWrapper key) {
    ScheduledFuture<?> future = expirationsMap.remove(key);
    if (future == null)
      return false;
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
    return this.expirationsMap.containsKey(key);
  }

  /**
   * Get remaining expiration time
   * 
   * @param key Key
   * @return Remaining time in milliseconds or 0 if no delay or key doesn't exist
   */
  public final long getExpirationDelayMillis(ByteArrayWrapper key) {
    ScheduledFuture<?> future = this.expirationsMap.get(key);
    return future != null ? future.getDelay(TimeUnit.MILLISECONDS) : 0L;
  }

  @Override
  public void close() {
    this.preparedQueries.clear();
  }

  public String dumpRegionsCache() {
    StringBuilder builder = new StringBuilder();
    for (Entry<ByteArrayWrapper, Region<?, ?>> e : this.regions.entrySet()) {
      builder.append(e.getKey() + " --> {" + e.getValue() + "}\n");
    }
    return builder.toString();
  }

}

package com.gemstone.gemfire.internal.redis;

import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheTransactionManager;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.TransactionId;
import com.gemstone.gemfire.cache.query.IndexNameConflictException;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.redis.executor.ExpirationExecutor;
import com.gemstone.gemfire.internal.redis.executor.ListQuery;
import com.gemstone.gemfire.internal.redis.executor.SortedSetQuery;
import com.gemstone.gemfire.internal.redis.executor.hll.HyperLogLogPlus;
import com.gemstone.gemfire.management.cli.Result;
import com.gemstone.gemfire.management.cli.Result.Status;
import com.gemstone.gemfire.management.internal.cli.commands.CreateAlterDestroyRegionCommands;
import com.gemstone.gemfire.redis.GemFireRedisServer;


public class RegionCache implements Closeable {

  private final ConcurrentHashMap<ByteArrayWrapper, Region<?, ?>> regions;

  /**
   * This is the Redis meta data {@link Region} that holds the {@link RedisDataType}
   * information for all Regions created. The mapping is a {@link String} key which is the name
   * of the {@link Region} created to hold the data to the RedisDataType it contains.
   */
  private final Region<String, RedisDataType> redisMetaRegion;

  /**
   * This is the {@link RedisDataType#REDIS_LIST} meta data {@link Region}. Here each list
   * stores the index of the head and tail of the list
   */
  private final Region<String, Integer> listsMetaRegion;

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
  private static final CreateAlterDestroyRegionCommands cliCmds = new CreateAlterDestroyRegionCommands();

  public RegionCache(Region<ByteArrayWrapper, ByteArrayWrapper> stringsRegion, Region<ByteArrayWrapper, HyperLogLogPlus> hLLRegion, Region<String, RedisDataType> redisMetaRegion, Region<String, Integer> listsMetaRegion, ConcurrentMap<ByteArrayWrapper, ScheduledFuture<?>> expirationsMap, ScheduledExecutorService expirationExecutor) {
    if (stringsRegion == null || hLLRegion == null || redisMetaRegion == null || listsMetaRegion == null)
      throw new NullPointerException();
    this.regions = new ConcurrentHashMap<ByteArrayWrapper, Region<?, ?>>();
    this.stringsRegion = stringsRegion;
    this.hLLRegion = hLLRegion;
    this.redisMetaRegion = redisMetaRegion;
    this.listsMetaRegion = listsMetaRegion;
    this.cache = GemFireCacheImpl.getInstance();
    this.queryService = cache.getQueryService();
    this.expirationsMap = expirationsMap;
    this.expirationExecutor = expirationExecutor;
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
    cancelKeyExpiration(key);
    this.regions.remove(key);
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
    }
  }

  public Region<?, ?> getOrCreateRegion(ByteArrayWrapper key, RedisDataType type, ExecutionHandlerContext context) {
    return getOrCreateRegion0(key, type, context, true);
  }

  public Region<?, ?> createRemoteRegionLocally(ByteArrayWrapper key, RedisDataType type) {
    return getOrCreateRegion0(key, type, null, false);
  }

  private Region<?, ?> getOrCreateRegion0(ByteArrayWrapper key, RedisDataType type, ExecutionHandlerContext context, boolean addToMeta) {
    checkDataType(key, type);
    Region<?, ?> r = this.regions.get(key);
    if (r == null) {
      String stringKey = key.toString();
      synchronized (stringKey) { // This object will be interned across the vm
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
            r = createRegionGlobally(stringKey);
            if (addToMeta)
              metaPut(key, type);
            if (type == RedisDataType.REDIS_LIST)
              doInitializeList(key, r.getFullPath());
            else if (type == RedisDataType.REDIS_SORTEDSET)
              doInitializeSortedSet(key, r);
            this.regions.put(key, r);
          } finally {
            if (hasTransaction)
              txm.resume(transactionId);
          }
        }
      }
    }
    return r;
  }

  private boolean destroyRegion(ByteArrayWrapper key, RedisDataType type) {
    String stringKey = key.toString();
    Region<?, ?> r = this.regions.get(key);
    if (r != null) {
      synchronized (stringKey) { // This object will be interned across the vm
        try {
          r.destroyRegion();
        } catch (Exception e) {
          return false;
        } finally {
          this.preparedQueries.remove(key);
          metaRemoveEntry(key);
          if (type == RedisDataType.REDIS_LIST) {
            this.listsMetaRegion.remove(stringKey + "head");
            this.listsMetaRegion.remove(stringKey + "tail");
          }
          this.regions.remove(key);
        }
      }
    }
    return true;
  }

  private void doInitializeSortedSet(ByteArrayWrapper key, Region<?, ?> r) {
    String fullpath = r.getFullPath();
    try {
      queryService.createIndex("scoreIndex", "value.score", r.getFullPath() + ".entrySet entry");
      queryService.createIndex("scoreIndex2", "value.score", r.getFullPath() + ".values value");
    } catch (Exception e) {
      if (!(e instanceof IndexNameConflictException)) {
        LogWriter logger = cache.getLogger();
        if (logger.errorEnabled()) {
          logger.error(e);
        }
      }
    }
    HashMap<Enum<?>, Query> queryList = new HashMap<Enum<?>, Query>();
    for (SortedSetQuery lq: SortedSetQuery.values()) {
      String queryString = lq.getQueryString(fullpath);
      Query query = this.queryService.newQuery(queryString);
      queryList.put(lq, query);
    }
    this.preparedQueries.put(key, queryList);
  }

  private void doInitializeList(ByteArrayWrapper key, String fullpath) {
    listsMetaRegion.put(key + "head", Integer.valueOf(0));
    listsMetaRegion.put(key + "tail", Integer.valueOf(0));
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
    Result result = cliCmds.createRegion(key, GemFireRedisServer.DEFAULT_REGION_TYPE, null, null, true, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null);
    r = cache.getRegion(key);
    if (result.getStatus() == Status.ERROR && r == null) {
      String err = "";
      while(result.hasNextLine())
        err += result.nextLine();
      throw new RegionCreationException(err);
    }
    if (r == null)
      throw new RegionCreationException();
    return r;
  }

  public Query getQuery(ByteArrayWrapper key, Enum<?> query) {
    return this.preparedQueries.get(key).get(query);
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

  public Region<String, Integer> getListsMetaRegion() {
    return this.listsMetaRegion;
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
   * @param context Context
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

}

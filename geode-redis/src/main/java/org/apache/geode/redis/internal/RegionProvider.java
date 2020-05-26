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
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.executor.ExpirationExecutor;
import org.apache.geode.redis.internal.executor.RedisKeyCommands;
import org.apache.geode.redis.internal.executor.RedisKeyCommandsFunctionExecutor;

public class RegionProvider implements Closeable {
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
  private final Region<ByteArrayWrapper, RedisData> stringsRegion;

  private final Region<ByteArrayWrapper, RedisData> dataRegion;

  private final ConcurrentMap<ByteArrayWrapper, ScheduledFuture<?>> expirationsMap;
  private final ScheduledExecutorService expirationExecutor;

  public RegionProvider(Region<ByteArrayWrapper, RedisData> stringsRegion,
      KeyRegistrar redisMetaRegion,
      ConcurrentMap<ByteArrayWrapper, ScheduledFuture<?>> expirationsMap,
      ScheduledExecutorService expirationExecutor,
      Region<ByteArrayWrapper, RedisData> dataRegion) {
    if (stringsRegion == null || redisMetaRegion == null) {
      throw new NullPointerException();
    }
    this.dataRegion = dataRegion;
    this.stringsRegion = stringsRegion;
    keyRegistrar = redisMetaRegion;
    this.expirationsMap = expirationsMap;
    this.expirationExecutor = expirationExecutor;
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

      case REDIS_PUBSUB:
      default:
        return null;
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
    if (!typeStoresDataInKeyRegistrar(type)) {
      keyRegistrar.unregister(key);
    }
    RedisKeyCommands redisKeyCommands = new RedisKeyCommandsFunctionExecutor(dataRegion);
    try {
      if (type == RedisDataType.REDIS_STRING) {
        return stringsRegion.remove(key) != null;
      } else if (type == RedisDataType.REDIS_SET || type == RedisDataType.REDIS_HASH) {
        return redisKeyCommands.del(key);
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
    }
  }

  public Region<ByteArrayWrapper, RedisData> getStringsRegion() {
    return stringsRegion;
  }

  public Region<ByteArrayWrapper, RedisData> getDataRegion() {
    return dataRegion;
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
  public void close() {}
}

/*
 * Copyright (c) 2018 SnappyData, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package org.apache.geode.internal.tcp;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.function.Predicate;

import org.apache.commons.pool2.KeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.apache.commons.pool2.impl.GenericObjectPool;

import org.apache.geode.CancelCriterion;

/**
 * A configurable <code>KeyedObjectPool</code> implementation.
 * <p>
 * This is extends common-pool2 GenericKeyedObjectPool and differs only
 * in additional methods to query objects by key.
 *
 * @param <K> The type of keys maintained by this pool.
 * @param <T> Type of element pooled in this pool.
 *
 * @see GenericObjectPool
 * @see GenericKeyedObjectPool
 */
public final class QueryKeyedObjectPool<K, T>
    extends GenericKeyedObjectPool<K, T> {

  /*
   * The "poolMap" field from the parent class obtained using reflection.
   */
  private final ConcurrentHashMap<?, ?> subPoolMap;

  /**
   * The "keyLock" field from the parent class obtained using reflection.
   */
  private final ReadWriteLock globalKeyLock;

  /**
   * Check for cancellation with this in borrowObject.
   */
  private final CancelCriterion cancelCriterion;

  /**
   * The "allObjects" field from ObjectDequeue class obtained using reflection.
   */
  private volatile Field allObjectsField;

  /**
   * Create a new <code>QueryKeyedObjectPool</code> using defaults from
   * {@link GenericKeyedObjectPoolConfig}.
   *
   * @param factory the factory to be used to create entries
   * @param cancelCriterion to check for cancellation of borrowObject
   */
  public QueryKeyedObjectPool(KeyedPooledObjectFactory<K, T> factory,
      CancelCriterion cancelCriterion) {
    this(factory, new GenericKeyedObjectPoolConfig(), cancelCriterion);
  }

  /**
   * Create a new <code>QueryKeyedObjectPool</code> using a specific
   * configuration.
   *
   * @param factory the factory to be used to create entries
   * @param config The configuration to use for this pool instance.
   *        The configuration is used by value. Subsequent
   *        changes to the configuration object will not be
   *        reflected in the pool.
   * @param cancelCriterion to check for cancellation of borrowObject
   */
  @SuppressWarnings("WeakerAccess")
  public QueryKeyedObjectPool(KeyedPooledObjectFactory<K, T> factory,
      GenericKeyedObjectPoolConfig config, CancelCriterion cancelCriterion) {
    super(factory, config);

    try {
      final Class<?> superClass = getClass().getSuperclass();
      Field f = superClass.getDeclaredField("poolMap");
      f.setAccessible(true);
      this.subPoolMap = (ConcurrentHashMap<?, ?>) f.get(this);

      f = superClass.getDeclaredField("keyLock");
      f.setAccessible(true);
      this.globalKeyLock = (ReadWriteLock) f.get(this);

      this.cancelCriterion = cancelCriterion;
    } catch (Exception e) {
      throw new IllegalStateException(
          "Failed to initialize QueryKeyedObjectPool", e);
    }
  }

  private Map<?, ?> getAllObjectsFromDeque(Object deque) {
    try {
      Field allObjects = this.allObjectsField;
      if (allObjects == null) {
        synchronized (this) {
          allObjects = this.allObjectsField;
          if (allObjects == null) {
            Field f = deque.getClass().getDeclaredField("allObjects");
            f.setAccessible(true);
            this.allObjectsField = allObjects = f;
          }
        }
      }
      return (Map<?, ?>) allObjects.get(deque);
    } catch (Exception e) {
      throw new IllegalStateException("Failed to read pool queue", e);
    }
  }

  @Override
  public T borrowObject(K key, long borrowMaxWaitMillis) throws Exception {
    // An issue with GenericKeyedObjectPool: it increments the createdCount
    // first, then checks if it exceeds maxTotalPerKey. It can happen due to
    // concurrency then that one of the threads goes into a wait due to that
    // even though connections have not actually been created yet. The creates
    // could all fail due to remote node going down, but then those threads
    // that started to wait will still keep waiting (SNAP-1508, SNAP-1509).
    // Hence this override does it with smaller timeout in a loop.
    if (borrowMaxWaitMillis < 0) {
      borrowMaxWaitMillis = Integer.MAX_VALUE;
    }
    // retries at max 1 second intervals
    final long loopWaitMillis = Math.min(1000L, borrowMaxWaitMillis);
    long startTime = 0L;
    while (true) {
      try {
        return super.borrowObject(key, loopWaitMillis);
      } catch (NoSuchElementException nse) {
        // check for cancellation
        this.cancelCriterion.checkCancelInProgress(nse);
        // retry till borrowMaxWaitMillis
        long currentTime = System.currentTimeMillis();
        if (startTime == 0L) {
          // first time assume it failed after loopWaitMillis to
          // optimistically avoid a currentTimeMillis call in first loop
          startTime = currentTime - loopWaitMillis;
        }
        if ((currentTime - startTime) >= borrowMaxWaitMillis) {
          throw nse;
        }
      }
    }
  }

  public int getNumTotal(K key) {
    final Object objectDeque = subPoolMap.get(key);
    if (objectDeque != null) {
      return getAllObjectsFromDeque(objectDeque).size();
    } else {
      return 0;
    }
  }

  /**
   * Apply the given function on each object for a key both idle (waiting
   * to be borrowed) and active (currently borrowed).
   */
  public void foreachObject(K key, Predicate<T> predicate) {
    Lock readLock = globalKeyLock.readLock();
    readLock.lock();
    try {
      Object queue = subPoolMap.get(key);
      if (queue != null) {
        for (Object p : getAllObjectsFromDeque(queue).values()) {
          @SuppressWarnings("unchecked")
          final PooledObject<T> pooledObject = (PooledObject<T>) p;
          if (!predicate.test(pooledObject.getObject())) {
            break;
          }
        }
      }
    } finally {
      readLock.unlock();
    }
  }
}

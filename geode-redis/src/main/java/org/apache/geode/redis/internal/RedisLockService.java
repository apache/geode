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
package org.apache.geode.redis.internal;

import java.util.Collections;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.geode.cache.TimeoutException;

/**
 * Locking mechanism to support Redis operations
 */
public class RedisLockService implements RedisLockServiceMBean {

  private static final int DEFAULT_TIMEOUT = 1000;
  private final int timeoutMS;
  private Map<ByteArrayWrapper, Lock> map = Collections.synchronizedMap(new WeakHashMap<>());

  /**
   * Construct with the default 1000ms timeout setting
   */
  public RedisLockService() {
    this(DEFAULT_TIMEOUT);
  }

  /**
   * Construct with the timeout setting
   *
   * @param timeoutMS the default timeout to wait for lock
   */
  public RedisLockService(int timeoutMS) {
    this.timeoutMS = timeoutMS;
  }

  @Override
  public int getLockCount() {
    return map.size();
  }

  /**
   * Attempt to obtain a lock against a given key. The actual lock is wrapped in a returned
   * {@link AutoCloseable}. The lock can be released either by calling {@code close} on the returned
   * object, or when the returned object becomes eligible for garbage collection (since the backing
   * data structure is based on a {@link WeakHashMap}).
   *
   * @param key the lock name/key
   * @return an {@link AutoCloseableLock}
   * @throws InterruptedException if the thread is interrupted
   * @throws TimeoutException if the lock cannot be acquired within the timeout period
   */
  public AutoCloseableLock lock(ByteArrayWrapper key) throws InterruptedException {
    if (key == null) {
      throw new IllegalArgumentException("key cannot be null");
    }

    Lock lock = new ReentrantLock();
    Lock oldLock = map.putIfAbsent(key, lock);
    if (oldLock != null) {
      lock = oldLock;
      // we need to get a reference to the actual key object so that the backing WeakHashMap does
      // not clean it up.
      for (ByteArrayWrapper keyInSet : map.keySet()) {
        if (keyInSet.equals(key)) {
          key = keyInSet;
          break;
        }
      }
    }

    if (!lock.tryLock(timeoutMS, TimeUnit.MILLISECONDS)) {
      throw new TimeoutException("Couldn't get lock for " + key.toString());
    }

    return new AutoCloseableLock(key, lock);
  }

  int getMapSize() {
    return map.size();
  }

}

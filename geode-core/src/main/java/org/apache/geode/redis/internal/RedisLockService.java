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

import java.util.WeakHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 
 * Locking mechanism to support Redis operations
 *
 */
public class RedisLockService {


  private final static int DEFAULT_TIMEOUT = 1000;
  private final int timeoutMS;
  private WeakHashMap<Object, Lock> map = new WeakHashMap<Object, Lock>();


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

  /**
   * Obtain a lock
   * 
   * @param name the lock name/key
   * @return true if lock establish prior to timeout
   */
  public synchronized boolean lock(Object name) {
    if (name == null)
      return false;

    Lock lock = map.get(name);

    if (lock == null) {
      lock = new ReentrantLock();
      map.put(name, lock);
    }

    try {
      return lock.tryLock(timeoutMS, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      return false;
    }
  }

  /**
   * Release the given lock
   * 
   * @param name the lock name
   */
  public synchronized void unlock(Object name) {
    if (name == null)
      return;

    Lock lock = map.get(name);

    if (lock == null) {
      return;
    }

    try {
      lock.unlock();
    } finally {
      map.remove(name);
    }
  }

  /**
   * Retrieve the managed lock
   * 
   * @param name the name key for lock
   * @return the manage lock (null if does not exist)
   */
  Lock getLock(Object name) {
    return map.get(name);
  }

}

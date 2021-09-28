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

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.geode.redis.internal.data.RedisKey;

/**
 * Implements {@link StripedCoordinator} by using {@link ReentrantLock}s synchronization. The thread
 * that calls execute will also be the thread that does the work. But it will do it under
 * synchronization. The hashCode of the stripeId is used to associate the id with a stripe.
 */
public class LockingStripedCoordinator implements StripedCoordinator {
  private static final int DEFAULT_CONCURRENCY_LEVEL = 4093; // use a prime
  private final ReentrantLock[] locks;

  public LockingStripedCoordinator() {
    this(DEFAULT_CONCURRENCY_LEVEL);
  }

  public LockingStripedCoordinator(int concurrencyLevel) {
    locks = new ReentrantLock[concurrencyLevel];
    for (int i = 0; i < concurrencyLevel; i++) {
      locks[i] = new ReentrantLock();
    }
  }

  @Override
  public <T> T execute(RedisKey stripeId, Callable<T> callable) {
    Lock lock = getLock(stripeId);
    lock.lock();
    try {
      return callable.call();
    } catch (RuntimeException re) {
      throw re;
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public <T> T execute(List<RedisKey> stripeIds, Callable<T> callable) {
    stripeIds.sort(this::compareStripes);
    stripeIds.forEach(k -> getLock(k).lock());
    try {
      return callable.call();
    } catch (RuntimeException re) {
      throw re;
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      stripeIds.forEach(k -> getLock(k).unlock());
    }
  }

  private Lock getLock(RedisKey stripeId) {
    return locks[getStripeIndex(stripeId)];
  }

  private int getStripeIndex(Object stripeId) {
    return Math.abs(stripeId.hashCode() % locks.length);
  }

  @Override
  public int compareStripes(Object object1, Object object2) {
    return getStripeIndex(object1) - getStripeIndex(object2);
  }
}

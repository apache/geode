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

import org.apache.geode.redis.internal.data.RedisKey;

/**
 * Implements {@link StripedCoordinator} by using
 * synchronization. The thread that calls execute will also be the thread that does the work. But it
 * will do it under synchronization. The hashCode of the stripeId is used to associate the id with a
 * stripe.
 */
public class SynchronizedStripedCoordinator implements StripedCoordinator {
  private static final int DEFAULT_CONCURRENCY_LEVEL = 4093; // use a prime
  private final Object[] syncs;

  public SynchronizedStripedCoordinator() {
    this(DEFAULT_CONCURRENCY_LEVEL);
  }

  public SynchronizedStripedCoordinator(int concurrencyLevel) {
    syncs = new Object[concurrencyLevel];
    for (int i = 0; i < concurrencyLevel; i++) {
      syncs[i] = new Object();
    }
  }

  @Override
  public <T> T execute(RedisKey stripeId, Callable<T> callable) {
    synchronized (getSync(stripeId)) {
      try {
        return callable.call();
      } catch (RuntimeException re) {
        throw re;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public <T> T execute(List<RedisKey> stripeIds, Callable<T> callable) {
    return execute(stripeIds, 0, callable);
  }

  private <T> T execute(List<RedisKey> stripeIds, int index, Callable<T> callable) {
    if (index + 1 == stripeIds.size()) {
      return execute(stripeIds.get(index), callable);
    }

    synchronized (getSync(stripeIds.get(index))) {
      return execute(stripeIds, ++index, callable);
    }
  }

  private Object getSync(Object stripeId) {
    return syncs[getStripeIndex(stripeId)];
  }

  private int getStripeIndex(Object stripeId) {
    return Math.abs(stripeId.hashCode() % syncs.length);
  }

  @Override
  public int compareStripes(Object object1, Object object2) {
    return getStripeIndex(object1) - getStripeIndex(object2);
  }
}

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

package org.apache.geode.cache.client.internal.pooling;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Responsible for counting connections.
 * The count maintained by this class will eventually be consistent with the actual number of
 * connections. Since the count is changed before and after the actual connections are created and
 * destroyed, and not changed while holding a lock, the count should be treated as an estimate of
 * the current number of connections.
 */
public class ConnectionAccounting {
  private final int minimum;
  private final int maximum;
  private final AtomicInteger count = new AtomicInteger();

  public ConnectionAccounting(int min, int max) {
    minimum = min;
    maximum = max;
  }

  public int getMinimum() {
    return minimum;
  }

  public int getMaximum() {
    return maximum;
  }

  public int getCount() {
    return count.get();
  }

  /**
   * Should be called when prefilling connections to reach minimum connections. Caller should only
   * create a connection if this method returns {@code true}. If connection creation fails then
   * {@link #cancelTryPrefill} must be called to revert the count increase.
   *
   * @return {@code true} if count was under minimum and we increased it, otherwise {@code false}.
   */
  public boolean tryPrefill() {
    return tryReserve(minimum);
  }

  /**
   * Should only be called if connection creation failed after calling {@link #tryPrefill()} ()}.
   */
  public void cancelTryPrefill() {
    count.getAndDecrement();
  }

  /**
   * Should be called when a new connection would be nice to have when count is under maximum.
   * Caller should only create a connection if this method returns {@code true}. If connection
   * creation fails then {@link #cancelTryCreate} must be called to revert the count increase.
   *
   * @return {@code true} if count was under maximum and we increased it, otherwise {@code false}.
   */
  public boolean tryCreate() {
    return tryReserve(maximum);
  }

  /**
   * Should only be called if connection creation failed after calling {@link #tryCreate()}.
   */
  public void cancelTryCreate() {
    count.decrementAndGet();
  }

  /**
   * Count a created connection regardless of maximum. Should not be called after
   * {@link #tryCreate()}.
   */
  public void create() {
    count.getAndIncrement();
  }

  /**
   * Should be called when a connection is being returned and the caller should destroy the
   * connection if {@code true} is returned. If connection destroy fails then
   * {@link #cancelTryDestroy()} must be called.
   *
   * @return {@code true} if count was over maximum and we decreased it, otherwise {@code false}.
   */
  public boolean tryDestroy() {
    int currentCount;
    while ((currentCount = count.get()) > maximum) {
      if (count.compareAndSet(currentCount, currentCount - 1)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Should only be called if connection destroy failed after calling {@link #tryDestroy()}.
   */
  public void cancelTryDestroy() {
    count.getAndIncrement();
  }

  /**
   * Should be called after any connection destroys are done. Should not be called
   * after {@link #tryDestroy()}.
   *
   * @param destroyCount number of connections being destroyed.
   *
   * @return {@code true} if after decreasing count it is under the minimum, otherwise
   *         {@code false}.
   */
  public boolean destroyAndIsUnderMinimum(int destroyCount) {
    int newCount = count.addAndGet(-destroyCount);
    return newCount < minimum;
  }

  public boolean isUnderMinimum() {
    return count.get() < minimum;
  }

  public boolean isOverMinimum() {
    return count.get() > minimum;
  }

  private boolean tryReserve(int upperBound) {
    int currentCount;
    while ((currentCount = count.get()) < upperBound) {
      if (count.compareAndSet(currentCount, currentCount + 1)) {
        return true;
      }
    }
    return false;
  }
}

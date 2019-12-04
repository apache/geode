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
package org.apache.geode.internal.util.concurrent;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Subclass of CountDownLatch which exposes the number of callers currently blocked awaiting this
 * latch. Tests may use this to await changes to the number of waiting callers.
 */
public class MeteredCountDownLatch extends CountDownLatch {

  private final AtomicInteger waitCount = new AtomicInteger();

  public MeteredCountDownLatch(int count) {
    super(count);
  }

  @Override
  public void await() throws InterruptedException {
    waitCount.incrementAndGet();
    try {
      super.await();
    } finally {
      waitCount.decrementAndGet();
    }
  }

  @Override
  public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
    waitCount.incrementAndGet();
    try {
      return super.await(timeout, unit);
    } finally {
      waitCount.decrementAndGet();
    }
  }

  /**
   * The number of callers currently blocked awaiting this latch.
   */
  public long getWaitCount() {
    return waitCount.get();
  }
}

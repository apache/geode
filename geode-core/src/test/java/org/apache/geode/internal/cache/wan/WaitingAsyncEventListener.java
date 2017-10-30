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
package org.apache.geode.internal.cache.wan;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.geode.cache.asyncqueue.AsyncEvent;
import org.apache.geode.cache.asyncqueue.AsyncEventListener;

public class WaitingAsyncEventListener implements AsyncEventListener {

  private final CountDownLatch latch = new CountDownLatch(1);

  private final AtomicInteger numberOfEvents = new AtomicInteger();

  public boolean processEvents(List<AsyncEvent> events) {
    try {
      waitToStartProcessingEvents();
    } catch (InterruptedException e) {
      throw new RuntimeException("WaitingAsyncEventListener processEvents was interrupted");
    }
    for (AsyncEvent event : events) {
      process(event);
    }
    return true;
  }

  private void process(AsyncEvent event) {
    incrementTotalEvents();
  }

  private void waitToStartProcessingEvents() throws InterruptedException {
    this.latch.await();
  }

  public void startProcessingEvents() {
    this.latch.countDown();
  }

  private int incrementTotalEvents() {
    return this.numberOfEvents.incrementAndGet();
  }

  public int getTotalEvents() {
    return this.numberOfEvents.get();
  }

  public void close() {}
}

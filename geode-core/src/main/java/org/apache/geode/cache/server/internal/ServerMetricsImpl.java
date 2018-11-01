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
package org.apache.geode.cache.server.internal;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.geode.cache.server.ServerMetrics;

/**
 * Metrics describing the load on a cache server.
 *
 * @since GemFire 5.7
 *
 */
public class ServerMetricsImpl implements ServerMetrics {
  private final AtomicInteger clientCount = new AtomicInteger();
  private final AtomicInteger connectionCount = new AtomicInteger();
  private final AtomicInteger queueCount = new AtomicInteger();
  private final int maxConnections;

  public ServerMetricsImpl(int maxConnections) {
    this.maxConnections = maxConnections;
  }

  public int getClientCount() {
    return clientCount.get();
  }

  public int getConnectionCount() {
    return connectionCount.get();
  }

  public int getMaxConnections() {
    return maxConnections;
  }

  public int getSubscriptionConnectionCount() {
    return queueCount.get();
  }

  public void incClientCount() {
    clientCount.incrementAndGet();
  }

  public void decClientCount() {
    clientCount.decrementAndGet();
  }

  public void incConnectionCount() {
    connectionCount.incrementAndGet();
  }

  public void decConnectionCount() {
    connectionCount.decrementAndGet();
  }

  public void incQueueCount() {
    queueCount.incrementAndGet();
  }

  public void decQueueCount() {
    queueCount.decrementAndGet();
  }

}

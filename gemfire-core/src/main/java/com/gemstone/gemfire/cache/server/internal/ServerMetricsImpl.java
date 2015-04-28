/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.server.internal;

import java.util.concurrent.atomic.AtomicInteger;

import com.gemstone.gemfire.cache.server.ServerMetrics;

/**
 * Metrics describing the load on a  bridge server.
 * @author dsmith
 * @since 5.7
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

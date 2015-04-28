/*
 *  =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *  ========================================================================
 */
package com.gemstone.gemfire.management.internal.beans;

import javax.management.NotificationBroadcasterSupport;

import com.gemstone.gemfire.management.AsyncEventQueueMXBean;
import com.gemstone.gemfire.management.internal.beans.stats.StatsKey;

/**
 * Concrete implementation of AsyncEventQueueMXBean
 * 
 * @author rishim
 * 
 */
public class AsyncEventQueueMBean extends NotificationBroadcasterSupport
    implements AsyncEventQueueMXBean {

  private AsyncEventQueueMBeanBridge bridge;

  public AsyncEventQueueMBean(AsyncEventQueueMBeanBridge bridge) {
    this.bridge = bridge;
  }

  @Override
  public String getAsyncEventListener() {
    return bridge.getAsyncEventListener();
  }

  @Override
  public int getBatchSize() {
    return bridge.getBatchSize();
  }
  
  @Override
  public long getBatchTimeInterval() {
    return bridge.getBatchTimeInteval();
  }

  @Override
  public boolean isBatchConflationEnabled() {
    return bridge.isBatchConflationEnabled();
  }

  @Override
  public String getId() {
    return bridge.getId();
  }

  @Override
  public int getMaximumQueueMemory() {
    return bridge.getMaximumQueueMemory();
  }

  @Override
  public String getOverflowDiskStoreName() {
    return bridge.getOverflowDiskStoreName();
  }

  @Override
  public boolean isPersistent() {
    return bridge.isPersistent();
  }

  @Override
  public boolean isPrimary() {
    return bridge.isPrimary();
  }
  
  @Override
  public boolean isParallel() {
    return bridge.isParallel();
  }
  
  @Override
  public int getDispatcherThreads() {
    return bridge.getDispatcherThreads();
  }
  
  @Override
  public String getOrderPolicy() {
    return bridge.getOrderPolicy();
  }
 
  @Override
  public boolean isDiskSynchronous() {
    return bridge.isDiskSynchronous();
  }

  public AsyncEventQueueMBeanBridge getBridge(){
    return bridge;
  }
  
  @Override
  public int getEventQueueSize() {
    return bridge.getEventQueueSize();
  }

}

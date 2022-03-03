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
package org.apache.geode.management.internal.beans;

import javax.management.NotificationBroadcasterSupport;

import org.apache.geode.management.AsyncEventQueueMXBean;

/**
 * Concrete implementation of AsyncEventQueueMXBean
 *
 *
 */
public class AsyncEventQueueMBean extends NotificationBroadcasterSupport
    implements AsyncEventQueueMXBean {

  private final AsyncEventQueueMBeanBridge bridge;

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

  public AsyncEventQueueMBeanBridge getBridge() {
    return bridge;
  }

  @Override
  public int getEventQueueSize() {
    return bridge.getEventQueueSize();
  }

  @Override
  public float getLRUEvictionsRate() {
    return bridge.getLRUEvictionsRate();
  }

  @Override
  public long getEntriesOverflowedToDisk() {
    return bridge.getEntriesOverflowedToDisk();
  }

  @Override
  public long getBytesOverflowedToDisk() {
    return bridge.getBytesOverflowedToDisk();
  }

  public void stopMonitor() {
    bridge.stopMonitor();
  }

  @Override
  public boolean isDispatchingPaused() {
    return bridge.isDispatchingPaused();
  }
}

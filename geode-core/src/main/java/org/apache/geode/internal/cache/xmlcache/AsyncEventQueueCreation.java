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
package org.apache.geode.internal.cache.xmlcache;

import java.util.ArrayList;
import java.util.List;

import org.apache.geode.cache.asyncqueue.AsyncEventListener;
import org.apache.geode.cache.asyncqueue.AsyncEventQueue;
import org.apache.geode.cache.wan.GatewayEventFilter;
import org.apache.geode.cache.wan.GatewayEventSubstitutionFilter;
import org.apache.geode.cache.wan.GatewaySender.OrderPolicy;
import org.apache.geode.internal.cache.wan.GatewaySenderAttributes;

public class AsyncEventQueueCreation implements AsyncEventQueue {

  private String id = null;
  private List<GatewayEventFilter> gatewayEventFilters = new ArrayList<GatewayEventFilter>();
  private GatewayEventSubstitutionFilter gatewayEventSubstitutionFilter = null;
  private AsyncEventListener asyncEventListener = null;
  private int batchSize = 0;
  private int batchTimeInterval = 0;
  private boolean isBatchConflationEnabled = false;
  private boolean isPersistent = false;
  private String diskStoreName = null;
  private boolean isDiskSynchronous = false;
  private int maxQueueMemory = 0;
  private boolean isParallel = false;
  private boolean isBucketSorted = false;
  private int dispatcherThreads = 1;
  private OrderPolicy orderPolicy = OrderPolicy.KEY;
  private boolean forwardExpirationDestroy = false;
  private boolean pauseEventDispatching = false;

  public AsyncEventQueueCreation() {}

  public AsyncEventQueueCreation(String id, GatewaySenderAttributes senderAttrs,
      AsyncEventListener eventListener) {
    this.id = id;
    this.batchSize = senderAttrs.getBatchSize();
    this.batchTimeInterval = senderAttrs.getBatchTimeInterval();
    this.isBatchConflationEnabled = senderAttrs.isBatchConflationEnabled();
    this.isPersistent = senderAttrs.isPersistenceEnabled();
    this.diskStoreName = senderAttrs.getDiskStoreName();
    this.isDiskSynchronous = senderAttrs.isDiskSynchronous();
    this.maxQueueMemory = senderAttrs.getMaximumQueueMemory();
    this.isParallel = senderAttrs.isParallel();
    this.dispatcherThreads = senderAttrs.getDispatcherThreads();
    this.orderPolicy = senderAttrs.getOrderPolicy();
    this.asyncEventListener = eventListener;
    this.isBucketSorted = senderAttrs.isBucketSorted();
    this.gatewayEventFilters = senderAttrs.getGatewayEventFilters();
    this.gatewayEventSubstitutionFilter = senderAttrs.getGatewayEventSubstitutionFilter();
    this.forwardExpirationDestroy = senderAttrs.isForwardExpirationDestroy();
  }

  @Override
  public AsyncEventListener getAsyncEventListener() {
    return this.asyncEventListener;
  }

  public void setAsyncEventListener(AsyncEventListener eventListener) {
    this.asyncEventListener = eventListener;
  }

  @Override
  public boolean isDispatchingPaused() {
    return pauseEventDispatching;
  }

  public void setPauseEventDispatching(boolean pauseEventDispatching) {
    this.pauseEventDispatching = pauseEventDispatching;
  }

  public void addGatewayEventFilter(GatewayEventFilter filter) {
    this.gatewayEventFilters.add(filter);
  }

  @Override
  public List<GatewayEventFilter> getGatewayEventFilters() {
    return this.gatewayEventFilters;
  }

  @Override
  public GatewayEventSubstitutionFilter getGatewayEventSubstitutionFilter() {
    return this.gatewayEventSubstitutionFilter;
  }

  public void setGatewayEventSubstitutionFilter(GatewayEventSubstitutionFilter filter) {
    this.gatewayEventSubstitutionFilter = filter;
  }

  @Override
  public int getBatchSize() {
    return this.batchSize;
  }

  public void setBatchSize(int batchSize) {
    this.batchSize = batchSize;
  }

  @Override
  public int getBatchTimeInterval() {
    return this.batchTimeInterval;
  }

  public void setBatchTimeInterval(int batchTimeInterval) {
    this.batchTimeInterval = batchTimeInterval;
  }

  @Override
  public boolean isBatchConflationEnabled() {
    return this.isBatchConflationEnabled;
  }

  public void setBatchConflationEnabled(boolean batchConflationEnabled) {
    this.isBatchConflationEnabled = batchConflationEnabled;
  }

  @Override
  public String getDiskStoreName() {
    return this.diskStoreName;
  }

  public void setDiskStoreName(String diskStore) {
    this.diskStoreName = diskStore;
  }

  @Override
  public boolean isDiskSynchronous() {
    return this.isDiskSynchronous;
  }

  public void setDiskSynchronous(boolean diskSynchronous) {
    this.isDiskSynchronous = diskSynchronous;
  }

  @Override
  public String getId() {
    return this.id;
  }

  public void setId(String id) {
    this.id = id;
  }

  @Override
  public int getMaximumQueueMemory() {
    return this.maxQueueMemory;
  }

  public void setMaximumQueueMemory(int maxQueueMemory) {
    this.maxQueueMemory = maxQueueMemory;
  }

  @Override
  public boolean isPersistent() {
    return this.isPersistent;
  }

  public void setPersistent(boolean isPersistent) {
    this.isPersistent = isPersistent;
  }

  public void setParallel(boolean isParallel) {
    this.isParallel = isParallel;
  }

  @Override
  public int getDispatcherThreads() {
    return this.dispatcherThreads;
  }

  public void setDispatcherThreads(int numThreads) {
    this.dispatcherThreads = numThreads;
  }

  @Override
  public OrderPolicy getOrderPolicy() {
    return this.orderPolicy;
  }

  public void setOrderPolicy(OrderPolicy policy) {
    this.orderPolicy = policy;
  }

  @Override
  public boolean isPrimary() {
    return true;
  }

  @Override
  public int size() {
    return 0;
  }

  public void start() {};

  public void stop() {};

  public void destroy() {};

  public void pause() {};

  public void resume() {}

  @Override
  public boolean isParallel() {
    return this.isParallel;
  }

  public boolean isBucketSorted() {
    return this.isBucketSorted;
  }

  public void setBucketSorted(boolean isBucketSorted) {
    this.isBucketSorted = isBucketSorted;
  }

  public void setForwardExpirationDestroy(boolean forward) {
    this.forwardExpirationDestroy = forward;
  }

  @Override
  public boolean isForwardExpirationDestroy() {
    return this.forwardExpirationDestroy;
  }

  @Override
  public void resumeEventDispatching() {
    this.pauseEventDispatching = false;
  }
}

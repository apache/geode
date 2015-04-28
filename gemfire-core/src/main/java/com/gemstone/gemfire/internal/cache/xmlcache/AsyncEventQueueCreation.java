/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.xmlcache;

import java.util.ArrayList;
import java.util.List;

import com.gemstone.gemfire.cache.asyncqueue.AsyncEventListener;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventQueue;
import com.gemstone.gemfire.cache.wan.GatewayEventFilter;
import com.gemstone.gemfire.cache.wan.GatewayEventSubstitutionFilter;
import com.gemstone.gemfire.cache.wan.GatewaySender.OrderPolicy;
import com.gemstone.gemfire.internal.cache.wan.GatewaySenderAttributes;

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
  
  public AsyncEventQueueCreation() {
  }
  
  public AsyncEventQueueCreation(String id, GatewaySenderAttributes senderAttrs, AsyncEventListener eventListener) {
    this.id = id;
    this.batchSize = senderAttrs.batchSize;
    this.batchTimeInterval = senderAttrs.batchTimeInterval;
    this.isBatchConflationEnabled = senderAttrs.isBatchConflationEnabled;
    this.isPersistent = senderAttrs.isPersistenceEnabled;
    this.diskStoreName = senderAttrs.diskStoreName;
    this.isDiskSynchronous = senderAttrs.isDiskSynchronous;
    this.maxQueueMemory = senderAttrs.maximumQueueMemory;
    this.isParallel = senderAttrs.isParallel;
    this.dispatcherThreads = senderAttrs.dispatcherThreads;
    this.orderPolicy = senderAttrs.policy;
    this.asyncEventListener = eventListener;
    this.isBucketSorted = senderAttrs.isBucketSorted; 
    this.gatewayEventFilters = senderAttrs.eventFilters;
    this.gatewayEventSubstitutionFilter = senderAttrs.eventSubstitutionFilter;
  }
  
  @Override
  public AsyncEventListener getAsyncEventListener() {
    return this.asyncEventListener;
  }
  
  public void setAsyncEventListener(AsyncEventListener eventListener) {
    this.asyncEventListener = eventListener;
  }
  
  public void addGatewayEventFilter(
      GatewayEventFilter filter) {
    this.gatewayEventFilters.add(filter);
  }

  public List<GatewayEventFilter> getGatewayEventFilters() {
    return this.gatewayEventFilters;
  }

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
  public void pause() {};
  public void resume() {}

  public boolean isParallel() {
    return this.isParallel;
  }

  public boolean isBucketSorted() {
    return this.isBucketSorted;
  }
  
  public void setBucketSorted(boolean isBucketSorted) {
    this.isBucketSorted = isBucketSorted;
  }
}

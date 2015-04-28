/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.wan;

import java.util.ArrayList;
import java.util.List;

import com.gemstone.gemfire.cache.asyncqueue.AsyncEventListener;
import com.gemstone.gemfire.cache.client.internal.LocatorDiscoveryCallback;
import com.gemstone.gemfire.cache.wan.GatewayEventFilter;
import com.gemstone.gemfire.cache.wan.GatewayEventSubstitutionFilter;
import com.gemstone.gemfire.cache.wan.GatewaySender;
import com.gemstone.gemfire.cache.wan.GatewaySender.OrderPolicy;
import com.gemstone.gemfire.cache.wan.GatewayTransportFilter;

public class GatewaySenderAttributes {

  public static final boolean DEFAULT_IS_BUCKETSORTED = false;


  public int socketBufferSize = GatewaySender.DEFAULT_SOCKET_BUFFER_SIZE;

  public int socketReadTimeout = GatewaySender.DEFAULT_SOCKET_READ_TIMEOUT;

  public int maximumQueueMemory = GatewaySender.DEFAULT_MAXIMUM_QUEUE_MEMORY;

  public int batchSize = GatewaySender.DEFAULT_BATCH_SIZE;

  public int batchTimeInterval = GatewaySender.DEFAULT_BATCH_TIME_INTERVAL;

  public boolean isBatchConflationEnabled = GatewaySender.DEFAULT_BATCH_CONFLATION;

  public boolean isPersistenceEnabled = GatewaySender.DEFAULT_PERSISTENCE_ENABLED;

  public int alertThreshold = GatewaySender.DEFAULT_ALERT_THRESHOLD;

  public boolean manualStart = GatewaySender.DEFAULT_MANUAL_START;

  public String diskStoreName;

  public List<GatewayEventFilter> eventFilters = new ArrayList<GatewayEventFilter>();

  public ArrayList<GatewayTransportFilter> transFilters = new ArrayList<GatewayTransportFilter>();

  public List<AsyncEventListener> listeners = new ArrayList<AsyncEventListener>();

  public GatewayEventSubstitutionFilter eventSubstitutionFilter;

  public String id;

  public int remoteDs = GatewaySender.DEFAULT_DISTRIBUTED_SYSTEM_ID;
  
  public LocatorDiscoveryCallback locatorDiscoveryCallback;
  
  public boolean isDiskSynchronous = GatewaySender.DEFAULT_DISK_SYNCHRONOUS; 
  
  public OrderPolicy policy;
  
  public int dispatcherThreads = GatewaySender.DEFAULT_DISPATCHER_THREADS;
  
  public int parallelism = GatewaySender.DEFAULT_PARALLELISM_REPLICATED_REGION;

  public boolean isParallel = GatewaySender.DEFAULT_IS_PARALLEL;
  
  public boolean isForInternalUse = GatewaySender.DEFAULT_IS_FOR_INTERNAL_USE;
  
  public boolean isBucketSorted = GatewaySenderAttributes.DEFAULT_IS_BUCKETSORTED;
  
  public int getSocketBufferSize() {
    return this.socketBufferSize;
  }
  
  public boolean isDiskSynchronous() {
    return this.isDiskSynchronous;
  }
  
  public int getSocketReadTimeout() {
    return this.socketReadTimeout;
  }

  public String getDiskStoreName() {
    return this.diskStoreName;
  }

  public int getMaximumQueueMemory() {
    return this.maximumQueueMemory;
  }

  public int getBatchSize() {
    return this.batchSize;
  }

  public int getBatchTimeInterval() {
    return this.batchTimeInterval;
  }

  public boolean isBatchConflationEnabled() {
    return this.isBatchConflationEnabled;
  }

  public boolean isPersistenceEnabled() {
    return this.isPersistenceEnabled;
  }

  public int getAlertThreshold() {
    return this.alertThreshold;
  }

  public List<GatewayEventFilter> getGatewayEventFilters() {
    return this.eventFilters;
  }

  public List<GatewayTransportFilter> getGatewayTransportFilters() {
    return this.transFilters;
  }

  public List<AsyncEventListener> getAsyncEventListeners() {
    return this.listeners;
  }

  public LocatorDiscoveryCallback getGatewayLocatoDiscoveryCallback() {
    return this.locatorDiscoveryCallback;
  }
  
  public boolean isManualStart() {
    return this.manualStart;
  }

  public boolean isParallel() {
    return this.isParallel;
  }
  
  public boolean isForInternalUse() {
    return this.isForInternalUse;
  }
  
  public void addGatewayEventFilter(GatewayEventFilter filter) {
    this.eventFilters.add(filter);
  }

  public void addGatewayTransportFilter(GatewayTransportFilter filter) {
    this.transFilters.add(filter);
  }

  public void addAsyncEventListener(AsyncEventListener listener) {
    this.listeners.add(listener);
  }

  public String getId() {
    return this.id;
  }

  public int getRemoteDSId() {
    return this.remoteDs;
  }

  public int getDispatcherThreads() {
    return dispatcherThreads;
  }

  public int getParallelismForReplicatedRegion() {
    return parallelism;
  }    
    
  public OrderPolicy getOrderPolicy() {
    return policy;
  }
  
  public boolean isBucketSorted() {
    return this.isBucketSorted;
  }
  public GatewayEventSubstitutionFilter getGatewayEventSubstitutionFilter() {
    return this.eventSubstitutionFilter;
  }
}

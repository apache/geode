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

import java.util.ArrayList;
import java.util.List;

import org.apache.geode.cache.asyncqueue.AsyncEventListener;
import org.apache.geode.cache.client.internal.LocatorDiscoveryCallback;
import org.apache.geode.cache.wan.GatewayEventFilter;
import org.apache.geode.cache.wan.GatewayEventSubstitutionFilter;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.cache.wan.GatewaySender.OrderPolicy;
import org.apache.geode.cache.wan.GatewayTransportFilter;

public class GatewaySenderAttributes {

  public static final boolean DEFAULT_IS_BUCKETSORTED = true;
  public static final boolean DEFAULT_IS_META_QUEUE = false;


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

  public boolean groupTransactionEvents = GatewaySender.DEFAULT_MUST_GROUP_TRANSACTION_EVENTS;

  public boolean isForInternalUse = GatewaySender.DEFAULT_IS_FOR_INTERNAL_USE;

  public boolean isBucketSorted = GatewaySenderAttributes.DEFAULT_IS_BUCKETSORTED;

  public boolean isMetaQueue = GatewaySenderAttributes.DEFAULT_IS_META_QUEUE;

  public boolean forwardExpirationDestroy = GatewaySender.DEFAULT_FORWARD_EXPIRATION_DESTROY;

  public boolean partitionedRegionClearSupported =
      GatewaySender.DEFAULT_PARTITIONED_REGION_CLEAR_SUPPORTED;

  public boolean enforceThreadsConnectSameReceiver =
      GatewaySender.DEFAULT_ENFORCE_THREADS_CONNECT_SAME_RECEIVER;

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

  public boolean mustGroupTransactionEvents() {
    return this.groupTransactionEvents;
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

  public boolean isMetaQueue() {
    return this.isMetaQueue;
  }

  public boolean isForwardExpirationDestroy() {
    return this.forwardExpirationDestroy;
  }

  public boolean isPartitionedRegionClearSupported() {
    return this.partitionedRegionClearSupported;
  }

  public boolean getEnforceThreadsConnectSameReceiver() {
    return this.enforceThreadsConnectSameReceiver;
  }
}

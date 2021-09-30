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

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import org.apache.geode.cache.asyncqueue.AsyncEventListener;
import org.apache.geode.cache.client.internal.LocatorDiscoveryCallback;
import org.apache.geode.cache.wan.GatewayEventFilter;
import org.apache.geode.cache.wan.GatewayEventSubstitutionFilter;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.cache.wan.GatewaySender.OrderPolicy;
import org.apache.geode.cache.wan.GatewayTransportFilter;

public class GatewaySenderAttributes {

  private static final boolean DEFAULT_IS_BUCKET_SORTED = true;

  private static final boolean DEFAULT_IS_META_QUEUE = false;

  private int socketBufferSize = GatewaySender.DEFAULT_SOCKET_BUFFER_SIZE;

  private int socketReadTimeout = GatewaySender.DEFAULT_SOCKET_READ_TIMEOUT;

  private int maximumQueueMemory = GatewaySender.DEFAULT_MAXIMUM_QUEUE_MEMORY;

  private int batchSize = GatewaySender.DEFAULT_BATCH_SIZE;

  private int batchTimeInterval = GatewaySender.DEFAULT_BATCH_TIME_INTERVAL;

  private boolean isBatchConflationEnabled = GatewaySender.DEFAULT_BATCH_CONFLATION;

  private boolean isPersistenceEnabled = GatewaySender.DEFAULT_PERSISTENCE_ENABLED;

  private int alertThreshold = GatewaySender.DEFAULT_ALERT_THRESHOLD;

  @Deprecated
  private boolean manualStart = GatewaySender.DEFAULT_MANUAL_START;

  private String diskStoreName;

  private final List<GatewayEventFilter> eventFilters = new ArrayList<>();

  private final ArrayList<GatewayTransportFilter> transFilters = new ArrayList<>();

  private final List<AsyncEventListener> listeners = new ArrayList<>();

  private GatewayEventSubstitutionFilter<?, ?> eventSubstitutionFilter;

  private String id;

  private int remoteDs = GatewaySender.DEFAULT_DISTRIBUTED_SYSTEM_ID;

  private LocatorDiscoveryCallback locatorDiscoveryCallback;

  private boolean isDiskSynchronous = GatewaySender.DEFAULT_DISK_SYNCHRONOUS;

  private OrderPolicy policy;

  private int dispatcherThreads = GatewaySender.DEFAULT_DISPATCHER_THREADS;

  private int parallelism = GatewaySender.DEFAULT_PARALLELISM_REPLICATED_REGION;

  private boolean isParallel = GatewaySender.DEFAULT_IS_PARALLEL;

  private boolean groupTransactionEvents = GatewaySender.DEFAULT_MUST_GROUP_TRANSACTION_EVENTS;

  private int retriesToGetTransactionEventsFromQueue =
      GatewaySender.GET_TRANSACTION_EVENTS_FROM_QUEUE_RETRIES;

  private boolean isForInternalUse = GatewaySender.DEFAULT_IS_FOR_INTERNAL_USE;

  private boolean isBucketSorted = GatewaySenderAttributes.DEFAULT_IS_BUCKET_SORTED;

  private boolean isMetaQueue = GatewaySenderAttributes.DEFAULT_IS_META_QUEUE;

  private boolean forwardExpirationDestroy = GatewaySender.DEFAULT_FORWARD_EXPIRATION_DESTROY;

  private boolean enforceThreadsConnectSameReceiver =
      GatewaySender.DEFAULT_ENFORCE_THREADS_CONNECT_SAME_RECEIVER;

  public void setSocketBufferSize(int bufferSize) {
    socketBufferSize = bufferSize;
  }

  public void setSocketReadTimeout(int readTimeout) {
    socketReadTimeout = readTimeout;
  }

  public void setMaximumQueueMemory(int maxQueueMemory) {
    maximumQueueMemory = maxQueueMemory;
  }

  public void setBatchSize(int batchSize) {
    this.batchSize = batchSize;
  }

  public void setBatchTimeInterval(int batchTimeInterval) {
    this.batchTimeInterval = batchTimeInterval;
  }

  public void setBatchConflationEnabled(boolean batchConfEnabled) {
    isBatchConflationEnabled = batchConfEnabled;
  }

  public void setPersistenceEnabled(boolean persistenceEnabled) {
    isPersistenceEnabled = persistenceEnabled;
  }

  public void setAlertThreshold(int alertThresh) {
    alertThreshold = alertThresh;
  }

  @Deprecated
  public void setManualStart(boolean manualStart) {
    this.manualStart = manualStart;
  }

  public void setDiskStoreName(String diskStoreName) {
    this.diskStoreName = diskStoreName;
  }

  public void setEventSubstitutionFilter(
      @Nullable GatewayEventSubstitutionFilter<?, ?> eventSubstitutionFilter) {
    this.eventSubstitutionFilter = eventSubstitutionFilter;
  }

  public void setId(String idString) {
    id = idString;
  }

  public void setRemoteDs(int rDs) {
    remoteDs = rDs;
  }

  public void setLocatorDiscoveryCallback(@Nullable LocatorDiscoveryCallback locatorDiscCall) {
    locatorDiscoveryCallback = locatorDiscCall;
  }

  public void setDiskSynchronous(boolean diskSynchronous) {
    isDiskSynchronous = diskSynchronous;
  }

  public void setOrderPolicy(@Nullable OrderPolicy orderpolicy) {
    policy = orderpolicy;
  }

  public void setDispatcherThreads(int dispatchThreads) {
    dispatcherThreads = dispatchThreads;
  }

  public void setParallelism(int tempParallelism) {
    parallelism = tempParallelism;
  }

  public void setParallel(boolean parallel) {
    isParallel = parallel;
  }

  public void setGroupTransactionEvents(boolean groupTransEvents) {
    groupTransactionEvents = groupTransEvents;
  }

  public void setRetriesToGetTransactionEventsFromQueue(int retries) {
    retriesToGetTransactionEventsFromQueue = retries;
  }

  public void setForInternalUse(boolean forInternalUse) {
    isForInternalUse = forInternalUse;
  }

  public void setBucketSorted(boolean bucketSorted) {
    isBucketSorted = bucketSorted;
  }

  public void setMetaQueue(boolean metaQueue) {
    isMetaQueue = metaQueue;
  }

  public void setForwardExpirationDestroy(boolean forwardExpirationDestroy) {
    this.forwardExpirationDestroy = forwardExpirationDestroy;
  }

  public void setEnforceThreadsConnectSameReceiver(boolean enforceThreadsConnectSameReceiver) {
    this.enforceThreadsConnectSameReceiver = enforceThreadsConnectSameReceiver;
  }

  public int getSocketBufferSize() {
    return socketBufferSize;
  }

  public boolean isDiskSynchronous() {
    return isDiskSynchronous;
  }

  public int getSocketReadTimeout() {
    return socketReadTimeout;
  }

  public String getDiskStoreName() {
    return diskStoreName;
  }

  public int getMaximumQueueMemory() {
    return maximumQueueMemory;
  }

  public int getBatchSize() {
    return batchSize;
  }

  public int getBatchTimeInterval() {
    return batchTimeInterval;
  }

  public boolean isBatchConflationEnabled() {
    return isBatchConflationEnabled;
  }

  public boolean isPersistenceEnabled() {
    return isPersistenceEnabled;
  }

  public int getAlertThreshold() {
    return alertThreshold;
  }

  public @NotNull List<GatewayEventFilter> getGatewayEventFilters() {
    return eventFilters;
  }

  public @NotNull List<GatewayTransportFilter> getGatewayTransportFilters() {
    return transFilters;
  }

  public @NotNull List<AsyncEventListener> getAsyncEventListeners() {
    return listeners;
  }

  public @Nullable LocatorDiscoveryCallback getGatewayLocatorDiscoveryCallback() {
    return locatorDiscoveryCallback;
  }

  @Deprecated
  public boolean isManualStart() {
    return manualStart;
  }

  public boolean isParallel() {
    return isParallel;
  }

  public boolean mustGroupTransactionEvents() {
    return groupTransactionEvents;
  }

  public int getRetriesToGetTransactionEventsFromQueue() {
    return retriesToGetTransactionEventsFromQueue;
  }

  public boolean isForInternalUse() {
    return isForInternalUse;
  }

  public void addGatewayEventFilter(GatewayEventFilter filter) {
    eventFilters.add(filter);
  }

  public void addGatewayTransportFilter(GatewayTransportFilter filter) {
    transFilters.add(filter);
  }

  public void addAsyncEventListener(AsyncEventListener listener) {
    listeners.add(listener);
  }

  public String getId() {
    return id;
  }

  public int getRemoteDSId() {
    return remoteDs;
  }

  public int getDispatcherThreads() {
    return dispatcherThreads;
  }

  public int getParallelismForReplicatedRegion() {
    return parallelism;
  }

  @Nullable
  public OrderPolicy getOrderPolicy() {
    return policy;
  }

  public boolean isBucketSorted() {
    return isBucketSorted;
  }

  public @Nullable GatewayEventSubstitutionFilter<?, ?> getGatewayEventSubstitutionFilter() {
    return eventSubstitutionFilter;
  }

  public boolean isMetaQueue() {
    return isMetaQueue;
  }

  public boolean isForwardExpirationDestroy() {
    return forwardExpirationDestroy;
  }

  public boolean getEnforceThreadsConnectSameReceiver() {
    return enforceThreadsConnectSameReceiver;
  }

}

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


  private int socketBufferSize = GatewaySender.DEFAULT_SOCKET_BUFFER_SIZE;

  private int socketReadTimeout = GatewaySender.DEFAULT_SOCKET_READ_TIMEOUT;

  private int maximumQueueMemory = GatewaySender.DEFAULT_MAXIMUM_QUEUE_MEMORY;

  private int batchSize = GatewaySender.DEFAULT_BATCH_SIZE;

  private int batchTimeInterval = GatewaySender.DEFAULT_BATCH_TIME_INTERVAL;

  private boolean isBatchConflationEnabled = GatewaySender.DEFAULT_BATCH_CONFLATION;

  private boolean isPersistenceEnabled = GatewaySender.DEFAULT_PERSISTENCE_ENABLED;

  private int alertThreshold = GatewaySender.DEFAULT_ALERT_THRESHOLD;

  private boolean manualStart = GatewaySender.DEFAULT_MANUAL_START;

  private String diskStoreName;

  private List<GatewayEventFilter> eventFilters = new ArrayList<GatewayEventFilter>();

  private ArrayList<GatewayTransportFilter> transFilters = new ArrayList<GatewayTransportFilter>();

  private List<AsyncEventListener> listeners = new ArrayList<AsyncEventListener>();

  private GatewayEventSubstitutionFilter eventSubstitutionFilter;

  private String id;

  private int remoteDs = GatewaySender.DEFAULT_DISTRIBUTED_SYSTEM_ID;

  private LocatorDiscoveryCallback locatorDiscoveryCallback;

  private boolean isDiskSynchronous = GatewaySender.DEFAULT_DISK_SYNCHRONOUS;

  private OrderPolicy policy;

  private int dispatcherThreads = GatewaySender.DEFAULT_DISPATCHER_THREADS;

  private int parallelism = GatewaySender.DEFAULT_PARALLELISM_REPLICATED_REGION;

  private boolean isParallel = GatewaySender.DEFAULT_IS_PARALLEL;

  private boolean groupTransactionEvents = GatewaySender.DEFAULT_MUST_GROUP_TRANSACTION_EVENTS;

  private boolean isForInternalUse = GatewaySender.DEFAULT_IS_FOR_INTERNAL_USE;

  private boolean isBucketSorted = GatewaySenderAttributes.DEFAULT_IS_BUCKETSORTED;

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

  public void setBatchSize(int batchsize) {
    batchSize = batchsize;
  }

  public void setBatchTimeInterval(int batchtimeinterval) {
    batchTimeInterval = batchtimeinterval;
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

  public void setManualStart(boolean manualstart) {
    manualStart = manualstart;
  }

  public void setDiskStoreName(String diskstorename) {
    diskStoreName = diskstorename;
  }

  public void setEventSubstitutionFilter(GatewayEventSubstitutionFilter eventsubstitutionfilter) {
    eventSubstitutionFilter = eventsubstitutionfilter;
  }

  public void setId(String idString) {
    id = idString;
  }

  public void setRemoteDs(int rDs) {
    remoteDs = rDs;
  }

  public void setLocatorDiscoveryCallback(LocatorDiscoveryCallback locatorDiscCall) {
    locatorDiscoveryCallback = locatorDiscCall;
  }

  public void setDiskSynchronous(boolean diskSynchronous) {
    isDiskSynchronous = diskSynchronous;
  }

  public void setOrderPolicy(OrderPolicy orderpolicy) {
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

  public void setForInternalUse(boolean forInternalUse) {
    isForInternalUse = forInternalUse;
  }

  public void setBucketSorted(boolean bucketSorted) {
    isBucketSorted = bucketSorted;
  }

  public void setMetaQueue(boolean metaQueue) {
    isMetaQueue = metaQueue;
  }

  public void setForwardExpirationDestroy(boolean forwardexpirationdestroy) {
    forwardExpirationDestroy = forwardexpirationdestroy;
  }

  public void setEnforceThreadsConnectSameReceiver(boolean enforcethreadsconnectsamereceiver) {
    enforceThreadsConnectSameReceiver = enforcethreadsconnectsamereceiver;
  }

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

  public boolean getEnforceThreadsConnectSameReceiver() {
    return this.enforceThreadsConnectSameReceiver;
  }

}

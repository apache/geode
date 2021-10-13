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

import java.util.List;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import org.apache.geode.cache.asyncqueue.AsyncEventListener;
import org.apache.geode.cache.client.internal.LocatorDiscoveryCallback;
import org.apache.geode.cache.wan.GatewayEventFilter;
import org.apache.geode.cache.wan.GatewayEventSubstitutionFilter;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.cache.wan.GatewayTransportFilter;

public interface GatewaySenderAttributes {
  int getSocketBufferSize();

  boolean isDiskSynchronous();

  int getSocketReadTimeout();

  String getDiskStoreName();

  int getMaximumQueueMemory();

  int getBatchSize();

  int getBatchTimeInterval();

  boolean isBatchConflationEnabled();

  boolean isPersistenceEnabled();

  int getAlertThreshold();

  @NotNull
  List<GatewayEventFilter> getGatewayEventFilters();

  @NotNull
  List<GatewayTransportFilter> getGatewayTransportFilters();

  @NotNull
  List<AsyncEventListener> getAsyncEventListeners();

  @Nullable
  LocatorDiscoveryCallback getGatewayLocatorDiscoveryCallback();

  @Deprecated
  boolean isManualStart();

  boolean isParallel();

  boolean mustGroupTransactionEvents();

  int getRetriesToGetTransactionEventsFromQueue();

  boolean isForInternalUse();

  String getId();

  int getRemoteDSId();

  int getDispatcherThreads();

  int getParallelismForReplicatedRegion();

  @Nullable
  GatewaySender.OrderPolicy getOrderPolicy();

  boolean isBucketSorted();

  @Nullable
  GatewayEventSubstitutionFilter<?, ?> getGatewayEventSubstitutionFilter();

  boolean isMetaQueue();

  boolean isForwardExpirationDestroy();

  boolean getEnforceThreadsConnectSameReceiver();
}

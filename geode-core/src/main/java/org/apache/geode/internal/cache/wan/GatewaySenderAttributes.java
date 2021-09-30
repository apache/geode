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

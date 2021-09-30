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
package org.apache.geode.cache.wan.internal;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import org.apache.geode.cache.asyncqueue.AsyncEventListener;
import org.apache.geode.cache.client.internal.LocatorDiscoveryCallback;
import org.apache.geode.cache.wan.GatewayEventFilter;
import org.apache.geode.cache.wan.GatewayEventSubstitutionFilter;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.cache.wan.GatewaySender.OrderPolicy;
import org.apache.geode.cache.wan.GatewaySenderFactory;
import org.apache.geode.cache.wan.GatewayTransportFilter;
import org.apache.geode.cache.wan.internal.parallel.ParallelGatewaySenderImpl;
import org.apache.geode.cache.wan.internal.serial.SerialGatewaySenderImpl;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.wan.GatewaySenderAttributesImpl;
import org.apache.geode.internal.cache.wan.GatewaySenderException;
import org.apache.geode.internal.cache.wan.InternalGatewaySenderFactory;
import org.apache.geode.internal.cache.xmlcache.CacheCreation;
import org.apache.geode.internal.cache.xmlcache.ParallelGatewaySenderCreation;
import org.apache.geode.internal.cache.xmlcache.SerialGatewaySenderCreation;
import org.apache.geode.internal.statistics.StatisticsClock;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * @since GemFire 7.0
 */
public class GatewaySenderFactoryImpl implements InternalGatewaySenderFactory {

  private static final Logger logger = LogService.getLogger();

  private static final AtomicBoolean GATEWAY_CONNECTION_READ_TIMEOUT_PROPERTY_CHECKED =
      new AtomicBoolean(false);

  /**
   * Used internally to pass the attributes from this factory to the real GatewaySender it is
   * creating.
   */
  private final GatewaySenderAttributesImpl attrs = new GatewaySenderAttributesImpl();

  private final InternalCache cache;

  private final StatisticsClock statisticsClock;

  public GatewaySenderFactoryImpl(InternalCache cache, StatisticsClock statisticsClock) {
    this.cache = cache;
    this.statisticsClock = statisticsClock;
  }

  @Override
  public GatewaySenderFactory setParallel(boolean isParallel) {
    attrs.setParallel(isParallel);
    return this;
  }

  @Override
  public GatewaySenderFactory setGroupTransactionEvents(boolean groupTransactionEvents) {
    attrs.setGroupTransactionEvents(groupTransactionEvents);
    return this;
  }

  @Override
  public GatewaySenderFactory setRetriesToGetTransactionEventsFromQueue(int retries) {
    attrs.setRetriesToGetTransactionEventsFromQueue(retries);
    return this;
  }

  @Override
  public GatewaySenderFactory setForInternalUse(boolean isForInternalUse) {
    attrs.setForInternalUse(isForInternalUse);
    return this;
  }

  @Override
  public GatewaySenderFactory addGatewayEventFilter(GatewayEventFilter filter) {
    attrs.addGatewayEventFilter(filter);
    return this;
  }

  @Override
  public GatewaySenderFactory addGatewayTransportFilter(GatewayTransportFilter filter) {
    attrs.addGatewayTransportFilter(filter);
    return this;
  }

  @Override
  public GatewaySenderFactory addAsyncEventListener(AsyncEventListener listener) {
    attrs.addAsyncEventListener(listener);
    return this;
  }

  @Override
  public GatewaySenderFactory setSocketBufferSize(int socketBufferSize) {
    attrs.setSocketBufferSize(socketBufferSize);
    return this;
  }

  @Override
  public GatewaySenderFactory setSocketReadTimeout(int socketReadTimeout) {
    attrs.setSocketReadTimeout(socketReadTimeout);
    return this;
  }

  @Override
  public GatewaySenderFactory setDiskStoreName(String diskStoreName) {
    attrs.setDiskStoreName(diskStoreName);
    return this;
  }

  @Override
  public GatewaySenderFactory setMaximumQueueMemory(int maximumQueueMemory) {
    attrs.setMaximumQueueMemory(maximumQueueMemory);
    return this;
  }

  @Override
  public GatewaySenderFactory setBatchSize(int batchSize) {
    attrs.setBatchSize(batchSize);
    return this;
  }

  @Override
  public GatewaySenderFactory setBatchTimeInterval(int batchTimeInterval) {
    attrs.setBatchTimeInterval(batchTimeInterval);
    return this;
  }

  @Override
  public GatewaySenderFactory setBatchConflationEnabled(boolean enableBatchConflation) {
    attrs.setBatchConflationEnabled(enableBatchConflation);
    return this;
  }

  @Override
  public GatewaySenderFactory setPersistenceEnabled(boolean enablePersistence) {
    attrs.setPersistenceEnabled(enablePersistence);
    return this;
  }

  @Override
  public GatewaySenderFactory setAlertThreshold(int threshold) {
    attrs.setAlertThreshold(threshold);
    return this;
  }

  @Override
  public GatewaySenderFactory setManualStart(boolean start) {
    attrs.setManualStart(start);
    return this;
  }

  @Override
  public GatewaySenderFactory setLocatorDiscoveryCallback(LocatorDiscoveryCallback locCallback) {
    attrs.setLocatorDiscoveryCallback(locCallback);
    return this;
  }

  @Override
  public GatewaySenderFactory setDiskSynchronous(boolean isSynchronous) {
    attrs.setDiskSynchronous(isSynchronous);
    return this;
  }

  @Override
  public GatewaySenderFactory setDispatcherThreads(int numThreads) {
    if ((numThreads > 1) && attrs.getOrderPolicy() == null) {
      attrs.setOrderPolicy(GatewaySender.DEFAULT_ORDER_POLICY);
    }
    attrs.setDispatcherThreads(numThreads);
    return this;
  }

  @Override
  public GatewaySenderFactory setParallelFactorForReplicatedRegion(int parallel) {
    attrs.setParallelism(parallel);
    attrs.setOrderPolicy(GatewaySender.DEFAULT_ORDER_POLICY);
    return this;
  }

  @Override
  public GatewaySenderFactory setOrderPolicy(OrderPolicy policy) {
    attrs.setOrderPolicy(policy);
    return this;
  }

  @Override
  public GatewaySenderFactory setBucketSorted(boolean isBucketSorted) {
    attrs.setBucketSorted(isBucketSorted);
    return this;
  }

  @Override
  public GatewaySenderFactory setEnforceThreadsConnectSameReceiver(
      boolean enforceThreadsConnectSameReceiver) {
    attrs.setEnforceThreadsConnectSameReceiver(enforceThreadsConnectSameReceiver);
    return this;
  }

  @Override
  public @NotNull GatewaySender create(final @NotNull String id, final int remoteDSId) {
    int myDSId = InternalDistributedSystem.getAnyInstance().getDistributionManager()
        .getDistributedSystemId();
    if (remoteDSId == myDSId) {
      throw new GatewaySenderException(
          String.format(
              "GatewaySender %s cannot be created with remote DS Id equal to this DS Id. ",
              id));
    }
    if (remoteDSId < 0) {
      throw new GatewaySenderException(
          String.format("GatewaySender %s cannot be created with remote DS Id less than 0. ",
              id));
    }
    attrs.setId(id);
    attrs.setRemoteDs(remoteDSId);
    GatewaySender sender;

    if (attrs.getDispatcherThreads() <= 0) {
      throw new GatewaySenderException(
          String.format("GatewaySender %s can not be created with dispatcher threads less than 1",
              id));
    }

    // Verify socket read timeout if a proper logger is available
    if (cache instanceof GemFireCacheImpl) {
      // If socket read timeout is less than the minimum, log a warning.
      // Ideally, this should throw a GatewaySenderException, but wan dunit tests
      // were failing, and we were running out of time to change them.
      if (attrs.getSocketReadTimeout() != 0
          && attrs.getSocketReadTimeout() < GatewaySender.MINIMUM_SOCKET_READ_TIMEOUT) {
        logger.warn(
            "{} cannot configure socket read timeout of {} milliseconds because it is less than the minimum of {} milliseconds. The default will be used instead.",
            new Object[] {"GatewaySender " + id, attrs.getSocketReadTimeout(),
                GatewaySender.MINIMUM_SOCKET_READ_TIMEOUT});
        attrs.setSocketReadTimeout(GatewaySender.MINIMUM_SOCKET_READ_TIMEOUT);
      }

      // Log a warning if the old system property is set.
      if (GATEWAY_CONNECTION_READ_TIMEOUT_PROPERTY_CHECKED.compareAndSet(false, true)) {
        if (System.getProperty(GatewaySender.GATEWAY_CONNECTION_READ_TIMEOUT_PROPERTY) != null) {
          logger.warn(
              "Obsolete java system property named {} was set to control {}. This property is no longer supported. Please use the GemFire API instead.",
              new Object[] {GatewaySender.GATEWAY_CONNECTION_READ_TIMEOUT_PROPERTY,
                  "GatewaySender socket read timeout"});
        }
      }
    }
    if (attrs.mustGroupTransactionEvents() && attrs.isBatchConflationEnabled()) {
      throw new GatewaySenderException(
          String.format(
              "GatewaySender %s cannot be created with both group transaction events set to true and batch conflation enabled",
              id));
    }

    if (attrs.isParallel()) {
      sender = createParallelGatewaySender(id);
    } else {
      sender = createSerialGatewaySender(id);
    }
    return sender;
  }

  private @NotNull GatewaySender createSerialGatewaySender(final @NotNull String id) {
    if (attrs.getAsyncEventListeners().size() > 0) {
      throw new GatewaySenderException(
          String.format(
              "SerialGatewaySender %s cannot define a remote site because at least AsyncEventListener is already added. Both listeners and remote site cannot be defined for the same gateway sender.",
              id));
    }
    if (attrs.mustGroupTransactionEvents() && attrs.getDispatcherThreads() > 1) {
      throw new GatewaySenderException(
          String.format(
              "SerialGatewaySender %s cannot be created with group transaction events set to true when dispatcher threads is greater than 1",
              id));
    }
    if (attrs.getOrderPolicy() == null && attrs.getDispatcherThreads() > 1) {
      attrs.setOrderPolicy(GatewaySender.DEFAULT_ORDER_POLICY);
    }

    GatewaySender sender = null;
    if (cache instanceof GemFireCacheImpl) {
      sender = new SerialGatewaySenderImpl(cache, statisticsClock, attrs);
      cache.addGatewaySender(sender);
      if (!attrs.isManualStart()) {
        sender.start();
      }
    } else if (cache instanceof CacheCreation) {
      sender = new SerialGatewaySenderCreation(cache, attrs);
      cache.addGatewaySender(sender);
    } else {
      throw new IllegalStateException();
    }
    return sender;
  }

  private @NotNull GatewaySender createParallelGatewaySender(final @NotNull String id) {
    if ((attrs.getOrderPolicy() != null)
        && attrs.getOrderPolicy().equals(OrderPolicy.THREAD)) {
      throw new GatewaySenderException(
          String.format("Parallel Gateway Sender %s can not be created with OrderPolicy %s",
              id, attrs.getOrderPolicy()));
    }

    final GatewaySender sender;
    if (cache instanceof GemFireCacheImpl) {
      sender = new ParallelGatewaySenderImpl(cache, statisticsClock, attrs);
      cache.addGatewaySender(sender);

      if (!attrs.isManualStart()) {
        sender.start();
      }
    } else if (cache instanceof CacheCreation) {
      sender = new ParallelGatewaySenderCreation(cache, attrs);
      cache.addGatewaySender(sender);
    } else {
      throw new IllegalStateException();
    }
    return sender;
  }

  @Override
  public GatewaySenderFactory removeGatewayEventFilter(GatewayEventFilter filter) {
    attrs.getGatewayEventFilters().remove(filter);
    return this;
  }

  @Override
  public GatewaySenderFactory removeGatewayTransportFilter(GatewayTransportFilter filter) {
    attrs.getGatewayTransportFilters().remove(filter);
    return this;
  }

  @Override
  public GatewaySenderFactory setGatewayEventSubstitutionFilter(
      GatewayEventSubstitutionFilter<?, ?> filter) {
    attrs.setEventSubstitutionFilter(filter);
    return this;
  }

  @Override
  public void configureGatewaySender(GatewaySender senderCreation) {
    attrs.setParallel(senderCreation.isParallel());
    attrs.setManualStart(senderCreation.isManualStart());
    attrs.setSocketBufferSize(senderCreation.getSocketBufferSize());
    attrs.setSocketReadTimeout(senderCreation.getSocketReadTimeout());
    attrs.setBatchConflationEnabled(senderCreation.isBatchConflationEnabled());
    attrs.setBatchSize(senderCreation.getBatchSize());
    attrs.setBatchTimeInterval(senderCreation.getBatchTimeInterval());
    attrs.setPersistenceEnabled(senderCreation.isPersistenceEnabled());
    attrs.setDiskStoreName(senderCreation.getDiskStoreName());
    attrs.setDiskSynchronous(senderCreation.isDiskSynchronous());
    attrs.setMaximumQueueMemory(senderCreation.getMaximumQueueMemory());
    attrs.setAlertThreshold(senderCreation.getAlertThreshold());
    attrs.setDispatcherThreads(senderCreation.getDispatcherThreads());
    attrs.setOrderPolicy(senderCreation.getOrderPolicy());
    for (GatewayEventFilter filter : senderCreation.getGatewayEventFilters()) {
      attrs.getGatewayEventFilters().add(filter);
    }
    for (GatewayTransportFilter filter : senderCreation.getGatewayTransportFilters()) {
      attrs.getGatewayTransportFilters().add(filter);
    }
    attrs.setEventSubstitutionFilter(senderCreation.getGatewayEventSubstitutionFilter());
    attrs.setGroupTransactionEvents(senderCreation.mustGroupTransactionEvents());
    attrs.setEnforceThreadsConnectSameReceiver(
        senderCreation.getEnforceThreadsConnectSameReceiver());
  }
}

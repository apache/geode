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

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.asyncqueue.AsyncEventListener;
import org.apache.geode.cache.client.internal.LocatorDiscoveryCallback;
import org.apache.geode.cache.wan.GatewayEventFilter;
import org.apache.geode.cache.wan.GatewayEventSubstitutionFilter;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.cache.wan.GatewaySender.OrderPolicy;
import org.apache.geode.cache.wan.GatewaySenderFactory;
import org.apache.geode.cache.wan.GatewayTransportFilter;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.wan.parallel.ParallelGatewaySenderImpl;
import org.apache.geode.internal.cache.wan.serial.SerialGatewaySenderImpl;
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
  private final GatewaySenderAttributes attrs = new GatewaySenderAttributes();

  private final InternalCache cache;

  private final StatisticsClock statisticsClock;

  public GatewaySenderFactoryImpl(InternalCache cache, StatisticsClock statisticsClock) {
    this.cache = cache;
    this.statisticsClock = statisticsClock;
  }

  @Override
  public GatewaySenderFactory setParallel(boolean isParallel) {
    attrs.isParallel = isParallel;
    return this;
  }

  @Override
  public GatewaySenderFactory setGroupTransactionEvents(boolean groupTransactionEvents) {
    this.attrs.groupTransactionEvents = groupTransactionEvents;
    return this;
  }

  @Override
  public GatewaySenderFactory setForInternalUse(boolean isForInternalUse) {
    attrs.isForInternalUse = isForInternalUse;
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
    attrs.socketBufferSize = socketBufferSize;
    return this;
  }

  @Override
  public GatewaySenderFactory setSocketReadTimeout(int socketReadTimeout) {
    attrs.socketReadTimeout = socketReadTimeout;
    return this;
  }

  @Override
  public GatewaySenderFactory setDiskStoreName(String diskStoreName) {
    attrs.diskStoreName = diskStoreName;
    return this;
  }

  @Override
  public GatewaySenderFactory setMaximumQueueMemory(int maximumQueueMemory) {
    attrs.maximumQueueMemory = maximumQueueMemory;
    return this;
  }

  @Override
  public GatewaySenderFactory setBatchSize(int batchSize) {
    attrs.batchSize = batchSize;
    return this;
  }

  @Override
  public GatewaySenderFactory setBatchTimeInterval(int batchTimeInterval) {
    attrs.batchTimeInterval = batchTimeInterval;
    return this;
  }

  @Override
  public GatewaySenderFactory setBatchConflationEnabled(boolean enableBatchConflation) {
    attrs.isBatchConflationEnabled = enableBatchConflation;
    return this;
  }

  @Override
  public GatewaySenderFactory setPersistenceEnabled(boolean enablePersistence) {
    attrs.isPersistenceEnabled = enablePersistence;
    return this;
  }

  @Override
  public GatewaySenderFactory setAlertThreshold(int threshold) {
    attrs.alertThreshold = threshold;
    return this;
  }

  @Deprecated
  @Override
  public GatewaySenderFactory setManualStart(boolean start) {
    attrs.manualStart = start;
    return this;
  }

  @Override
  public GatewaySenderFactory setLocatorDiscoveryCallback(LocatorDiscoveryCallback locCallback) {
    attrs.locatorDiscoveryCallback = locCallback;
    return this;
  }

  @Override
  public GatewaySenderFactory setDiskSynchronous(boolean isSynchronous) {
    attrs.isDiskSynchronous = isSynchronous;
    return this;
  }

  @Override
  public GatewaySenderFactory setDispatcherThreads(int numThreads) {
    if ((numThreads > 1) && attrs.policy == null) {
      attrs.policy = GatewaySender.DEFAULT_ORDER_POLICY;
    }
    attrs.dispatcherThreads = numThreads;
    return this;
  }

  @Override
  public GatewaySenderFactory setParallelFactorForReplicatedRegion(int parallel) {
    attrs.parallelism = parallel;
    attrs.policy = GatewaySender.DEFAULT_ORDER_POLICY;
    return this;
  }

  @Override
  public GatewaySenderFactory setOrderPolicy(OrderPolicy policy) {
    attrs.policy = policy;
    return this;
  }

  @Override
  public GatewaySenderFactory setBucketSorted(boolean isBucketSorted) {
    attrs.isBucketSorted = isBucketSorted;
    return this;
  }

  @Override
  public GatewaySenderFactory setEnforceThreadsConnectSameReceiver(
      boolean enforceThreadsConnectSameReceiver) {
    this.attrs.enforceThreadsConnectSameReceiver = enforceThreadsConnectSameReceiver;
    return this;
  }

  @Override
  public GatewaySender create(String id, int remoteDSId) {
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
    attrs.id = id;
    attrs.remoteDs = remoteDSId;
    GatewaySender sender = null;

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
        attrs.socketReadTimeout = GatewaySender.MINIMUM_SOCKET_READ_TIMEOUT;
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
    if (this.attrs.mustGroupTransactionEvents() && this.attrs.isBatchConflationEnabled()) {
      throw new GatewaySenderException(
          String.format(
              "GatewaySender %s cannot be created with both group transaction events set to true and batch conflation enabled",
              id));
    }

    if (attrs.isParallel()) {
      if ((attrs.getOrderPolicy() != null)
          && attrs.getOrderPolicy().equals(OrderPolicy.THREAD)) {
        throw new GatewaySenderException(
            String.format("Parallel Gateway Sender %s can not be created with OrderPolicy %s",
                id, attrs.getOrderPolicy()));
      }
      if (cache instanceof GemFireCacheImpl) {
        sender = new ParallelGatewaySenderImpl(cache, statisticsClock, attrs);
        cache.addGatewaySender(sender);

        if (!attrs.isManualStart()) {
          sender.start();
        }
      } else if (cache instanceof CacheCreation) {
        sender = new ParallelGatewaySenderCreation(cache, attrs);
        cache.addGatewaySender(sender);
      }
    } else {
      if (attrs.getAsyncEventListeners().size() > 0) {
        throw new GatewaySenderException(
            String.format(
                "SerialGatewaySender %s cannot define a remote site because at least AsyncEventListener is already added. Both listeners and remote site cannot be defined for the same gateway sender.",
                id));
      }
      if (this.attrs.mustGroupTransactionEvents() && this.attrs.getDispatcherThreads() > 1) {
        throw new GatewaySenderException(
            String.format(
                "SerialGatewaySender %s cannot be created with group transaction events set to true when dispatcher threads is greater than 1",
                id));
      }
      if (attrs.getOrderPolicy() == null && attrs.getDispatcherThreads() > 1) {
        attrs.policy = GatewaySender.DEFAULT_ORDER_POLICY;
      }
      if (cache instanceof GemFireCacheImpl) {
        sender = new SerialGatewaySenderImpl(cache, statisticsClock, attrs);
        cache.addGatewaySender(sender);
        if (!attrs.isManualStart()) {
          sender.start();
        }
      } else if (cache instanceof CacheCreation) {
        sender = new SerialGatewaySenderCreation(cache, attrs);
        cache.addGatewaySender(sender);
      }
    }
    return sender;
  }

  @Override
  public GatewaySender create(String id) {
    attrs.id = id;
    GatewaySender sender = null;

    if (attrs.getDispatcherThreads() <= 0) {
      throw new AsyncEventQueueConfigurationException(
          String.format("AsyncEventQueue %s can not be created with dispatcher threads less than 1",
              id));
    }

    if (attrs.isParallel()) {
      if ((attrs.getOrderPolicy() != null)
          && attrs.getOrderPolicy().equals(OrderPolicy.THREAD)) {
        throw new AsyncEventQueueConfigurationException(
            String.format(
                "AsyncEventQueue %s can not be created with OrderPolicy %s when it is set parallel",
                id, attrs.getOrderPolicy()));
      }

      if (cache instanceof GemFireCacheImpl) {
        sender = new ParallelGatewaySenderImpl(cache, statisticsClock, attrs);
        cache.addGatewaySender(sender);
        if (!attrs.isManualStart()) {
          sender.start();
        }
      } else if (cache instanceof CacheCreation) {
        sender = new ParallelGatewaySenderCreation(cache, attrs);
        cache.addGatewaySender(sender);
      }
    } else {
      if (attrs.getOrderPolicy() == null && attrs.getDispatcherThreads() > 1) {
        attrs.policy = GatewaySender.DEFAULT_ORDER_POLICY;
      }
      if (cache instanceof GemFireCacheImpl) {
        sender = new SerialGatewaySenderImpl(cache, statisticsClock, attrs);
        cache.addGatewaySender(sender);
        if (!attrs.isManualStart()) {
          sender.start();
        }
      } else if (cache instanceof CacheCreation) {
        sender = new SerialGatewaySenderCreation(cache, attrs);
        cache.addGatewaySender(sender);
      }
    }
    return sender;
  }

  @Override
  public GatewaySenderFactory removeGatewayEventFilter(GatewayEventFilter filter) {
    attrs.eventFilters.remove(filter);
    return this;
  }

  @Override
  public GatewaySenderFactory removeGatewayTransportFilter(GatewayTransportFilter filter) {
    attrs.transFilters.remove(filter);
    return this;
  }

  @Override
  public GatewaySenderFactory setGatewayEventSubstitutionFilter(
      @SuppressWarnings("rawtypes") GatewayEventSubstitutionFilter filter) {
    attrs.eventSubstitutionFilter = filter;
    return this;
  }

  @SuppressWarnings("deprecation")
  @Override
  public void configureGatewaySender(GatewaySender senderCreation) {
    attrs.isParallel = senderCreation.isParallel();
    attrs.manualStart = senderCreation.isManualStart();
    attrs.socketBufferSize = senderCreation.getSocketBufferSize();
    attrs.socketReadTimeout = senderCreation.getSocketReadTimeout();
    attrs.isBatchConflationEnabled = senderCreation.isBatchConflationEnabled();
    attrs.batchSize = senderCreation.getBatchSize();
    attrs.batchTimeInterval = senderCreation.getBatchTimeInterval();
    attrs.isPersistenceEnabled = senderCreation.isPersistenceEnabled();
    attrs.diskStoreName = senderCreation.getDiskStoreName();
    attrs.isDiskSynchronous = senderCreation.isDiskSynchronous();
    attrs.maximumQueueMemory = senderCreation.getMaximumQueueMemory();
    attrs.alertThreshold = senderCreation.getAlertThreshold();
    attrs.dispatcherThreads = senderCreation.getDispatcherThreads();
    attrs.policy = senderCreation.getOrderPolicy();
    for (GatewayEventFilter filter : senderCreation.getGatewayEventFilters()) {
      attrs.eventFilters.add(filter);
    }
    for (GatewayTransportFilter filter : senderCreation.getGatewayTransportFilters()) {
      attrs.transFilters.add(filter);
    }
    attrs.eventSubstitutionFilter = senderCreation.getGatewayEventSubstitutionFilter();
    attrs.groupTransactionEvents = senderCreation.mustGroupTransactionEvents();
    attrs.enforceThreadsConnectSameReceiver = senderCreation.getEnforceThreadsConnectSameReceiver();
  }
}

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
import org.apache.geode.internal.cache.wan.AsyncEventQueueConfigurationException;
import org.apache.geode.internal.cache.wan.GatewaySenderAttributes;
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
  private final GatewaySenderAttributes attrs = new GatewaySenderAttributes();

  private final InternalCache cache;

  private final StatisticsClock statisticsClock;

  public GatewaySenderFactoryImpl(InternalCache cache, StatisticsClock statisticsClock) {
    this.cache = cache;
    this.statisticsClock = statisticsClock;
  }

  @Override
  public GatewaySenderFactory setParallel(boolean isParallel) {
    this.attrs.isParallel = isParallel;
    return this;
  }

  @Override
  public GatewaySenderFactory setGroupTransactionEvents(boolean groupTransactionEvents) {
    this.attrs.groupTransactionEvents = groupTransactionEvents;
    return this;
  }

  @Override
  public GatewaySenderFactory setForInternalUse(boolean isForInternalUse) {
    this.attrs.isForInternalUse = isForInternalUse;
    return this;
  }

  @Override
  public GatewaySenderFactory addGatewayEventFilter(GatewayEventFilter filter) {
    this.attrs.addGatewayEventFilter(filter);
    return this;
  }

  @Override
  public GatewaySenderFactory addGatewayTransportFilter(GatewayTransportFilter filter) {
    this.attrs.addGatewayTransportFilter(filter);
    return this;
  }

  @Override
  public GatewaySenderFactory addAsyncEventListener(AsyncEventListener listener) {
    this.attrs.addAsyncEventListener(listener);
    return this;
  }

  @Override
  public GatewaySenderFactory setSocketBufferSize(int socketBufferSize) {
    this.attrs.socketBufferSize = socketBufferSize;
    return this;
  }

  @Override
  public GatewaySenderFactory setSocketReadTimeout(int socketReadTimeout) {
    this.attrs.socketReadTimeout = socketReadTimeout;
    return this;
  }

  @Override
  public GatewaySenderFactory setDiskStoreName(String diskStoreName) {
    this.attrs.diskStoreName = diskStoreName;
    return this;
  }

  @Override
  public GatewaySenderFactory setMaximumQueueMemory(int maximumQueueMemory) {
    this.attrs.maximumQueueMemory = maximumQueueMemory;
    return this;
  }

  @Override
  public GatewaySenderFactory setBatchSize(int batchSize) {
    this.attrs.batchSize = batchSize;
    return this;
  }

  @Override
  public GatewaySenderFactory setBatchTimeInterval(int batchTimeInterval) {
    this.attrs.batchTimeInterval = batchTimeInterval;
    return this;
  }

  @Override
  public GatewaySenderFactory setBatchConflationEnabled(boolean enableBatchConflation) {
    this.attrs.isBatchConflationEnabled = enableBatchConflation;
    return this;
  }

  @Override
  public GatewaySenderFactory setPersistenceEnabled(boolean enablePersistence) {
    this.attrs.isPersistenceEnabled = enablePersistence;
    return this;
  }

  @Override
  public GatewaySenderFactory setAlertThreshold(int threshold) {
    this.attrs.alertThreshold = threshold;
    return this;
  }

  @Override
  public GatewaySenderFactory setManualStart(boolean start) {
    this.attrs.manualStart = start;
    return this;
  }

  @Override
  public GatewaySenderFactory setLocatorDiscoveryCallback(LocatorDiscoveryCallback locCallback) {
    this.attrs.locatorDiscoveryCallback = locCallback;
    return this;
  }

  @Override
  public GatewaySenderFactory setDiskSynchronous(boolean isSynchronous) {
    this.attrs.isDiskSynchronous = isSynchronous;
    return this;
  }

  @Override
  public GatewaySenderFactory setDispatcherThreads(int numThreads) {
    if ((numThreads > 1) && this.attrs.policy == null) {
      this.attrs.policy = GatewaySender.DEFAULT_ORDER_POLICY;
    }
    this.attrs.dispatcherThreads = numThreads;
    return this;
  }

  @Override
  public GatewaySenderFactory setParallelFactorForReplicatedRegion(int parallel) {
    this.attrs.parallelism = parallel;
    this.attrs.policy = GatewaySender.DEFAULT_ORDER_POLICY;
    return this;
  }

  @Override
  public GatewaySenderFactory setOrderPolicy(OrderPolicy policy) {
    this.attrs.policy = policy;
    return this;
  }

  @Override
  public GatewaySenderFactory setBucketSorted(boolean isBucketSorted) {
    this.attrs.isBucketSorted = isBucketSorted;
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
    this.attrs.id = id;
    this.attrs.remoteDs = remoteDSId;
    GatewaySender sender = null;

    if (this.attrs.getDispatcherThreads() <= 0) {
      throw new GatewaySenderException(
          String.format("GatewaySender %s can not be created with dispatcher threads less than 1",
              id));
    }

    // Verify socket read timeout if a proper logger is available
    if (this.cache instanceof GemFireCacheImpl) {
      // If socket read timeout is less than the minimum, log a warning.
      // Ideally, this should throw a GatewaySenderException, but wan dunit tests
      // were failing, and we were running out of time to change them.
      if (this.attrs.getSocketReadTimeout() != 0
          && this.attrs.getSocketReadTimeout() < GatewaySender.MINIMUM_SOCKET_READ_TIMEOUT) {
        logger.warn(
            "{} cannot configure socket read timeout of {} milliseconds because it is less than the minimum of {} milliseconds. The default will be used instead.",
            new Object[] {"GatewaySender " + id, this.attrs.getSocketReadTimeout(),
                GatewaySender.MINIMUM_SOCKET_READ_TIMEOUT});
        this.attrs.socketReadTimeout = GatewaySender.MINIMUM_SOCKET_READ_TIMEOUT;
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

    if (this.attrs.isParallel()) {
      if ((this.attrs.getOrderPolicy() != null)
          && this.attrs.getOrderPolicy().equals(OrderPolicy.THREAD)) {
        throw new GatewaySenderException(
            String.format("Parallel Gateway Sender %s can not be created with OrderPolicy %s",
                id, this.attrs.getOrderPolicy()));
      }
      if (this.cache instanceof GemFireCacheImpl) {
        sender = new ParallelGatewaySenderImpl(cache, statisticsClock, attrs);
        this.cache.addGatewaySender(sender);

        if (!this.attrs.isManualStart()) {
          sender.start();
        }
      } else if (this.cache instanceof CacheCreation) {
        sender = new ParallelGatewaySenderCreation(this.cache, this.attrs);
        this.cache.addGatewaySender(sender);
      }
    } else {
      if (this.attrs.getAsyncEventListeners().size() > 0) {
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
      if (this.attrs.getOrderPolicy() == null && this.attrs.getDispatcherThreads() > 1) {
        this.attrs.policy = GatewaySender.DEFAULT_ORDER_POLICY;
      }
      if (this.cache instanceof GemFireCacheImpl) {
        sender = new SerialGatewaySenderImpl(cache, statisticsClock, attrs);
        this.cache.addGatewaySender(sender);
        if (!this.attrs.isManualStart()) {
          sender.start();
        }
      } else if (this.cache instanceof CacheCreation) {
        sender = new SerialGatewaySenderCreation(this.cache, this.attrs);
        this.cache.addGatewaySender(sender);
      }
    }
    return sender;
  }

  @Override
  public GatewaySender create(String id) {
    this.attrs.id = id;
    GatewaySender sender = null;

    if (this.attrs.getDispatcherThreads() <= 0) {
      throw new AsyncEventQueueConfigurationException(
          String.format("AsyncEventQueue %s can not be created with dispatcher threads less than 1",
              id));
    }

    if (this.attrs.isParallel()) {
      if ((this.attrs.getOrderPolicy() != null)
          && this.attrs.getOrderPolicy().equals(OrderPolicy.THREAD)) {
        throw new AsyncEventQueueConfigurationException(
            String.format(
                "AsyncEventQueue %s can not be created with OrderPolicy %s when it is set parallel",
                id, this.attrs.getOrderPolicy()));
      }

      if (this.cache instanceof GemFireCacheImpl) {
        sender = new ParallelGatewaySenderImpl(cache, statisticsClock, attrs);
        this.cache.addGatewaySender(sender);
        if (!this.attrs.isManualStart()) {
          sender.start();
        }
      } else if (this.cache instanceof CacheCreation) {
        sender = new ParallelGatewaySenderCreation(this.cache, this.attrs);
        this.cache.addGatewaySender(sender);
      }
    } else {
      if (this.attrs.getOrderPolicy() == null && this.attrs.getDispatcherThreads() > 1) {
        this.attrs.policy = GatewaySender.DEFAULT_ORDER_POLICY;
      }
      if (this.cache instanceof GemFireCacheImpl) {
        sender = new SerialGatewaySenderImpl(cache, statisticsClock, attrs);
        this.cache.addGatewaySender(sender);
        if (!this.attrs.isManualStart()) {
          sender.start();
        }
      } else if (this.cache instanceof CacheCreation) {
        sender = new SerialGatewaySenderCreation(this.cache, this.attrs);
        this.cache.addGatewaySender(sender);
      }
    }
    return sender;
  }

  @Override
  public GatewaySenderFactory removeGatewayEventFilter(GatewayEventFilter filter) {
    this.attrs.eventFilters.remove(filter);
    return this;
  }

  @Override
  public GatewaySenderFactory removeGatewayTransportFilter(GatewayTransportFilter filter) {
    this.attrs.transFilters.remove(filter);
    return this;
  }

  @Override
  public GatewaySenderFactory setGatewayEventSubstitutionFilter(
      GatewayEventSubstitutionFilter filter) {
    this.attrs.eventSubstitutionFilter = filter;
    return this;
  }

  @Override
  public void configureGatewaySender(GatewaySender senderCreation) {
    this.attrs.isParallel = senderCreation.isParallel();
    this.attrs.manualStart = senderCreation.isManualStart();
    this.attrs.socketBufferSize = senderCreation.getSocketBufferSize();
    this.attrs.socketReadTimeout = senderCreation.getSocketReadTimeout();
    this.attrs.isBatchConflationEnabled = senderCreation.isBatchConflationEnabled();
    this.attrs.batchSize = senderCreation.getBatchSize();
    this.attrs.batchTimeInterval = senderCreation.getBatchTimeInterval();
    this.attrs.isPersistenceEnabled = senderCreation.isPersistenceEnabled();
    this.attrs.diskStoreName = senderCreation.getDiskStoreName();
    this.attrs.isDiskSynchronous = senderCreation.isDiskSynchronous();
    this.attrs.maximumQueueMemory = senderCreation.getMaximumQueueMemory();
    this.attrs.alertThreshold = senderCreation.getAlertThreshold();
    this.attrs.dispatcherThreads = senderCreation.getDispatcherThreads();
    this.attrs.policy = senderCreation.getOrderPolicy();
    for (GatewayEventFilter filter : senderCreation.getGatewayEventFilters()) {
      this.attrs.eventFilters.add(filter);
    }
    for (GatewayTransportFilter filter : senderCreation.getGatewayTransportFilters()) {
      this.attrs.transFilters.add(filter);
    }
    this.attrs.eventSubstitutionFilter = senderCreation.getGatewayEventSubstitutionFilter();
    this.attrs.groupTransactionEvents = senderCreation.mustGroupTransactionEvents();
    this.attrs.enforceThreadsConnectSameReceiver =
        senderCreation.getEnforceThreadsConnectSameReceiver();
  }
}

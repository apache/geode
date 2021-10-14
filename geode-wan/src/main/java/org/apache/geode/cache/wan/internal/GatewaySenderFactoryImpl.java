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

import static java.lang.String.format;
import static java.util.ServiceLoader.load;
import static java.util.stream.StreamSupport.stream;

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
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.wan.GatewaySenderAttributes;
import org.apache.geode.internal.cache.wan.GatewaySenderAttributesImpl;
import org.apache.geode.internal.cache.wan.GatewaySenderException;
import org.apache.geode.internal.cache.wan.InternalGatewaySenderFactory;
import org.apache.geode.internal.cache.xmlcache.CacheCreation;
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
    attrs.setId(id);
    attrs.setRemoteDs(remoteDSId);

    validate(cache, attrs);

    final GatewaySenderTypeFactory factory = getGatewaySenderTypeFactory(attrs);
    factory.validate(attrs);

    return createGatewaySender(factory, cache, statisticsClock, attrs);
  }

  static @NotNull GatewaySenderTypeFactory getGatewaySenderTypeFactory(
      final @NotNull GatewaySenderAttributes attributes) {
    if (attributes.isParallel()) {
      if (attributes.mustGroupTransactionEvents()) {
        return findGatewaySenderTypeFactory("TxGroupingParallelGatewaySender");
      } else {
        return findGatewaySenderTypeFactory("ParallelGatewaySender");
      }
    } else {
      if (attributes.mustGroupTransactionEvents()) {
        return findGatewaySenderTypeFactory("TxGroupingSerialGatewaySender");
      } else {
        return findGatewaySenderTypeFactory("SerialGatewaySender");
      }
    }
  }

  private static @NotNull GatewaySenderTypeFactory findGatewaySenderTypeFactory(
      final @NotNull String name) {
    return stream(load(GatewaySenderTypeFactory.class).spliterator(), false)
        .filter(factory -> factory.getType().equals(name)
            || factory.getClass().getName().startsWith(name))
        .findFirst()
        .orElseThrow(() -> new GatewaySenderException("No factory found for " + name));
  }

  static void validate(final @NotNull InternalCache cache,
      final @NotNull GatewaySenderAttributesImpl attributes) {
    final int myDSId = cache.getDistributionManager().getDistributedSystemId();
    final int remoteDSId = attributes.getRemoteDSId();

    if (remoteDSId == myDSId) {
      throw new GatewaySenderException(
          format(
              "GatewaySender %s cannot be created with remote DS Id equal to this DS Id. ",
              attributes.getId()));
    }
    if (remoteDSId < 0) {
      throw new GatewaySenderException(
          format("GatewaySender %s cannot be created with remote DS Id less than 0. ",
              attributes.getId()));
    }

    if (attributes.getDispatcherThreads() <= 0) {
      throw new GatewaySenderException(
          format("GatewaySender %s can not be created with dispatcher threads less than 1",
              attributes.getId()));
    }

    // TODO jbarrett why only check these for a real cache.
    // Verify socket read timeout if a proper logger is available
    if (cache instanceof GemFireCacheImpl) {
      // If socket read timeout is less than the minimum, log a warning.
      // Ideally, this should throw a GatewaySenderException, but wan dunit tests
      // were failing, and we were running out of time to change them.
      if (attributes.getSocketReadTimeout() != 0
          && attributes.getSocketReadTimeout() < GatewaySender.MINIMUM_SOCKET_READ_TIMEOUT) {
        logger.warn(
            "{} cannot configure socket read timeout of {} milliseconds because it is less than the minimum of {} milliseconds. The default will be used instead.",
            "GatewaySender " + attributes.getId(), attributes.getSocketReadTimeout(),
            GatewaySender.MINIMUM_SOCKET_READ_TIMEOUT);
        attributes.setSocketReadTimeout(GatewaySender.MINIMUM_SOCKET_READ_TIMEOUT);
      }

      // Log a warning if the old system property is set.
      if (GATEWAY_CONNECTION_READ_TIMEOUT_PROPERTY_CHECKED.compareAndSet(false, true)) {
        if (System.getProperty(GatewaySender.GATEWAY_CONNECTION_READ_TIMEOUT_PROPERTY) != null) {
          logger.warn(
              "Obsolete java system property named {} was set to control {}. This property is no longer supported. Please use the GemFire API instead.",
              GatewaySender.GATEWAY_CONNECTION_READ_TIMEOUT_PROPERTY,
              "GatewaySender socket read timeout");
        }
      }
    }
  }

  @NotNull
  private static GatewaySender createGatewaySender(final @NotNull GatewaySenderTypeFactory factory,
      final @NotNull InternalCache cache,
      final @NotNull StatisticsClock clock,
      final @NotNull GatewaySenderAttributesImpl attributes) {
    final GatewaySender sender;
    if (cache instanceof GemFireCacheImpl) {
      sender = factory.create(cache, clock, attributes);
      cache.addGatewaySender(sender);
      if (!attributes.isManualStart()) {
        sender.start();
      }
    } else if (cache instanceof CacheCreation) {
      sender = factory.createCreation(cache, attributes);
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

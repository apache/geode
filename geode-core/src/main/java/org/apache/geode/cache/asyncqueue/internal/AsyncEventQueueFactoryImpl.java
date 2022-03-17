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
package org.apache.geode.cache.asyncqueue.internal;

import static org.apache.geode.cache.asyncqueue.internal.AsyncEventQueueImpl.getSenderIdFromAsyncEventQueueId;

import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.asyncqueue.AsyncEventListener;
import org.apache.geode.cache.asyncqueue.AsyncEventQueue;
import org.apache.geode.cache.asyncqueue.AsyncEventQueueFactory;
import org.apache.geode.cache.wan.GatewayEventFilter;
import org.apache.geode.cache.wan.GatewayEventSubstitutionFilter;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.cache.wan.GatewaySender.OrderPolicy;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.wan.AsyncEventQueueConfigurationException;
import org.apache.geode.internal.cache.wan.GatewaySenderAttributesImpl;
import org.apache.geode.internal.cache.wan.InternalGatewaySender;
import org.apache.geode.internal.cache.xmlcache.AsyncEventQueueCreation;
import org.apache.geode.internal.cache.xmlcache.CacheCreation;
import org.apache.geode.internal.cache.xmlcache.ParallelAsyncEventQueueCreation;
import org.apache.geode.internal.cache.xmlcache.SerialAsyncEventQueueCreation;
import org.apache.geode.logging.internal.log4j.api.LogService;

public class AsyncEventQueueFactoryImpl implements AsyncEventQueueFactory {

  private static final Logger logger = LogService.getLogger();

  /**
   * The default batchTimeInterval for AsyncEventQueue in milliseconds.
   */
  public static final int DEFAULT_BATCH_TIME_INTERVAL = 5;

  private final InternalCache cache;

  private boolean pauseEventsDispatching = false;

  /**
   * Used internally to pass the attributes from this factory to the real GatewaySender it is
   * creating.
   */
  private final GatewaySenderAttributesImpl gatewaySenderAttributes;

  public AsyncEventQueueFactoryImpl(InternalCache cache) {
    this(cache, new GatewaySenderAttributesImpl(), DEFAULT_BATCH_TIME_INTERVAL);
  }

  AsyncEventQueueFactoryImpl(InternalCache cache,
      GatewaySenderAttributesImpl gatewaySenderAttributes,
      int batchTimeInterval) {
    this.cache = cache;
    this.gatewaySenderAttributes = gatewaySenderAttributes;
    // set a different default for batchTimeInterval for AsyncEventQueue
    this.gatewaySenderAttributes.setBatchTimeInterval(batchTimeInterval);
  }

  @Override
  public AsyncEventQueueFactory setBatchSize(int size) {
    gatewaySenderAttributes.setBatchSize(size);
    return this;
  }

  @Override
  public AsyncEventQueueFactory setPersistent(boolean isPersistent) {
    gatewaySenderAttributes.setPersistenceEnabled(isPersistent);
    return this;
  }

  @Override
  public AsyncEventQueueFactory setDiskStoreName(String name) {
    gatewaySenderAttributes.setDiskStoreName(name);
    return this;
  }

  @Override
  public AsyncEventQueueFactory setMaximumQueueMemory(int memory) {
    gatewaySenderAttributes.setMaximumQueueMemory(memory);
    return this;
  }

  @Override
  public AsyncEventQueueFactory setDiskSynchronous(boolean isSynchronous) {
    gatewaySenderAttributes.setDiskSynchronous(isSynchronous);
    return this;
  }

  @Override
  public AsyncEventQueueFactory setBatchTimeInterval(int batchTimeInterval) {
    gatewaySenderAttributes.setBatchTimeInterval(batchTimeInterval);
    return this;
  }

  @Override
  public AsyncEventQueueFactory setBatchConflationEnabled(boolean isConflation) {
    gatewaySenderAttributes.setBatchConflationEnabled(isConflation);
    return this;
  }

  @Override
  public AsyncEventQueueFactory setDispatcherThreads(int numThreads) {
    gatewaySenderAttributes.setDispatcherThreads(numThreads);
    return this;
  }

  @Override
  public AsyncEventQueueFactory setOrderPolicy(OrderPolicy policy) {
    gatewaySenderAttributes.setOrderPolicy(policy);
    return this;
  }

  @Override
  public AsyncEventQueueFactory addGatewayEventFilter(GatewayEventFilter filter) {
    gatewaySenderAttributes.addGatewayEventFilter(filter);
    return this;
  }

  @Override
  public AsyncEventQueueFactory removeGatewayEventFilter(GatewayEventFilter filter) {
    gatewaySenderAttributes.getGatewayEventFilters().remove(filter);
    return this;
  }

  @Override
  public AsyncEventQueueFactory setGatewayEventSubstitutionListener(
      GatewayEventSubstitutionFilter filter) {
    gatewaySenderAttributes.setEventSubstitutionFilter(filter);
    return this;
  }

  public AsyncEventQueueFactory removeGatewayEventAlternateValueProvider(
      GatewayEventSubstitutionFilter provider) {
    return this;
  }

  public AsyncEventQueueFactory addAsyncEventListener(AsyncEventListener listener) {
    gatewaySenderAttributes.addAsyncEventListener(listener);
    return this;
  }

  @Override
  public AsyncEventQueue create(String asyncQueueId, AsyncEventListener listener) {
    if (listener == null) {
      throw new IllegalArgumentException(
          "AsyncEventListener cannot be null");
    }

    AsyncEventQueue asyncEventQueue;

    if (cache instanceof CacheCreation) {
      asyncEventQueue =
          new AsyncEventQueueCreation(asyncQueueId, gatewaySenderAttributes, listener);
      if (pauseEventsDispatching) {
        ((AsyncEventQueueCreation) asyncEventQueue).setPauseEventDispatching(true);
      }
      ((CacheCreation) cache).addAsyncEventQueue(asyncEventQueue);
    } else {
      if (logger.isDebugEnabled()) {
        logger.debug("Creating GatewaySender that underlies the AsyncEventQueue");
      }

      addAsyncEventListener(listener);
      InternalGatewaySender sender =
          (InternalGatewaySender) create(getSenderIdFromAsyncEventQueueId(asyncQueueId));
      AsyncEventQueueImpl asyncEventQueueImpl = new AsyncEventQueueImpl(sender, listener);
      asyncEventQueue = asyncEventQueueImpl;
      cache.addAsyncEventQueue(asyncEventQueueImpl);
      if (pauseEventsDispatching) {
        sender.setStartEventProcessorInPausedState();
      }
      if (!gatewaySenderAttributes.isManualStart()) {
        sender.start();
      }
    }

    if (logger.isDebugEnabled()) {
      logger.debug("Returning AsyncEventQueue" + asyncEventQueue);
    }
    return asyncEventQueue;
  }

  private GatewaySender create(String id) {
    gatewaySenderAttributes.setId(id);

    if (gatewaySenderAttributes.getDispatcherThreads() <= 0) {
      throw new AsyncEventQueueConfigurationException(
          String.format("AsyncEventQueue %s can not be created with dispatcher threads less than 1",
              id));
    }

    GatewaySender sender;
    if (gatewaySenderAttributes.isParallel()) {
      if (gatewaySenderAttributes.getOrderPolicy() != null
          && gatewaySenderAttributes.getOrderPolicy().equals(OrderPolicy.THREAD)) {
        throw new AsyncEventQueueConfigurationException(
            String.format(
                "AsyncEventQueue %s can not be created with OrderPolicy %s when it is set parallel",
                id, gatewaySenderAttributes.getOrderPolicy()));
      }

      if (cache instanceof CacheCreation) {
        sender = new ParallelAsyncEventQueueCreation(cache, gatewaySenderAttributes);
      } else {
        sender = new ParallelAsyncEventQueueImpl(cache,
            cache.getInternalDistributedSystem().getStatisticsManager(), cache.getStatisticsClock(),
            gatewaySenderAttributes);
      }
      cache.addGatewaySender(sender);

    } else {
      if (gatewaySenderAttributes.getOrderPolicy() == null
          && gatewaySenderAttributes.getDispatcherThreads() > 1) {
        gatewaySenderAttributes.setOrderPolicy(GatewaySender.DEFAULT_ORDER_POLICY);
      }

      if (cache instanceof CacheCreation) {
        sender = new SerialAsyncEventQueueCreation(cache, gatewaySenderAttributes);
      } else {
        sender = new SerialAsyncEventQueueImpl(cache,
            cache.getInternalDistributedSystem().getStatisticsManager(), cache.getStatisticsClock(),
            gatewaySenderAttributes);
      }
      cache.addGatewaySender(sender);
    }
    return sender;
  }

  public void configureAsyncEventQueue(AsyncEventQueue asyncQueueCreation) {
    gatewaySenderAttributes.setBatchSize(asyncQueueCreation.getBatchSize());
    gatewaySenderAttributes.setBatchTimeInterval(asyncQueueCreation.getBatchTimeInterval());
    gatewaySenderAttributes
        .setBatchConflationEnabled(asyncQueueCreation.isBatchConflationEnabled());
    gatewaySenderAttributes.setPersistenceEnabled(asyncQueueCreation.isPersistent());
    gatewaySenderAttributes.setDiskStoreName(asyncQueueCreation.getDiskStoreName());
    gatewaySenderAttributes.setDiskSynchronous(asyncQueueCreation.isDiskSynchronous());
    gatewaySenderAttributes.setMaximumQueueMemory(asyncQueueCreation.getMaximumQueueMemory());
    gatewaySenderAttributes.setParallel(asyncQueueCreation.isParallel());
    gatewaySenderAttributes
        .setBucketSorted(((AsyncEventQueueCreation) asyncQueueCreation).isBucketSorted());
    gatewaySenderAttributes.setDispatcherThreads(asyncQueueCreation.getDispatcherThreads());
    gatewaySenderAttributes.setOrderPolicy(asyncQueueCreation.getOrderPolicy());
    for (GatewayEventFilter filter : asyncQueueCreation.getGatewayEventFilters()) {
      gatewaySenderAttributes.getGatewayEventFilters().add(filter);
    }
    gatewaySenderAttributes
        .setEventSubstitutionFilter(asyncQueueCreation.getGatewayEventSubstitutionFilter());
    gatewaySenderAttributes.setForInternalUse(true);
    gatewaySenderAttributes
        .setForwardExpirationDestroy(asyncQueueCreation.isForwardExpirationDestroy());
  }

  @Override
  public AsyncEventQueueFactory setParallel(boolean isParallel) {
    gatewaySenderAttributes.setParallel(isParallel);
    return this;
  }

  public AsyncEventQueueFactory setBucketSorted(boolean isbucketSorted) {
    gatewaySenderAttributes.setBucketSorted(isbucketSorted);
    return this;
  }

  public AsyncEventQueueFactory setIsMetaQueue(boolean isMetaQueue) {
    gatewaySenderAttributes.setMetaQueue(isMetaQueue);
    return this;
  }

  @Override
  public AsyncEventQueueFactory setForwardExpirationDestroy(boolean forward) {
    gatewaySenderAttributes.setForwardExpirationDestroy(forward);
    return this;
  }

  @Override
  public AsyncEventQueueFactory pauseEventDispatching() {
    pauseEventsDispatching = true;
    return this;
  }

  @VisibleForTesting
  protected boolean isPauseEventsDispatching() {
    return pauseEventsDispatching;
  }
}

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

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.asyncqueue.AsyncEventListener;
import org.apache.geode.cache.asyncqueue.AsyncEventQueue;
import org.apache.geode.cache.asyncqueue.AsyncEventQueueFactory;
import org.apache.geode.cache.wan.GatewayEventFilter;
import org.apache.geode.cache.wan.GatewayEventSubstitutionFilter;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.cache.wan.GatewaySender.OrderPolicy;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.wan.AsyncEventQueueConfigurationException;
import org.apache.geode.internal.cache.wan.GatewaySenderAttributes;
import org.apache.geode.internal.cache.xmlcache.AsyncEventQueueCreation;
import org.apache.geode.internal.cache.xmlcache.CacheCreation;
import org.apache.geode.internal.cache.xmlcache.ParallelAsyncEventQueueCreation;
import org.apache.geode.internal.cache.xmlcache.SerialAsyncEventQueueCreation;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.logging.LogService;

public class AsyncEventQueueFactoryImpl implements AsyncEventQueueFactory {

  private static final Logger logger = LogService.getLogger();

  /**
   * Used internally to pass the attributes from this factory to the real GatewaySender it is
   * creating.
   */
  private GatewaySenderAttributes attrs = new GatewaySenderAttributes();

  private InternalCache cache;

  /**
   * The default batchTimeInterval for AsyncEventQueue in milliseconds.
   */
  public static final int DEFAULT_BATCH_TIME_INTERVAL = 5;

  public AsyncEventQueueFactoryImpl(InternalCache cache) {
    this.cache = cache;
    this.attrs = new GatewaySenderAttributes();
    // set a different default for batchTimeInterval for AsyncEventQueue
    this.attrs.batchTimeInterval = DEFAULT_BATCH_TIME_INTERVAL;
  }

  @Override
  public AsyncEventQueueFactory setBatchSize(int size) {
    this.attrs.batchSize = size;
    return this;
  }

  public AsyncEventQueueFactory setPersistent(boolean isPersistent) {
    this.attrs.isPersistenceEnabled = isPersistent;
    return this;
  }

  @Override
  public AsyncEventQueueFactory setDiskStoreName(String name) {
    this.attrs.diskStoreName = name;
    return this;
  }

  @Override
  public AsyncEventQueueFactory setMaximumQueueMemory(int memory) {
    this.attrs.maximumQueueMemory = memory;
    return this;
  }

  @Override
  public AsyncEventQueueFactory setDiskSynchronous(boolean isSynchronous) {
    this.attrs.isDiskSynchronous = isSynchronous;
    return this;
  }

  @Override
  public AsyncEventQueueFactory setBatchTimeInterval(int batchTimeInterval) {
    this.attrs.batchTimeInterval = batchTimeInterval;
    return this;
  }

  @Override
  public AsyncEventQueueFactory setBatchConflationEnabled(boolean isConflation) {
    this.attrs.isBatchConflationEnabled = isConflation;
    return this;
  }

  @Override
  public AsyncEventQueueFactory setDispatcherThreads(int numThreads) {
    this.attrs.dispatcherThreads = numThreads;
    return this;
  }

  @Override
  public AsyncEventQueueFactory setOrderPolicy(OrderPolicy policy) {
    this.attrs.policy = policy;
    return this;
  }

  @Override
  public AsyncEventQueueFactory addGatewayEventFilter(GatewayEventFilter filter) {
    this.attrs.addGatewayEventFilter(filter);
    return this;
  }

  @Override
  public AsyncEventQueueFactory removeGatewayEventFilter(GatewayEventFilter filter) {
    this.attrs.eventFilters.remove(filter);
    return this;
  }

  @Override
  public AsyncEventQueueFactory setGatewayEventSubstitutionListener(
      GatewayEventSubstitutionFilter filter) {
    this.attrs.eventSubstitutionFilter = filter;
    return this;
  }

  public AsyncEventQueueFactory removeGatewayEventAlternateValueProvider(
      GatewayEventSubstitutionFilter provider) {
    return this;
  }

  public AsyncEventQueueFactory addAsyncEventListener(AsyncEventListener listener) {
    this.attrs.addAsyncEventListener(listener);
    return this;
  }

  public AsyncEventQueue create(String asyncQueueId, AsyncEventListener listener) {
    if (listener == null) {
      throw new IllegalArgumentException(
          LocalizedStrings.AsyncEventQueue_ASYNC_EVENT_LISTENER_CANNOT_BE_NULL.toLocalizedString());
    }

    AsyncEventQueue asyncEventQueue = null;
    if (this.cache instanceof GemFireCacheImpl) {
      if (logger.isDebugEnabled()) {
        logger.debug("Creating GatewaySender that underlies the AsyncEventQueue");
      }

      addAsyncEventListener(listener);
      GatewaySender sender =
          create(AsyncEventQueueImpl.getSenderIdFromAsyncEventQueueId(asyncQueueId));
      AsyncEventQueueImpl queue = new AsyncEventQueueImpl(sender, listener);
      asyncEventQueue = queue;
      this.cache.addAsyncEventQueue(queue);
      if (!this.attrs.isManualStart()) {
        sender.start();
      }
    } else if (this.cache instanceof CacheCreation) {
      asyncEventQueue = new AsyncEventQueueCreation(asyncQueueId, attrs, listener);
      ((CacheCreation) cache).addAsyncEventQueue(asyncEventQueue);
    }
    if (logger.isDebugEnabled()) {
      logger.debug("Returning AsyncEventQueue" + asyncEventQueue);
    }
    return asyncEventQueue;
  }

  private GatewaySender create(String id) {
    this.attrs.id = id;
    GatewaySender sender = null;

    if (this.attrs.getDispatcherThreads() <= 0) {
      throw new AsyncEventQueueConfigurationException(
          LocalizedStrings.AsyncEventQueue_0_CANNOT_HAVE_DISPATCHER_THREADS_LESS_THAN_1
              .toLocalizedString(id));
    }

    if (this.attrs.isParallel()) {
      if ((this.attrs.getOrderPolicy() != null)
          && this.attrs.getOrderPolicy().equals(OrderPolicy.THREAD)) {
        throw new AsyncEventQueueConfigurationException(
            LocalizedStrings.AsyncEventQueue_0_CANNOT_BE_CREATED_WITH_ORDER_POLICY_1
                .toLocalizedString(id, this.attrs.getOrderPolicy()));
      }

      if (this.cache instanceof GemFireCacheImpl) {
        sender = new ParallelAsyncEventQueueImpl(this.cache, this.attrs);
        this.cache.addGatewaySender(sender);
      } else if (this.cache instanceof CacheCreation) {
        sender = new ParallelAsyncEventQueueCreation(this.cache, this.attrs);
        ((CacheCreation) this.cache).addGatewaySender(sender);
      }
    } else {
      if (this.attrs.getOrderPolicy() == null && this.attrs.getDispatcherThreads() > 1) {
        this.attrs.policy = GatewaySender.DEFAULT_ORDER_POLICY;
      }
      if (this.cache instanceof GemFireCacheImpl) {
        sender = new SerialAsyncEventQueueImpl(this.cache, this.attrs);
        this.cache.addGatewaySender(sender);
      } else if (this.cache instanceof CacheCreation) {
        sender = new SerialAsyncEventQueueCreation(this.cache, this.attrs);
        ((CacheCreation) this.cache).addGatewaySender(sender);
      }
    }
    return sender;
  }

  public void configureAsyncEventQueue(AsyncEventQueue asyncQueueCreation) {
    this.attrs.batchSize = asyncQueueCreation.getBatchSize();
    this.attrs.batchTimeInterval = asyncQueueCreation.getBatchTimeInterval();
    this.attrs.isBatchConflationEnabled = asyncQueueCreation.isBatchConflationEnabled();
    this.attrs.isPersistenceEnabled = asyncQueueCreation.isPersistent();
    this.attrs.diskStoreName = asyncQueueCreation.getDiskStoreName();
    this.attrs.isDiskSynchronous = asyncQueueCreation.isDiskSynchronous();
    this.attrs.maximumQueueMemory = asyncQueueCreation.getMaximumQueueMemory();
    this.attrs.isParallel = asyncQueueCreation.isParallel();
    this.attrs.isBucketSorted = ((AsyncEventQueueCreation) asyncQueueCreation).isBucketSorted();
    this.attrs.dispatcherThreads = asyncQueueCreation.getDispatcherThreads();
    this.attrs.policy = asyncQueueCreation.getOrderPolicy();
    this.attrs.eventFilters = asyncQueueCreation.getGatewayEventFilters();
    this.attrs.eventSubstitutionFilter = asyncQueueCreation.getGatewayEventSubstitutionFilter();
    this.attrs.isForInternalUse = true;
    this.attrs.forwardExpirationDestroy = asyncQueueCreation.isForwardExpirationDestroy();
  }

  public AsyncEventQueueFactory setParallel(boolean isParallel) {
    this.attrs.isParallel = isParallel;
    return this;
  }

  public AsyncEventQueueFactory setBucketSorted(boolean isbucketSorted) {
    this.attrs.isBucketSorted = isbucketSorted;
    return this;
  }

  public AsyncEventQueueFactory setIsMetaQueue(boolean isMetaQueue) {
    this.attrs.isMetaQueue = isMetaQueue;
    return this;
  }

  @Override
  public AsyncEventQueueFactory setForwardExpirationDestroy(boolean forward) {
    this.attrs.forwardExpirationDestroy = forward;
    return this;
  }
}

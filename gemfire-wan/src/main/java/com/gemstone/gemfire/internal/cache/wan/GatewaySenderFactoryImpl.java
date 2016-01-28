/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.internal.cache.wan;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventListener;
import com.gemstone.gemfire.cache.client.internal.LocatorDiscoveryCallback;
import com.gemstone.gemfire.cache.wan.GatewayEventFilter;
import com.gemstone.gemfire.cache.wan.GatewayEventSubstitutionFilter;
import com.gemstone.gemfire.cache.wan.GatewaySender;
import com.gemstone.gemfire.cache.wan.GatewaySender.OrderPolicy;
import com.gemstone.gemfire.cache.wan.GatewaySenderFactory;
import com.gemstone.gemfire.cache.wan.GatewayTransportFilter;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.wan.parallel.ParallelGatewaySenderImpl;
import com.gemstone.gemfire.internal.cache.wan.serial.SerialGatewaySenderImpl;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheCreation;
import com.gemstone.gemfire.internal.cache.xmlcache.ParallelGatewaySenderCreation;
import com.gemstone.gemfire.internal.cache.xmlcache.SerialGatewaySenderCreation;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;

/**
 * @author Suranjan Kumar
 * @author Yogesh Mahajan
 * @author Kishor Bachhav
 * 
 * @since 7.0
 * 
 */
public class GatewaySenderFactoryImpl implements
    InternalGatewaySenderFactory {

  private static final Logger logger = LogService.getLogger();
  
  /**
   * Used internally to pass the attributes from this factory to the real
   * GatewaySender it is creating.
   */
  private GatewaySenderAttributes attrs = new GatewaySenderAttributes();

  private Cache cache;

  private static final AtomicBoolean GATEWAY_CONNECTION_READ_TIMEOUT_PROPERTY_CHECKED = new AtomicBoolean(false);

  public GatewaySenderFactoryImpl(Cache cache) {
    this.cache = cache;
  }

  public GatewaySenderFactory setParallel(boolean isParallel){
    this.attrs.isParallel = isParallel;
    return this;
  }
  
  public GatewaySenderFactory setForInternalUse(boolean isForInternalUse) {
    this.attrs.isForInternalUse = isForInternalUse;
    return this;
  }
  
  public GatewaySenderFactory addGatewayEventFilter(
      GatewayEventFilter filter) {
    this.attrs.addGatewayEventFilter(filter);
    return this;
  }

  public GatewaySenderFactory addGatewayTransportFilter(
      GatewayTransportFilter filter) {
    this.attrs.addGatewayTransportFilter(filter);
    return this;
  }

  public GatewaySenderFactory addAsyncEventListener(
      AsyncEventListener listener) {
    this.attrs.addAsyncEventListener(listener);
    return this;
  }
  
  public GatewaySenderFactory setSocketBufferSize(int socketBufferSize) {
    this.attrs.socketBufferSize = socketBufferSize;
    return this;
  }

  public GatewaySenderFactory setSocketReadTimeout(int socketReadTimeout) {
    this.attrs.socketReadTimeout = socketReadTimeout;
    return this;
  }

  public GatewaySenderFactory setDiskStoreName(String diskStoreName) {
    this.attrs.diskStoreName = diskStoreName;
    return this;
  }

  public GatewaySenderFactory setMaximumQueueMemory(int maximumQueueMemory) {
    this.attrs.maximumQueueMemory = maximumQueueMemory;
    return this;
  }

  public GatewaySenderFactory setBatchSize(int batchSize) {
    this.attrs.batchSize = batchSize;
    return this;
  }

  public GatewaySenderFactory setBatchTimeInterval(int batchTimeInterval) {
    this.attrs.batchTimeInterval = batchTimeInterval;
    return this;
  }

  public GatewaySenderFactory setBatchConflationEnabled(
      boolean enableBatchConflation) {
    this.attrs.isBatchConflationEnabled = enableBatchConflation;
    return this;
  }

  public GatewaySenderFactory setPersistenceEnabled(
      boolean enablePersistence) {
    this.attrs.isPersistenceEnabled = enablePersistence;
    return this;
  }

  public GatewaySenderFactory setAlertThreshold(int threshold) {
    this.attrs.alertThreshold = threshold;
    return this;
  }

  public GatewaySenderFactory setManualStart(boolean start) {
    this.attrs.manualStart = start;
    return this;
  }

  public GatewaySenderFactory setLocatorDiscoveryCallback(
      LocatorDiscoveryCallback locCallback) {
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
  
  public GatewaySenderFactory setBucketSorted(boolean isBucketSorted){
    this.attrs.isBucketSorted = isBucketSorted;
    return this;
  }
  public GatewaySenderFactory setIsHDFSQueue(boolean isHDFSQueue){
    this.attrs.isHDFSQueue = isHDFSQueue;
    return this;
  }
  public GatewaySender create(String id, int remoteDSId) {
    int myDSId = InternalDistributedSystem.getAnyInstance()
        .getDistributionManager().getDistributedSystemId();
    if (remoteDSId == myDSId) {
      throw new GatewaySenderException(
          LocalizedStrings.GatewaySenderImpl_GATEWAY_0_CANNOT_BE_CREATED_WITH_REMOTE_SITE_ID_EQUAL_TO_THIS_SITE_ID
              .toLocalizedString(id));
    }
    if (remoteDSId < 0) {
      throw new GatewaySenderException(
          LocalizedStrings.GatewaySenderImpl_GATEWAY_0_CANNOT_BE_CREATED_WITH_REMOTE_SITE_ID_LESS_THAN_ZERO
              .toLocalizedString(id));
    }
    this.attrs.id = id;
    this.attrs.remoteDs = remoteDSId;
    GatewaySender sender = null;

    if(this.attrs.getDispatcherThreads() <= 0){
      throw new GatewaySenderException(
          LocalizedStrings.GatewaySenderImpl_GATEWAY_SENDER_0_CANNOT_HAVE_DISPATCHER_THREADS_LESS_THAN_1
              .toLocalizedString(id));
    }

    // Verify socket read timeout if a proper logger is available
    if (this.cache instanceof GemFireCacheImpl) {
      // If socket read timeout is less than the minimum, log a warning.
      // Ideally, this should throw a GatewaySenderException, but wan dunit tests
      // were failing, and we were running out of time to change them.
      if (this.attrs.getSocketReadTimeout() != 0
          && this.attrs.getSocketReadTimeout() < GatewaySender.MINIMUM_SOCKET_READ_TIMEOUT) {
        logger.warn(LocalizedMessage.create(LocalizedStrings.Gateway_CONFIGURED_SOCKET_READ_TIMEOUT_TOO_LOW,
            new Object[] { "GatewaySender " + id, this.attrs.getSocketReadTimeout(), GatewaySender.MINIMUM_SOCKET_READ_TIMEOUT }));
        this.attrs.socketReadTimeout = GatewaySender.MINIMUM_SOCKET_READ_TIMEOUT;
      }

      // Log a warning if the old system property is set.
      if (GATEWAY_CONNECTION_READ_TIMEOUT_PROPERTY_CHECKED.compareAndSet(false, true)) {
        if (System.getProperty(GatewaySender.GATEWAY_CONNECTION_READ_TIMEOUT_PROPERTY) != null) {
          logger.warn(LocalizedMessage.create(LocalizedStrings.Gateway_OBSOLETE_SYSTEM_POPERTY,
              new Object[] { GatewaySender.GATEWAY_CONNECTION_READ_TIMEOUT_PROPERTY, "GatewaySender socket read timeout" }));
        }
      }
    }

    if (this.attrs.isParallel()) {
//      if(this.attrs.getDispatcherThreads() != 1){
//        throw new GatewaySenderException(
//            LocalizedStrings.GatewaySenderImpl_PARALLEL_GATEWAY_SENDER_0_CANNOT_BE_CREATED_WITH_DISPATHER_THREADS_OTHER_THAN_1
//                .toLocalizedString(id));
//      }
      if ((this.attrs.getOrderPolicy() != null)
          && this.attrs.getOrderPolicy().equals(OrderPolicy.THREAD)) {
        throw new GatewaySenderException(
            LocalizedStrings.GatewaySenderImpl_PARALLEL_GATEWAY_SENDER_0_CANNOT_BE_CREATED_WITH_ORDER_POLICY_1
                .toLocalizedString(id, this.attrs.getOrderPolicy()));
      }
      if (this.cache instanceof GemFireCacheImpl) {
        sender = new ParallelGatewaySenderImpl(this.cache, this.attrs);
        ((GemFireCacheImpl)this.cache).addGatewaySender(sender);
        
        if (!this.attrs.isManualStart()) {
          sender.start();
        }
      }
      else if (this.cache instanceof CacheCreation) {
        sender = new ParallelGatewaySenderCreation(this.cache, this.attrs);
        ((CacheCreation)this.cache).addGatewaySender(sender);
      }
    }
    else {
      if (this.attrs.getAsyncEventListeners().size() > 0) {
        throw new GatewaySenderException(
            LocalizedStrings.SerialGatewaySenderImpl_GATEWAY_0_CANNOT_DEFINE_A_REMOTE_SITE_BECAUSE_AT_LEAST_ONE_LISTENER_IS_ALREADY_ADDED
                .toLocalizedString(id));
      }
//      if (this.attrs.getOrderPolicy() != null) {
//        if (this.attrs.getDispatcherThreads() == GatewaySender.DEFAULT_DISPATCHER_THREADS) {
//          throw new GatewaySenderException(
//              LocalizedStrings.SerialGatewaySender_INVALID_GATEWAY_SENDER_ORDER_POLICY_CONCURRENCY_0
//                  .toLocalizedString(id));
//        }
//      }
      if (this.attrs.getOrderPolicy() == null && this.attrs.getDispatcherThreads() > 1) {
        this.attrs.policy = GatewaySender.DEFAULT_ORDER_POLICY;
      }
      if (this.cache instanceof GemFireCacheImpl) {
        sender = new SerialGatewaySenderImpl(this.cache, this.attrs);
        ((GemFireCacheImpl)this.cache).addGatewaySender(sender);

        if (!this.attrs.isManualStart()) {
          sender.start();
        }
      }
      else if (this.cache instanceof CacheCreation) {
        sender = new SerialGatewaySenderCreation(this.cache, this.attrs);
        ((CacheCreation)this.cache).addGatewaySender(sender);
      }
    }
    return sender;
  }

  public GatewaySender create(String id) {
    this.attrs.id = id;
    GatewaySender sender = null;

    if(this.attrs.getDispatcherThreads() <= 0) {
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
        sender = new ParallelGatewaySenderImpl(this.cache, this.attrs);
        ((GemFireCacheImpl)this.cache).addGatewaySender(sender);
        if (!this.attrs.isManualStart()) {
          sender.start();
        }
      }
      else if (this.cache instanceof CacheCreation) {
        sender = new ParallelGatewaySenderCreation(this.cache, this.attrs);
        ((CacheCreation)this.cache).addGatewaySender(sender);
      }
    }
    else {
//      if (this.attrs.getOrderPolicy() != null) {
//        if (this.attrs.getDispatcherThreads() == GatewaySender.DEFAULT_DISPATCHER_THREADS) {
//          throw new AsyncEventQueueConfigurationException(
//              LocalizedStrings.AsyncEventQueue_INVALID_ORDER_POLICY_CONCURRENCY_0
//                  .toLocalizedString(id));
//        }
//      }
      if (this.attrs.getOrderPolicy() == null && this.attrs.getDispatcherThreads() > 1) {
         this.attrs.policy = GatewaySender.DEFAULT_ORDER_POLICY;
      }
      if (this.cache instanceof GemFireCacheImpl) {
        sender = new SerialGatewaySenderImpl(this.cache, this.attrs);
        ((GemFireCacheImpl)this.cache).addGatewaySender(sender);
        if (!this.attrs.isManualStart()) {
          sender.start();
        }
      }
      else if (this.cache instanceof CacheCreation) {
        sender = new SerialGatewaySenderCreation(this.cache, this.attrs);
        ((CacheCreation)this.cache).addGatewaySender(sender);
      }
    }
    return sender;
  }
  
  public GatewaySenderFactory removeGatewayEventFilter(
      GatewayEventFilter filter) {
    this.attrs.eventFilters.remove(filter);
    return this;
  }

  public GatewaySenderFactory removeGatewayTransportFilter(
      GatewayTransportFilter filter) {
    this.attrs.transFilters.remove(filter);
    return this;
  } 
  
  public GatewaySenderFactory setGatewayEventSubstitutionFilter(
      GatewayEventSubstitutionFilter filter) {
    this.attrs.eventSubstitutionFilter = filter;
    return this;
  }

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
    for(GatewayEventFilter filter : senderCreation.getGatewayEventFilters()){
      this.attrs.eventFilters.add(filter);
    }
    for(GatewayTransportFilter filter : senderCreation.getGatewayTransportFilters()){
      this.attrs.transFilters.add(filter);
    }
    this.attrs.eventSubstitutionFilter = senderCreation.getGatewayEventSubstitutionFilter();
  }
}

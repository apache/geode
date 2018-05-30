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
package org.apache.geode.management.internal.beans;

import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.asyncqueue.AsyncEventQueue;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache.wan.GatewayReceiver;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.ResourceEvent;
import org.apache.geode.distributed.internal.ResourceEventsListener;
import org.apache.geode.distributed.internal.locks.DLockService;
import org.apache.geode.i18n.LogWriterI18n;
import org.apache.geode.internal.cache.CacheService;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.internal.AlertDetails;

/**
 * This Listener listens on various resource creation in GemFire and create/destroys GemFire
 * specific MBeans accordingly
 */
public class ManagementListener implements ResourceEventsListener {

  /**
   * Adapter to co-ordinate between GemFire and Federation framework
   */
  private ManagementAdapter adapter;

  private LogWriterI18n logger;

  // having a readwrite lock to synchronize between handling cache creation/removal vs handling
  // other notifications
  private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();

  /**
   * Constructor
   */
  public ManagementListener() {
    this.adapter = new ManagementAdapter();
    this.logger = InternalDistributedSystem.getLoggerI18n();
  }

  /**
   * Checks various conditions which might arise due to race condition for lock of
   * GemFireCacheImpl.class which is obtained while GemFireCacheImpl constructor, cache.close(),
   * DistributedSystem.disconnect().
   *
   * As ManagementService creation logic is called in cache.init() method it leaves a small window
   * of loosing the lock of GemFireCacheImpl.class
   *
   * These checks ensures that something unwanted has not happened during that small window
   *
   * @return true or false depending on the status of Cache and System
   */
  private boolean shouldProceed(ResourceEvent event) {
    DistributedSystem system = InternalDistributedSystem.getConnectedInstance();

    // CACHE_REMOVE is a special event . It may happen that a
    // ForcedDisconnectExcpetion will raise this event

    // No need to check system.isConnected as
    // InternalDistributedSystem.getConnectedInstance() does that internally.

    if (system == null && !event.equals(ResourceEvent.CACHE_REMOVE)) {
      return false;
    }

    InternalCache currentCache = GemFireCacheImpl.getInstance();
    if (currentCache == null) {
      return false;
    }
    if (currentCache.isClosed()) {
      return false;
    }
    return true;
  }

  /**
   * Handles various GFE resource life-cycle methods vis-a-vis Management and Monitoring
   *
   * It checks for race conditions cases by calling shouldProceed();
   *
   * @param event Management event for which invocation has happened
   * @param resource the GFE resource type
   */
  public void handleEvent(ResourceEvent event, Object resource) {
    if (!shouldProceed(event)) {
      return;
    }
    try {
      if (event == ResourceEvent.CACHE_CREATE || event == ResourceEvent.CACHE_REMOVE) {
        readWriteLock.writeLock().lock();
      } else {
        readWriteLock.readLock().lock();
      }
      switch (event) {
        case CACHE_CREATE:
          InternalCache createdCache = (InternalCache) resource;
          adapter.handleCacheCreation(createdCache);
          break;
        case CACHE_REMOVE:
          InternalCache removedCache = (InternalCache) resource;
          adapter.handleCacheRemoval(removedCache);
          break;
        case REGION_CREATE:
          Region createdRegion = (Region) resource;
          adapter.handleRegionCreation(createdRegion);
          break;
        case REGION_REMOVE:
          Region removedRegion = (Region) resource;
          adapter.handleRegionRemoval(removedRegion);
          break;
        case DISKSTORE_CREATE:
          DiskStore createdDisk = (DiskStore) resource;
          adapter.handleDiskCreation(createdDisk);
          break;
        case DISKSTORE_REMOVE:
          DiskStore removedDisk = (DiskStore) resource;
          adapter.handleDiskRemoval(removedDisk);
          break;
        case GATEWAYRECEIVER_CREATE:
          GatewayReceiver createdRecv = (GatewayReceiver) resource;
          adapter.handleGatewayReceiverCreate(createdRecv);
          break;
        case GATEWAYRECEIVER_DESTROY:
          GatewayReceiver destroyedRecv = (GatewayReceiver) resource;
          adapter.handleGatewayReceiverDestroy(destroyedRecv);
          break;
        case GATEWAYRECEIVER_START:
          GatewayReceiver startedRecv = (GatewayReceiver) resource;
          adapter.handleGatewayReceiverStart(startedRecv);
          break;
        case GATEWAYRECEIVER_STOP:
          GatewayReceiver stoppededRecv = (GatewayReceiver) resource;
          adapter.handleGatewayReceiverStop(stoppededRecv);
          break;
        case GATEWAYSENDER_CREATE:
          GatewaySender sender = (GatewaySender) resource;
          adapter.handleGatewaySenderCreation(sender);
          break;
        case GATEWAYSENDER_START:
          GatewaySender startedSender = (GatewaySender) resource;
          adapter.handleGatewaySenderStart(startedSender);
          break;
        case GATEWAYSENDER_STOP:
          GatewaySender stoppedSender = (GatewaySender) resource;
          adapter.handleGatewaySenderStop(stoppedSender);
          break;
        case GATEWAYSENDER_PAUSE:
          GatewaySender pausedSender = (GatewaySender) resource;
          adapter.handleGatewaySenderPaused(pausedSender);
          break;
        case GATEWAYSENDER_RESUME:
          GatewaySender resumedSender = (GatewaySender) resource;
          adapter.handleGatewaySenderResumed(resumedSender);
          break;
        case GATEWAYSENDER_REMOVE:
          GatewaySender removedSender = (GatewaySender) resource;
          adapter.handleGatewaySenderRemoved(removedSender);
          break;
        case LOCKSERVICE_CREATE:
          DLockService createdLockService = (DLockService) resource;
          adapter.handleLockServiceCreation(createdLockService);
          break;
        case LOCKSERVICE_REMOVE:
          DLockService removedLockService = (DLockService) resource;
          adapter.handleLockServiceRemoval(removedLockService);
          break;
        case MANAGER_CREATE:
          adapter.handleManagerCreation();
          break;
        case MANAGER_START:
          adapter.handleManagerStart();
          break;
        case MANAGER_STOP:
          adapter.handleManagerStop();
          break;
        case ASYNCEVENTQUEUE_CREATE:
          AsyncEventQueue queue = (AsyncEventQueue) resource;
          adapter.handleAsyncEventQueueCreation(queue);
          break;
        case ASYNCEVENTQUEUE_REMOVE:
          AsyncEventQueue removedQueue = (AsyncEventQueue) resource;
          adapter.handleAsyncEventQueueRemoval(removedQueue);
          break;
        case SYSTEM_ALERT:
          AlertDetails details = (AlertDetails) resource;
          adapter.handleSystemNotification(details);
          break;
        case CACHE_SERVER_START:
          CacheServer startedServer = (CacheServer) resource;
          adapter.handleCacheServerStart(startedServer);
          break;
        case CACHE_SERVER_STOP:
          CacheServer stoppedServer = (CacheServer) resource;
          adapter.handleCacheServerStop(stoppedServer);
          break;
        case LOCATOR_START:
          Locator loc = (Locator) resource;
          adapter.handleLocatorStart(loc);
          break;
        case CACHE_SERVICE_CREATE:
          CacheService service = (CacheService) resource;
          adapter.handleCacheServiceCreation(service);
          break;
        default:
          break;
      }
    } finally {
      if (event == ResourceEvent.CACHE_CREATE || event == ResourceEvent.CACHE_REMOVE) {
        readWriteLock.writeLock().unlock();
      } else {
        readWriteLock.readLock().unlock();
      }
    }
  }

}

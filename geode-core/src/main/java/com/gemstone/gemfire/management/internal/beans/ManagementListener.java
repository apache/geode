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
package com.gemstone.gemfire.management.internal.beans;

import com.gemstone.gemfire.cache.DiskStore;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventQueue;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache.wan.GatewayReceiver;
import com.gemstone.gemfire.cache.wan.GatewaySender;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.Locator;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.ResourceEvent;
import com.gemstone.gemfire.distributed.internal.ResourceEventsListener;
import com.gemstone.gemfire.distributed.internal.locks.DLockService;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.cache.CacheService;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.management.ManagementException;
import com.gemstone.gemfire.management.internal.AlertDetails;

/**
 * This Listener listens on various resource creation in GemFire and
 * create/destroys GemFire specific MBeans accordingly
 * 
 * 
 */
public class ManagementListener implements ResourceEventsListener{

  /**
   * Adapter to co-ordinate between GemFire and Federation framework
   */
  private ManagementAdapter adapter;
  
  private LogWriterI18n logger;

  /**
   * Constructor
   */
  public ManagementListener() {

    this.adapter = new ManagementAdapter();
    this.logger = InternalDistributedSystem.getLoggerI18n();

  }
  
  /**
   * Checks various conditions which might arise due to race condition for lock
   * of GemFireCacheImpl.class which is obtained while GemFireCacheImpl
   * constructor, cache.close(), DistributedSystem.disconnect().
   * 
   * As ManagementService creation logic is called in cache.init() method it
   * leaves a small window of loosing the lock of GemFireCacheImpl.class
   * 
   * These checks ensures that something unwanted has not happened during that
   * small window
   * 
   * @return true or false depending on the status of Cache and System
   */
  private boolean shouldProceed(ResourceEvent event){
    DistributedSystem system =  InternalDistributedSystem.getConnectedInstance();
    
    // CACHE_REMOVE is a special event . It may happen that a
    // ForcedDisconnectExcpetion will raise this event
    
    // No need to check system.isConnected as
    // InternalDistributedSystem.getConnectedInstance() does that internally.
    
    if(system == null && !event.equals(ResourceEvent.CACHE_REMOVE)){
      return false;
    }

    GemFireCacheImpl currentCache = GemFireCacheImpl.getInstance();
    if(currentCache == null){
      return false;
    }
    if(currentCache.isClosed()){
      return false;
    }
    return true;
  }
  


  /**
   * Handles various GFE resource life-cycle methods vis-a-vis Management and
   * Monitoring
   * 
   * It checks for race conditions cases by calling shouldProceed();
   *  
   * 
   * @param event
   *          Management event for which invocation has happened
   * @param resource
   *          the GFE resource type
   */
  public void handleEvent(ResourceEvent event, Object resource) {
      if(!shouldProceed(event)){
       return;
      }
      switch (event) {
      case CACHE_CREATE:
        GemFireCacheImpl createdCache = (GemFireCacheImpl) resource;
        adapter.handleCacheCreation(createdCache);
        break;
      case CACHE_REMOVE:
        GemFireCacheImpl removedCache = (GemFireCacheImpl) resource;
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
        GatewayReceiver createdRecv = (GatewayReceiver)resource;
        adapter.handleGatewayReceiverCreate(createdRecv);
        break;
      case GATEWAYRECEIVER_START:
        GatewayReceiver startedRecv = (GatewayReceiver)resource;
        adapter.handleGatewayReceiverStart(startedRecv);
        break;
      case GATEWAYRECEIVER_STOP:
        GatewayReceiver stoppededRecv = (GatewayReceiver)resource;
        adapter.handleGatewayReceiverStop(stoppededRecv);
        break;
      case GATEWAYSENDER_CREATE:
        GatewaySender sender = (GatewaySender)resource;
        adapter.handleGatewaySenderCreation(sender);
        break;
      case GATEWAYSENDER_START:
        GatewaySender startedSender = (GatewaySender)resource;
        adapter.handleGatewaySenderStart(startedSender);
        break;
      case GATEWAYSENDER_STOP:
        GatewaySender stoppedSender = (GatewaySender)resource;
        adapter.handleGatewaySenderStop(stoppedSender);
        break;
      case GATEWAYSENDER_PAUSE:
        GatewaySender pausedSender = (GatewaySender)resource;
        adapter.handleGatewaySenderPaused(pausedSender);
        break;
      case GATEWAYSENDER_RESUME:
        GatewaySender resumedSender = (GatewaySender)resource;
        adapter.handleGatewaySenderResumed(resumedSender);
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
      case SYSTEM_ALERT:
        AlertDetails details = (AlertDetails)resource;
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
  }

}

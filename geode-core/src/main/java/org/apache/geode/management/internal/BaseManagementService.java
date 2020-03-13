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
package org.apache.geode.management.internal;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.annotations.internal.MakeNotStatic;
import org.apache.geode.distributed.DistributedSystemDisconnectedException;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalCacheForClientAccess;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.ManagementService;

/**
 * Super class to all Management Service
 *
 * @since GemFire 7.0
 */
public abstract class BaseManagementService extends ManagementService {

  private static final Logger logger = LogService.getLogger();

  /**
   * The main mapping between different resources and Service instance Object can be Cache
   */
  @MakeNotStatic
  protected static final Map<Object, BaseManagementService> instances =
      new HashMap<Object, BaseManagementService>();

  /** List of connected <code>DistributedSystem</code>s */
  @MakeNotStatic
  private static final List<InternalDistributedSystem> systems =
      new ArrayList<InternalDistributedSystem>(1);

  /** Protected constructor. */
  protected BaseManagementService() {}

  // Static block to initialize the ConnectListener on the System
  static {
    initInternalDistributedSystem();
  }

  /**
   * This method will close the service. Any operation on the service instance will throw exception
   */
  protected abstract void close();

  /**
   * This method will close the service. Any operation on the service instance will throw exception
   */
  protected abstract boolean isClosed();

  /**
   * Returns a ManagementService to use for the specified Cache.
   *
   * @param cache defines the scope of resources to be managed
   */
  public static ManagementService getManagementService(InternalCacheForClientAccess cache) {
    synchronized (instances) {
      BaseManagementService service = instances.get(cache);
      if (service == null) {
        service = SystemManagementService.newSystemManagementService(cache);
        instances.put(cache, service);

      }
      return service;
    }
  }

  @VisibleForTesting
  public static void setManagementService(InternalCacheForClientAccess cache,
      BaseManagementService service) {
    synchronized (instances) {
      instances.put(cache, service);
    }
  }

  public static ManagementService getExistingManagementService(InternalCache cache) {
    synchronized (instances) {
      BaseManagementService service = instances.get(cache.getCacheForProcessingClientRequests());
      return service;
    }
  }

  /**
   * Initialises the distributed system listener
   */
  private static void initInternalDistributedSystem() {
    synchronized (instances) {
      // Initialize our own list of distributed systems via a connect listener
      @SuppressWarnings("unchecked")
      List<InternalDistributedSystem> existingSystems = InternalDistributedSystem
          .addConnectListener(new InternalDistributedSystem.ConnectListener() {
            @Override
            public void onConnect(InternalDistributedSystem sys) {
              addInternalDistributedSystem(sys);
            }
          });

      // While still holding the lock on systems, add all currently known
      // systems to our own list
      for (InternalDistributedSystem sys : existingSystems) {
        try {
          if (sys.isConnected()) {
            addInternalDistributedSystem(sys);
          }
        } catch (DistributedSystemDisconnectedException e) {
          if (logger.isDebugEnabled()) {
            logger.debug("DistributedSystemDisconnectedException {}", e.getMessage(), e);
          }
        }
      }
    }
  }

  /**
   * Add an Distributed System and adds a Discon Listener
   */
  private static void addInternalDistributedSystem(InternalDistributedSystem sys) {
    synchronized (instances) {
      sys.addDisconnectListener(new InternalDistributedSystem.DisconnectListener() {
        @Override
        public String toString() {
          return "Disconnect listener for BaseManagementService";
        }

        @Override
        public void onDisconnect(InternalDistributedSystem ss) {
          removeInternalDistributedSystem(ss);
        }
      });
      systems.add(sys);
    }
  }

  /**
   * Remove a Distributed System from the system lists. If list is empty it closes down all the
   * services if not closed
   */
  private static void removeInternalDistributedSystem(InternalDistributedSystem sys) {
    synchronized (instances) {
      systems.remove(sys);
      if (systems.isEmpty()) {
        for (Object key : instances.keySet()) {
          BaseManagementService service = (BaseManagementService) instances.get(key);
          try {
            if (!service.isClosed()) {
              // Service close method should take care of the cleaning up
              // activities
              service.close();
            }

          } catch (Exception e) {
            if (logger.isDebugEnabled()) {
              logger.debug("ManagementException while removing InternalDistributedSystem {}",
                  e.getMessage(), e);
            }
          }
        }
        instances.clear();
      }
    }
  }
}

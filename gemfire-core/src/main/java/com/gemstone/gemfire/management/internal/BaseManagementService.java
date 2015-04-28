/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management.internal;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.distributed.DistributedSystemDisconnectedException;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.management.ManagementService;

/**
 * Super class to all Management Service
 * 
 * @author rishim.
 * @since 7.0
 */
public abstract class BaseManagementService extends ManagementService {

  private static final Logger logger = LogService.getLogger();
  
  /**
   * The main mapping between different resources and Service instance Object
   * can be Cache
   */
  protected static final Map<Object, BaseManagementService> instances = new HashMap<Object, BaseManagementService>();

  /** List of connected <code>DistributedSystem</code>s */
  private static final List<InternalDistributedSystem> systems = new ArrayList<InternalDistributedSystem>(
      1);

  /** Protected constructor. */
  protected BaseManagementService() {
  }

  // Static block to initialize the ConnectListener on the System

  static {
    initInternalDistributedSystem();

  }
  
  /**
   * This method will close the service. Any operation on the service instance
   * will throw exception
   */

  protected abstract void close();

  /**
   * This method will close the service. Any operation on the service instance
   * will throw exception
   */

  protected abstract boolean isClosed();

  /**
   * Returns a ManagementService to use for the specified Cache.
   * 
   * @param cache
   *          defines the scope of resources to be managed
   */
  public static ManagementService getManagementService(Cache cache) {
    synchronized (instances) {
      BaseManagementService service = (BaseManagementService) instances.get(cache);
      if (service == null) {
        service = SystemManagementService.newSystemManagementService(cache);
        instances.put(cache, service);

      }
      return service;
    }
  }
  public static ManagementService getExistingManagementService(Cache cache) {
    synchronized (instances) {
      BaseManagementService service = (BaseManagementService) instances.get(cache);
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
   * 
   * @param sys
   */
  private static void addInternalDistributedSystem(InternalDistributedSystem sys) {
    synchronized (instances) {
      sys
          .addDisconnectListener(new InternalDistributedSystem.DisconnectListener() {
            @Override
            public String toString() {
              return "Disconnect listener for BaseManagementService";
            }

            public void onDisconnect(InternalDistributedSystem ss) {
              removeInternalDistributedSystem(ss);
            }
          });
      systems.add(sys);
    }
  }

  /**
   * Remove a Distributed System from the system lists. If list is empty it
   * closes down all the services if not closed
   * 
   * @param sys
   */
  private static void removeInternalDistributedSystem(
      InternalDistributedSystem sys) {
    synchronized (instances) {
      systems.remove(sys);
      if (systems.isEmpty()) {
        for (Object key : instances.keySet()) {
          BaseManagementService service = (BaseManagementService)instances.get(key);
          try {
            if (!service.isClosed()) {
              // Service close method should take care of the cleaning up
              // activities
              service.close();
            }

          } catch (Exception e) {
            if (logger.isDebugEnabled()) {
              logger.debug("ManagementException while removing InternalDistributedSystem {}", e.getMessage(), e);
            }
          }
        }
        instances.clear();
      }

    }
  }
}

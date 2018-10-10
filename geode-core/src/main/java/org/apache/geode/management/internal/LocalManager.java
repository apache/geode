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

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.management.MalformedObjectNameException;
import javax.management.Notification;
import javax.management.ObjectName;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.GemFireException;
import org.apache.geode.SystemFailure;
import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionExistsException;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.TimeoutException;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.CachePerfStats;
import org.apache.geode.internal.cache.HasCachePerfStats;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalRegionArguments;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.LoggingExecutors;
import org.apache.geode.management.ManagementException;

/**
 * DistributionHelper solves the following problems
 *
 * a) Handles proxy creation when Management node comes up b) Handles proxy creation when a member
 * joins c) Remove proxies when a member leaves or node stops being management node. d) Takes care
 * to create resources like hidden regions for MBean and notification federation.
 */
public class LocalManager extends Manager {
  private static final Logger logger = LogService.getLogger();

  /**
   * Management Task pushes data to the admin regions
   */
  private ManagementTask managementTask;

  /**
   * This service will be responsible for executing ManagementTasks and periodically push data to
   * localMonitoringRegion
   */
  protected ScheduledExecutorService singleThreadFederationScheduler;

  /**
   * This map holds all the components which are eligible for federation. Although filters might
   * prevent any of the component from getting federated.
   */
  private Map<ObjectName, FederationComponent> federatedComponentMap;

  private Object lock = new Object();

  private SystemManagementService service;

  /**
   * @param repo management resource repo
   * @param system internal distributed system
   */
  public LocalManager(ManagementResourceRepo repo, InternalDistributedSystem system,
      SystemManagementService service, InternalCache cache) {
    super(repo, system, cache);
    this.service = service;
    this.federatedComponentMap = new ConcurrentHashMap<ObjectName, FederationComponent>();
  }

  /**
   * Managed Node side resources are created
   *
   * Management Region : its a Replicated NO_ACK region Notification Region : its a Replicated Proxy
   * NO_ACK region
   */
  private void startLocalManagement(Map<ObjectName, FederationComponent> federatedComponentMap) {

    synchronized (this) {
      if (repo.getLocalMonitoringRegion() != null) {
        return;
      } else {
        singleThreadFederationScheduler =
            LoggingExecutors.newSingleThreadScheduledExecutor("Management Task");

        if (logger.isDebugEnabled()) {
          logger.debug("Creating  Management Region :");
        }

        /*
         * Sharing the same Internal Argument for both notification region and monitoring region
         */
        InternalRegionArguments internalArgs = new InternalRegionArguments();
        internalArgs.setIsUsedForMetaRegion(true);

        // Create anonymous stats holder for Management Regions
        final HasCachePerfStats monitoringRegionStats = new HasCachePerfStats() {
          public CachePerfStats getCachePerfStats() {
            return new CachePerfStats(cache.getDistributedSystem(), "managementRegionStats");
          }
        };

        internalArgs.setCachePerfStatsHolder(monitoringRegionStats);

        AttributesFactory<String, Object> monitorRegionAttributeFactory =
            new AttributesFactory<String, Object>();
        monitorRegionAttributeFactory.setScope(Scope.DISTRIBUTED_NO_ACK);
        monitorRegionAttributeFactory.setDataPolicy(DataPolicy.REPLICATE);
        monitorRegionAttributeFactory.setConcurrencyChecksEnabled(false);
        MonitoringRegionCacheListener localListener = new MonitoringRegionCacheListener(service);
        monitorRegionAttributeFactory.addCacheListener(localListener);

        RegionAttributes<String, Object> monitoringRegionAttrs =
            monitorRegionAttributeFactory.create();

        AttributesFactory<NotificationKey, Notification> notificationRegionAttributeFactory =
            new AttributesFactory<NotificationKey, Notification>();
        notificationRegionAttributeFactory.setScope(Scope.DISTRIBUTED_NO_ACK);
        notificationRegionAttributeFactory.setDataPolicy(DataPolicy.EMPTY);
        notificationRegionAttributeFactory.setConcurrencyChecksEnabled(false);

        RegionAttributes<NotificationKey, Notification> notifRegionAttrs =
            notificationRegionAttributeFactory.create();

        String appender = MBeanJMXAdapter
            .getUniqueIDForMember(cache.getDistributedSystem().getDistributedMember());

        boolean monitoringRegionCreated = false;
        boolean notifRegionCreated = false;

        try {
          repo.setLocalMonitoringRegion(
              cache.createVMRegion(ManagementConstants.MONITORING_REGION + "_" + appender,
                  monitoringRegionAttrs, internalArgs));
          monitoringRegionCreated = true;

        } catch (TimeoutException e) {
          throw new ManagementException(e);
        } catch (RegionExistsException e) {
          throw new ManagementException(e);
        } catch (IOException e) {
          throw new ManagementException(e);
        } catch (ClassNotFoundException e) {
          throw new ManagementException(e);
        }

        try {
          repo.setLocalNotificationRegion(
              cache.createVMRegion(ManagementConstants.NOTIFICATION_REGION + "_" + appender,
                  notifRegionAttrs, internalArgs));
          notifRegionCreated = true;
        } catch (TimeoutException e) {
          throw new ManagementException(e);
        } catch (RegionExistsException e) {
          throw new ManagementException(e);
        } catch (IOException e) {
          throw new ManagementException(e);
        } catch (ClassNotFoundException e) {
          throw new ManagementException(e);
        } finally {
          if (!notifRegionCreated && monitoringRegionCreated) {
            repo.getLocalMonitoringRegion().localDestroyRegion();

          }
        }

        managementTask = new ManagementTask(federatedComponentMap);
        // call run to get us initialized immediately with a sync call
        managementTask.run();
        // All local resources are created for the ManagementTask
        // Now Management tasks can proceed.
        int updateRate = cache.getInternalDistributedSystem().getConfig().getJmxManagerUpdateRate();
        singleThreadFederationScheduler.scheduleAtFixedRate(managementTask, updateRate, updateRate,
            TimeUnit.MILLISECONDS);

        if (logger.isDebugEnabled()) {
          logger.debug("Management Region created with Name : {}",
              repo.getLocalMonitoringRegion().getName());
          logger.debug("Notification Region created with Name : {}",
              repo.getLocalNotificationRegion().getName());
        }
      }
    }
  }

  public void markForFederation(ObjectName objName, FederationComponent fedComp) {
    federatedComponentMap.put(objName, fedComp);
  }

  public void unMarkForFederation(ObjectName objName) {
    synchronized (lock) {
      if (federatedComponentMap.get(objName) != null) {
        federatedComponentMap.remove(objName);
      }
      if (repo.getLocalMonitoringRegion() != null
          && repo.getLocalMonitoringRegion().get(objName.toString()) != null) {
        // To delete an entry from the region
        repo.getLocalMonitoringRegion().remove(objName.toString());
      }
    }
  }

  /**
   * This method will shutdown various tasks running for management
   */
  private void shutdownTasks() {
    // No need of pooledGIIExecutor as this node wont do GII again
    // so better to release resources
    if (this.singleThreadFederationScheduler != null) {
      this.singleThreadFederationScheduler.shutdownNow();
    }
  }

  /**
   * Clean up management artifacts
   */
  private void cleanUpResources() {
    // InternalDistributedSystem.getConnectedInstance will return an instance if
    // there is at least one connected instance and is not in the process of
    // being disconnected.
    if (!cache.isClosed() && InternalDistributedSystem.getConnectedInstance() != null) {
      if (repo.getLocalMonitoringRegion() != null) {
        // To delete an entry from the region
        for (String name : repo.getLocalMonitoringRegion().keySet()) {
          ObjectName objName = null;
          try {
            objName = ObjectName.getInstance(name);
            unMarkForFederation(objName);
          } catch (MalformedObjectNameException e) {
            if (logger.isDebugEnabled()) {
              logger.debug("Unable to clean MBean: {} due to {}", objName, e.getMessage(), e);
            }
          } catch (NullPointerException e) {
            if (logger.isDebugEnabled()) {
              logger.debug("Unable to clean MBean: {} due to {}", objName, e.getMessage(), e);
            }
          }
        }
        repo.destroyLocalMonitoringRegion();
      }

      if (repo.getLocalNotificationRegion() != null) {
        repo.destroyLocalNotifRegion();
      }
    }
  }

  /**
   * For internal Use only
   */
  public ScheduledExecutorService getFederationSheduler() {
    return singleThreadFederationScheduler;
  }

  /**
   * Internal testing hook.Not to be used from any where else. As soon as a mbean is created we can
   * push the data into local region which will make the data available at managing node site.
   */
  public void runManagementTaskAdhoc() {
    managementTask.run();
  }

  /**
   * This task is responsible for pushing data to the hidden region. It is executed in a single
   * thread from newSingleThreadScheduledExecutor; Only one thread will be responsible
   * for pushing the data to the hidden region.
   *
   * (Note however that if this single thread terminates due to a failure during execution prior to
   * shutdown, a new one will take its place if needed to execute subsequent tasks.) Tasks are
   * guaranteed to execute sequentially, and no more than one task will be active at any given time.
   * Unlike the otherwise equivalent <tt>newScheduledThreadPool(1)</tt> the returned executor is
   * guaranteed not to be reconfigurable to use additional threads.
   */
  private class ManagementTask implements Runnable {

    private Map<String, FederationComponent> replicaMap;

    public ManagementTask(Map<ObjectName, FederationComponent> federatedComponentMap)
        throws ManagementException {
      this.replicaMap = new HashMap<String, FederationComponent>();
    }

    @Override
    public void run() {
      if (logger.isTraceEnabled()) {
        logger.trace("Federation started at managed node : ");
      }

      try {
        synchronized (lock) {
          replicaMap.clear();
          Set<ObjectName> keySet = federatedComponentMap.keySet();
          if (keySet.size() == 0) {
            return;
          }

          Iterator<ObjectName> it = keySet.iterator();

          while (it.hasNext()) {

            ObjectName objectName = it.next();
            FederationComponent fedCompInstance = federatedComponentMap.get(objectName);

            if (Thread.interrupted()) {
              replicaMap.clear();
              return;
            }

            if (fedCompInstance != null) {
              boolean stateChanged = fedCompInstance.refreshObjectState(service.isManager());
              if (!stopCacheOps) {
                String key = objectName.toString();
                if (stateChanged || !repo.keyExistsInLocalMonitoringRegion(key)) {
                  replicaMap.put(key, fedCompInstance);
                }
              }
            }
          }

          if (stopCacheOps) {
            return;
          }
          if (Thread.interrupted()) {
            replicaMap.clear();
            return;
          }
          repo.putAllInLocalMonitoringRegion(replicaMap);
        }
      } catch (CancelException ex) {
        if (logger.isDebugEnabled())
          logger.debug("Management Task Cancelled");
        return;
      } catch (GemFireException ex) {
        if (!cache.isClosed() && logger.isDebugEnabled()) {
          logger.debug(ex.getMessage(), ex);
        }
        return; // Ignore Exception if cache is closing
      } catch (VirtualMachineError e) {
        SystemFailure.initiateFailure(e);
        throw e;
      } catch (Throwable th) {
        SystemFailure.checkFailure();
        throw th;
      }
      if (logger.isTraceEnabled()) {
        logger.trace("Federation completed at managed node : ");
      }
    }
  }

  @Override
  public boolean isRunning() {
    return running;
  }

  @Override
  public void startManager() {
    startLocalManagement(federatedComponentMap);
    running = true;
  }

  @Override
  public void stopManager() {
    // Shutting down the GII executor as this node wont require it anymore
    shutdownTasks();
    // Clean up management Resources
    cleanUpResources();
    running = false;
  }

  public void stopCacheOps() {
    stopCacheOps = true;
  }

  public void startCacheOps() {
    stopCacheOps = false;
  }

  public Map<ObjectName, FederationComponent> getFedComponents() {
    return federatedComponentMap;
  }

}

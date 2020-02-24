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

import java.util.HashMap;
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
import org.apache.geode.StatisticsFactory;
import org.apache.geode.SystemFailure;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.RegionExistsException;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.TimeoutException;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.CachePerfStats;
import org.apache.geode.internal.cache.HasCachePerfStats;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalRegionFactory;
import org.apache.geode.internal.statistics.StatisticsClock;
import org.apache.geode.logging.internal.executors.LoggingExecutors;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.ManagementException;

/**
 * <pre>
 * a) Handles proxy creation when Management node comes up
 * b) Handles proxy creation when a member joins
 * c) Remove proxies when a member leaves or node stops being management node.
 * d) Takes care to create resources like hidden regions for MBean and notification federation.
 * </pre>
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
  private ScheduledExecutorService singleThreadFederationScheduler;

  /**
   * This map holds all the components which are eligible for federation. Although filters might
   * prevent any of the component from getting federated.
   */
  private final Map<ObjectName, FederationComponent> federatedComponentMap;

  private final Object lock = new Object();

  private final SystemManagementService service;

  /**
   * @param repo management resource repo
   * @param system internal distributed system
   */
  LocalManager(ManagementResourceRepo repo, InternalDistributedSystem system,
      SystemManagementService service, InternalCache cache, StatisticsFactory statisticsFactory,
      StatisticsClock statisticsClock) {
    super(repo, system, cache, statisticsFactory, statisticsClock);
    this.service = service;
    federatedComponentMap = new ConcurrentHashMap<>();
  }

  /**
   * Managed Node side resources are created
   *
   * <p>
   * Management Region : its a Replicated NO_ACK region Notification Region : its a Replicated Proxy
   * NO_ACK region
   */
  private void startLocalManagement() {
    synchronized (this) {
      if (repo.getLocalMonitoringRegion() != null) {
        return;
      }
      singleThreadFederationScheduler =
          LoggingExecutors.newSingleThreadScheduledExecutor("Management Task");

      if (logger.isDebugEnabled()) {
        logger.debug("Creating  Management Region :");
      }

      // Create anonymous stats holder for Management Regions
      HasCachePerfStats monitoringRegionStats = new HasCachePerfStats() {

        @Override
        public CachePerfStats getCachePerfStats() {
          return new CachePerfStats(cache.getDistributedSystem(),
              "RegionStats-managementRegionStats", statisticsClock);
        }

        @Override
        public boolean hasOwnStats() {
          return true;
        }
      };

      InternalRegionFactory<String, Object> monitorFactory = cache.createInternalRegionFactory();
      monitorFactory.setScope(Scope.DISTRIBUTED_NO_ACK);
      monitorFactory.setDataPolicy(DataPolicy.REPLICATE);
      monitorFactory.setConcurrencyChecksEnabled(false);
      CacheListener<String, Object> localListener = new MonitoringRegionCacheListener(service);
      monitorFactory.addCacheListener(localListener);
      monitorFactory.setIsUsedForMetaRegion(true);
      monitorFactory.setCachePerfStatsHolder(monitoringRegionStats);

      InternalRegionFactory<NotificationKey, Notification> notificationFactory =
          cache.createInternalRegionFactory();
      notificationFactory.setScope(Scope.DISTRIBUTED_NO_ACK);
      notificationFactory.setDataPolicy(DataPolicy.EMPTY);
      notificationFactory.setConcurrencyChecksEnabled(false);
      notificationFactory.setIsUsedForMetaRegion(true);
      notificationFactory.setCachePerfStatsHolder(monitoringRegionStats);

      String appender = MBeanJMXAdapter.getUniqueIDForMember(system.getDistributedMember());

      try {
        repo.setLocalMonitoringRegion(
            monitorFactory.create(ManagementConstants.MONITORING_REGION + "_" + appender));
      } catch (TimeoutException | RegionExistsException e) {
        throw new ManagementException(e);
      }

      boolean notifRegionCreated = false;
      try {
        repo.setLocalNotificationRegion(
            notificationFactory.create(ManagementConstants.NOTIFICATION_REGION + "_" + appender));
        notifRegionCreated = true;
      } catch (TimeoutException | RegionExistsException e) {
        throw new ManagementException(e);
      } finally {
        if (!notifRegionCreated) {
          repo.getLocalMonitoringRegion().localDestroyRegion();

        }
      }

      managementTask = new ManagementTask();
      // call run to get us initialized immediately with a sync call
      managementTask.run();
      // All local resources are created for the ManagementTask
      // Now Management tasks can proceed.
      int updateRate = system.getConfig().getJmxManagerUpdateRate();
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

  void markForFederation(ObjectName objName, FederationComponent fedComp) {
    federatedComponentMap.put(objName, fedComp);
  }

  void unMarkForFederation(ObjectName objName) {
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
    if (singleThreadFederationScheduler != null) {
      singleThreadFederationScheduler.shutdownNow();
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
          } catch (MalformedObjectNameException | NullPointerException e) {
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
  @VisibleForTesting
  public ScheduledExecutorService getFederationScheduler() {
    return singleThreadFederationScheduler;
  }

  /**
   * Internal testing hook.Not to be used from any where else. As soon as a mbean is created we can
   * push the data into local region which will make the data available at managing node site.
   */
  @VisibleForTesting
  public void runManagementTaskAdhoc() {
    managementTask.run();
  }

  @Override
  public boolean isRunning() {
    return running;
  }

  @Override
  public void startManager() {
    startLocalManagement();
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

  public Map<ObjectName, FederationComponent> getFedComponents() {
    return federatedComponentMap;
  }

  private void doManagementTask(Map<String, FederationComponent> replicaMap) {
    if (logger.isTraceEnabled()) {
      logger.trace("Federation started at managed node : ");
    }

    try {
      synchronized (lock) {
        replicaMap.clear();
        Set<ObjectName> keySet = federatedComponentMap.keySet();
        if (keySet.isEmpty()) {
          return;
        }

        for (ObjectName objectName : keySet) {
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
      if (logger.isDebugEnabled()) {
        logger.debug("Management Task Cancelled");
      }
      return;
    } catch (GemFireException ex) {
      if (!cache.isClosed() && logger.isDebugEnabled()) {
        logger.debug(ex.getMessage(), ex);
      }
      // Ignore Exception if cache is closing
      return;
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

    private final Map<String, FederationComponent> replicaMap;

    private ManagementTask() {
      replicaMap = new HashMap<>();
    }

    @Override
    public void run() {
      doManagementTask(replicaMap);
    }
  }
}

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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicReference;

import javax.management.Notification;
import javax.management.ObjectName;

import org.apache.logging.log4j.Logger;

import org.apache.geode.StatisticsFactory;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.RegionExistsException;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.TimeoutException;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.CachePerfStats;
import org.apache.geode.internal.cache.HasCachePerfStats;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalRegionArguments;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.LoggingExecutors;
import org.apache.geode.internal.statistics.StatisticsClock;
import org.apache.geode.management.ManagementException;

/**
 * Manager implementation which manages federated MBeans for the entire DistributedSystem and
 * controls the JMX server endpoints for JMX clients to connect, such as an RMI server.
 *
 * <p>
 * The FederatingManager is only appropriate for a peer or server in a GemFire distributed system.
 *
 * @since GemFire 7.0
 */
public class FederatingManager extends Manager {
  public static final Logger logger = LogService.getLogger();

  /**
   * This Executor uses a pool of thread to execute the member addition /removal tasks, This will
   * utilize the processing powers available. Going with unbounded queue because tasks wont be
   * unbounded in practical situation as number of members will be a finite set at any given point
   * of time
   */
  private ExecutorService pooledMembershipExecutor;

  /**
   * Proxy factory is used to create , remove proxies
   */
  private MBeanProxyFactory proxyFactory;

  private MemberMessenger messenger;

  private final SystemManagementService service;

  private final AtomicReference<Exception> latestException = new AtomicReference<>(null);

  FederatingManager(MBeanJMXAdapter jmxAdapter, ManagementResourceRepo repo,
      InternalDistributedSystem system, SystemManagementService service, InternalCache cache,
      StatisticsFactory statisticsFactory, StatisticsClock statisticsClock) {
    super(repo, system, cache, statisticsFactory, statisticsClock);
    this.service = service;
    proxyFactory = new MBeanProxyFactory(jmxAdapter, service);
    messenger = new MemberMessenger(jmxAdapter, system);
  }

  @VisibleForTesting
  void setProxyFactory(MBeanProxyFactory newProxyFactory) {
    proxyFactory = newProxyFactory;
  }

  /**
   * This method will be invoked whenever a member wants to be a managing node. The exception
   * Management exception has to be handled by the caller.
   */
  @Override
  public synchronized void startManager() {
    try {
      if (logger.isDebugEnabled()) {
        logger.debug("Starting the Federating Manager.... ");
      }

      pooledMembershipExecutor = LoggingExecutors.newFixedThreadPool("FederatingManager", true,
          Runtime.getRuntime().availableProcessors());

      running = true;
      startManagingActivity();
      messenger.broadcastManagerInfo();
    } catch (Exception e) {
      running = false;
      throw new ManagementException(e);
    }
  }

  @Override
  public synchronized void stopManager() {
    // remove hidden management regions and federatedMBeans
    if (!running) {
      return;
    }
    running = false;
    if (logger.isDebugEnabled()) {
      logger.debug("Stopping the Federating Manager.... ");
    }
    stopManagingActivity();
  }

  /**
   * This method will be invoked whenever a member stops being a managing node. The
   * {@code ManagementException} has to be handled by the caller.
   */
  private void stopManagingActivity() {
    try {
      pooledMembershipExecutor.shutdownNow();

      for (DistributedMember distributedMember : repo.getMonitoringRegionMap().keySet()) {
        removeMemberArtifacts(distributedMember, false);
      }
    } catch (Exception e) {
      throw new ManagementException(e);
    }
  }

  @Override
  public boolean isRunning() {
    return running;
  }

  /**
   * This method will be invoked from MembershipListener which is registered when the member becomes
   * a Management node.
   *
   * <p>
   * This method will delegate task to another thread and exit, so that it wont block the membership
   * listener
   */
  @VisibleForTesting
  public void addMember(InternalDistributedMember member) {
    GIITask giiTask = new GIITask(member);
    executeTask(() -> {
      try {
        giiTask.call();
      } catch (RuntimeException e) {
        logger.warn("Error federating new member {}", member.getId(), e);
        latestException.set(e);
      }
    });
  }

  /**
   * This method will be invoked from MembershipListener which is registered when the member becomes
   * a Management node.
   *
   * <p>
   * This method will delegate task to another thread and exit, so that it wont block the membership
   * listener
   */
  void removeMember(DistributedMember member, boolean crashed) {
    executeTask(new RemoveMemberTask(member, crashed));
  }

  private void executeTask(Runnable task) {
    try {
      pooledMembershipExecutor.execute(task);
    } catch (RejectedExecutionException ignored) {
      // Ignore, we are getting shutdown
    }
  }

  private void removeMemberArtifacts(DistributedMember member, boolean crashed) {
    Region<String, Object> proxyRegion = repo.getEntryFromMonitoringRegionMap(member);
    Region<NotificationKey, Notification> notificationRegion =
        repo.getEntryFromNotifRegionMap(member);

    if (proxyRegion == null && notificationRegion == null) {
      return;
    }

    repo.romoveEntryFromMonitoringRegionMap(member);
    repo.removeEntryFromNotifRegionMap(member);

    // If cache is closed all the regions would have been destroyed implicitly
    if (!cache.isClosed()) {
      proxyFactory.removeAllProxies(member, proxyRegion);
      try {
        proxyRegion.localDestroyRegion();
      } catch (RegionDestroyedException rde) {
        logger.info("Region has been destroyed during closing cache. " + rde.getMessage());
      }
      try {
        notificationRegion.localDestroyRegion();
      } catch (RegionDestroyedException rde) {
        logger.info("Region has been destroyed during closing cache. " + rde.getMessage());
      }
    }

    if (!system.getDistributedMember().equals(member)) {
      service.memberDeparted((InternalDistributedMember) member, crashed);
    }
  }

  /**
   * This method will be invoked from MembershipListener which is registered when the member becomes
   * a Management node.
   *
   * <p>
   * this method will delegate task to another thread and exit, so that it wont block the membership
   * listener
   */
  void suspectMember(DistributedMember member, InternalDistributedMember whoSuspected,
      String reason) {
    service.memberSuspect((InternalDistributedMember) member, whoSuspected, reason);
  }

  /**
   * This method will be invoked when a node transitions from managed node to managing node This
   * method will block for all GIIs to be completed But each GII is given a specific time frame.
   * After that the task will be marked as cancelled.
   */
  private void startManagingActivity() {
    boolean isDebugEnabled = logger.isDebugEnabled();

    Collection<Callable<InternalDistributedMember>> giiTaskList = new ArrayList<>();

    for (InternalDistributedMember member : system.getDistributionManager()
        .getOtherDistributionManagerIds()) {
      giiTaskList.add(new GIITask(member));
    }

    try {
      if (isDebugEnabled) {
        logger.debug("Management Resource creation started  : ");
      }
      List<Future<InternalDistributedMember>> futureTaskList =
          pooledMembershipExecutor.invokeAll(giiTaskList);

      for (Future<InternalDistributedMember> futureTask : futureTaskList) {
        try {
          DistributedMember returnedMember = futureTask.get();
          String memberId = returnedMember != null ? returnedMember.getId() : null;

          if (futureTask.isDone()) {
            if (isDebugEnabled) {
              logger.debug("Monitoring Resource Created for : {}", memberId);
            }

          }
          if (futureTask.isCancelled()) {
            // Retry mechanism can be added here after discussions
            if (isDebugEnabled) {
              logger.debug("Monitoring resource Creation Failed for : {}", memberId);
            }

          }
        } catch (ExecutionException e) {
          if (isDebugEnabled) {
            logger.debug("ExecutionException during Management GII", e);
          }

        } catch (CancellationException e) {
          if (isDebugEnabled) {
            logger.debug("InterruptedException while creating Monitoring resource with error",
                new ManagementException(e));
          }
        }
      }
    } catch (InterruptedException e) {
      if (isDebugEnabled) {
        logger.debug("InterruptedException while creating Monitoring resource with error",
            new ManagementException(e));
      }

    } finally {
      if (isDebugEnabled) {
        logger.debug("Management Resource creation completed");
      }
    }
  }

  /**
   * For internal Use only
   */
  @VisibleForTesting
  public MBeanProxyFactory getProxyFactory() {
    return proxyFactory;
  }

  /**
   * This will return the last updated time of the proxyMBean.
   *
   * @param objectName {@link ObjectName} of the MBean
   *
   * @return last updated time of the proxy
   */
  long getLastUpdateTime(ObjectName objectName) {
    return proxyFactory.getLastUpdateTime(objectName);
  }

  /**
   * Find a particular proxy instance for a {@link ObjectName}, {@link DistributedMember} and
   * interface class If the proxy interface does not implement the given interface class a
   * {@link ClassCastException} will be thrown
   *
   * @param objectName {@link ObjectName} of the MBean
   * @param interfaceClass interface class implemented by proxy
   *
   * @return an instance of proxy exposing the given interface
   */
  <T> T findProxy(ObjectName objectName, Class<T> interfaceClass) {
    return proxyFactory.findProxy(objectName, interfaceClass);
  }

  /**
   * Find a set of proxies given a {@link DistributedMember}.
   *
   * @param member {@link DistributedMember}
   *
   * @return a set of {@link ObjectName}
   */
  Set<ObjectName> findAllProxies(DistributedMember member) {
    return proxyFactory.findAllProxies(member);
  }

  public MemberMessenger getMessenger() {
    return messenger;
  }

  @VisibleForTesting
  public void setMessenger(MemberMessenger messenger) {
    this.messenger = messenger;
  }

  @VisibleForTesting
  public synchronized Exception getAndResetLatestException() {
    return latestException.getAndSet(null);
  }

  @VisibleForTesting
  void addMemberArtifacts(InternalDistributedMember member) {
    synchronized (member) {
      String appender = MBeanJMXAdapter.getUniqueIDForMember(member);
      String monitoringRegionName = ManagementConstants.MONITORING_REGION + "_" + appender;
      String notificationRegionName = ManagementConstants.NOTIFICATION_REGION + "_" + appender;

      if (cache.getInternalRegion(monitoringRegionName) != null
          && cache.getInternalRegion(notificationRegionName) != null) {
        return;
      }

      try {

        // GII wont start at all if its interrupted
        if (!Thread.currentThread().isInterrupted()) {

          // as the regions will be internal regions
          InternalRegionArguments internalRegionArguments = new InternalRegionArguments();
          internalRegionArguments.setIsUsedForMetaRegion(true);

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

          internalRegionArguments.setCachePerfStatsHolder(monitoringRegionStats);

          // Monitoring region for member is created
          AttributesFactory<String, Object> monitorAttributesFactory = new AttributesFactory<>();
          monitorAttributesFactory.setScope(Scope.DISTRIBUTED_NO_ACK);
          monitorAttributesFactory.setDataPolicy(DataPolicy.REPLICATE);
          monitorAttributesFactory.setConcurrencyChecksEnabled(false);
          ManagementCacheListener managementCacheListener =
              new ManagementCacheListener(proxyFactory);
          monitorAttributesFactory.addCacheListener(managementCacheListener);

          RegionAttributes<String, Object> monitoringRegionAttrs =
              monitorAttributesFactory.create();

          // Notification region for member is created
          AttributesFactory<NotificationKey, Notification> notificationAttributesFactory =
              new AttributesFactory<>();
          notificationAttributesFactory.setScope(Scope.DISTRIBUTED_NO_ACK);
          notificationAttributesFactory.setDataPolicy(DataPolicy.REPLICATE);
          notificationAttributesFactory.setConcurrencyChecksEnabled(false);

          // Fix for issue #49638, evict the internal region _notificationRegion
          notificationAttributesFactory
              .setEvictionAttributes(EvictionAttributes.createLRUEntryAttributes(
                  ManagementConstants.NOTIF_REGION_MAX_ENTRIES, EvictionAction.LOCAL_DESTROY));

          NotificationCacheListener notifListener = new NotificationCacheListener(proxyFactory);
          notificationAttributesFactory.addCacheListener(notifListener);

          RegionAttributes<NotificationKey, Notification> notifRegionAttrs =
              notificationAttributesFactory.create();

          Region<String, Object> proxyMonitoringRegion;
          try {
            if (!running) {
              return;
            }
            proxyMonitoringRegion =
                cache.createInternalRegion(monitoringRegionName, monitoringRegionAttrs,
                    internalRegionArguments);

          } catch (TimeoutException | RegionExistsException | IOException
              | ClassNotFoundException e) {
            if (logger.isDebugEnabled()) {
              logger.debug("Error During Internal Region creation", e);
            }
            throw new ManagementException(e);
          }

          boolean proxyNotificationRegionCreated = false;
          Region<NotificationKey, Notification> proxyNotificationRegion;
          try {
            if (!running) {
              return;
            }
            proxyNotificationRegion =
                cache.createInternalRegion(notificationRegionName, notifRegionAttrs,
                    internalRegionArguments);
            proxyNotificationRegionCreated = true;
          } catch (TimeoutException | RegionExistsException | IOException
              | ClassNotFoundException e) {
            if (logger.isDebugEnabled()) {
              logger.debug("Error During Internal Region creation", e);
            }
            throw new ManagementException(e);
          } finally {
            if (!proxyNotificationRegionCreated) {
              // Destroy the proxy region if proxy notification region is not created
              proxyMonitoringRegion.localDestroyRegion();
            }
          }

          if (logger.isDebugEnabled()) {
            logger.debug("Management Region created with Name : {}",
                proxyMonitoringRegion.getName());
            logger.debug("Notification Region created with Name : {}",
                proxyNotificationRegion.getName());
          }

          // Only the exception case would have destroyed the proxy
          // regions. We can safely proceed here.
          repo.putEntryInMonitoringRegionMap(member, proxyMonitoringRegion);
          repo.putEntryInNotifRegionMap(member, proxyNotificationRegion);
          try {
            if (!running) {
              return;
            }
            proxyFactory.createAllProxies(member, proxyMonitoringRegion);

            managementCacheListener.markReady();
            notifListener.markReady();
          } catch (Exception e) {
            if (logger.isDebugEnabled()) {
              logger.debug("Error During GII Proxy creation", e);
            }

            throw new ManagementException(e);
          }
        }

      } catch (Exception e) {
        throw new ManagementException(e);
      }

      // Before completing task intimate all listening ProxyListener which might send
      // notifications.
      service.memberJoined((InternalDistributedMember) member);

      // Send manager info to the added member
      messenger.sendManagerInfo(member);
    }
  }

  /**
   * Actual task of doing the GII
   *
   * <p>
   * It will perform the GII request which might originate from TransitionListener or Membership
   * Listener.
   *
   * <p>
   * Managing Node side resources are created per member which is visible to this node:
   *
   * <pre>
   * 1)Management Region : its a Replicated NO_ACK region
   * 2)Notification Region : its a Replicated Proxy NO_ACK region
   * </pre>
   *
   * <p>
   * Listeners are added to the above two regions:
   *
   * <pre>
   * 1) ManagementCacheListener
   * 2) NotificationCacheListener
   * </pre>
   *
   * <p>
   * This task can be cancelled from the calling thread if a timeout happens. In that case we have
   * to handle the thread interrupt
   */
  private class GIITask implements Callable<InternalDistributedMember> {

    private final InternalDistributedMember member;

    GIITask(InternalDistributedMember member) {
      this.member = member;
    }

    @Override
    public InternalDistributedMember call() {
      addMemberArtifacts(member);
      return member;
    }
  }

  private class RemoveMemberTask implements Runnable {

    private final DistributedMember member;
    private final boolean crashed;

    RemoveMemberTask(DistributedMember member, boolean crashed) {
      this.member = member;
      this.crashed = crashed;
    }

    @Override
    public void run() {
      removeMemberArtifacts(member, crashed);
    }
  }
}

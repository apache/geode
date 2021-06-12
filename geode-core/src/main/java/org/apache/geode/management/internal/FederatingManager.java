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
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import javax.management.Notification;
import javax.management.ObjectName;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.StatisticsFactory;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.Region;
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
import org.apache.geode.internal.cache.InternalRegionFactory;
import org.apache.geode.internal.statistics.StatisticsClock;
import org.apache.geode.logging.internal.log4j.api.LogService;
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
public class FederatingManager extends Manager implements ManagerMembership {
  private static final Logger logger = LogService.getLogger();

  /**
   * This Executor uses a pool of thread to execute the member addition /removal tasks, This will
   * utilize the processing powers available. Going with unbounded queue because tasks wont be
   * unbounded in practical situation as number of members will be a finite set at any given point
   * of time
   */
  private final AtomicReference<ExecutorService> executorService = new AtomicReference<>();
  private final AtomicReference<Exception> latestException = new AtomicReference<>();
  private final List<Runnable> pendingTasks = new CopyOnWriteArrayList<>();

  private final SystemManagementService service;
  private final Supplier<ExecutorService> executorServiceSupplier;
  private final MBeanProxyFactory proxyFactory;
  private final MemberMessenger messenger;
  private final ReentrantLock lifecycleLock;

  private volatile boolean starting;

  @VisibleForTesting
  FederatingManager(ManagementResourceRepo repo, InternalDistributedSystem system,
      SystemManagementService service, InternalCache cache, StatisticsFactory statisticsFactory,
      StatisticsClock statisticsClock, MBeanProxyFactory proxyFactory, MemberMessenger messenger,
      ExecutorService executorService) {
    this(repo, system, service, cache, statisticsFactory, statisticsClock, proxyFactory, messenger,
        () -> executorService);
  }

  FederatingManager(ManagementResourceRepo repo, InternalDistributedSystem system,
      SystemManagementService service, InternalCache cache, StatisticsFactory statisticsFactory,
      StatisticsClock statisticsClock, MBeanProxyFactory proxyFactory, MemberMessenger messenger,
      Supplier<ExecutorService> executorServiceSupplier) {
    super(repo, system, cache, statisticsFactory, statisticsClock);
    this.service = service;
    this.proxyFactory = proxyFactory;
    this.messenger = messenger;
    this.executorServiceSupplier = executorServiceSupplier;
    lifecycleLock = new ReentrantLock();
  }

  /**
   * This method will be invoked whenever a member wants to be a managing node. The exception
   * Management exception has to be handled by the caller.
   */
  @Override
  public void startManager() {
    try {
      lifecycleLock.lock();
      try {
        if (starting || running) {
          return;
        }
        if (logger.isDebugEnabled()) {
          logger.debug("Starting the Federating Manager.... ");
        }
        starting = true;
        executorService.set(executorServiceSupplier.get());
        running = true;
      } finally {
        lifecycleLock.unlock();
      }

      startManagingActivity();

      lifecycleLock.lock();
      try {
        for (Runnable task : pendingTasks) {
          executeTask(task);
        }
      } finally {
        pendingTasks.clear();
        starting = false;
        lifecycleLock.unlock();
      }

      messenger.broadcastManagerInfo();

    } catch (Exception e) {
      cleanupFailedStart();
      throw new ManagementException(e);
    }
  }

  private void cleanupFailedStart() {
    lifecycleLock.lock();
    try {
      pendingTasks.clear();
      running = false;
      starting = false;
    } finally {
      lifecycleLock.unlock();
    }
  }

  @Override
  public void stopManager() {
    lifecycleLock.lock();
    try {
      // remove hidden management regions and federatedMBeans
      if (!running) {
        return;
      }
      if (logger.isDebugEnabled()) {
        logger.debug("Stopping the Federating Manager.... ");
      }
      running = false;
    } finally {
      lifecycleLock.unlock();
    }

    stopManagingActivity();
  }

  @Override
  public boolean isRunning() {
    return running;
  }

  /**
   * This method will delegate task to another thread and exit, so that it wont block the membership
   * listener
   */
  @Override
  public void addMember(InternalDistributedMember member) {
    lifecycleLock.lock();
    try {
      if (!running) {
        return;
      }
      executeTask(() -> new AddMemberTask(member).call());
    } finally {
      lifecycleLock.unlock();
    }
  }

  /**
   * This method will delegate task to another thread and exit, so that it wont block the membership
   * listener
   */
  @Override
  public void removeMember(DistributedMember member, boolean crashed) {
    lifecycleLock.lock();
    try {
      Runnable task = new RemoveMemberTask(member, crashed);
      if (starting) {
        pendingTasks.add(task);
      } else if (running) {
        executeTask(task);
      }
    } finally {
      lifecycleLock.unlock();
    }
  }

  /**
   * this method will delegate task to another thread and exit, so that it wont block the membership
   * listener
   */
  @Override
  public void suspectMember(DistributedMember member, InternalDistributedMember whoSuspected,
      String reason) {
    service.memberSuspect((InternalDistributedMember) member, whoSuspected, reason);
  }

  public MemberMessenger getMessenger() {
    return messenger;
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
      giiTaskList.add(new AddMemberTask(member));
    }

    try {
      if (isDebugEnabled) {
        logger.debug("Management Resource creation started  : ");
      }
      List<Future<InternalDistributedMember>> futureTaskList =
          executorService.get().invokeAll(giiTaskList);

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
   * This method will be invoked whenever a member stops being a managing node. The
   * {@code ManagementException} has to be handled by the caller.
   */
  private void stopManagingActivity() {
    try {
      executorService.get().shutdownNow();

      for (DistributedMember distributedMember : repo.getMonitoringRegionMap().keySet()) {
        removeMemberArtifacts(distributedMember, false);
      }
    } catch (Exception e) {
      throw new ManagementException(e);
    }
  }

  private void executeTask(Runnable task) {
    try {
      executorService.get().execute(task);
    } catch (RejectedExecutionException ignored) {
      // Ignore, we are getting shutdown
    }
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

          // Monitoring region for member is created
          InternalRegionFactory<String, Object> monitorFactory =
              cache.createInternalRegionFactory();
          monitorFactory.setScope(Scope.DISTRIBUTED_NO_ACK);
          monitorFactory.setDataPolicy(DataPolicy.REPLICATE);
          monitorFactory.setConcurrencyChecksEnabled(false);
          ManagementCacheListener managementCacheListener =
              new ManagementCacheListener(proxyFactory);
          monitorFactory.addCacheListener(managementCacheListener);
          monitorFactory.setIsUsedForMetaRegion(true);
          monitorFactory.setCachePerfStatsHolder(monitoringRegionStats);

          // Notification region for member is created
          InternalRegionFactory<NotificationKey, Notification> notificationFactory =
              cache.createInternalRegionFactory();
          notificationFactory.setScope(Scope.DISTRIBUTED_NO_ACK);
          notificationFactory.setDataPolicy(DataPolicy.REPLICATE);
          notificationFactory.setConcurrencyChecksEnabled(false);

          // Fix for issue #49638, evict the internal region _notificationRegion
          notificationFactory
              .setEvictionAttributes(EvictionAttributes.createLRUEntryAttributes(
                  ManagementConstants.NOTIF_REGION_MAX_ENTRIES, EvictionAction.LOCAL_DESTROY));

          NotificationCacheListener notifListener = new NotificationCacheListener(proxyFactory);
          notificationFactory.addCacheListener(notifListener);
          notificationFactory.setIsUsedForMetaRegion(true);
          notificationFactory.setCachePerfStatsHolder(monitoringRegionStats);

          Region<String, Object> proxyMonitoringRegion;
          try {
            if (!running) {
              return;
            }
            proxyMonitoringRegion = monitorFactory.create(monitoringRegionName);
          } catch (TimeoutException | RegionExistsException e) {
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
            proxyNotificationRegion = notificationFactory.create(notificationRegionName);
            proxyNotificationRegionCreated = true;
          } catch (TimeoutException | RegionExistsException e) {
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
      service.memberJoined(member);

      // Send manager info to the added member
      messenger.sendManagerInfo(member);
    }
  }

  @VisibleForTesting
  void removeMemberArtifacts(DistributedMember member, boolean crashed) {
    Region<String, Object> monitoringRegion = repo.getEntryFromMonitoringRegionMap(member);
    Region<NotificationKey, Notification> notificationRegion =
        repo.getEntryFromNotifRegionMap(member);

    if (monitoringRegion == null && notificationRegion == null) {
      return;
    }

    repo.romoveEntryFromMonitoringRegionMap(member);
    repo.removeEntryFromNotifRegionMap(member);

    // If cache is closed all the regions would have been destroyed implicitly
    if (!cache.isClosed()) {
      try {
        if (monitoringRegion != null) {
          proxyFactory.removeAllProxies(member, monitoringRegion);
          monitoringRegion.localDestroyRegion();
        }
      } catch (CancelException | RegionDestroyedException ignore) {
        // ignored
      }

      try {
        if (notificationRegion != null) {
          notificationRegion.localDestroyRegion();
        }
      } catch (CancelException | RegionDestroyedException ignore) {
        // ignored
      }
    }

    if (!system.getDistributedMember().equals(member)) {
      try {
        service.memberDeparted((InternalDistributedMember) member, crashed);
      } catch (CancelException | RegionDestroyedException ignore) {
        // ignored
      }
    }
  }

  @VisibleForTesting
  public MBeanProxyFactory proxyFactory() {
    return proxyFactory;
  }

  @VisibleForTesting
  Exception latestException() {
    return latestException.getAndSet(null);
  }

  @VisibleForTesting
  List<Runnable> pendingTasks() {
    return pendingTasks;
  }

  @VisibleForTesting
  boolean isStarting() {
    return starting;
  }

  /**
   * Actual task of adding a member.
   *
   * <p>
   * Perform the GII request which might originate from transition listener or membership listener.
   *
   * <p>
   * Manager resources are created per member which is visible to this node:
   *
   * <pre>
   * 1) Management Region : its a Replicated NO_ACK region
   * 2) Notification Region : its a Replicated Proxy NO_ACK region
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
   * to handle the thread interrupt.
   */
  private class AddMemberTask implements Callable<InternalDistributedMember> {

    private final InternalDistributedMember member;

    private AddMemberTask(InternalDistributedMember member) {
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

    private RemoveMemberTask(DistributedMember member, boolean crashed) {
      this.member = member;
      this.crashed = crashed;
    }

    @Override
    public void run() {
      removeMemberArtifacts(member, crashed);
    }
  }
}

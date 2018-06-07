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
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicReference;

import javax.management.Notification;
import javax.management.ObjectName;

import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.TestingOnly;
import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
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
import org.apache.geode.management.ManagementException;

/**
 * Manager implementation which manages federated MBeans for the entire DistributedSystem and
 * controls the JMX server endpoints for JMX clients to connect, such as an RMI server.
 *
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
  protected MBeanProxyFactory proxyFactory;

  private MBeanJMXAdapter jmxAdapter;

  private MemberMessenger messenger;

  private SystemManagementService service;

  private AtomicReference<Exception> latestException = new AtomicReference<>(null);

  /**
   * @param jmxAdapter JMX Adapter
   * @param repo Management resource repo
   * @param system Internal Distributed System
   * @param service SystemManagement Service
   */
  public FederatingManager(MBeanJMXAdapter jmxAdapter, ManagementResourceRepo repo,
      InternalDistributedSystem system, SystemManagementService service, InternalCache cache) {
    super(repo, system, cache);
    this.service = service;
    this.proxyFactory = new MBeanProxyFactory(jmxAdapter, service);
    this.jmxAdapter = jmxAdapter;
    this.messenger = new MemberMessenger(jmxAdapter, repo, system);
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

      Runtime rt = Runtime.getRuntime();
      this.pooledMembershipExecutor = Executors.newFixedThreadPool(rt.availableProcessors());

      running = true;
      startManagingActivity();

      messenger.broadcastManagerInfo();

    } catch (Exception e) {
      running = false;
      throw new ManagementException(e);
    }
  }

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
   * This method will be invoked whenever a member stops being a managing node. The exception
   * Management exception has to be handled by the caller. *
   */
  private void stopManagingActivity() {
    try {
      this.pooledMembershipExecutor.shutdownNow();

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
   * This method will delegate task to another thread and exit, so that it wont block the membership
   * listener
   */
  public void addMember(DistributedMember member) {
    GIITask giiTask = new GIITask(member);
    executeTask(() -> {
      try {
        giiTask.call();
      } catch (Exception e) {
        logger.warn("Error federating new member {}: {}", member.getId(), e.getMessage());
        latestException.set(e);
      }
    });
  }

  /**
   * This method will be invoked from MembershipListener which is registered when the member becomes
   * a Management node.
   *
   * This method will delegate task to another thread and exit, so that it wont block the membership
   * listener
   */
  public void removeMember(DistributedMember member, boolean crashed) {
    RemoveMemberTask removeTask = new RemoveMemberTask(member, crashed);
    executeTask(removeTask);
  }

  private void executeTask(Runnable task) {
    try {
      pooledMembershipExecutor.execute(task);
    } catch (RejectedExecutionException ex) {
      // Ignore, we are getting shutdown
    }
  }

  private class RemoveMemberTask implements Runnable {

    private DistributedMember member;

    boolean crashed;

    protected RemoveMemberTask(DistributedMember member, boolean crashed) {
      this.member = member;
      this.crashed = crashed;
    }

    public void run() {
      removeMemberArtifacts(member, crashed);
    }
  }

  private DistributedMember removeMemberArtifacts(DistributedMember member, boolean crashed) {
    Region<String, Object> proxyRegion = repo.getEntryFromMonitoringRegionMap(member);
    Region<NotificationKey, Notification> notifRegion = repo.getEntryFromNotifRegionMap(member);

    if (proxyRegion == null && notifRegion == null) {
      return member;
    }
    repo.romoveEntryFromMonitoringRegionMap(member);
    repo.removeEntryFromNotifRegionMap(member);
    // If cache is closed all the regions would have been
    // destroyed implicitly
    if (!cache.isClosed()) {
      proxyFactory.removeAllProxies(member, proxyRegion);
      proxyRegion.localDestroyRegion();
      notifRegion.localDestroyRegion();
    }
    if (!cache.getDistributedSystem().getDistributedMember().equals(member)) {
      service.memberDeparted((InternalDistributedMember) member, crashed);
    }
    return member;
  }

  /**
   * This method will be invoked from MembershipListener which is registered when the member becomes
   * a Management node.
   *
   * this method will delegate task to another thread and exit, so that it wont block the membership
   * listener
   */
  public void suspectMember(DistributedMember member, InternalDistributedMember whoSuspected,
      String reason) {
    service.memberSuspect((InternalDistributedMember) member, whoSuspected, reason);
  }

  /**
   * This method will be invoked when a node transitions from managed node to managing node This
   * method will block for all GIIs to be completed But each GII is given a specific time frame.
   * After that the task will be marked as cancelled.
   */
  public void startManagingActivity() throws Exception {
    final boolean isDebugEnabled = logger.isDebugEnabled();

    Set<DistributedMember> members =
        cache.getDistributionManager().getOtherDistributionManagerIds();

    Iterator<DistributedMember> it = members.iterator();
    DistributedMember member;

    final List<Callable<DistributedMember>> giiTaskList = new ArrayList<>();

    List<Future<DistributedMember>> futureTaskList;

    while (it.hasNext()) {
      member = it.next();
      giiTaskList.add(new GIITask(member));
    }

    try {
      if (isDebugEnabled) {
        logger.debug("Management Resource creation started  : ");
      }
      futureTaskList = pooledMembershipExecutor.invokeAll(giiTaskList);

      for (Future<DistributedMember> futureTask : futureTaskList) {

        String memberId = null;
        try {

          DistributedMember returnedMember = futureTask.get();
          if (returnedMember != null) {
            memberId = returnedMember.getId();
          }

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
            logger.debug("ExecutionException during Management GII: {}", e.getMessage(), e);
          }

        } catch (CancellationException e) {
          if (isDebugEnabled) {
            ManagementException mgEx = new ManagementException(e.fillInStackTrace());
            logger.debug("InterruptedException while creating Monitoring resource with error : {}",
                mgEx.getMessage(), mgEx);
          }
        }
      }
    } catch (InterruptedException e) {
      if (isDebugEnabled) {
        ManagementException mgEx = new ManagementException(e.fillInStackTrace());
        logger.debug("InterruptedException while creating Monitoring resource with error : ",
            mgEx.getMessage(), mgEx);
      }

    } finally {
      if (isDebugEnabled) {
        logger.debug("Management Resource creation completed");
      }
    }
  }

  /**
   * Actual task of doing the GII
   *
   * It will perform the GII request which might originate from TransitionListener or Membership
   * Listener.
   *
   * Managing Node side resources are created per member which is visible to this node
   *
   * 1)Management Region : its a Replicated NO_ACK region 2)Notification Region : its a Replicated
   * Proxy NO_ACK region
   *
   * Listeners are added to the above two regions 1) ManagementCacheListener() 2)
   * NotificationCacheListener
   *
   * This task can be cancelled from the calling thread if a timeout happens. In that case we have
   * to handle the thread interrupt
   */
  private class GIITask implements Callable<DistributedMember> {

    private DistributedMember member;

    protected GIITask(DistributedMember member) {

      this.member = member;
    }

    public DistributedMember call() {
      synchronized (member) {
        Region<String, Object> proxyMonitoringRegion;
        Region<NotificationKey, Notification> proxyNotificationRegion;
        String appender = MBeanJMXAdapter.getUniqueIDForMember(member);
        String monitoringRegionName = ManagementConstants.MONITORING_REGION + "_" + appender;
        String notificationRegionName = ManagementConstants.NOTIFICATION_REGION + "_" + appender;

        if (cache.getRegion(monitoringRegionName) != null
            && cache.getRegion(notificationRegionName) != null) {
          return member;
        }

        try {

          // GII wont start at all if its interrupted
          if (!Thread.currentThread().isInterrupted()) {

            // as the regions will be internal regions
            InternalRegionArguments internalArgs = new InternalRegionArguments();
            internalArgs.setIsUsedForMetaRegion(true);

            // Create anonymous stats holder for Management Regions
            final HasCachePerfStats monitoringRegionStats = new HasCachePerfStats() {
              public CachePerfStats getCachePerfStats() {
                return new CachePerfStats(cache.getDistributedSystem(), "managementRegionStats");
              }
            };

            internalArgs.setCachePerfStatsHolder(monitoringRegionStats);

            // Monitoring region for member is created
            AttributesFactory<String, Object> monitorAttrFactory = new AttributesFactory<>();
            monitorAttrFactory.setScope(Scope.DISTRIBUTED_NO_ACK);
            monitorAttrFactory.setDataPolicy(DataPolicy.REPLICATE);
            monitorAttrFactory.setConcurrencyChecksEnabled(false);
            ManagementCacheListener mgmtCacheListener = new ManagementCacheListener(proxyFactory);
            monitorAttrFactory.addCacheListener(mgmtCacheListener);

            RegionAttributes<String, Object> monitoringRegionAttrs = monitorAttrFactory.create();

            // Notification region for member is created
            AttributesFactory<NotificationKey, Notification> notifAttrFactory =
                new AttributesFactory<>();
            notifAttrFactory.setScope(Scope.DISTRIBUTED_NO_ACK);
            notifAttrFactory.setDataPolicy(DataPolicy.REPLICATE);
            notifAttrFactory.setConcurrencyChecksEnabled(false);

            // Fix for issue #49638, evict the internal region _notificationRegion
            notifAttrFactory.setEvictionAttributes(EvictionAttributes.createLRUEntryAttributes(
                ManagementConstants.NOTIF_REGION_MAX_ENTRIES, EvictionAction.LOCAL_DESTROY));

            NotificationCacheListener notifListener = new NotificationCacheListener(proxyFactory);
            notifAttrFactory.addCacheListener(notifListener);

            RegionAttributes<NotificationKey, Notification> notifRegionAttrs =
                notifAttrFactory.create();

            boolean proxyMonitoringRegionCreated = false;
            boolean proxyNotifRegionCreated = false;

            try {
              if (!running) {
                return null;
              }
              proxyMonitoringRegion =
                  cache.createVMRegion(monitoringRegionName, monitoringRegionAttrs, internalArgs);
              proxyMonitoringRegionCreated = true;

            } catch (TimeoutException | RegionExistsException | IOException
                | ClassNotFoundException e) {
              if (logger.isDebugEnabled()) {
                logger.debug("Error During Internal Region creation {}", e.getMessage(), e);
              }
              throw new ManagementException(e);
            }

            try {
              if (!running) {
                return null;
              }
              proxyNotificationRegion =
                  cache.createVMRegion(notificationRegionName, notifRegionAttrs, internalArgs);
              proxyNotifRegionCreated = true;
            } catch (TimeoutException | RegionExistsException | IOException
                | ClassNotFoundException e) {
              if (logger.isDebugEnabled()) {
                logger.debug("Error During Internal Region creation {}", e.getMessage(), e);
              }
              throw new ManagementException(e);
            } finally {
              if (!proxyNotifRegionCreated && proxyMonitoringRegionCreated) {
                // Destroy the proxy region if proxy notification
                // region is not created
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
                return null;
              }
              proxyFactory.createAllProxies(member, proxyMonitoringRegion);

              mgmtCacheListener.markReady();
              notifListener.markReady();
            } catch (Exception e) {
              if (logger.isDebugEnabled()) {
                logger.debug("Error During GII Proxy creation {}", e.getMessage(), e);
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

        return member;
      }
    }
  }

  /**
   * For internal Use only
   */
  public MBeanProxyFactory getProxyFactory() {
    return proxyFactory;
  }

  /**
   * This will return the last updated time of the proxyMBean
   *
   * @param objectName {@link javax.management.ObjectName} of the MBean
   * @return last updated time of the proxy
   */
  public long getLastUpdateTime(ObjectName objectName) {
    return proxyFactory.getLastUpdateTime(objectName);
  }

  /**
   * Find a particular proxy instance for a {@link javax.management.ObjectName} ,
   * {@link org.apache.geode.distributed.DistributedMember} and interface class If the proxy
   * interface does not implement the given interface class a {@link java.lang.ClassCastException}.
   * will be thrown
   *
   * @param objectName {@link javax.management.ObjectName} of the MBean
   * @param interfaceClass interface class implemented by proxy
   * @return an instance of proxy exposing the given interface
   */
  public <T> T findProxy(ObjectName objectName, Class<T> interfaceClass) {
    return proxyFactory.findProxy(objectName, interfaceClass);
  }

  /**
   * Find a set of proxies given a {@link org.apache.geode.distributed.DistributedMember}
   *
   * @param member {@link org.apache.geode.distributed.DistributedMember}
   * @return a set of {@link javax.management.ObjectName}
   */
  public Set<ObjectName> findAllProxies(DistributedMember member) {
    return proxyFactory.findAllProxies(member);
  }

  public MemberMessenger getMessenger() {
    return messenger;
  }

  @TestingOnly
  public void setMessenger(MemberMessenger messenger) {
    this.messenger = messenger;
  }

  @TestingOnly
  public synchronized Exception getAndResetLatestException() {
    return latestException.getAndSet(null);
  }
}

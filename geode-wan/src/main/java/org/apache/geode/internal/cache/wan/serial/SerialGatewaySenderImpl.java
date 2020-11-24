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
package org.apache.geode.internal.cache.wan.serial;

import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.asyncqueue.AsyncEventListener;
import org.apache.geode.cache.wan.GatewayTransportFilter;
import org.apache.geode.distributed.DistributedLockService;
import org.apache.geode.distributed.internal.DistributionAdvisor.Profile;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.ResourceEvent;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.RegionQueue;
import org.apache.geode.internal.cache.UpdateAttributesProcessor;
import org.apache.geode.internal.cache.ha.ThreadIdentifier;
import org.apache.geode.internal.cache.wan.AbstractGatewaySenderEventProcessor;
import org.apache.geode.internal.cache.wan.AbstractRemoteGatewaySender;
import org.apache.geode.internal.cache.wan.GatewaySenderAdvisor.GatewaySenderProfile;
import org.apache.geode.internal.cache.wan.GatewaySenderAttributes;
import org.apache.geode.internal.cache.wan.GatewaySenderConfigurationException;
import org.apache.geode.internal.monitoring.ThreadsMonitoring;
import org.apache.geode.internal.statistics.StatisticsClock;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * @since GemFire 7.0
 */
public class SerialGatewaySenderImpl extends AbstractRemoteGatewaySender {

  private static final Logger logger = LogService.getLogger();

  public SerialGatewaySenderImpl(InternalCache cache, StatisticsClock statisticsClock,
      GatewaySenderAttributes attrs) {
    super(cache, statisticsClock, attrs);
  }

  @Override
  public void start() {
    this.start(false);
  }

  @Override
  public void startWithCleanQueue() {
    this.start(true);
  }

  private void start(boolean cleanQueues) {
    if (logger.isDebugEnabled()) {
      logger.debug("Starting gatewaySender : {}", this);
    }

    this.getLifeCycleLock().writeLock().lock();
    try {
      if (isRunning()) {
        logger.warn("Gateway Sender {} is already running", this.getId());
        return;
      }
      if (this.remoteDSId != DEFAULT_DISTRIBUTED_SYSTEM_ID) {
        String locators = this.cache.getInternalDistributedSystem().getConfig().getLocators();
        if (locators.length() == 0) {
          throw new GatewaySenderConfigurationException(
              "Locators must be configured before starting gateway-sender.");
        }
      }
      getSenderAdvisor().initDLockService();
      if (!isPrimary()) {
        if (getSenderAdvisor().volunteerForPrimary()) {
          getSenderAdvisor().makePrimary();
        } else {
          getSenderAdvisor().makeSecondary();
        }
      }

      eventProcessor = createEventProcessor(cleanQueues);

      if (isStartEventProcessorInPausedState()) {
        this.pauseEvenIfProcessorStopped();
      }
      eventProcessor.start();
      waitForRunningStatus();
      this.startTime = System.currentTimeMillis();

      // Only notify the type registry if this is a WAN gateway queue
      if (!isAsyncEventQueue()) {
        getCache().getPdxRegistry().gatewaySenderStarted(this);
      }
      new UpdateAttributesProcessor(this).distribute(false);

      InternalDistributedSystem system = this.cache.getInternalDistributedSystem();
      system.handleResourceEvent(ResourceEvent.GATEWAYSENDER_START, this);

      logger
          .info("Started  {}", this);

      enqueueTempEvents();
    } finally {
      this.getLifeCycleLock().writeLock().unlock();
    }
  }

  protected AbstractGatewaySenderEventProcessor createEventProcessor(boolean cleanQueues) {
    AbstractGatewaySenderEventProcessor eventProcessor;
    if (getDispatcherThreads() > 1) {
      eventProcessor = new RemoteConcurrentSerialGatewaySenderEventProcessor(
          SerialGatewaySenderImpl.this, getThreadMonitorObj(), cleanQueues);
    } else {
      eventProcessor = new RemoteSerialGatewaySenderEventProcessor(SerialGatewaySenderImpl.this,
          getId(), getThreadMonitorObj(), cleanQueues);
    }
    return eventProcessor;
  }

  @Override
  public void stop() {
    if (logger.isDebugEnabled()) {
      logger.debug("Stopping Gateway Sender : {}", this);
    }
    this.getLifeCycleLock().writeLock().lock();
    try {
      // Stop the dispatcher
      stopProcessing();
      // Stop the proxy (after the dispatcher, so the socket is still
      // alive until after the dispatcher has stopped)
      stompProxyDead();
      // Close the listeners
      for (AsyncEventListener listener : this.listeners) {
        listener.close();
      }
      logger.info("Stopped  {}", this);

      clearTempEventsAfterSenderStopped();
    } finally {
      this.getLifeCycleLock().writeLock().unlock();
    }

    if (this.isPrimary()) {
      try {
        DistributedLockService.destroy(getSenderAdvisor().getDLockServiceName());
      } catch (IllegalArgumentException e) {
        // service not found... ignore
      }
    }
    Set<RegionQueue> queues = getQueues();
    if (queues != null && !queues.isEmpty()) {
      for (RegionQueue q : queues) {
        ((SerialGatewaySenderQueue) q).cleanUp();
      }
    }

    this.setIsPrimary(false);
    new UpdateAttributesProcessor(this).distribute(false);
    Thread lockObtainingThread = getSenderAdvisor().getLockObtainingThread();
    if (lockObtainingThread != null && lockObtainingThread.isAlive()) {
      // wait a while for thread to terminate
      try {
        lockObtainingThread.join(3000);
      } catch (InterruptedException ex) {
        // we allowed our join to be canceled
        // reset interrupt bit so this thread knows it has been interrupted
        Thread.currentThread().interrupt();
      }
      if (lockObtainingThread.isAlive()) {
        logger.info("Could not stop lock obtaining thread during gateway sender stop");
      }
    }

    InternalDistributedSystem system =
        (InternalDistributedSystem) this.cache.getDistributedSystem();
    system.handleResourceEvent(ResourceEvent.GATEWAYSENDER_STOP, this);

    this.eventProcessor = null;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("SerialGatewaySender{");
    sb.append("id=").append(getId());
    sb.append(",remoteDsId=").append(getRemoteDSId());
    sb.append(",isRunning =").append(isRunning());
    sb.append(",isPrimary =").append(isPrimary());
    sb.append("}");
    return sb.toString();
  }

  @Override
  public void fillInProfile(Profile profile) {
    assert profile instanceof GatewaySenderProfile;
    GatewaySenderProfile pf = (GatewaySenderProfile) profile;
    pf.Id = getId();
    pf.startTime = getStartTime();
    pf.remoteDSId = getRemoteDSId();
    pf.isRunning = isRunning();
    pf.isPrimary = isPrimary();
    pf.isParallel = false;
    pf.isBatchConflationEnabled = isBatchConflationEnabled();
    pf.isPersistenceEnabled = isPersistenceEnabled();
    pf.alertThreshold = getAlertThreshold();
    pf.manualStart = isManualStart();
    for (org.apache.geode.cache.wan.GatewayEventFilter filter : getGatewayEventFilters()) {
      pf.eventFiltersClassNames.add(filter.getClass().getName());
    }
    for (GatewayTransportFilter filter : getGatewayTransportFilters()) {
      pf.transFiltersClassNames.add(filter.getClass().getName());
    }
    for (AsyncEventListener listener : getAsyncEventListeners()) {
      pf.senderEventListenerClassNames.add(listener.getClass().getName());
    }
    pf.isDiskSynchronous = isDiskSynchronous();
    pf.dispatcherThreads = getDispatcherThreads();
    pf.orderPolicy = getOrderPolicy();
    pf.serverLocation = this.getServerLocation();
  }

  @Override
  public void setModifiedEventId(EntryEventImpl clonedEvent) {
    EventID originalEventId = clonedEvent.getEventId();
    long originalThreadId = originalEventId.getThreadID();
    long newThreadId = originalThreadId;
    if (ThreadIdentifier.isWanTypeThreadID(newThreadId)) {
      // This thread id has already been converted. Do nothing.
    } else {
      newThreadId = ThreadIdentifier.createFakeThreadIDForParallelGSPrimaryBucket(0,
          originalThreadId, getEventIdIndex());
    }
    EventID newEventId = new EventID(originalEventId.getMembershipID(), newThreadId,
        originalEventId.getSequenceID());
    if (logger.isDebugEnabled()) {
      logger.debug(
          "{}: Generated event id for event with key={}, original event id={}, originalThreadId={}, new event id={}, newThreadId={}",
          this, clonedEvent.getKey(), originalEventId,
          ThreadIdentifier.toDisplayString(originalThreadId), newEventId,
          ThreadIdentifier.toDisplayString(newThreadId));
    }
    clonedEvent.setEventId(newEventId);
  }

  private ThreadsMonitoring getThreadMonitorObj() {
    DistributionManager distributionManager = this.cache.getDistributionManager();
    if (distributionManager != null) {
      return distributionManager.getThreadMonitoring();
    } else {
      return null;
    }
  }
}

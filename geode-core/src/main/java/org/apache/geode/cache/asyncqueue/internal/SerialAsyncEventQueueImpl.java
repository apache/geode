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
package org.apache.geode.cache.asyncqueue.internal;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.cache.asyncqueue.AsyncEventListener;
import org.apache.geode.cache.wan.GatewayTransportFilter;
import org.apache.geode.distributed.DistributedLockService;
import org.apache.geode.distributed.internal.DistributionAdvisor.Profile;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.ResourceEvent;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.RegionQueue;
import org.apache.geode.internal.cache.UpdateAttributesProcessor;
import org.apache.geode.internal.cache.ha.ThreadIdentifier;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.internal.cache.wan.GatewaySenderAdvisor.GatewaySenderProfile;
import org.apache.geode.internal.cache.wan.GatewaySenderAttributes;
import org.apache.geode.internal.cache.wan.GatewaySenderConfigurationException;
import org.apache.geode.internal.cache.wan.serial.ConcurrentSerialGatewaySenderEventProcessor;
import org.apache.geode.internal.cache.wan.serial.SerialGatewaySenderEventProcessor;
import org.apache.geode.internal.cache.wan.serial.SerialGatewaySenderQueue;
import org.apache.geode.internal.cache.xmlcache.CacheCreation;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LocalizedMessage;

public class SerialAsyncEventQueueImpl extends AbstractGatewaySender {

  private static final Logger logger = LogService.getLogger();

  public SerialAsyncEventQueueImpl(InternalCache cache, GatewaySenderAttributes attrs) {
    super(cache, attrs);
    if (!(this.cache instanceof CacheCreation)) {
      // this sender lies underneath the AsyncEventQueue. Need to have
      // AsyncEventQueueStats
      this.statistics = new AsyncEventQueueStats(cache.getDistributedSystem(),
          AsyncEventQueueImpl.getAsyncEventQueueIdFromSenderId(id));
    }
  }

  @Override
  public void start() {
    if (logger.isDebugEnabled()) {
      logger.debug("Starting gatewaySender : {}", this);
    }

    this.getLifeCycleLock().writeLock().lock();
    try {
      if (isRunning()) {
        logger.warn(LocalizedMessage
            .create(LocalizedStrings.GatewaySender_SENDER_0_IS_ALREADY_RUNNING, this.getId()));
        return;
      }
      if (this.remoteDSId != DEFAULT_DISTRIBUTED_SYSTEM_ID) {
        String locators = this.cache.getInternalDistributedSystem().getConfig().getLocators();
        if (locators.length() == 0) {
          throw new GatewaySenderConfigurationException(
              LocalizedStrings.AbstractGatewaySender_LOCATOR_SHOULD_BE_CONFIGURED_BEFORE_STARTING_GATEWAY_SENDER
                  .toLocalizedString());
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
      if (getDispatcherThreads() > 1) {
        eventProcessor =
            new ConcurrentSerialGatewaySenderEventProcessor(SerialAsyncEventQueueImpl.this);
      } else {
        eventProcessor =
            new SerialGatewaySenderEventProcessor(SerialAsyncEventQueueImpl.this, getId());
      }
      eventProcessor.start();
      waitForRunningStatus();
      this.startTime = System.currentTimeMillis();

      // Only notify the type registry if this is a WAN gateway queue
      if (!isAsyncEventQueue()) {
        getCache().getPdxRegistry().gatewaySenderStarted(this);
      }
      new UpdateAttributesProcessor(this).distribute(false);


      InternalDistributedSystem system =
          (InternalDistributedSystem) this.cache.getDistributedSystem();
      system.handleResourceEvent(ResourceEvent.GATEWAYSENDER_START, this);

      logger
          .info(LocalizedMessage.create(LocalizedStrings.SerialGatewaySenderImpl_STARTED__0, this));

      enqueueTempEvents();
    } finally {
      this.getLifeCycleLock().writeLock().unlock();
    }
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
      logger.info(LocalizedMessage.create(LocalizedStrings.GatewayImpl_STOPPED__0, this));

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
    if (getQueues() != null && !getQueues().isEmpty()) {
      for (RegionQueue q : getQueues()) {
        ((SerialGatewaySenderQueue) q).cleanUp();
      }
    }
    this.setIsPrimary(false);
    try {
      new UpdateAttributesProcessor(this).distribute(false);
    } catch (CancelException e) {
    }
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
        logger.info(LocalizedMessage.create(
            LocalizedStrings.GatewaySender_COULD_NOT_STOP_LOCK_OBTAINING_THREAD_DURING_GATEWAY_SENDER_STOP));
      }
    }

    InternalDistributedSystem system =
        (InternalDistributedSystem) this.cache.getDistributedSystem();
    system.handleResourceEvent(ResourceEvent.GATEWAYSENDER_STOP, this);
  }

  @Override
  public String toString() {
    StringBuffer sb = new StringBuffer();
    sb.append("SerialGatewaySender{");
    sb.append("id=" + getId());
    sb.append(",remoteDsId=" + getRemoteDSId());
    sb.append(",isRunning =" + isRunning());
    sb.append(",isPrimary =" + isPrimary());
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

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.geode.internal.cache.wan.AbstractGatewaySender#setModifiedEventId(org.apache.geode.
   * internal.cache.EntryEventImpl)
   */
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
          this, clonedEvent.getKey(), originalEventId, originalThreadId, newEventId, newThreadId);
    }
    clonedEvent.setEventId(newEventId);
  }

}

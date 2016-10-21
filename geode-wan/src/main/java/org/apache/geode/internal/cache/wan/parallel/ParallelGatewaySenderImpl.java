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
package org.apache.geode.internal.cache.wan.parallel;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.EntryOperation;
import org.apache.geode.cache.asyncqueue.AsyncEventListener;
import org.apache.geode.cache.asyncqueue.internal.AsyncEventQueueImpl;
import org.apache.geode.cache.asyncqueue.internal.AsyncEventQueueStats;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.cache.wan.GatewayTransportFilter;
import org.apache.geode.distributed.internal.DistributionAdvisor.Profile;
import org.apache.geode.distributed.internal.DistributionAdvisor;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.ResourceEvent;
import org.apache.geode.internal.cache.DistributedRegion;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.PartitionedRegionHelper;
import org.apache.geode.internal.cache.UpdateAttributesProcessor;
import org.apache.geode.internal.cache.ha.ThreadIdentifier;
import org.apache.geode.internal.cache.wan.AbstractRemoteGatewaySender;
import org.apache.geode.internal.cache.wan.AbstractGatewaySenderEventProcessor;
import org.apache.geode.internal.cache.wan.GatewaySenderAdvisor;
import org.apache.geode.internal.cache.wan.GatewaySenderStats;
import org.apache.geode.internal.cache.wan.GatewaySenderAdvisor.GatewaySenderProfile;
import org.apache.geode.internal.cache.wan.GatewaySenderAttributes;
import org.apache.geode.internal.cache.xmlcache.CacheCreation;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.LoggingThreadGroup;
import org.apache.geode.internal.logging.log4j.LocalizedMessage;

/**
 * @since GemFire 7.0
 *
 */
public class ParallelGatewaySenderImpl extends AbstractRemoteGatewaySender {

  private static final Logger logger = LogService.getLogger();

  final ThreadGroup loggerGroup =
      LoggingThreadGroup.createThreadGroup("Remote Site Discovery Logger Group", logger);

  public ParallelGatewaySenderImpl() {
    super();
    this.isParallel = true;
  }

  public ParallelGatewaySenderImpl(Cache cache, GatewaySenderAttributes attrs) {
    super(cache, attrs);
  }

  @Override
  public void start() {
    this.getLifeCycleLock().writeLock().lock();
    try {
      if (isRunning()) {
        logger.warn(LocalizedMessage
            .create(LocalizedStrings.GatewaySender_SENDER_0_IS_ALREADY_RUNNING, this.getId()));
        return;
      }

      if (this.remoteDSId != DEFAULT_DISTRIBUTED_SYSTEM_ID) {
        String locators =
            ((GemFireCacheImpl) this.cache).getDistributedSystem().getConfig().getLocators();
        if (locators.length() == 0) {
          throw new IllegalStateException(
              LocalizedStrings.AbstractGatewaySender_LOCATOR_SHOULD_BE_CONFIGURED_BEFORE_STARTING_GATEWAY_SENDER
                  .toLocalizedString());
        }
      }
      /*
       * Now onwards all processing will happen through
       * "ConcurrentParallelGatewaySenderEventProcessor" we have made
       * "ParallelGatewaySenderEventProcessor" and "ParallelGatewaySenderQueue" as a utility classes
       * of Concurrent version of processor and queue.
       */
      eventProcessor = new RemoteConcurrentParallelGatewaySenderEventProcessor(this);
      /*
       * if (getDispatcherThreads() > 1) { eventProcessor = new
       * ConcurrentParallelGatewaySenderEventProcessor(this); } else { eventProcessor = new
       * ParallelGatewaySenderEventProcessor(this); }
       */

      eventProcessor.start();
      waitForRunningStatus();
      // Only notify the type registry if this is a WAN gateway queue
      if (!isAsyncEventQueue()) {
        ((GemFireCacheImpl) getCache()).getPdxRegistry().gatewaySenderStarted(this);
      }
      new UpdateAttributesProcessor(this).distribute(false);

      InternalDistributedSystem system =
          (InternalDistributedSystem) this.cache.getDistributedSystem();
      system.handleResourceEvent(ResourceEvent.GATEWAYSENDER_START, this);

      logger.info(
          LocalizedMessage.create(LocalizedStrings.ParallelGatewaySenderImpl_STARTED__0, this));

      enqueueTempEvents();
    } finally {
      this.getLifeCycleLock().writeLock().unlock();
    }
  }

  // /**
  // * The sender is not started but only the message queue i.e. shadowPR is created on the node.
  // * @param targetPr
  // */
  // private void createMessageQueueOnAccessorNode(PartitionedRegion targetPr) {
  // eventProcessor = new ParallelGatewaySenderEventProcessor(this, targetPr);
  // }


  @Override
  public void stop() {
    this.getLifeCycleLock().writeLock().lock();
    try {
      if (!this.isRunning()) {
        return;
      }
      // Stop the dispatcher
      AbstractGatewaySenderEventProcessor ev = this.eventProcessor;
      // try {
      if (ev != null && !ev.isStopped()) {
        ev.stopProcessing();
      }

      // Stop the proxy (after the dispatcher, so the socket is still
      // alive until after the dispatcher has stopped)
      stompProxyDead();

      // Close the listeners
      for (AsyncEventListener listener : this.listeners) {
        listener.close();
      }
      // stop the running threads, open sockets if any
      ((ConcurrentParallelGatewaySenderQueue) this.eventProcessor.getQueue()).cleanUp();

      logger.info(LocalizedMessage.create(LocalizedStrings.GatewayImpl_STOPPED__0, this));

      InternalDistributedSystem system =
          (InternalDistributedSystem) this.cache.getDistributedSystem();
      system.handleResourceEvent(ResourceEvent.GATEWAYSENDER_STOP, this);

      clearTempEventsAfterSenderStopped();
      // Keep the eventProcessor around so we can ask it for the regionQueues later.
      // Tests expect to be able to do this.
      // } finally {
      // this.eventProcessor = null;
      // }
    } finally {
      this.getLifeCycleLock().writeLock().unlock();
    }
  }

  @Override
  public String toString() {
    StringBuffer sb = new StringBuffer();
    sb.append("ParallelGatewaySender{");
    sb.append("id=" + getId());
    sb.append(",remoteDsId=" + getRemoteDSId());
    sb.append(",isRunning =" + isRunning());
    sb.append("}");
    return sb.toString();
  }

  public void fillInProfile(Profile profile) {
    assert profile instanceof GatewaySenderProfile;
    GatewaySenderProfile pf = (GatewaySenderProfile) profile;
    pf.Id = getId();
    pf.remoteDSId = getRemoteDSId();
    pf.isRunning = isRunning();
    pf.isPrimary = isPrimary();
    pf.isParallel = true;
    pf.isBatchConflationEnabled = isBatchConflationEnabled();
    pf.isPersistenceEnabled = isPersistenceEnabled();
    pf.alertThreshold = getAlertThreshold();
    pf.manualStart = isManualStart();
    pf.dispatcherThreads = getDispatcherThreads();
    pf.orderPolicy = getOrderPolicy();
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
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.geode.internal.cache.wan.AbstractGatewaySender#setModifiedEventId(org.apache.geode.
   * internal.cache.EntryEventImpl)
   */
  @Override
  protected void setModifiedEventId(EntryEventImpl clonedEvent) {
    int bucketId = -1;
    // merged from 42004
    if (clonedEvent.getRegion() instanceof DistributedRegion) {
      // if (getOrderPolicy() == OrderPolicy.THREAD) {
      // bucketId = PartitionedRegionHelper.getHashKey(
      // ((EntryEventImpl)clonedEvent).getEventId().getThreadID(),
      // getMaxParallelismForReplicatedRegion());
      // }
      // else
      bucketId = PartitionedRegionHelper.getHashKey(clonedEvent.getKey(),
          getMaxParallelismForReplicatedRegion());
    } else {
      bucketId = PartitionedRegionHelper.getHashKey((EntryOperation) clonedEvent);
    }
    EventID originalEventId = clonedEvent.getEventId();
    long originatingThreadId = ThreadIdentifier.getRealThreadID(originalEventId.getThreadID());

    long newThreadId = ThreadIdentifier.createFakeThreadIDForParallelGSPrimaryBucket(bucketId,
        originatingThreadId, getEventIdIndex());

    // In case of parallel as all events go through primary buckets
    // we don't need to generate different threadId for secondary buckets
    // as they will be rejected if seen at PR level itself

    // boolean isPrimary = ((PartitionedRegion)getQueue().getRegion())
    // .getRegionAdvisor().getBucketAdvisor(bucketId).isPrimary();
    // if (isPrimary) {
    // newThreadId = ThreadIdentifier
    // .createFakeThreadIDForParallelGSPrimaryBucket(bucketId,
    // originatingThreadId);
    // } else {
    // newThreadId = ThreadIdentifier
    // .createFakeThreadIDForParallelGSSecondaryBucket(bucketId,
    // originatingThreadId);
    // }

    EventID newEventId = new EventID(originalEventId.getMembershipID(), newThreadId,
        originalEventId.getSequenceID(), bucketId);
    if (logger.isDebugEnabled()) {
      logger.debug(
          "{}: Generated event id for event with key={}, bucketId={}, original event id={}, threadId={}, new event id={}, newThreadId={}",
          this, clonedEvent.getKey(), bucketId, originalEventId, originatingThreadId, newEventId,
          newThreadId);
    }
    clonedEvent.setEventId(newEventId);
  }

}

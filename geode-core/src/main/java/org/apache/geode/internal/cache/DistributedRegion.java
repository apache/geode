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
package org.apache.geode.internal.cache;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.InternalGemFireError;
import org.apache.geode.InvalidDeltaException;
import org.apache.geode.SystemFailure;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.CacheEvent;
import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.CacheLoader;
import org.apache.geode.cache.CacheLoaderException;
import org.apache.geode.cache.CacheWriter;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.DiskAccessException;
import org.apache.geode.cache.EntryExistsException;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.LossAction;
import org.apache.geode.cache.MembershipAttributes;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.RegionAccessException;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.RegionDistributionException;
import org.apache.geode.cache.RegionMembershipListener;
import org.apache.geode.cache.ResumptionAction;
import org.apache.geode.cache.RoleException;
import org.apache.geode.cache.TimeoutException;
import org.apache.geode.cache.TransactionDataNotColocatedException;
import org.apache.geode.cache.TransactionException;
import org.apache.geode.cache.asyncqueue.internal.AsyncEventQueueImpl;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.cache.persistence.PersistentReplicatesOfflineException;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.distributed.DistributedLockService;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.LockServiceDestroyedException;
import org.apache.geode.distributed.Role;
import org.apache.geode.distributed.internal.DM;
import org.apache.geode.distributed.internal.DistributionAdvisee;
import org.apache.geode.distributed.internal.DistributionAdvisor;
import org.apache.geode.distributed.internal.DistributionAdvisor.Profile;
import org.apache.geode.distributed.internal.DistributionAdvisor.ProfileVisitor;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.MembershipListener;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.locks.DLockRemoteToken;
import org.apache.geode.distributed.internal.locks.DLockService;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.i18n.StringId;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.cache.AbstractRegionMap.ARMLockTestHook;
import org.apache.geode.internal.cache.CacheDistributionAdvisor.CacheProfile;
import org.apache.geode.internal.cache.InitialImageOperation.GIIStatus;
import org.apache.geode.internal.cache.RemoteFetchVersionMessage.FetchVersionResponse;
import org.apache.geode.internal.cache.control.InternalResourceManager.ResourceType;
import org.apache.geode.internal.cache.control.MemoryEvent;
import org.apache.geode.internal.cache.event.DistributedEventTracker;
import org.apache.geode.internal.cache.event.EventTracker;
import org.apache.geode.internal.cache.execute.DistributedRegionFunctionExecutor;
import org.apache.geode.internal.cache.execute.DistributedRegionFunctionResultSender;
import org.apache.geode.internal.cache.execute.DistributedRegionFunctionResultWaiter;
import org.apache.geode.internal.cache.execute.FunctionStats;
import org.apache.geode.internal.cache.execute.LocalResultCollector;
import org.apache.geode.internal.cache.execute.RegionFunctionContextImpl;
import org.apache.geode.internal.cache.execute.ServerToClientFunctionResultSender;
import org.apache.geode.internal.cache.lru.LRUEntry;
import org.apache.geode.internal.cache.partitioned.RemoteSizeMessage;
import org.apache.geode.internal.cache.persistence.CreatePersistentRegionProcessor;
import org.apache.geode.internal.cache.persistence.PersistenceAdvisor;
import org.apache.geode.internal.cache.persistence.PersistenceAdvisorImpl;
import org.apache.geode.internal.cache.persistence.PersistentMemberID;
import org.apache.geode.internal.cache.persistence.PersistentMemberManager;
import org.apache.geode.internal.cache.persistence.PersistentMemberView;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.tier.sockets.VersionedObjectList;
import org.apache.geode.internal.cache.versions.ConcurrentCacheModificationException;
import org.apache.geode.internal.cache.versions.RegionVersionVector;
import org.apache.geode.internal.cache.versions.VersionSource;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.internal.cache.wan.AsyncEventQueueConfigurationException;
import org.apache.geode.internal.cache.wan.GatewaySenderConfigurationException;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LocalizedMessage;
import org.apache.geode.internal.offheap.annotations.Released;
import org.apache.geode.internal.offheap.annotations.Retained;
import org.apache.geode.internal.sequencelog.RegionLogger;
import org.apache.geode.internal.util.concurrent.StoppableCountDownLatch;

@SuppressWarnings("deprecation")
public class DistributedRegion extends LocalRegion implements InternalDistributedRegion {
  private static final Logger logger = LogService.getLogger();

  /** causes cache profile to be added to afterRemoteRegionCreate notification for testing */
  public static boolean TEST_HOOK_ADD_PROFILE = false;

  /** Used to sync accesses to this.dlockService to allow lazy construction */
  private final Object dlockMonitor = new Object();

  final CacheDistributionAdvisor distAdvisor;

  /**
   * GuardedBy {@link #dlockMonitor}
   */
  private DistributedLockService dlockService;

  final AdvisorListener advisorListener = new AdvisorListener();

  /** Set of currently missing required roles */
  final HashSet missingRequiredRoles = new HashSet(); // package-private to avoid synthetic accessor

  /** True if this region is currently missing any required roles */
  private volatile boolean isMissingRequiredRoles = false;

  /**
   * True if this region is has any required roles defined and the LossAction is either NO_ACCESS or
   * LIMITED_ACCESS. Reliability checks will only happen if this is true.
   */
  private final boolean requiresReliabilityCheck;

  /**
   * Latch that is opened after initialization waits for required roles up to the
   * <a href="DistributedSystem#member-timeout">member-timeout </a>.
   */
  private final StoppableCountDownLatch initializationLatchAfterMemberTimeout;

  private final PersistenceAdvisor persistenceAdvisor;

  private final PersistentMemberID persistentId;

  /**
   * This boolean is set to false when this region is non-persistent, but there are persistent
   * members in the distributed system to which all region modifications should be forwarded see bug
   * 45186
   */
  private volatile boolean generateVersionTag = true;

  /** Tests can set this to true and ignore reliability triggered reconnects */
  public static boolean ignoreReconnect = false;

  /**
   * Lock to prevent multiple threads on this member from performing a clear at the same time.
   */
  private final Object clearLock = new Object();

  private static final AtomicBoolean loggedNetworkPartitionWarning = new AtomicBoolean(false);

  /** Creates a new instance of DistributedRegion */
  protected DistributedRegion(String regionName, RegionAttributes attrs, LocalRegion parentRegion,
      InternalCache cache, InternalRegionArguments internalRegionArgs) {
    super(regionName, attrs, parentRegion, cache, internalRegionArgs);
    this.initializationLatchAfterMemberTimeout =
        new StoppableCountDownLatch(getCancelCriterion(), 1);
    this.distAdvisor = createDistributionAdvisor(internalRegionArgs);

    if (getDistributionManager().getConfig().getEnableNetworkPartitionDetection()
        && !isInternalRegion() && !attrs.getScope().isAck() && !doesNotDistribute()
        && attrs.getDataPolicy().withStorage()) {
      logger.warn(LocalizedMessage.create(
          LocalizedStrings.DistributedRegion_REGION_0_1_SPLITBRAIN_CONFIG_WARNING,
          new Object[] {regionName, attrs.getScope()}));
    }
    if (!getDistributionManager().getConfig().getEnableNetworkPartitionDetection()
        && attrs.getDataPolicy().withPersistence()
        && !loggedNetworkPartitionWarning.getAndSet(true)) {
      logger.warn(LocalizedMessage.create(
          LocalizedStrings.DistributedRegion_REGION_0_ENABLE_NETWORK_PARTITION_WARNING,
          new Object[] {regionName, attrs.getScope()}));
    }

    boolean setRequiresReliabilityCheck = attrs.getMembershipAttributes().hasRequiredRoles() &&
    // note that the following includes NO_ACCESS, LIMITED_ACCESS,
        !attrs.getMembershipAttributes().getLossAction().isAllAccess()
        && !attrs.getMembershipAttributes().getLossAction().isReconnect();

    // this optimization is safe for as long as Roles and Required Roles are
    // immutable
    // if this VM fulfills all required roles, make requiresReliabilityCheck
    // false
    Set reqRoles = new HashSet(attrs.getMembershipAttributes().getRequiredRoles());
    reqRoles.removeAll(getSystem().getDistributedMember().getRoles());
    if (reqRoles.isEmpty()) {
      setRequiresReliabilityCheck = false;
    }

    this.requiresReliabilityCheck = setRequiresReliabilityCheck;

    if (internalRegionArgs.isUsedForPartitionedRegionBucket()) {
      this.persistenceAdvisor = internalRegionArgs.getPersistenceAdvisor();
    } else if (allowsPersistence()) {
      // TODO: prpersist - using this lock service is a hack. Maybe? Or maybe
      // it's ok if we have one (rarely used) lock service for many operations?
      // What does the resource manager do?
      DistributedLockService dl = cache.getPartitionedRegionLockService();
      try {
        // TODO: prpersist - this is just a quick and dirty storage mechanism so that
        // I can test the storage.
        DiskRegionStats diskStats;
        PersistentMemberView storage;
        if (getDataPolicy().withPersistence()) {
          storage = getDiskRegion();
          diskStats = getDiskRegion().getStats();
        } else {
          storage = new InMemoryPersistentMemberView();
          diskStats = null;
        }
        PersistentMemberManager memberManager = cache.getPersistentMemberManager();
        this.persistenceAdvisor = new PersistenceAdvisorImpl(this.distAdvisor, dl, storage,
            this.getFullPath(), diskStats, memberManager);
      } catch (Exception ignore) {
        throw new InternalGemFireError("Couldn't recover persistence");
      }
    } else {
      this.persistenceAdvisor = null;
    }
    if (this.persistenceAdvisor != null) {
      this.persistentId = this.persistenceAdvisor.generatePersistentID();
    } else {
      this.persistentId = null;
    }
  }

  @Override
  protected EventTracker createEventTracker() {
    EventTracker tracker = new DistributedEventTracker(cache, stopper, getName());
    tracker.start();
    return tracker;
  }

  /**
   * Record the event state from image provider
   * 
   * @param provider the member that provided the initial image and event state
   */
  protected void recordEventStateFromImageProvider(InternalDistributedMember provider) {
    // No Op. Only Bucket region will initiate event states
  }

  /**
   * Intended for used during construction of a DistributedRegion
   * 
   * @return the advisor to be used by the region
   */
  protected CacheDistributionAdvisor createDistributionAdvisor(
      InternalRegionArguments internalRegionArgs) {
    // Warning: potential early escape of object before full construction
    return CacheDistributionAdvisor.createCacheDistributionAdvisor(this);
  }

  /**
   * Does this region support persistence?
   */
  public boolean allowsPersistence() {
    return true;
  }

  @Override
  public boolean requiresOneHopForMissingEntry(EntryEventImpl event) {
    // received from another member - don't use one-hop
    if (event.isOriginRemote()) {
      return false;
    }
    // local ops aren't distributed
    if (event.getOperation().isLocal()) {
      return false;
    }
    // if it already has a valid version tag it can go out with a DistributedCacheOperation
    if (event.getVersionTag() != null && event.getVersionTag().getRegionVersion() > 0) {
      return false;
    }
    // if we're not allowed to generate a version tag we need to send it to someone who can
    if (!this.generateVersionTag) {
      return true;
    }
    return this.concurrencyChecksEnabled && (this.serverRegionProxy == null) && !isTX()
        && this.scope.isDistributed() && !this.dataPolicy.withReplication();
  }


  @Override
  protected boolean virtualPut(EntryEventImpl event, boolean ifNew, boolean ifOld,
      Object expectedOldValue, boolean requireOldValue, long lastModified,
      boolean overwriteDestroyed) throws TimeoutException, CacheWriterException {
    final boolean isTraceEnabled = logger.isTraceEnabled();

    Lock dlock = null;
    if (this.scope.isGlobal() && // lock only applies to global scope
        !event.isOriginRemote() && // only if operation originating locally
        !event.isNetSearch() && // search and load processor handles own locking
        !event.isNetLoad() &&
        // @todo darrel/kirk: what about putAll?
        !event.isLocalLoad() && !event.isSingleHopPutOp()) {
      // Single Hop Op means dlock is already taken at origin node.
      dlock = this.getDistributedLockIfGlobal(event.getKey());
    }
    if (isTraceEnabled) {
      logger.trace("virtualPut invoked for event {}", event);
    }
    try {
      if (!hasSeenEvent(event)) {
        if (this.requiresOneHopForMissingEntry(event)) {
          // bug #45704: see if a one-hop must be done for this operation
          RegionEntry re = getRegionEntry(event.getKey());
          if (re == null /* || re.isTombstone() */ || !this.generateVersionTag) {
            if (!event.isBulkOpInProgress() || this.dataPolicy.withStorage()) {
              // putAll will send a single one-hop for empty regions. for other missing entries
              // we need to get a valid version number before modifying the local cache
              boolean didDistribute = RemotePutMessage.distribute(event, lastModified, false, false,
                  expectedOldValue, requireOldValue, !this.generateVersionTag);

              if (!didDistribute && isTraceEnabled) {
                logger.trace("Unable to perform one-hop messaging");
              }
              if (!this.generateVersionTag && !didDistribute) {
                throw new PersistentReplicatesOfflineException();
              }
              if (didDistribute) {
                if (isTraceEnabled) {
                  logger.trace("Event after remotePut operation: {}", event);
                }
                if (event.getVersionTag() == null) {
                  // if the event wasn't applied by the one-hop replicate it will not have a version
                  // tag
                  // and so should not be applied to this cache
                  return false;
                }
              }
            }
          }
        }
        return super.virtualPut(event, ifNew, ifOld, expectedOldValue, requireOldValue,
            lastModified, overwriteDestroyed);
      } else {
        if (event.getDeltaBytes() != null && event.getRawNewValue() == null) {
          // This means that this event has delta bytes but no full value.
          // Request the full value of this event.
          // The value in this vm may not be same as this event's value.
          throw new InvalidDeltaException(
              "Cache encountered replay of event containing delta bytes for key " + event.getKey());
        }
        // if the listeners have already seen this event, then it has already
        // been successfully applied to the cache. Distributed messages and
        // return
        if (isTraceEnabled) {
          logger.trace("DR.virtualPut: this cache has already seen this event {}", event);
        }

        // Fix 39014: when hasSeenEvent, put will still distribute
        // event, but putAll did not. We add the logic back here, not to put
        // back into DR.distributeUpdate() because we moved this part up into
        // LR.basicPutPart3 in purpose.
        if (event.isBulkOpInProgress() && !event.isOriginRemote()) {
          event.getPutAllOperation().addEntry(event, true);
        }

        /*
         * doing this so that other VMs will apply this no matter what. If it is an "update" they
         * will not apply it if they don't have the key. Because this is probably a retry, it will
         * never get applied to this local AbstractRegionMap, and so will never be flipped to a
         * 'create'
         */
        event.makeCreate();
        if (!getConcurrencyChecksEnabled() || event.hasValidVersionTag()) {
          distributeUpdate(event, lastModified, ifNew, ifOld, expectedOldValue, requireOldValue);
          event.invokeCallbacks(this, true, true);
        }
        return true;
      }
    } finally {
      if (dlock != null) {
        dlock.unlock();
      }
    }
  }

  @Override
  protected RegionEntry basicPutEntry(EntryEventImpl event, long lastModified)
      throws TimeoutException, CacheWriterException {

    final boolean isTraceEnabled = logger.isTraceEnabled();

    if (isTraceEnabled) {
      logger.trace("basicPutEntry invoked for event {}", event);
    }
    if (this.requiresOneHopForMissingEntry(event)) {
      // bug #45704: see if a one-hop must be done for this operation
      RegionEntry re = getRegionEntry(event.getKey());
      if (re == null /* || re.isTombstone() */ || !this.generateVersionTag) {
        final boolean ifNew = false;
        final boolean ifOld = false;
        boolean didDistribute = RemotePutMessage.distribute(event, lastModified, ifNew, ifOld, null,
            false, !this.generateVersionTag);
        if (!this.generateVersionTag && !didDistribute) {
          throw new PersistentReplicatesOfflineException();
        }
        if (didDistribute && isTraceEnabled) {
          logger.trace("Event after remotePut for basicPutEntry: {}", event);
        }
      }
    }
    return super.basicPutEntry(event, lastModified);
  }

  @Override
  public void performPutAllEntry(EntryEventImpl event) {
    /*
     * force shared data view so that we just do the virtual op, accruing things in the put all
     * operation for later
     */
    if (isTX()) {
      event.getPutAllOperation().addEntry(event);
    } else {
      getSharedDataView().putEntry(event, false, false, null, false, 0L, false);
    }
  }

  @Override
  public void performRemoveAllEntry(EntryEventImpl event) {
    // force shared data view so that we just do the virtual op, accruing things in the bulk
    // operation for later
    if (isTX()) {
      event.getRemoveAllOperation().addEntry(event);
    } else {
      basicDestroy(event, true, null);
    }
  }

  /**
   * distribution and listener notification
   */
  @Override
  public void basicPutPart3(EntryEventImpl event, RegionEntry entry, boolean isInitialized,
      long lastModified, boolean invokeCallbacks, boolean ifNew, boolean ifOld,
      Object expectedOldValue, boolean requireOldValue) {

    distributeUpdate(event, lastModified, false, false, null, false);
    super.basicPutPart3(event, entry, isInitialized, lastModified, invokeCallbacks, ifNew, ifOld,
        expectedOldValue, requireOldValue);
  }

  /** distribute an update operation */
  protected void distributeUpdate(EntryEventImpl event, long lastModified, boolean ifNew,
      boolean ifOld, Object expectedOldValue, boolean requireOldValue) {
    // an update from a netSearch is not distributed
    if (!event.isOriginRemote() && !event.isNetSearch() && !event.isBulkOpInProgress()) {
      boolean distribute = true;
      if (event.getInhibitDistribution()) {
        // this has already been distributed by a one-hop operation
        distribute = false;
      }
      if (distribute) {
        // before distribute: DR's put, it has notified gateway sender earlier
        UpdateOperation op = new UpdateOperation(event, lastModified);
        if (logger.isTraceEnabled()) {
          logger.trace("distributing operation for event : {} : for region : {}", event,
              this.getName());
        }
        op.distribute();
      }
    }
  }

  @Override
  public boolean hasSeenEvent(EntryEventImpl event) {
    boolean isDuplicate = false;

    isDuplicate = getEventTracker().hasSeenEvent(event);
    if (isDuplicate) {
      markEventAsDuplicate(event);
    } else {
      // bug #48205 - a retried PR operation may already have a version assigned to it
      // in another VM
      if (event.isPossibleDuplicate() && event.getRegion().concurrencyChecksEnabled
          && event.getVersionTag() == null && event.getEventId() != null) {
        boolean isBulkOp = event.getOperation().isPutAll() || event.getOperation().isRemoveAll();
        VersionTag tag =
            FindVersionTagOperation.findVersionTag(event.getRegion(), event.getEventId(), isBulkOp);
        event.setVersionTag(tag);
      }
    }
    return isDuplicate;
  }

  private void markEventAsDuplicate(EntryEventImpl event) {
    event.setPossibleDuplicate(true);
    if (concurrencyChecksEnabled && event.getVersionTag() == null) {
      if (event.isBulkOpInProgress()) {
        event.setVersionTag(getEventTracker().findVersionTagForBulkOp(event.getEventId()));
      } else {
        event.setVersionTag(getEventTracker().findVersionTagForSequence(event.getEventId()));
      }
    }
  }

  void setGeneratedVersionTag(boolean generateVersionTag) {
    // there is at-least one other persistent member, so turn on concurrencyChecks
    enableConcurrencyChecks();

    this.generateVersionTag = generateVersionTag;
  }

  protected boolean getGenerateVersionTag() {
    return this.generateVersionTag;
  }

  @Override
  protected boolean shouldGenerateVersionTag(RegionEntry entry, EntryEventImpl event) {
    if (logger.isTraceEnabled()) {
      logger.trace(
          "shouldGenerateVersionTag this.generateVersionTag={} ccenabled={} dataPolicy={} event:{}",
          this.generateVersionTag, this.concurrencyChecksEnabled, this.dataPolicy, event);
    }
    if (!this.concurrencyChecksEnabled || this.dataPolicy == DataPolicy.EMPTY
        || !this.generateVersionTag) {
      return false;
    }
    if (this.serverRegionProxy != null) { // client
      return false;
    }
    if (event.getVersionTag() != null && !event.getVersionTag().isGatewayTag()) {
      return false;
    }
    if (event.getOperation().isLocal()) { // bug #45402 - localDestroy generated a version tag
      return false;
    }
    if (!event.isOriginRemote() && this.dataPolicy.withReplication()) {
      return true;
    }
    if (!this.dataPolicy.withReplication() && !this.dataPolicy.withPersistence()) {
      if (!entry.getVersionStamp().hasValidVersion()) {
        // do not generate a version stamp in a region that has no replication if it's not based
        // on an existing version from a replicate region
        return false;
      }
      return true;
    }
    if (!event.isOriginRemote() && event.getDistributedMember() != null) {
      if (!event.getDistributedMember().equals(this.getMyId())) {
        return event.getVersionTag() == null; // one-hop remote message
      }
    }
    return false;
  }

  /**
   * Throws RegionAccessException if required roles are missing and the LossAction is NO_ACCESS
   * 
   * @throws RegionAccessException if required roles are missing and the LossAction is NO_ACCESS
   */
  @Override
  protected void checkForNoAccess() {
    if (this.requiresReliabilityCheck && this.isMissingRequiredRoles) {
      if (getMembershipAttributes().getLossAction().isNoAccess()) {
        synchronized (this.missingRequiredRoles) {
          if (!this.isMissingRequiredRoles)
            return;
          Set roles = Collections.unmodifiableSet(new HashSet(this.missingRequiredRoles));
          throw new RegionAccessException(
              LocalizedStrings.DistributedRegion_OPERATION_IS_DISALLOWED_BY_LOSSACTION_0_BECAUSE_THESE_REQUIRED_ROLES_ARE_MISSING_1
                  .toLocalizedString(getMembershipAttributes().getLossAction(), roles),
              getFullPath(), roles);
        }
      }
    }
  }

  /**
   * Throws RegionAccessException is required roles are missing and the LossAction is either
   * NO_ACCESS or LIMITED_ACCESS.
   * 
   * @throws RegionAccessException if required roles are missing and the LossAction is either
   *         NO_ACCESS or LIMITED_ACCESS
   */
  @Override
  protected void checkForLimitedOrNoAccess() {
    if (this.requiresReliabilityCheck && this.isMissingRequiredRoles) {
      if (getMembershipAttributes().getLossAction().isNoAccess()
          || getMembershipAttributes().getLossAction().isLimitedAccess()) {
        synchronized (this.missingRequiredRoles) {
          if (!this.isMissingRequiredRoles)
            return;
          Set roles = Collections.unmodifiableSet(new HashSet(this.missingRequiredRoles));
          Assert.assertTrue(!roles.isEmpty());
          throw new RegionAccessException(
              LocalizedStrings.DistributedRegion_OPERATION_IS_DISALLOWED_BY_LOSSACTION_0_BECAUSE_THESE_REQUIRED_ROLES_ARE_MISSING_1
                  .toLocalizedString(getMembershipAttributes().getLossAction(), roles),
              getFullPath(), roles);
        }
      }
    }
  }

  @Override
  protected void handleReliableDistribution(Set successfulRecipients) {
    handleReliableDistribution(successfulRecipients, Collections.emptySet(),
        Collections.emptySet());
  }

  private void handleReliableDistribution(Set successfulRecipients, Set otherRecipients1,
      Set otherRecipients2) {
    if (this.requiresReliabilityCheck) {
      MembershipAttributes ra = getMembershipAttributes();
      // determine the successful roles
      Set roles = new HashSet();
      for (Object successfulRecipient : successfulRecipients) {
        InternalDistributedMember mbr = (InternalDistributedMember) successfulRecipient;
        if (mbr != null) {
          roles.addAll(mbr.getRoles());
        }
      }
      for (Object anOtherRecipients1 : otherRecipients1) {
        InternalDistributedMember mbr = (InternalDistributedMember) anOtherRecipients1;
        if (mbr != null) {
          roles.addAll(mbr.getRoles());
        }
      }
      for (Object anOtherRecipients2 : otherRecipients2) {
        InternalDistributedMember mbr = (InternalDistributedMember) anOtherRecipients2;
        if (mbr != null) {
          roles.addAll(mbr.getRoles());
        }
      }
      // determine the missing roles
      Set failedRoles = new HashSet(ra.getRequiredRoles());
      failedRoles.removeAll(roles);
      if (failedRoles.isEmpty()) {
        return;
      }

      throw new RegionDistributionException(
          LocalizedStrings.DistributedRegion_OPERATION_DISTRIBUTION_MAY_HAVE_FAILED_TO_NOTIFY_THESE_REQUIRED_ROLES_0
              .toLocalizedString(failedRoles),
          getFullPath(), failedRoles);
    }
  }

  /**
   * Called when we do a distributed operation and don't have anyone to distributed it too. Since
   * this is only called when no distribution was done (i.e. no recipients) we do not check
   * isMissingRequiredRoles because it might not longer be true due to race conditions
   * 
   * @return false if this region has at least one required role and queuing is configured. Returns
   *         true if sending to no one is ok.
   * @throws RoleException if a required role is missing and the LossAction is either NO_ACCESS or
   *         LIMITED_ACCESS.
   * @since GemFire 5.0
   */
  boolean isNoDistributionOk() {
    if (this.requiresReliabilityCheck) {
      MembershipAttributes ra = getMembershipAttributes();
      Set<Role> failedRoles = ra.getRequiredRoles();
      throw new RegionDistributionException(
          LocalizedStrings.DistributedRegion_OPERATION_DISTRIBUTION_WAS_NOT_DONE_TO_THESE_REQUIRED_ROLES_0
              .toLocalizedString(failedRoles),
          getFullPath(), failedRoles);
    }
    return true;
  }

  /**
   * returns true if this Region does not distribute its operations to other members.
   * 
   * @since GemFire 6.0
   * @see HARegion#localDestroyNoCallbacks(Object)
   */
  public boolean doesNotDistribute() {
    return false;
  }

  @Override
  public boolean shouldSyncForCrashedMember(InternalDistributedMember id) {
    return !doesNotDistribute() && super.shouldSyncForCrashedMember(id);
  }

  @Override
  public boolean requiresReliabilityCheck() {
    return this.requiresReliabilityCheck;
  }

  /**
   * Returns true if the ExpiryTask is currently allowed to expire.
   * <p>
   * If the region is in NO_ACCESS due to reliability configuration, then no expiration actions are
   * allowed.
   * <p>
   * If the region is in LIMITED_ACCESS due to reliability configuration, then only non-distributed
   * expiration actions are allowed.
   */
  @Override
  protected boolean isExpirationAllowed(ExpiryTask expiry) {
    if (this.requiresReliabilityCheck && this.isMissingRequiredRoles) {
      if (getMembershipAttributes().getLossAction().isNoAccess()) {
        return false;
      }
      if (getMembershipAttributes().getLossAction().isLimitedAccess()
          && expiry.isDistributedAction()) {
        return false;
      }
    }
    return true;
  }

  /**
   * Performs the resumption action when reliability is resumed.
   * 
   * @return true if asynchronous resumption is triggered
   */
  private boolean resumeReliability(InternalDistributedMember id, Set newlyAcquiredRoles) {
    boolean async = false;
    try {
      ResumptionAction ra = getMembershipAttributes().getResumptionAction();
      if (ra.isNone()) {
        if (logger.isDebugEnabled()) {
          logger.debug("Reliability resumption for action of none");
        }
        resumeExpiration();
      } else if (ra.isReinitialize()) {
        async = true;
        asyncResumeReliability(id, newlyAcquiredRoles);
      }
    } catch (Exception e) {
      logger.fatal(LocalizedMessage.create(LocalizedStrings.DistributedRegion_UNEXPECTED_EXCEPTION),
          e);
    }
    return async;
  }

  /**
   * Handles asynchronous ResumptionActions such as region reinitialize.
   */
  private void asyncResumeReliability(final InternalDistributedMember id,
      final Set newlyAcquiredRoles) throws RejectedExecutionException {
    final ResumptionAction ra = getMembershipAttributes().getResumptionAction();
    getDistributionManager().getWaitingThreadPool().execute(new Runnable() {
      @Override
      public void run() {
        try {
          if (ra.isReinitialize()) {
            if (logger.isDebugEnabled()) {
              logger.debug("Reliability resumption for action of reinitialize");
            }
            if (!isDestroyed() && !cache.isClosed()) {
              RegionEventImpl event = new RegionEventImpl(DistributedRegion.this,
                  Operation.REGION_REINITIALIZE, null, false, getMyId(), generateEventID());
              reinitialize(null, event);
            }
            synchronized (missingRequiredRoles) {
              // any number of threads may be waiting on missingRequiredRoles
              missingRequiredRoles.notifyAll();
              if (hasListener() && id != null) {
                // fire afterRoleGain event
                RoleEventImpl relEvent = new RoleEventImpl(DistributedRegion.this,
                    Operation.REGION_CREATE, null, true, id, newlyAcquiredRoles);
                dispatchListenerEvent(EnumListenerEvent.AFTER_ROLE_GAIN, relEvent);
              }
            }
          }
        } catch (Exception e) {
          logger.fatal(
              LocalizedMessage.create(LocalizedStrings.DistributedRegion_UNEXPECTED_EXCEPTION), e);
        }
      }
    });
  }

  /** Reschedules expiry tasks when reliability is resumed. */
  private void resumeExpiration() {
    boolean isNoAccess = getMembershipAttributes().getLossAction().isNoAccess();
    boolean isLimitedAccess = getMembershipAttributes().getLossAction().isLimitedAccess();
    if (!(isNoAccess || isLimitedAccess)) {
      return; // early out: expiration was never affected by reliability
    }

    if (getEntryTimeToLive().getTimeout() > 0
        && (isNoAccess || (isLimitedAccess && getEntryTimeToLive().getAction().isDistributed()))) {
      rescheduleEntryExpiryTasks();
    } else if (getEntryIdleTimeout().getTimeout() > 0
        && (isNoAccess || (isLimitedAccess && getEntryIdleTimeout().getAction().isDistributed()))) {
      rescheduleEntryExpiryTasks();
    } else if (getCustomEntryTimeToLive() != null || getCustomEntryIdleTimeout() != null) {
      // Force all entries to be rescheduled
      rescheduleEntryExpiryTasks();
    }

    if (getRegionTimeToLive().getTimeout() > 0
        && (isNoAccess || (isLimitedAccess && getRegionTimeToLive().getAction().isDistributed()))) {
      addTTLExpiryTask();
    }
    if (getRegionIdleTimeout().getTimeout() > 0 && (isNoAccess
        || (isLimitedAccess && getRegionIdleTimeout().getAction().isDistributed()))) {
      addIdleExpiryTask();
    }
  }

  /**
   * A boolean used to indicate if its the intialization time i.e the distributed Region is created
   * for the first time. The variable is used at the time of lost reliablility.
   */
  private boolean isInitializingThread = false;

  /**
   * Called when reliability is lost. If MembershipAttributes are configured with
   * {@link LossAction#RECONNECT}then DistributedSystem reconnect will be called asynchronously.
   * 
   * @return true if asynchronous resumption is triggered
   */
  private boolean lostReliability(final InternalDistributedMember id, final Set newlyMissingRoles) {
    if (DistributedRegion.ignoreReconnect) { // test hook
      return false;
    }

    boolean async = false;
    try {
      if (getMembershipAttributes().getLossAction().isReconnect()) {
        async = true;
        if (this.isInitializingThread) {
          doLostReliability(true, id, newlyMissingRoles);
        } else {
          doLostReliability(false, id, newlyMissingRoles);
        }
        // we don't do this in the waiting pool because we're going to
        // disconnect
        // the distributed system, and it will wait for the pool to empty
      }
    } catch (CancelException cce) {
      throw cce;
    } catch (Exception e) {
      logger.fatal(LocalizedMessage.create(LocalizedStrings.DistributedRegion_UNEXPECTED_EXCEPTION),
          e);
    }
    return async;
  }

  private void doLostReliability(boolean isInitializing, final InternalDistributedMember id,
      final Set newlyMissingRoles) {
    try {
      if (!isInitializing) {
        // moved code to a new thread.
        Thread thread = new Thread(
            LocalizedStrings.DistributedRegion_RECONNECT_DISTRIBUTED_SYSTEM.toLocalizedString()) {
          @Override
          public void run() {
            try {
              logger.debug(
                  "Reliability loss with policy of reconnect and membership thread doing reconnect");

              initializationLatchAfterMemberTimeout.await();
              getSystem().tryReconnect(false, "Role Loss", getCache());

              synchronized (missingRequiredRoles) {
                // any number of threads may be waiting on missingRequiredRoles
                missingRequiredRoles.notifyAll();
                // need to fire an event if id is not null
                if (hasListener() && id != null) {
                  RoleEventImpl relEvent = new RoleEventImpl(DistributedRegion.this,
                      Operation.CACHE_RECONNECT, null, true, id, newlyMissingRoles);
                  dispatchListenerEvent(EnumListenerEvent.AFTER_ROLE_LOSS, relEvent);
                }
              }
            } catch (Exception e) {
              logger.fatal(
                  LocalizedMessage.create(LocalizedStrings.DistributedRegion_UNEXPECTED_EXCEPTION),
                  e);
            }
          }
        };
        thread.setDaemon(true);
        thread.start();

      } else {
        getSystem().tryReconnect(false, "Role Loss", getCache()); // added for
        // reconnect.
        synchronized (this.missingRequiredRoles) {
          // any number of threads may be waiting on missingRequiredRoles
          this.missingRequiredRoles.notifyAll();
          // need to fire an event if id is not null
          if (hasListener() && id != null) {
            RoleEventImpl relEvent = new RoleEventImpl(DistributedRegion.this,
                Operation.CACHE_RECONNECT, null, true, id, newlyMissingRoles);
            dispatchListenerEvent(EnumListenerEvent.AFTER_ROLE_LOSS, relEvent);
          }
        }
      }
    } catch (CancelException ignor) {
      throw ignor;
    } catch (Exception e) {
      logger.fatal(LocalizedMessage.create(LocalizedStrings.DistributedRegion_UNEXPECTED_EXCEPTION),
          e);
    }
  }

  void lockCheckReadiness() { // package-private to avoid synthetic accessor
    // fix for bug 32610
    this.cache.getCancelCriterion().checkCancelInProgress(null);
    checkReadiness();
  }

  @Override
  public Object validatedDestroy(Object key, EntryEventImpl event)
      throws TimeoutException, EntryNotFoundException, CacheWriterException {
    Lock dlock = this.getDistributedLockIfGlobal(key);
    try {
      return super.validatedDestroy(key, event);
    } finally {
      if (dlock != null) {
        dlock.unlock();
      }
    }
  }

  @Override
  public void localDestroyNoCallbacks(Object key) {
    super.localDestroyNoCallbacks(key);
    if (getScope().isGlobal()) {
      try {
        this.getLockService().freeResources(key);
      } catch (LockServiceDestroyedException ignore) {
      }
    }
  }

  @Override
  public void localDestroy(Object key, Object aCallbackArgument) throws EntryNotFoundException {
    super.localDestroy(key, aCallbackArgument);
    if (getScope().isGlobal()) {
      try {
        this.getLockService().freeResources(key);
      } catch (LockServiceDestroyedException ignore) {
      }
    }
  }

  @Override
  public void invalidate(Object key, Object aCallbackArgument)
      throws TimeoutException, EntryNotFoundException {
    validateKey(key);
    checkReadiness();
    checkForLimitedOrNoAccess();
    Lock dlock = this.getDistributedLockIfGlobal(key);
    try {
      validatedInvalidate(key, aCallbackArgument);
    } finally {
      if (dlock != null)
        dlock.unlock();
    }
  }

  @Override
  public Lock getRegionDistributedLock() throws IllegalStateException {
    lockCheckReadiness();
    checkForLimitedOrNoAccess();
    if (!this.scope.isGlobal()) {
      throw new IllegalStateException(
          LocalizedStrings.DistributedRegion_DISTRIBUTION_LOCKS_ARE_ONLY_SUPPORTED_FOR_REGIONS_WITH_GLOBAL_SCOPE_NOT_0
              .toLocalizedString(this.scope));
    }
    return new RegionDistributedLock();
  }

  @Override
  public Lock getDistributedLock(Object key) throws IllegalStateException {
    validateKey(key);
    lockCheckReadiness();
    checkForLimitedOrNoAccess();
    if (!this.scope.isGlobal()) {
      throw new IllegalStateException(
          LocalizedStrings.DistributedRegion_DISTRIBUTION_LOCKS_ARE_ONLY_SUPPORTED_FOR_REGIONS_WITH_GLOBAL_SCOPE_NOT_0
              .toLocalizedString(this.scope));
    }
    if (isLockingSuspendedByCurrentThread()) {
      throw new IllegalStateException(
          LocalizedStrings.DistributedRegion_THIS_THREAD_HAS_SUSPENDED_ALL_LOCKING_FOR_THIS_REGION
              .toLocalizedString());
    }
    return new DistributedLock(key);
  }

  /**
   * Called while NOT holding lock on parent's subregions
   * 
   * @throws IllegalStateException if region is not compatible with a region in another VM.
   */
  @Override
  protected void initialize(InputStream snapshotInputStream, InternalDistributedMember imageTarget,
      InternalRegionArguments internalRegionArgs)
      throws TimeoutException, IOException, ClassNotFoundException {
    Assert.assertTrue(!isInitialized());
    if (logger.isDebugEnabled()) {
      logger.debug("DistributedRegion.initialize BEGIN: {}", getFullPath());
    }

    // if we're versioning entries we need a region-level version vector
    if (this.scope.isDistributed() && this.concurrencyChecksEnabled) {
      createVersionVector();
    }

    if (this.scope.isGlobal()) {
      getLockService(); // create lock service eagerly now
    }

    try {
      try {
        PersistentMemberID persistentMemberId = null;
        boolean recoverFromDisk = isRecoveryNeeded();
        DiskRegion dskRgn = getDiskRegion();
        if (recoverFromDisk) {
          if (logger.isDebugEnabled()) {
            logger.debug("DistributedRegion.getInitialImageAndRecovery: Starting Recovery");
          }
          dskRgn.initializeOwner(this); // do recovery
          if (logger.isDebugEnabled()) {
            logger.debug("DistributedRegion.getInitialImageAndRecovery: Finished Recovery");
          }
          persistentMemberId = dskRgn.getMyPersistentID();
        }

        // Create OQL indexes before starting GII.
        createOQLIndexes(internalRegionArgs, recoverFromDisk);

        if (getDataPolicy().withReplication() || getDataPolicy().withPreloaded()) {
          getInitialImageAndRecovery(snapshotInputStream, imageTarget, internalRegionArgs,
              recoverFromDisk, persistentMemberId);
        } else {
          new CreateRegionProcessor(this).initializeRegion();
          if (snapshotInputStream != null) {
            releaseBeforeGetInitialImageLatch();
            loadSnapshotDuringInitialization(snapshotInputStream);
          }
        }
      } catch (DiskAccessException dae) {
        this.handleDiskAccessException(dae, true);
        throw dae;
      }

      initMembershipRoles();
      this.isInitializingThread = false;
      // makes sure all latches are released if they haven't been already
      super.initialize(null, null, null);
    } finally {
      getEventTracker().setInitialized();
    }
  }

  @Override
  public void initialized() {
    new UpdateAttributesProcessor(this).distribute(false);
  }

  /** True if GII was impacted by missing required roles */
  private boolean giiMissingRequiredRoles = false;

  /**
   * A reference counter to protected the memoryThresholdReached boolean
   */
  private final Set<DistributedMember> memoryThresholdReachedMembers = new CopyOnWriteArraySet<>();

  // TODO: cleanup getInitialImageAndRecovery
  private void getInitialImageAndRecovery(InputStream snapshotInputStream,
      InternalDistributedMember imageSrc, InternalRegionArguments internalRegionArgs,
      boolean recoverFromDisk, PersistentMemberID persistentId) throws TimeoutException {
    logger.info(LocalizedMessage.create(LocalizedStrings.DistributedRegion_INITIALIZING_REGION_0,
        this.getName()));

    ImageState imgState = getImageState();
    imgState.init();
    boolean targetRecreated = internalRegionArgs.getRecreateFlag();
    Boolean isCBool = (Boolean) isConversion.get();
    boolean isForConversion = isCBool != null ? isCBool : false;

    if (recoverFromDisk && snapshotInputStream != null && !isForConversion) {
      throw new InternalGemFireError(
          LocalizedStrings.DistributedRegion_IF_LOADING_A_SNAPSHOT_THEN_SHOULD_NOT_BE_RECOVERING_ISRECOVERING_0_SNAPSHOTSTREAM_1
              .toLocalizedString(new Object[] {true, snapshotInputStream}));
    }

    ProfileExchangeProcessor targetProvider;
    if (this.dataPolicy.withPersistence()) {
      targetProvider =
          new CreatePersistentRegionProcessor(this, getPersistenceAdvisor(), recoverFromDisk);
    } else {
      // this will go in the advisor profile
      targetProvider = new CreateRegionProcessor(this);
    }
    imgState.setInRecovery(false);
    RegionVersionVector recovered_rvv = null;
    if (this.dataPolicy.withPersistence()) {
      recovered_rvv = this.getVersionVector() == null ? null
          : this.getVersionVector().getCloneForTransmission();
    }
    // initializeRegion will send out our profile
    targetProvider.initializeRegion();

    if (this.persistenceAdvisor != null) {
      this.persistenceAdvisor.initialize();
    }

    // Register listener here so that the remote members are known
    // since registering calls initializeCriticalMembers (which needs to know about
    // remote members
    if (!isInternalRegion()) {
      if (!this.isDestroyed) {
        this.cache.getInternalResourceManager().addResourceListener(ResourceType.MEMORY, this);
      }
    }

    releaseBeforeGetInitialImageLatch();

    // allow GII to invoke test hooks. Do this just after releasing the
    // before-gii latch for bug #48962. See ConcurrentLeaveDuringGIIDUnitTest
    InitialImageOperation.beforeGetInitialImage(this);

    if (snapshotInputStream != null) {
      try {
        if (logger.isDebugEnabled()) {
          logger.debug(
              "DistributedRegion.getInitialImageAndRecovery: About to load snapshot, isInitialized={}; {}",
              isInitialized(), getFullPath());
        }
        loadSnapshotDuringInitialization(snapshotInputStream);
      } catch (IOException e) {
        throw new RuntimeException(e); // TODO: change this exception?
      } catch (ClassNotFoundException e) {
        throw new RuntimeException(e); // TODO: change this exception?
      }
      cleanUpDestroyedTokensAndMarkGIIComplete(GIIStatus.NO_GII);
      return;
    }

    // No snapshot provided, use the imageTarget(s)

    // if we were given a recommended imageTarget, use that first, and
    // treat it like it is a replicate (regardless of whether it actually is
    // or not)

    InitialImageOperation iiop = new InitialImageOperation(this, this.entries);

    CacheDistributionAdvisor.InitialImageAdvice advice = null;
    boolean done = false;
    while (!done && !isDestroyed()) {
      advice = targetProvider.getInitialImageAdvice(advice);
      boolean attemptGetFromOne = imageSrc != null // we were given a specific member
          || this.dataPolicy.withPreloaded() && !advice.preloaded.isEmpty() // this is a preloaded
                                                                            // region
          || (!advice.replicates.isEmpty());
      // That is: if we have 0 or 1 giiProvider then we can do a getFromOne gii;
      // if we have 2 or more giiProviders then we must do a getFromAll gii.

      if (attemptGetFromOne) {
        if (recoverFromDisk) {
          if (LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER) {
            CacheObserverHolder.getInstance().afterMarkingGIIStarted();
          }
        }
        {
          // If we have an imageSrc and the target is reinitializing mark the
          // getInitialImage so that it will wait until the target region is fully initialized
          // before responding to the get image request. Otherwise, the
          // source may respond with no data because it is still initializing,
          // e.g. loading a snapshot.

          // Plan A: use specified imageSrc, if specified
          if (imageSrc != null) {
            try {
              GIIStatus ret = iiop.getFromOne(Collections.singleton(imageSrc), targetRecreated,
                  advice, recoverFromDisk, recovered_rvv);
              if (GIIStatus.didGII(ret)) {
                this.giiMissingRequiredRoles = false;
                cleanUpDestroyedTokensAndMarkGIIComplete(ret);
                done = true;
                return;
              }
            } finally {
              imageSrc = null;
            }
          }

          // Plan C: use a replicate, if one exists
          GIIStatus ret =
              iiop.getFromOne(advice.replicates, false, advice, recoverFromDisk, recovered_rvv);
          if (GIIStatus.didGII(ret)) {
            cleanUpDestroyedTokensAndMarkGIIComplete(ret);
            done = true;
            return;
          }

          // Plan D: if this is a PRELOADED region, fetch from another PRELOADED
          if (this.dataPolicy.isPreloaded()) {
            GIIStatus ret_preload =
                iiop.getFromOne(advice.preloaded, false, advice, recoverFromDisk, recovered_rvv);
            if (GIIStatus.didGII(ret_preload)) {
              cleanUpDestroyedTokensAndMarkGIIComplete(ret_preload);
              done = true;
              return;
            }
          } // isPreloaded
        }

        // If we got to this point, we failed in the GII. Cleanup
        // any partial image we received
        cleanUpAfterFailedGII(recoverFromDisk);

      } // attemptGetFromOne
      else {
        if (!isDestroyed()) {
          if (recoverFromDisk) {
            logger.info(
                LocalizedMessage.create(LocalizedStrings.DistributedRegion_INITIALIZED_FROM_DISK,
                    new Object[] {this.getFullPath(), persistentId, getPersistentID()}));
            if (persistentId != null) {
              RegionLogger.logRecovery(this.getFullPath(), persistentId,
                  getDistributionManager().getDistributionManagerId());
            }
          } else {
            RegionLogger.logCreate(this.getFullPath(),
                getDistributionManager().getDistributionManagerId());

            if (getPersistentID() != null) {
              RegionLogger.logPersistence(this.getFullPath(),
                  getDistributionManager().getDistributionManagerId(), getPersistentID());
              logger.info(LocalizedMessage.create(
                  LocalizedStrings.DistributedRegion_NEW_PERSISTENT_REGION_CREATED,
                  new Object[] {this.getFullPath(), getPersistentID()}));
            }
          }

          cleanUpDestroyedTokensAndMarkGIIComplete(GIIStatus.NO_GII);
          done = true;
          return;
        }
        break;
      }
    }
  }

  /**
   * If this region has concurrency controls enabled this will pull any missing changes from other
   * replicates using InitialImageOperation and a filtered chunking protocol.
   */
  public void synchronizeForLostMember(InternalDistributedMember lostMember,
      VersionSource lostVersionID) {
    if (!this.concurrencyChecksEnabled) {
      return;
    }
    CacheDistributionAdvisor advisor = getCacheDistributionAdvisor();
    Set<InternalDistributedMember> targets = advisor.adviseInitializedReplicates();
    for (InternalDistributedMember target : targets) {
      synchronizeWith(target, lostVersionID, lostMember);
    }
  }

  /**
   * synchronize with another member wrt messages from the given "lost" member. This can be used
   * when a primary bucket crashes to ensure that interrupted message distribution is mended.
   */
  private void synchronizeWith(InternalDistributedMember target, VersionSource versionMember,
      InternalDistributedMember lostMember) {
    InitialImageOperation op = new InitialImageOperation(this, this.entries);
    op.synchronizeWith(target, versionMember, lostMember);
  }

  /** remove any partial entries received in a failed GII */
  void cleanUpAfterFailedGII(boolean recoverFromDisk) {
    DiskRegion dskRgn = getDiskRegion();
    // if we have a persistent region, instead of deleting everything on disk,
    // we will just reset the "recovered from disk" flag. After
    // the next GII we will delete these entries if they do not come
    // in as part of the GII.
    if (recoverFromDisk && dskRgn != null && dskRgn.isBackup()) {
      dskRgn.resetRecoveredEntries(this);
      return;
    }

    if (!this.entries.isEmpty()) {
      closeEntries();
      if (getDiskRegion() != null) {
        getDiskRegion().clear(this, null);
      }
      // clear the left-members and version-tags sets in imageState
      getImageState().getLeftMembers();
      getImageState().getVersionTags();
      // Clear OQL indexes
      if (this.indexManager != null) {
        try {
          this.indexManager.rerunIndexCreationQuery();
        } catch (Exception ex) {
          if (logger.isDebugEnabled()) {
            logger.debug("Exception while clearing indexes after GII failure.", ex);
          }
        }
      }
    }
  }

  private void initMembershipRoles() {
    synchronized (this.advisorListener) {
      // hold sync to prevent listener from changing initial members
      Set others = this.distAdvisor.addMembershipListenerAndAdviseGeneric(this.advisorListener);
      this.advisorListener.addMembers(others);
      // initialize missing required roles with initial member info
      if (getMembershipAttributes().hasRequiredRoles()) {
        // AdvisorListener will also sync on missingRequiredRoles
        synchronized (this.missingRequiredRoles) {
          this.missingRequiredRoles.addAll(getMembershipAttributes().getRequiredRoles());
          // remove all the roles we are playing since they will never be
          // missing
          this.missingRequiredRoles.removeAll(getSystem().getDistributedMember().getRoles());
          for (Object other1 : others) {
            DistributedMember other = (DistributedMember) other1;
            this.missingRequiredRoles.removeAll(other.getRoles());
          }
        }
      }
    }
    if (getMembershipAttributes().hasRequiredRoles()) {
      // wait up to memberTimeout for required roles...
      // boolean requiredRolesAreMissing = false;
      int memberTimeout = getSystem().getConfig().getMemberTimeout();
      if (logger.isDebugEnabled()) {
        logger.debug("Waiting up to {} for required roles.", memberTimeout);
      }
      try {
        if (this.giiMissingRequiredRoles) {
          // force reliability loss and possibly resumption
          this.isInitializingThread = true;
          synchronized (this.advisorListener) {
            synchronized (this.missingRequiredRoles) {
              // forcing state of loss because of bad GII
              this.isMissingRequiredRoles = true;
              getCachePerfStats().incReliableRegionsMissing(1);
              if (getMembershipAttributes().getLossAction().isAllAccess())
                getCachePerfStats().incReliableRegionsMissingFullAccess(1); // rahul
              else if (getMembershipAttributes().getLossAction().isLimitedAccess())
                getCachePerfStats().incReliableRegionsMissingLimitedAccess(1);
              else if (getMembershipAttributes().getLossAction().isNoAccess())
                getCachePerfStats().incReliableRegionsMissingNoAccess(1);
              // pur code to increment the stats.
              if (logger.isDebugEnabled()) {
                logger.debug("GetInitialImage had missing required roles.");
              }
              // TODO: will this work with RECONNECT and REINITIALIZE?
              this.isInitializingThread = true;
              lostReliability(null, null);
              if (this.missingRequiredRoles.isEmpty()) {
                // all required roles are present so force resumption
                this.isMissingRequiredRoles = false;
                getCachePerfStats().incReliableRegionsMissing(-1);
                if (getMembershipAttributes().getLossAction().isAllAccess())
                  getCachePerfStats().incReliableRegionsMissingFullAccess(-1); // rahul
                else if (getMembershipAttributes().getLossAction().isLimitedAccess())
                  getCachePerfStats().incReliableRegionsMissingLimitedAccess(-1);
                else if (getMembershipAttributes().getLossAction().isNoAccess())
                  getCachePerfStats().incReliableRegionsMissingNoAccess(-1);
                // pur code to increment the stats.
                boolean async = resumeReliability(null, null);
                if (async) {
                  this.advisorListener.destroyed = true;
                }
              }
            }
          }
        } else {
          if (!getSystem().isLoner()) {
            waitForRequiredRoles(memberTimeout);
          }
          synchronized (this.advisorListener) {
            synchronized (this.missingRequiredRoles) {
              if (this.missingRequiredRoles.isEmpty()) {
                Assert.assertTrue(!this.isMissingRequiredRoles);
                if (logger.isDebugEnabled()) {
                  logger.debug("Initialization completed with all required roles present.");
                }
              } else {
                // starting in state of loss...
                this.isMissingRequiredRoles = true;
                getCachePerfStats().incReliableRegionsMissing(1);
                if (getMembershipAttributes().getLossAction().isAllAccess())
                  getCachePerfStats().incReliableRegionsMissingFullAccess(1); // rahul
                else if (getMembershipAttributes().getLossAction().isLimitedAccess())
                  getCachePerfStats().incReliableRegionsMissingLimitedAccess(1);
                else if (getMembershipAttributes().getLossAction().isNoAccess())
                  getCachePerfStats().incReliableRegionsMissingNoAccess(1);

                if (logger.isDebugEnabled()) {
                  logger.debug("Initialization completed with missing required roles: {}",
                      this.missingRequiredRoles);
                }
                this.isInitializingThread = true;
                lostReliability(null, null);
              }
            }
          }
        }
      } catch (RegionDestroyedException ignore) {
        // ignore to fix bug 34639 may be thrown by waitForRequiredRoles
      } catch (CancelException e) {
        // ignore to fix bug 34639 may be thrown by waitForRequiredRoles
        if (this.isInitializingThread) {
          throw e;
        }
      } catch (Exception e) {
        logger.fatal(
            LocalizedMessage.create(LocalizedStrings.DistributedRegion_UNEXPECTED_EXCEPTION), e);
      }

    }
    // open latch which will allow any threads in lostReliability to proceed
    this.initializationLatchAfterMemberTimeout.countDown();
  }

  private boolean isRecoveryNeeded() {
    return getDataPolicy().withPersistence() && getDiskRegion().isRecreated();
  }

  // called by InitialImageOperation to clean up destroyed tokens
  // release afterGetInitialImageInitializationLatch before unlocking
  // cleanUpLock
  @SuppressWarnings("UL_UNRELEASED_LOCK")
  protected void cleanUpDestroyedTokensAndMarkGIIComplete(GIIStatus giiStatus) {
    // We need to clean up the disk before we release the after get initial image latch
    DiskRegion dskRgn = getDiskRegion();
    if (dskRgn != null && dskRgn.isBackup()) {
      dskRgn.finishInitializeOwner(this, giiStatus);
    }
    ImageState is = getImageState();
    is.lockGII();
    // clear the version tag and left-members sets
    is.getVersionTags();
    is.getLeftMembers();
    // remove DESTROYED tokens
    RegionVersionVector rvv = is.getClearRegionVersionVector();
    try {
      Iterator/* <Object> */ keysIt = getImageState().getDestroyedEntries();
      while (keysIt.hasNext()) {
        this.entries.removeIfDestroyed(keysIt.next());
      }
      if (rvv != null) {
        // clear any entries received in the GII that are older than the RVV versions.
        // this can happen if entry chunks were received prior to the clear() being
        // processed
        clearEntries(rvv);
      }
      // need to do this before we release the afterGetInitialImageLatch
      if (this.persistenceAdvisor != null) {
        this.persistenceAdvisor.setOnline(GIIStatus.didGII(giiStatus), false, getPersistentID());
      }
    } finally {
      // release after gii lock first so basicDestroy will see isInitialized()
      // be true
      // when they get the cleanUp lock.
      try {
        releaseAfterGetInitialImageLatch();
      } finally { // make sure unlockGII is done for bug 40001
        is.unlockGII();
      }
    }

    if (LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER) {
      CacheObserverHolder.getInstance().afterMarkingGIICompleted();
    }

    // "Initializing region {0}" which is not acompanied by a completed message. Users think thread
    // is stuck in some operation. Hence adding this log
    logger.info(LocalizedMessage.create(
        LocalizedStrings.DistributedRegion_INITIALIZING_REGION_COMPLETED_0, this.getName()));
  }

  @Override
  public void basicBridgeRemove(Object key, Object expectedOldValue, Object p_callbackArg,
      ClientProxyMembershipID memberId, boolean fromClient, EntryEventImpl clientEvent)
      throws TimeoutException, EntryNotFoundException, CacheWriterException {
    Lock lock = getDistributedLockIfGlobal(key);
    try {
      super.basicBridgeRemove(key, expectedOldValue, p_callbackArg, memberId, fromClient,
          clientEvent);
    } finally {
      if (lock != null) {
        logger.debug("releasing distributed lock on {}", key);
        lock.unlock();
        getLockService().freeResources(key);
      }
    }
  }

  @Override
  public void basicBridgeDestroy(Object key, Object p_callbackArg, ClientProxyMembershipID memberId,
      boolean fromClient, EntryEventImpl clientEvent)
      throws TimeoutException, EntryNotFoundException, CacheWriterException {
    Lock lock = getDistributedLockIfGlobal(key);
    try {
      super.basicBridgeDestroy(key, p_callbackArg, memberId, fromClient, clientEvent);
    } finally {
      if (lock != null) {
        logger.debug("releasing distributed lock on {}", key);
        lock.unlock();
        getLockService().freeResources(key);
      }
    }
  }

  @Override
  public void basicBridgeInvalidate(Object key, Object p_callbackArg,
      ClientProxyMembershipID memberId, boolean fromClient, EntryEventImpl clientEvent)
      throws TimeoutException, EntryNotFoundException, CacheWriterException {
    Lock lock = getDistributedLockIfGlobal(key);
    try {
      super.basicBridgeInvalidate(key, p_callbackArg, memberId, fromClient, clientEvent);
    } finally {
      if (lock != null) {
        logger.debug("releasing distributed lock on {}", key);
        lock.unlock();
      }
    }
  }

  @Override
  protected void basicDestroy(EntryEventImpl event, boolean cacheWrite, Object expectedOldValue)
      throws EntryNotFoundException, CacheWriterException, TimeoutException {
    // disallow local destruction for mirrored keysvalues regions
    boolean hasSeen = false;
    if (hasSeenEvent(event)) {
      hasSeen = true;
    }
    checkIfReplicatedAndLocalDestroy(event);

    try {
      boolean invokeWriter = cacheWrite;
      if (this.requiresOneHopForMissingEntry(event)) {
        // bug #45704: see if a one-hop must be done for this operation
        RegionEntry re = getRegionEntry(event.getKey());
        if (re == null /* || re.isTombstone() */ || !this.generateVersionTag) {
          if (this.serverRegionProxy == null) {
            // only assert for non-client regions.
            Assert.assertTrue(!this.dataPolicy.withReplication() || !this.generateVersionTag);
          }
          if (!event.isBulkOpInProgress() || this.dataPolicy.withStorage()) {
            // removeAll will send a single one-hop for empty regions. for other missing entries
            // we need to get a valid version number before modifying the local cache
            // TODO: deltaGII: verify that delegating to a peer when this region is also a client is
            // acceptable
            boolean didDistribute =
                RemoteDestroyMessage.distribute(event, expectedOldValue, !this.generateVersionTag);

            if (!this.generateVersionTag && !didDistribute) {
              throw new PersistentReplicatesOfflineException();
            }

            if (didDistribute) {
              if (logger.isTraceEnabled()) {
                logger.trace("Event after remoteDestroy operation: {}", event);
              }
              invokeWriter = false; // remote cache invoked the writer
              if (event.getVersionTag() == null) {
                // if the event wasn't applied by the one-hop replicate it will not have a version
                // tag
                // and so should not be applied to this cache
                return;
              }
            }
          }
        }
      }

      super.basicDestroy(event, invokeWriter, expectedOldValue);

      // if this is a destroy coming in from remote source, free up lock resources
      // if this is a local origin destroy, this will happen after lock is
      // released
      if (this.scope.isGlobal() && event.isOriginRemote()) {
        try {
          getLockService().freeResources(event.getKey());
        } catch (LockServiceDestroyedException ignore) {
        }
      }

    } finally {
      if (hasSeen) {
        if (event.isBulkOpInProgress() && !event.isOriginRemote()) {
          event.getRemoveAllOperation().addEntry(event, true);
        }
        if (!getConcurrencyChecksEnabled() || event.hasValidVersionTag()) {
          // before distribute: DR.destroy, hasSeenEvent. no to notifyGateway
          distributeDestroy(event, expectedOldValue);
          event.invokeCallbacks(this, true, false);
        }
      }
    }
  }

  @Override
  void basicDestroyPart3(RegionEntry re, EntryEventImpl event, boolean inTokenMode,
      boolean duringRI, boolean invokeCallbacks, Object expectedOldValue) {

    distributeDestroy(event, expectedOldValue);
    super.basicDestroyPart3(re, event, inTokenMode, duringRI, invokeCallbacks, expectedOldValue);
  }

  void distributeDestroy(EntryEventImpl event, Object expectedOldValue) {
    if (event.isDistributed() && !event.isOriginRemote() && !event.isBulkOpInProgress()) {
      boolean distribute = !event.getInhibitDistribution();
      if (distribute) {
        // before distribute: DR.destroy, it has notifiedGatewaySender ealier
        DestroyOperation op = new DestroyOperation(event);
        op.distribute();
      }
    }
  }

  @Override
  boolean evictDestroy(LRUEntry entry) {
    boolean evictDestroyWasDone = super.evictDestroy(entry);
    if (evictDestroyWasDone) {
      if (this.scope.isGlobal()) {
        try {
          getLockService().freeResources(entry.getKey());
        } catch (LockServiceDestroyedException ignore) {
        }
      }
    }
    return evictDestroyWasDone;
  }

  @Override
  void basicInvalidateRegion(RegionEventImpl event) {
    // disallow local invalidation for replicated regions
    if (!event.getOperation().isDistributed() && getScope().isDistributed()
        && getDataPolicy().withReplication()) {
      throw new IllegalStateException(
          LocalizedStrings.DistributedRegion_NOT_ALLOWED_TO_DO_A_LOCAL_INVALIDATION_ON_A_REPLICATED_REGION
              .toLocalizedString());
    }
    if (shouldDistributeInvalidateRegion(event)) {
      distributeInvalidateRegion(event);
    }
    super.basicInvalidateRegion(event);
  }

  /**
   * decide if InvalidateRegionOperation should be sent to peers. broken out so that BucketRegion
   * can override
   * 
   * @return true if {@link InvalidateRegionOperation} should be distributed, false otherwise
   */
  protected boolean shouldDistributeInvalidateRegion(RegionEventImpl event) {
    return event.getOperation().isDistributed() && !event.isOriginRemote();
  }

  /**
   * Distribute the invalidate of a region given its event. This implementation sends the invalidate
   * to peers.
   * 
   * @since GemFire 5.7
   */
  protected void distributeInvalidateRegion(RegionEventImpl event) {
    new InvalidateRegionOperation(event).distribute();
  }

  @Override
  void basicDestroyRegion(RegionEventImpl event, boolean cacheWrite, boolean lock,
      boolean callbackEvents) throws CacheWriterException, TimeoutException {
    final String path = getFullPath();
    // Keep track of regions that are being destroyed. This helps avoid a race
    // when another member concurrently creates this region. See bug 42051.
    boolean isClose = event.getOperation().isClose();
    if (!isClose) {
      this.cache.beginDestroy(path, this);
    }
    try {
      super.basicDestroyRegion(event, cacheWrite, lock, callbackEvents);
      // send destroy region operation even if this is a localDestroyRegion (or
      // close)
      if (!event.isOriginRemote()) {
        distributeDestroyRegion(event, true);
      } else {
        if (!event.isReinitializing()) {
          RegionEventImpl localEvent =
              new RegionEventImpl(this, Operation.REGION_LOCAL_DESTROY, event.getCallbackArgument(),
                  false, getMyId(), generateEventID()/* generate EventID */);
          distributeDestroyRegion(localEvent, false/* fixes bug 41111 */);
        }
      }
      notifyBridgeClients(event);
    } catch (CancelException e) {
      if (logger.isDebugEnabled()) {
        logger.debug("basicDestroyRegion short-circuited due to cancellation");
      }
    } finally {
      if (!isClose) {
        this.cache.endDestroy(path, this);
      }
      RegionLogger.logDestroy(path, getMyId(), getPersistentID(), isClose);
    }
  }

  @Override
  protected void distributeDestroyRegion(RegionEventImpl event, boolean notifyOfRegionDeparture) {
    if (this.persistenceAdvisor != null) {
      this.persistenceAdvisor.releaseTieLock();
    }
    new DestroyRegionOperation(event, notifyOfRegionDeparture).distribute();
  }

  /**
   * Return true if invalidation occurred; false if it did not, for example if it was already
   * invalidated
   */
  @Override
  void basicInvalidate(EntryEventImpl event) throws EntryNotFoundException {
    boolean hasSeen = false;
    if (hasSeenEvent(event)) {
      hasSeen = true;
    }

    try {
      // disallow local invalidation for replicated regions
      if (event.isLocalInvalid() && !event.getOperation().isLocal() && getScope().isDistributed()
          && getDataPolicy().withReplication()) {
        throw new IllegalStateException(
            LocalizedStrings.DistributedRegion_NOT_ALLOWED_TO_DO_A_LOCAL_INVALIDATION_ON_A_REPLICATED_REGION
                .toLocalizedString());
      }
      if (this.requiresOneHopForMissingEntry(event)) {
        // bug #45704: see if a one-hop must be done for this operation
        RegionEntry re = getRegionEntry(event.getKey());
        if (re == null/* || re.isTombstone() */ || !this.generateVersionTag) {
          if (this.serverRegionProxy == null) {
            // only assert for non-client regions.
            Assert.assertTrue(!this.dataPolicy.withReplication() || !this.generateVersionTag);
          }
          // TODO: deltaGII: verify that delegating to a peer when this region is also a client is
          // acceptable
          boolean didDistribute =
              RemoteInvalidateMessage.distribute(event, !this.generateVersionTag);
          if (!this.generateVersionTag && !didDistribute) {
            throw new PersistentReplicatesOfflineException();
          }
          if (didDistribute) {
            if (logger.isDebugEnabled()) {
              logger.debug("Event after remoteInvalidate operation: {}", event);
            }
            if (event.getVersionTag() == null) {
              // if the event wasn't applied by the one-hop replicate it will not have a version tag
              // and so should not be applied to this cache
              return;
            }
          }
        }
      }

      super.basicInvalidate(event);

    } finally {
      if (hasSeen) {
        if (!getConcurrencyChecksEnabled() || event.hasValidVersionTag()) {
          distributeInvalidate(event);
          event.invokeCallbacks(this, true, false);
        }
      }
    }
  }

  @Override
  void basicInvalidatePart3(RegionEntry re, EntryEventImpl event, boolean invokeCallbacks) {
    distributeInvalidate(event);
    super.basicInvalidatePart3(re, event, invokeCallbacks);
  }

  void distributeInvalidate(EntryEventImpl event) {
    if (!this.regionInvalid && event.isDistributed() && !event.isOriginRemote()
        && !isTX() /* only distribute if non-tx */) {
      if (event.isDistributed() && !event.isOriginRemote()) {
        boolean distribute = !event.getInhibitDistribution();
        if (distribute) {
          // before distribute: DR.invalidate, it has triggered callback earlier
          InvalidateOperation op = new InvalidateOperation(event);
          op.distribute();
        }
      }
    }
  }

  @Override
  void basicUpdateEntryVersion(EntryEventImpl event) throws EntryNotFoundException {
    LocalRegion localRegion = event.getLocalRegion();
    AbstractRegionMap regionMap = (AbstractRegionMap) localRegion.getRegionMap();
    try {
      regionMap.lockForCacheModification(localRegion, event);
      try {
        if (!hasSeenEvent(event)) {
          super.basicUpdateEntryVersion(event);
        }
      } finally {
        if (!getConcurrencyChecksEnabled() || event.hasValidVersionTag()) {
          distributeUpdateEntryVersion(event);
        }
      }
    } finally {
      regionMap.releaseCacheModificationLock(localRegion, event);
    }
  }

  void distributeUpdateEntryVersion(EntryEventImpl event) {
    if (!this.regionInvalid && event.isDistributed() && !event.isOriginRemote()
        && !isTX() /* only distribute if non-tx */) {
      if (event.isDistributed() && !event.isOriginRemote()) {
        // before distribute: DR has sent callback earlier
        UpdateEntryVersionOperation op = new UpdateEntryVersionOperation(event);
        op.distribute();
      }
    }
  }

  @Override
  protected void basicClear(RegionEventImpl regionEvent) {
    Lock dlock = this.getRegionDistributedLockIfGlobal();
    try {
      super.basicClear(regionEvent);
    } finally {
      if (dlock != null)
        dlock.unlock();
    }
  }

  @Override
  void basicClear(RegionEventImpl regionEvent, boolean cacheWrite) {
    if (this.concurrencyChecksEnabled && !this.dataPolicy.withReplication()) {
      boolean retry = false;
      do {
        // non-replicate regions must defer to a replicate for clear/invalidate of region
        Set<InternalDistributedMember> repls = this.distAdvisor.adviseReplicates();
        if (!repls.isEmpty()) {
          InternalDistributedMember mbr = repls.iterator().next();
          RemoteRegionOperation op = RemoteRegionOperation.clear(mbr, this);
          try {
            op.distribute();
            return;
          } catch (CancelException e) {
            this.stopper.checkCancelInProgress(e);
            retry = true;
          } catch (RemoteOperationException e) {
            this.stopper.checkCancelInProgress(e);
            retry = true;
          }
        }
      } while (retry);
    }
    // if no version vector or if no replicates are around, use the default mechanism
    super.basicClear(regionEvent, cacheWrite);
  }


  @Override
  void cmnClearRegion(RegionEventImpl regionEvent, boolean cacheWrite, boolean useRVV) {
    boolean enableRVV = useRVV && this.dataPolicy.withReplication() && this.concurrencyChecksEnabled
        && !getDistributionManager().isLoner();

    // Fix for 46338 - apparently multiple threads from the same VM are allowed
    // to suspend locking, which is what distributedLockForClear() does. We don't
    // want that to happen, so we'll synchronize to make sure only one thread on
    // this member performs a clear.
    synchronized (this.clearLock) {
      if (enableRVV) {

        distributedLockForClear();
        try {
          Set<InternalDistributedMember> participants =
              getCacheDistributionAdvisor().adviseInvalidateRegion();
          // pause all generation of versions and flush from the other members to this one
          try {
            obtainWriteLocksForClear(regionEvent, participants);
            clearRegionLocally(regionEvent, cacheWrite, null);
            if (!regionEvent.isOriginRemote() && regionEvent.getOperation().isDistributed()) {
              DistributedClearOperation.clear(regionEvent, null, participants);
            }
          } finally {
            releaseWriteLocksForClear(regionEvent, participants);
          }
        } finally {
          distributedUnlockForClear();
        }
      } else {
        Set<InternalDistributedMember> participants =
            getCacheDistributionAdvisor().adviseInvalidateRegion();
        clearRegionLocally(regionEvent, cacheWrite, null);
        if (!regionEvent.isOriginRemote() && regionEvent.getOperation().isDistributed()) {
          DistributedClearOperation.clear(regionEvent, null, participants);
        }
      }
    }

    // since clients do not maintain RVVs except for tombstone GC
    // we need to ensure that current ops reach the client queues
    // before queuing a clear, but there is no infrastructure for doing so
    notifyBridgeClients(regionEvent);
  }

  /**
   * Obtain a distributed lock for the clear operation.
   */
  private void distributedLockForClear() {
    if (!this.scope.isGlobal()) { // non-global regions must lock when using RVV
      try {
        getLockService().lock("_clearOperation", -1, -1);
      } catch (IllegalStateException e) {
        lockCheckReadiness();
        throw e;
      }
    }
  }

  /**
   * Release the distributed lock for the clear operation.
   */
  private void distributedUnlockForClear() {
    if (!this.scope.isGlobal()) {
      try {
        getLockService().unlock("_clearOperation");
      } catch (IllegalStateException e) {
        lockCheckReadiness();
        throw e;
      }
    }
  }


  /**
   * obtain locks preventing generation of new versions in other members
   */
  private void obtainWriteLocksForClear(RegionEventImpl regionEvent,
      Set<InternalDistributedMember> participants) {
    lockLocallyForClear(getDistributionManager(), getMyId(), regionEvent);
    DistributedClearOperation.lockAndFlushToOthers(regionEvent, participants);
  }

  /**
   * pause local operations so that a clear() can be performed and flush comm channels to the given
   * member
   */
  void lockLocallyForClear(DM dm, InternalDistributedMember locker, CacheEvent event) {
    RegionVersionVector rvv = getVersionVector();

    ARMLockTestHook armLockTestHook = getRegionMap().getARMLockTestHook();
    if (armLockTestHook != null) {
      armLockTestHook.beforeLock(this, event);
    }

    if (rvv != null) {
      // block new operations from being applied to the region map
      rvv.lockForClear(getFullPath(), dm, locker);
      // Check for region destroyed after we have locked, to make sure
      // we don't continue a clear if the region has been destroyed.
      checkReadiness();
      // Only need to flush if NOACK at this point
      if (this.getAttributes().getScope().isDistributedNoAck()) {
        Set<InternalDistributedMember> members = getDistributionAdvisor().adviseCacheOp();
        StateFlushOperation.flushTo(members, this);
      }
    }

    if (armLockTestHook != null) {
      armLockTestHook.afterLock(this, null);
    }
  }

  /**
   * releases the locks obtained in obtainWriteLocksForClear
   */
  private void releaseWriteLocksForClear(RegionEventImpl regionEvent,
      Set<InternalDistributedMember> participants) {

    ARMLockTestHook armLockTestHook = getRegionMap().getARMLockTestHook();
    if (armLockTestHook != null) {
      armLockTestHook.beforeRelease(this, regionEvent);
    }

    getVersionVector().unlockForClear(getMyId());
    DistributedClearOperation.releaseLocks(regionEvent, participants);

    if (armLockTestHook != null) {
      armLockTestHook.afterRelease(this, regionEvent);
    }
  }

  /**
   * Wait for in progress clears that were initiated by this member.
   */
  private void waitForInProgressClear() {
    RegionVersionVector rvv = getVersionVector();
    if (rvv != null) {
      synchronized (this.clearLock) {
        // do nothing;
        // I'm a little scared that the compiler might optimize
        // away this synchronization if we really do nothing. Hence
        // my fine log message below. This might not be necessary.
        if (logger.isDebugEnabled()) {
          logger.debug("Done waiting for clear");
        }
      }
    }

  }

  /**
   * Distribute Tombstone garbage-collection information to all peers with storage
   */
  protected EventID distributeTombstoneGC(Set<Object> keysRemoved) {
    this.getCachePerfStats().incTombstoneGCCount();
    EventID eventId = new EventID(getSystem());
    DistributedTombstoneOperation gc = DistributedTombstoneOperation.gc(this, eventId);
    gc.distribute();
    notifyClientsOfTombstoneGC(getVersionVector().getTombstoneGCVector(), keysRemoved, eventId,
        null);
    return eventId;
  }

  @Override
  void basicLocalClear(RegionEventImpl rEvent) {
    if (getScope().isDistributed() && getDataPolicy().withReplication()) {
      throw new UnsupportedOperationException(
          LocalizedStrings.DistributedRegion_LOCALCLEAR_IS_NOT_SUPPORTED_ON_DISTRIBUTED_REPLICATED_REGIONS
              .toLocalizedString());
    }
    super.basicLocalClear(rEvent);
  }

  public DistributionConfig getDistributionConfig() {
    return getSystem().getDistributionManager().getConfig();
  }

  @Override
  public CacheDistributionAdvisor getDistributionAdvisor() {
    return this.distAdvisor;
  }

  @Override
  public CacheDistributionAdvisor getCacheDistributionAdvisor() {
    return this.distAdvisor;
  }

  public PersistenceAdvisor getPersistenceAdvisor() {
    return this.persistenceAdvisor;
  }

  public PersistentMemberID getPersistentID() {
    return this.persistentId;
  }

  /** Returns the distribution profile; lazily creates one if needed */
  @Override
  public Profile getProfile() {
    return this.distAdvisor.createProfile();
  }

  @Override
  public void fillInProfile(Profile profile) {
    assert profile instanceof CacheProfile;
    CacheProfile cacheProfile = (CacheProfile) profile;
    cacheProfile.dataPolicy = getDataPolicy();
    cacheProfile.hasCacheLoader = basicGetLoader() != null;
    cacheProfile.hasCacheWriter = basicGetWriter() != null;
    cacheProfile.hasCacheListener = hasListener();
    Assert.assertTrue(this.scope.isDistributed());
    cacheProfile.scope = this.scope;
    cacheProfile.inRecovery = getImageState().getInRecovery();
    cacheProfile.isPersistent = getDataPolicy().withPersistence();
    cacheProfile.setSubscriptionAttributes(getSubscriptionAttributes());

    // Below PDX check is added for rolling upgrade support. We are
    // removing Old wan in this checkin. PDX region are always gatewayEnabled
    // irrespective whether gatewayHub is configured or not.
    // Old version Pdx region always has this attribute true so to avoid region
    // attribute comparison across member we are setting it to true.

    cacheProfile.isGatewayEnabled = isPdxTypesRegion();
    cacheProfile.serialNumber = getSerialNumber();
    cacheProfile.regionInitialized = isInitialized();
    cacheProfile.persistentID = getPersistentID();
    if (getPersistenceAdvisor() != null) {
      cacheProfile.persistenceInitialized = getPersistenceAdvisor().isOnline();
    }
    cacheProfile.hasCacheServer = this.cache.getCacheServers().size() > 0 ? true : false;
    cacheProfile.requiresOldValueInEvents = this.dataPolicy.withReplication()
        && this.filterProfile != null && this.filterProfile.hasCQs();
    cacheProfile.gatewaySenderIds = getGatewaySenderIds();
    cacheProfile.asyncEventQueueIds = getVisibleAsyncEventQueueIds();
    cacheProfile.isOffHeap = getOffHeap();
  }

  /**
   * Return the DistributedLockService associated with this Region. This method will lazily create
   * that service the first time it is invoked on this region.
   */
  public DistributedLockService getLockService() {
    synchronized (this.dlockMonitor) {
      String dlsName = getFullPath();

      if (this.dlockService == null) {
        this.dlockService = DistributedLockService.getServiceNamed(dlsName);
        if (this.dlockService == null) {
          // region destroy will destroy dls and manual freeResources only
          this.dlockService = DLockService.create(getFullPath(), getSystem(), true, false, false);
        }
        // handle is-lock-grantor region attribute...
        if (this.isLockGrantor) {
          this.dlockService.becomeLockGrantor();
        }
        if (logger.isDebugEnabled()) {
          logger.debug("LockService for {} is using LockLease={}, LockTimeout={}", dlsName,
              getCache().getLockLease(), getCache().getLockTimeout());
        }
      }
      return this.dlockService;
    }
  }

  @Override
  protected boolean isCurrentlyLockGrantor() {
    return this.scope.isGlobal() && getLockService().isLockGrantor();
  }

  @Override
  public boolean isLockGrantor() {
    return this.scope.isGlobal() && this.isLockGrantor;
  }

  @Override
  public void becomeLockGrantor() {
    checkReadiness();
    checkForLimitedOrNoAccess();
    if (!this.scope.isGlobal()) {
      throw new IllegalStateException(
          LocalizedStrings.DistributedRegion_DISTRIBUTION_LOCKS_ARE_ONLY_SUPPORTED_FOR_REGIONS_WITH_GLOBAL_SCOPE_NOT_0
              .toLocalizedString(this.scope));
    }

    DistributedLockService svc = getLockService();
    try {
      super.becomeLockGrantor();
      if (!svc.isLockGrantor()) {
        svc.becomeLockGrantor();
      }
    } finally {
      if (!svc.isLockGrantor()) {
        if (logger.isDebugEnabled()) {
          logger.debug("isLockGrantor is false after becomeLockGrantor for {}", getFullPath());
        }
      }
    }
  }

  /** @return the deserialized value */
  @Override
  @Retained
  protected Object findObjectInSystem(KeyInfo keyInfo, boolean isCreate, TXStateInterface txState,
      boolean generateCallbacks, Object localValue, boolean disableCopyOnRead, boolean preferCD,
      ClientProxyMembershipID requestingClient, EntryEventImpl clientEvent,
      boolean returnTombstones) throws CacheLoaderException, TimeoutException {

    @Released
    EntryEventImpl event = null;

    checkForLimitedOrNoAccess();
    final Operation op = isCreate ? Operation.CREATE : Operation.UPDATE;
    long lastModified = 0L;

    try {
      event = findOnServer(keyInfo, op, generateCallbacks, clientEvent);
      if (event == null) {
        event = createEventForLoad(keyInfo, generateCallbacks, requestingClient, op);
        lastModified = findUsingSearchLoad(txState, localValue, clientEvent, keyInfo, event);
      }
      // Update region with new value.
      if (event.hasNewValue() && !isMemoryThresholdReachedForLoad()) {
        putNewValueInRegion(isCreate, clientEvent, lastModified, event);
      } else if (isCreate) {
        recordMiss(null, event.getKey());
      }
      return determineResult(preferCD, event);
    } finally {
      if (event != null) {
        event.release();
      }
    }
  }

  private EntryEventImpl createEventForLoad(KeyInfo keyInfo, boolean generateCallbacks,
      ClientProxyMembershipID requestingClient, Operation op) {
    // Do not generate Event ID
    EntryEventImpl event = EntryEventImpl.create(this, op, keyInfo.getKey(), null /* newValue */,
        keyInfo.getCallbackArg(), false, getMyId(), generateCallbacks);
    if (requestingClient != null) {
      event.setContext(requestingClient);
    }
    return event;
  }

  private Object determineResult(boolean preferCD, EntryEventImpl event) {
    if (preferCD) {
      return event.getRawNewValueAsHeapObject();
    }
    return event.getNewValue();
  }

  private void putNewValueInRegion(boolean isCreate, EntryEventImpl clientEvent, long lastModified,
      EntryEventImpl event) {
    RegionEntry re = null;
    // Set eventId. Required for interested clients.
    event.setNewEventId(cache.getDistributedSystem());

    long startPut = CachePerfStats.getStatTime();
    validateKey(event.getKey());
    // this next step also distributes the object to other processes, if necessary
    try {
      re = basicPutEntry(event, lastModified);

      // Update client event with latest version tag from re.
      if (re != null && clientEvent != null) {
        clientEvent.setVersionTag(event.getVersionTag());
      }
      if (!isTX()) {
        getCachePerfStats().endPut(startPut, event.isOriginRemote());
      }
    } catch (ConcurrentCacheModificationException e) {
      // the cache was modified while we were searching for this entry and
      // the netsearch result was elided. Return the current value from the cache
      updateEventWithCurrentRegionEntry(event, clientEvent);
    } catch (CacheWriterException cwe) {
      if (logger.isDebugEnabled()) {
        logger.debug("findObjectInSystem: writer exception putting entry {} : {}", event, cwe);
      }
    }
    if (isCreate) {
      recordMiss(re, event.getKey());
    }
  }

  private void updateEventWithCurrentRegionEntry(EntryEventImpl event, EntryEventImpl clientEvent) {
    // defer the lruUpdateCallback to prevent a deadlock (see bug 51121).
    final boolean disabled = this.entries.disableLruUpdateCallback();
    try {
      RegionEntry re = getRegionEntry(event.getKey());
      if (re != null) {
        synchronized (re) { // bug #51059 value & version must be obtained atomically
          // Update client event with latest version tag from re
          if (clientEvent != null) {
            clientEvent.setVersionTag(re.getVersionStamp().asVersionTag());
          }
          // OFFHEAP: need to incrc, copy to heap to setNewValue, decrc
          event.setNewValue(re.getValue(this));
        }
      }
    } finally {
      if (disabled) {
        this.entries.enableLruUpdateCallback();
      }
      try {
        this.entries.lruUpdateCallback();
      } catch (DiskAccessException dae) {
        this.handleDiskAccessException(dae);
        throw dae;
      }
    }
  }

  /**
   * If its client, get the value from server.
   */
  private EntryEventImpl findOnServer(KeyInfo keyInfo, Operation op, boolean generateCallbacks,
      EntryEventImpl clientEvent) {
    if (this.serverRegionProxy == null) {
      return null;
    }
    EntryEventImpl event = null;
    VersionTagHolder holder = new VersionTagHolder();
    Object aCallbackArgument = keyInfo.getCallbackArg();
    Object value = this.serverRegionProxy.get(keyInfo.getKey(), aCallbackArgument, holder);
    if (value != null) {
      event = EntryEventImpl.create(this, op, keyInfo.getKey(), value, aCallbackArgument, false,
          getMyId(), generateCallbacks);
      event.setVersionTag(holder.getVersionTag());
      event.setFromServer(true); // fix for bug 39358
      if (clientEvent != null && clientEvent.getVersionTag() == null) {
        clientEvent.setVersionTag(holder.getVersionTag());
      }
    }
    return event;
  }

  private long findUsingSearchLoad(TXStateInterface txState, Object localValue,
      EntryEventImpl clientEvent, final KeyInfo keyInfo, EntryEventImpl event) {
    long lastModified = 0L;
    // If this event is because of a register interest call, don't invoke the CacheLoader
    boolean getForRegisterInterest = clientEvent != null && clientEvent.getOperation() != null
        && clientEvent.getOperation().isGetForRegisterInterest();
    if (!getForRegisterInterest) {
      SearchLoadAndWriteProcessor processor = SearchLoadAndWriteProcessor.getProcessor();
      try {
        processor.initialize(this, keyInfo.getKey(), keyInfo.getCallbackArg());
        // processor fills in event
        processor.doSearchAndLoad(event, txState, localValue);
        if (clientEvent != null && clientEvent.getVersionTag() == null) {
          clientEvent.setVersionTag(event.getVersionTag());
        }
        lastModified = processor.getLastModified();
      } finally {
        processor.release();
      }
    } else {
      if (logger.isDebugEnabled()) {
        logger.debug("DistributedRegion.findObjectInSystem skipping loader for region="
            + getFullPath() + "; key=" + keyInfo.getKey());
      }
    }
    return lastModified;
  }

  /**
   * @return true if cacheWrite was performed
   */
  @Override
  public boolean cacheWriteBeforeDestroy(EntryEventImpl event, Object expectedOldValue)
      throws CacheWriterException, EntryNotFoundException, TimeoutException {

    boolean result = false;
    if (event.isDistributed()) {
      CacheWriter localWriter = basicGetWriter();
      Set netWriteRecipients = localWriter == null ? this.distAdvisor.adviseNetWrite() : null;

      if ((localWriter != null || (netWriteRecipients != null && !netWriteRecipients.isEmpty()))
          && !event.inhibitAllNotifications()) {
        final long start = getCachePerfStats().startCacheWriterCall();
        try {
          event.setOldValueFromRegion();
          SearchLoadAndWriteProcessor processor = SearchLoadAndWriteProcessor.getProcessor();
          try {
            processor.initialize(this, event.getKey(), null);
            processor.doNetWrite(event, netWriteRecipients, localWriter,
                SearchLoadAndWriteProcessor.BEFOREDESTROY);
            result = true;
          } finally {
            processor.release();
          }
        } finally {
          getCachePerfStats().endCacheWriterCall(start);
        }
      }
      serverDestroy(event, expectedOldValue);
    }
    return result;
  }

  @Override
  boolean cacheWriteBeforeRegionDestroy(RegionEventImpl event)
      throws CacheWriterException, TimeoutException {
    boolean result = false;
    if (event.getOperation().isDistributed()) {
      CacheWriter localWriter = basicGetWriter();
      Set netWriteRecipients = localWriter == null ? this.distAdvisor.adviseNetWrite() : null;

      if (localWriter != null || netWriteRecipients != null && !netWriteRecipients.isEmpty()) {
        final long start = getCachePerfStats().startCacheWriterCall();
        try {
          SearchLoadAndWriteProcessor processor = SearchLoadAndWriteProcessor.getProcessor();
          try {
            processor.initialize(this, "preDestroyRegion", null);
            processor.doNetWrite(event, netWriteRecipients, localWriter,
                SearchLoadAndWriteProcessor.BEFOREREGIONDESTROY);
            result = true;
          } finally {
            processor.release();
          }
        } finally {
          getCachePerfStats().endCacheWriterCall(start);
        }
      }
      serverRegionDestroy(event);
    }
    return result;
  }

  protected void distributedRegionCleanup(RegionEventImpl event) {
    if (event == null || event.getOperation() != Operation.REGION_REINITIALIZE) {
      // only perform this if reinitialize is not due to resumption
      // (REGION_REINITIALIZE)
      // or if event is null then this was a failed initialize (create)
      // wake up any threads in waitForRequiredRoles... they will checkReadiness
      synchronized (this.missingRequiredRoles) {
        this.missingRequiredRoles.notifyAll();
      }
    }

    if (this.persistenceAdvisor != null) {
      this.persistenceAdvisor.close(); // fix for bug 41094
    }
    this.distAdvisor.close();

    // Fix for bug 46338. Wait for in progress clears before destroying the
    // lock service, because destroying the service immediately releases the dlock
    waitForInProgressClear();

    DLockService dls = null;
    synchronized (this.dlockMonitor) {
      if (this.dlockService != null) {
        dls = (DLockService) this.dlockService;
      }
    }
    if (dls != null) {
      try {
        dls.destroyAndRemove();
      } catch (CancelException e) {
        // bug 37118
        if (logger.isDebugEnabled()) {
          logger.debug("DLS destroy abridged due to shutdown", e);
        }
      } catch (Exception ex) {
        logger.warn(LocalizedMessage.create(
            LocalizedStrings.DistributedRegion_DLS_DESTROY_MAY_HAVE_FAILED_FOR_0,
            this.getFullPath()), ex);
      }
    }
    // Fix for #48066 - make sure that region operations are completely
    // distributed to peers before destroying the region.
    long timeout =
        1000L * getCache().getInternalDistributedSystem().getConfig().getAckWaitThreshold();
    Boolean flushOnClose =
        !Boolean.getBoolean(DistributionConfig.GEMFIRE_PREFIX + "no-flush-on-close"); // test hook
    if (!this.cache.forcedDisconnect() && flushOnClose
        && this.getDistributionManager().getMembershipManager() != null
        && this.getDistributionManager().getMembershipManager().isConnected()) {
      getDistributionAdvisor().forceNewMembershipVersion();
      try {
        getDistributionAdvisor().waitForCurrentOperations(timeout);
      } catch (Exception e) {
        // log this but try to close the region so that listeners are invoked
        logger.warn(LocalizedMessage.create(LocalizedStrings.GemFireCache_0_ERROR_CLOSING_REGION_1,
            new Object[] {this, getFullPath()}), e);
      }
    }
  }

  /**
   * In addition to inherited code this method also invokes RegionMembershipListeners
   */
  @Override
  protected void postCreateRegion() {
    super.postCreateRegion();
    // should we sync on this.distAdvisor first to prevent bug 44369?
    synchronized (this.advisorListener) {
      Set others = this.advisorListener.getInitialMembers();
      CacheListener[] listeners = fetchCacheListenersField();
      if (listeners != null) {
        for (CacheListener listener : listeners) {
          if (listener instanceof RegionMembershipListener) {
            RegionMembershipListener regionMembershipListener = (RegionMembershipListener) listener;
            try {
              DistributedMember[] otherDms = new DistributedMember[others.size()];
              others.toArray(otherDms);
              regionMembershipListener.initialMembers(this, otherDms);
            } catch (VirtualMachineError err) {
              SystemFailure.initiateFailure(err);
              // If this ever returns, rethrow the error. We're poisoned
              // now, so don't let this thread continue.
              throw err;
            } catch (Throwable t) {
              // Whenever you catch Error or Throwable, you must also
              // catch VirtualMachineError (see above). However, there is
              // _still_ a possibility that you are dealing with a cascading
              // error condition, so you also need to check to see if the JVM
              // is still usable:
              SystemFailure.checkFailure();
              logger.error(
                  LocalizedMessage.create(
                      LocalizedStrings.DistributedRegion_EXCEPTION_OCCURRED_IN_REGIONMEMBERSHIPLISTENER),
                  t);
            }
          }
        }
      }
      Set<String> allGatewaySenderIds = getAllGatewaySenderIds();
      if (!allGatewaySenderIds.isEmpty()) {
        for (GatewaySender sender : this.cache.getAllGatewaySenders()) {
          if (sender.isParallel() && allGatewaySenderIds.contains(sender.getId())) {
            // Fix for Bug#51491. Once decided to support this configuration we have call
            // addShadowPartitionedRegionForUserRR
            if (sender.getId().contains(AsyncEventQueueImpl.ASYNC_EVENT_QUEUE_PREFIX)) {
              throw new AsyncEventQueueConfigurationException(
                  LocalizedStrings.ParallelAsyncEventQueue_0_CAN_NOT_BE_USED_WITH_REPLICATED_REGION_1
                      .toLocalizedString(new Object[] {
                          AsyncEventQueueImpl.getAsyncEventQueueIdFromSenderId(sender.getId()),
                          this.getFullPath()}));
            }
            throw new GatewaySenderConfigurationException(
                LocalizedStrings.ParallelGatewaySender_0_CAN_NOT_BE_USED_WITH_REPLICATED_REGION_1
                    .toLocalizedString(new Object[] {sender.getId(), this.getFullPath()}));
          }
        }
      }
    }
  }

  /**
   * Free resources held by this region. This method is invoked after isDestroyed has been set to
   * true.
   */
  @Override
  protected void postDestroyRegion(boolean destroyDiskRegion, RegionEventImpl event) {
    distributedRegionCleanup(event);

    try {
      super.postDestroyRegion(destroyDiskRegion, event);
    } catch (CancelException e) {
      // I don't think this should ever happens: bulletproofing for bug 39454
      logger.warn("postDestroyRegion: encountered cancellation", e);
    }
  }

  @Override
  void cleanupFailedInitialization() {
    super.cleanupFailedInitialization();
    try {
      RegionEventImpl ev = new RegionEventImpl(this, Operation.REGION_CLOSE, null, false, getMyId(),
          generateEventID());
      distributeDestroyRegion(ev, true);
      distributedRegionCleanup(null);
    } catch (RegionDestroyedException ignore) {
      // someone else must have concurrently destroyed the region (maybe a distributed destroy)
    } catch (CancelException ignore) {
      // cache or DS is closed, ignore
    } catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    } catch (Throwable t) {
      logger.warn(LocalizedMessage.create(
          LocalizedStrings.DistributedRegion_ERROR_CLEANING_UP_FAILED_INITIALIZATION, this), t);
    }
  }

  @Override
  void handleCacheClose(Operation operation) {
    try {
      super.handleCacheClose(operation);
    } finally {
      distributedRegionCleanup(null);
    }
  }

  /**
   * invoke a cache writer before a put is performed elsewhere
   */
  @Override
  protected void cacheWriteBeforePut(EntryEventImpl event, Set netWriteRecipients,
      CacheWriter localWriter, boolean requireOldValue, Object expectedOldValue)
      throws CacheWriterException, TimeoutException {
    if ((localWriter != null || (netWriteRecipients != null && !netWriteRecipients.isEmpty()))
        && !event.inhibitAllNotifications()) {
      final boolean isNewKey = event.getOperation().isCreate();
      final long start = getCachePerfStats().startCacheWriterCall();
      try {
        SearchLoadAndWriteProcessor processor = SearchLoadAndWriteProcessor.getProcessor();
        processor.initialize(this, "preUpdate", null);
        try {
          if (!isNewKey) {
            processor.doNetWrite(event, netWriteRecipients, localWriter,
                SearchLoadAndWriteProcessor.BEFOREUPDATE);
          } else {
            processor.doNetWrite(event, netWriteRecipients, localWriter,
                SearchLoadAndWriteProcessor.BEFORECREATE);
          }
        } finally {
          processor.release();
        }
      } finally {
        getCachePerfStats().endCacheWriterCall(start);
      }
    }

    serverPut(event, requireOldValue, expectedOldValue);
  }

  @Override
  protected void cacheListenersChanged(boolean nowHasListener) {
    if (nowHasListener) {
      this.advisorListener.initRMLWrappers();
    }
    new UpdateAttributesProcessor(this).distribute();
  }

  @Override
  protected void cacheWriterChanged(CacheWriter oldWriter) {
    super.cacheWriterChanged(oldWriter);
    if (oldWriter == null ^ basicGetWriter() == null) {
      new UpdateAttributesProcessor(this).distribute();
    }
  }

  @Override
  protected void cacheLoaderChanged(CacheLoader oldLoader) {
    super.cacheLoaderChanged(oldLoader);
    if (oldLoader == null ^ basicGetLoader() == null) {
      new UpdateAttributesProcessor(this).distribute();
    }
  }

  @Override
  public void addGatewaySenderId(String gatewaySenderId) {
    super.addGatewaySenderId(gatewaySenderId);
    new UpdateAttributesProcessor(this).distribute();
  }

  @Override
  public void removeGatewaySenderId(String gatewaySenderId) {
    super.removeGatewaySenderId(gatewaySenderId);
    new UpdateAttributesProcessor(this).distribute();
  }

  @Override
  public void addAsyncEventQueueId(String asyncEventQueueId) {
    super.addAsyncEventQueueId(asyncEventQueueId);
    new UpdateAttributesProcessor(this).distribute();
  }

  @Override
  public void removeAsyncEventQueueId(String asyncEventQueueId) {
    super.removeAsyncEventQueueId(asyncEventQueueId);
    new UpdateAttributesProcessor(this).distribute();
  }

  @Override
  public void checkSameSenderIdsAvailableOnAllNodes() {
    List<Set<String>> senderIds =
        this.getCacheDistributionAdvisor().adviseSameGatewaySenderIds(getGatewaySenderIds());
    if (!senderIds.isEmpty()) {
      throw new GatewaySenderConfigurationException(
          LocalizedStrings.Region_REGION_0_HAS_1_GATEWAY_SENDER_IDS_ANOTHER_CACHE_HAS_THE_SAME_REGION_WITH_2_GATEWAY_SENDER_IDS_FOR_REGION_ACROSS_ALL_MEMBERS_IN_DS_GATEWAY_SENDER_IDS_SHOULD_BE_SAME
              .toLocalizedString(
                  new Object[] {this.getName(), senderIds.get(0), senderIds.get(1)}));
    }

    List<Set<String>> asycnQueueIds = this.getCacheDistributionAdvisor()
        .adviseSameAsyncEventQueueIds(getVisibleAsyncEventQueueIds());
    if (!asycnQueueIds.isEmpty()) {
      throw new GatewaySenderConfigurationException(
          LocalizedStrings.Region_REGION_0_HAS_1_ASYNC_EVENT_QUEUE_IDS_ANOTHER_CACHE_HAS_THE_SAME_REGION_WITH_2_ASYNC_EVENT_QUEUE_IDS_FOR_REGION_ACROSS_ALL_MEMBERS_IN_DS_ASYNC_EVENT_QUEUE_IDS_SHOULD_BE_SAME
              .toLocalizedString(
                  new Object[] {this.getName(), asycnQueueIds.get(0), asycnQueueIds.get(1)}));
    }
  }

  /**
   * Wraps call to dlock service in order to throw RegionDestroyedException if dlock service throws
   * IllegalStateException and isDestroyed is true.
   */
  private boolean isLockingSuspendedByCurrentThread() {
    try {
      return getLockService().isLockingSuspendedByCurrentThread();
    } catch (IllegalStateException e) {
      lockCheckReadiness();
      throw e;
    }
  }

  /**
   * If this region's scope is GLOBAL, get a distributed lock on the given key, and return the Lock.
   * The sender is responsible for unlocking.
   * 
   * @return the acquired Lock if the region is GLOBAL, otherwise null.
   * 
   * @throws NullPointerException if key is null
   */
  private Lock getDistributedLockIfGlobal(Object key) throws TimeoutException {
    if (getScope().isGlobal()) {
      if (isLockingSuspendedByCurrentThread())
        return null;
      long start = System.currentTimeMillis();
      long timeLeft = getCache().getLockTimeout();
      long lockTimeout = timeLeft;
      StringId msg = null;
      Object[] msgArgs = null;
      while (timeLeft > 0 || lockTimeout == -1) {
        this.cache.getCancelCriterion().checkCancelInProgress(null);
        boolean interrupted = Thread.interrupted();
        try {
          Lock dlock = getDistributedLock(key);
          if (!dlock.tryLock(timeLeft, TimeUnit.SECONDS)) {
            msg =
                LocalizedStrings.DistributedRegion_ATTEMPT_TO_ACQUIRE_DISTRIBUTED_LOCK_FOR_0_FAILED_AFTER_WAITING_1_SECONDS;
            msgArgs = new Object[] {key, (System.currentTimeMillis() - start) / 1000L};
            break;
          }

          return dlock;
        } catch (InterruptedException ex) {
          interrupted = true;
          this.cache.getCancelCriterion().checkCancelInProgress(ex);
          // TODO: Why is it OK to keep going?
          if (lockTimeout > -1) {
            timeLeft = getCache().getLockTimeout() - (System.currentTimeMillis() - start) / 1000L;
          }
        } finally {
          if (interrupted) {
            Thread.currentThread().interrupt();
          }
        }
      } // while
      if (msg == null) {
        msg =
            LocalizedStrings.DistributedRegion_TIMED_OUT_AFTER_WAITING_0_SECONDS_FOR_THE_DISTRIBUTED_LOCK_FOR_1;
        msgArgs = new Object[] {getCache().getLockTimeout(), key};
      }
      throw new TimeoutException(msg.toLocalizedString(msgArgs));
    } else {
      return null;
    }
  }

  /**
   * Checks if the entry is a valid entry
   * 
   * @return true if entry not null or entry is not removed
   */
  protected boolean checkEntryNotValid(RegionEntry mapEntry) {
    return mapEntry == null || mapEntry.isRemoved() && !mapEntry.isTombstone();
  }

  /**
   * Get the best iterator for iterating over the contents of this region. This method will either
   * an iterator that uses hash ordering from the entry map, or, in the case of an overflow region,
   * an iterator that iterates over the entries in disk order.
   */
  @Override
  public Iterator<RegionEntry> getBestIterator(boolean includeValues) {
    DiskRegion dr = this.getDiskRegion();

    if (DiskPage.DISK_PAGE_SIZE > 0 && includeValues && dr != null) {
      // Wait for the disk region to recover values first.
      dr.waitForAsyncRecovery();
      if (dr.getNumOverflowOnDisk() > 0) {
        return new DiskSavvyIterator();
      }
    }
    return this.entries.regionEntries().iterator();
  }

  /**
   * Should only be used if this region has entries on disk that are not in memory. This currently
   * happens for overflow and for recovery when values are not recovered. The first iteration does a
   * normal iteration of the regionEntries. But if it finds an entry that is currently only on disk
   * it saves it in a list sorted by the location on disk. Once the regionEntries iterator has
   * nothing more to iterate it starts iterating over, in disk order, the entries on disk.
   */
  private class DiskSavvyIterator implements Iterator<RegionEntry> {
    private boolean usingIt = true;

    private Iterator<?> it = entries.regionEntries().iterator();

    // iterator for nested ArrayLists
    private Iterator<RegionEntry> subIt = null;

    private final TreeMap<DiskPage, Object> diskMap = new TreeMap<>();

    DiskSavvyIterator() {}

    @Override
    public boolean hasNext() {
      boolean result;
      if (this.subIt != null) {
        result = this.subIt.hasNext();
        if (!result) {
          this.subIt = null;
        } else {
          return result;
        }
      }

      result = this.it.hasNext();
      if (this.usingIt && !result) {
        this.usingIt = false;
        this.it = this.diskMap.values().iterator();
        result = this.it.hasNext();
      }
      return result;
    }

    @Override
    public RegionEntry next() {
      for (;;) {
        if (this.subIt != null) {
          return this.subIt.next();
        }
        if (this.usingIt) {
          RegionEntry regionEntry = (RegionEntry) this.it.next();
          DiskPosition diskPosition = new DiskPosition();
          if (regionEntry.isOverflowedToDisk(DistributedRegion.this, diskPosition)) {
            // add diskPosition to sorted list
            DiskPage dPage = new DiskPage(diskPosition);
            Object value = this.diskMap.get(dPage);
            if (value == null) {
              this.diskMap.put(dPage, regionEntry);
            } else if (value instanceof ArrayList) {
              List list = (List) value;
              list.add(regionEntry);
            } else {
              List list = new ArrayList();
              list.add(value);
              list.add(regionEntry);
              this.diskMap.put(dPage, list);
            }
            if (!hasNext()) {
              assert false; // must be true
            }
          } else {
            return regionEntry;
          }
        } else {
          Object value = this.it.next();
          if (value instanceof ArrayList) {
            List list = (List) value;
            this.subIt = list.iterator();
            return this.subIt.next();
          } else {
            return (RegionEntry) value;
          }
        }
      }
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  public static class DiskPosition implements Comparable<DiskPosition> {
    long oplogId; // package-private to avoid synthetic accessor
    long offset; // package-private to avoid synthetic accessor

    DiskPosition() {}

    public void setPosition(long oplogId, long offset) {
      this.oplogId = oplogId;
      this.offset = offset;
    }

    @Override
    public int hashCode() {
      // TODO: Object instantiation inside 'hashCode()' is bad
      return Long.valueOf(this.oplogId ^ this.offset).hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof DiskPosition) {
        DiskPosition other = (DiskPosition) obj;
        return this.oplogId == other.oplogId && this.offset == other.offset;
      } else {
        return false;
      }
    }

    @Override
    public int compareTo(DiskPosition o) {
      int result = Long.signum(this.oplogId - o.oplogId);
      if (result == 0) {
        result = Long.signum(this.offset - o.offset);
      }
      return result;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append('<').append(this.oplogId).append(':').append(this.offset).append('>');
      return sb.toString();
    }
  }

  static class DiskPage extends DiskPosition {

    static final long DISK_PAGE_SIZE =
        Long.getLong(DistributionConfig.GEMFIRE_PREFIX + "DISK_PAGE_SIZE", 8 << 10);

    DiskPage(DiskPosition diskPosition) {
      this.setPosition(diskPosition.oplogId, diskPosition.offset / DISK_PAGE_SIZE);
    }
  }

  /**
   * Returns the lock lease value to use for DistributedLock and RegionDistributedLock. -1 is
   * supported as non-expiring lock.
   */
  long getLockLeaseForLock() { // package-private to avoid synthetic accessor
    if (getCache().getLockLease() == -1) {
      return -1;
    }
    return getCache().getLockLease() * 1000;
  }

  /**
   * Returns the lock timeout value to use for DistributedLock and RegionDistributedLock. -1 is
   * supported as a lock that never times out.
   */
  long getLockTimeoutForLock(long time, TimeUnit unit) { // package-private to avoid synthetic
                                                         // accessor
    if (time == -1) {
      return -1;
    }
    return TimeUnit.MILLISECONDS.convert(time, unit);
  }

  private class DistributedLock implements Lock {
    private final Object key;

    DistributedLock(Object key) {
      this.key = key;
    }

    @Override
    public void lock() {
      try {
        boolean locked = basicTryLock(-1, TimeUnit.MILLISECONDS, false);
        if (!locked) {
          lockCheckReadiness();
        }
        Assert.assertTrue(locked, "Failed to acquire DistributedLock");
      } catch (IllegalStateException ex) {
        lockCheckReadiness();
        throw ex;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        lockCheckReadiness();
        Assert.assertTrue(false, "Failed to acquire DistributedLock");
      }
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
      try {
        boolean locked = basicTryLock(-1, TimeUnit.MILLISECONDS, true);
        if (!locked) {
          lockCheckReadiness();
        }
        Assert.assertTrue(locked, "Failed to acquire DistributedLock");
      } catch (IllegalStateException ex) {
        lockCheckReadiness();
        throw ex;
      }
    }

    @Override
    public boolean tryLock() {
      try {
        ReplyProcessor21.forceSevereAlertProcessing();
        return getLockService().lock(this.key, 0, getLockLeaseForLock());
      } catch (IllegalStateException ex) {
        lockCheckReadiness();
        throw ex;
      } finally {
        ReplyProcessor21.unforceSevereAlertProcessing();
      }
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
      return basicTryLock(time, unit, true);
    }

    // TODO: basicTryLock method is too complex for IDE to analyze
    private boolean basicTryLock(long time, TimeUnit unit, boolean interruptible)
        throws InterruptedException {
      // if (Thread.interrupted()) throw new InterruptedException(); not necessary lockInterruptibly
      // does this
      final DM dm = getDistributionManager();

      long start = System.currentTimeMillis();
      long timeoutMS = getLockTimeoutForLock(time, unit);
      long end;
      if (timeoutMS < 0) {
        timeoutMS = Long.MAX_VALUE;
        end = Long.MAX_VALUE;
      } else {
        end = start + timeoutMS;
      }

      long ackSAThreshold = getSystem().getConfig().getAckSevereAlertThreshold() * 1000;

      long waitInterval;
      long ackWaitThreshold;

      if (ackSAThreshold > 0) {
        ackWaitThreshold = getSystem().getConfig().getAckWaitThreshold() * 1000;
        waitInterval = ackWaitThreshold;
      } else {
        waitInterval = timeoutMS;
        ackWaitThreshold = 0;
      }

      boolean suspected = false;
      boolean severeAlertIssued = false;
      DistributedMember lockHolder = null;
      do {
        try {
          waitInterval = Math.min(end - System.currentTimeMillis(), waitInterval);
          ReplyProcessor21.forceSevereAlertProcessing();
          final boolean gotLock;
          if (interruptible) {
            gotLock =
                getLockService().lockInterruptibly(this.key, waitInterval, getLockLeaseForLock());
          } else {
            gotLock = getLockService().lock(this.key, waitInterval, getLockLeaseForLock());
          }
          if (gotLock) {
            return true;
          }
          if (ackSAThreshold > 0) {
            long elapsed = System.currentTimeMillis() - start;
            if (elapsed > ackWaitThreshold) {
              if (!suspected) {
                // start suspect processing on the holder of the lock
                suspected = true;
                severeAlertIssued = false; // in case this is a new lock holder
                waitInterval = ackSAThreshold;
                DLockRemoteToken remoteToken = ((DLockService) getLockService()).queryLock(key);
                lockHolder = remoteToken.getLessee();
                if (lockHolder != null) {
                  dm.getMembershipManager().suspectMember(lockHolder,
                      "Has not released a global region entry lock in over "
                          + ackWaitThreshold / 1000 + " seconds");
                }
              } else if (elapsed > ackSAThreshold) {
                DLockRemoteToken remoteToken = ((DLockService) getLockService()).queryLock(key);
                if (lockHolder != null && remoteToken.getLessee() != null
                    && lockHolder.equals(remoteToken.getLessee())) {
                  if (!severeAlertIssued) {
                    severeAlertIssued = true;
                    logger.fatal(LocalizedMessage.create(
                        LocalizedStrings.DistributedRegion_0_SECONDS_HAVE_ELAPSED_WAITING_FOR_GLOBAL_REGION_ENTRY_LOCK_HELD_BY_1,
                        new Object[] {ackWaitThreshold + ackSAThreshold, lockHolder}));
                  }
                } else {
                  // the lock holder has changed
                  suspected = false;
                  waitInterval = ackWaitThreshold;
                  lockHolder = null;
                }
              }
            }
          } // ackSAThreshold processing
        } catch (IllegalStateException ex) {
          lockCheckReadiness();
          throw ex;
        } finally {
          ReplyProcessor21.unforceSevereAlertProcessing();
        }
      } while (System.currentTimeMillis() < end);

      return false;
    }

    @Override
    public void unlock() {
      try {
        ReplyProcessor21.forceSevereAlertProcessing();
        getLockService().unlock(this.key);
        if (!DistributedRegion.this.entries.containsKey(this.key)) {
          getLockService().freeResources(this.key);
        }
      } catch (IllegalStateException ex) {
        lockCheckReadiness();
        throw ex;
      } finally {
        ReplyProcessor21.unforceSevereAlertProcessing();
      }
    }

    @Override
    public Condition newCondition() {
      throw new UnsupportedOperationException(
          LocalizedStrings.DistributedRegion_NEWCONDITION_UNSUPPORTED.toLocalizedString());
    }
  }

  private class RegionDistributedLock implements Lock {

    RegionDistributedLock() {}

    @Override
    public void lock() {
      try {
        boolean locked = getLockService().suspendLocking(-1);
        Assert.assertTrue(locked, "Failed to acquire RegionDistributedLock");
      } catch (IllegalStateException ex) {
        lockCheckReadiness();
        throw ex;
      }
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
      // suspendLockingInterruptibly checks Thread.interrupted() to throw InterruptedException
      try {
        boolean locked = getLockService().suspendLockingInterruptibly(-1);
        Assert.assertTrue(locked, "Failed to acquire RegionDistributedLock");
      } catch (IllegalStateException ex) {
        lockCheckReadiness();
        throw ex;
      }
    }

    @Override
    public boolean tryLock() {
      try {
        return getLockService().suspendLocking(0);
      } catch (IllegalStateException ex) {
        lockCheckReadiness();
        throw ex;
      }
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
      // suspendLockingInterruptibly checks Thread.interrupted() to throw InterruptedException
      try {
        return getLockService().suspendLockingInterruptibly(getLockTimeoutForLock(time, unit));
      } catch (IllegalStateException ex) {
        lockCheckReadiness();
        throw ex;
      }
    }

    @Override
    public void unlock() {
      try {
        getLockService().resumeLocking();
      } catch (IllegalStateException ex) {
        lockCheckReadiness();
        throw ex;
      }
    }

    @Override
    public Condition newCondition() {
      throw new UnsupportedOperationException(
          LocalizedStrings.DistributedRegion_NEWCONDITION_UNSUPPORTED.toLocalizedString());
    }
  }

  /**
   * If this region's scope is GLOBAL, get the region distributed lock. The sender is responsible
   * for unlocking.
   * 
   * @return the acquired Lock if the region is GLOBAL and not already suspend, otherwise null.
   */
  private Lock getRegionDistributedLockIfGlobal() throws TimeoutException {
    if (getScope().isGlobal()) {
      if (isLockingSuspendedByCurrentThread())
        return null;
      Lock dlock = getRegionDistributedLock();
      dlock.lock(); // caller is expected to use a try-finally to unlock
      return dlock;
    }
    return null;
  }

  /**
   * Distribute the PutAllOp. This implementation distributes it to peers.
   * 
   * @return token >0 means startOperation finished distribution
   * @since GemFire 5.7
   */
  @Override
  public long postPutAllSend(DistributedPutAllOperation putAllOp,
      VersionedObjectList successfulPuts) {
    long token = -1;
    if (putAllOp.putAllDataSize > 0) {
      token = putAllOp.startOperation();
    } else {
      if (logger.isDebugEnabled()) {
        logger.debug("DR.postPutAll: no data to distribute");
      }
    }
    return token;
  }

  @Override
  public long postRemoveAllSend(DistributedRemoveAllOperation op,
      VersionedObjectList successfulOps) {
    long token = -1;
    if (op.removeAllDataSize > 0) {
      token = op.startOperation();
    } else {
      getCache().getLoggerI18n().fine("DR.postRemoveAll: no data to distribute");
    }
    return token;
  }

  @Override
  public VersionedObjectList basicPutAll(final Map<?, ?> map,
      final DistributedPutAllOperation putAllOp, final Map<Object, VersionTag> retryVersions) {
    Lock dlock = this.getRegionDistributedLockIfGlobal();
    try {
      return super.basicPutAll(map, putAllOp, retryVersions);
    } finally {
      if (dlock != null) {
        dlock.unlock();
      }
    }
  }

  @Override
  public VersionedObjectList basicRemoveAll(final Collection<Object> keys,
      final DistributedRemoveAllOperation removeAllOp, final List<VersionTag> retryVersions) {
    Lock dlock = this.getRegionDistributedLockIfGlobal();
    try {
      return super.basicRemoveAll(keys, removeAllOp, retryVersions);
    } finally {
      if (dlock != null) {
        dlock.unlock();
      }
    }
  }

  /**
   * Returns the missing required roles after waiting up to the timeout
   * 
   * @throws IllegalStateException if region is not configured with required roles
   */
  public Set waitForRequiredRoles(long timeout) throws InterruptedException {
    if (Thread.interrupted()) {
      throw new InterruptedException();
    }
    checkReadiness();

    if (!getMembershipAttributes().hasRequiredRoles()) {
      throw new IllegalStateException(
          LocalizedStrings.DistributedRegion_REGION_HAS_NOT_BEEN_CONFIGURED_WITH_REQUIRED_ROLES
              .toLocalizedString());
    }
    if (!this.isMissingRequiredRoles) { // should we delete this check?
      if (logger.isDebugEnabled()) {
        logger.debug("No missing required roles to wait for.");
      }
      return Collections.emptySet(); // early-out: no missing required roles
    }
    if (timeout != 0) { // if timeout is zero then fall through past waits
      if (timeout == -1) { // infinite timeout
        while (this.isMissingRequiredRoles) {
          checkReadiness();
          // bail if distribution has stopped
          this.cache.getCancelCriterion().checkCancelInProgress(null);
          synchronized (this.missingRequiredRoles) {
            // one more check while synced
            if (this.isMissingRequiredRoles) {
              if (logger.isDebugEnabled()) {
                logger.debug("About to wait for missing required roles.");
              }
              // TODO an infinite wait here might be a problem...
              this.missingRequiredRoles.wait(); // spurious wakeup ok
            }
          }
        }
      } else { // use the timeout
        long endTime = System.currentTimeMillis() + timeout;
        while (this.isMissingRequiredRoles) {
          checkReadiness();
          // bail if distribution has stopped
          this.cache.getCancelCriterion().checkCancelInProgress(null);
          synchronized (this.missingRequiredRoles) {
            // one more check while synced
            if (this.isMissingRequiredRoles) {
              long timeToWait = endTime - System.currentTimeMillis();
              if (timeToWait > 0) {
                if (logger.isDebugEnabled()) {
                  logger.debug("About to wait up to {} milliseconds for missing required roles.",
                      timeToWait);
                }
                this.missingRequiredRoles.wait(timeToWait); // spurious wakeup ok
              } else {
                break;
              }
            }
          }
        }
      }
    }
    // check readiness again: thread may have been notified at destroy time
    checkReadiness();
    if (this.isMissingRequiredRoles) {
      // sync on missingRequiredRoles to prevent mods to required role status...
      synchronized (this.missingRequiredRoles) {
        return Collections.unmodifiableSet(new HashSet<>(this.missingRequiredRoles));
      }
    } else {
      return Collections.emptySet();
    }
  }

  /** Returns true if the role is currently present this region's membership. */
  public boolean isRoleInRegionMembership(Role role) {
    checkReadiness();
    return basicIsRoleInRegionMembership(role);
  }

  private boolean basicIsRoleInRegionMembership(Role role) {
    if (getSystem().getDistributedMember().getRoles().contains(role)) {
      // since we are playing the role
      return true;
    }
    for (DistributedMember member : this.distAdvisor.adviseGeneric()) {
      Set<Role> roles = member.getRoles();
      if (roles.contains(role)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public void remoteRegionInitialized(CacheProfile profile) {
    synchronized (this.advisorListener) {
      if (this.advisorListener.members == null && hasListener()) {
        Object callback = TEST_HOOK_ADD_PROFILE ? profile : null;
        RegionEventImpl event = new RegionEventImpl(this, Operation.REGION_CREATE, callback, true,
            profile.peerMemberId);
        dispatchListenerEvent(EnumListenerEvent.AFTER_REMOTE_REGION_CREATE, event);
      }
    }
  }

  @Override
  protected void removeSenderFromAdvisor(InternalDistributedMember sender, int serial,
      boolean regionDestroyed) {
    getDistributionAdvisor().removeIdWithSerial(sender, serial, regionDestroyed);
  }

  /** doesn't throw RegionDestroyedException, used by CacheDistributionAdvisor */
  @Override
  public DistributionAdvisee getParentAdvisee() {
    return (DistributionAdvisee) basicGetParentRegion();
  }

  /**
   * Used to get membership events from our advisor to implement RegionMembershipListener
   * invocations.
   * 
   * @since GemFire 5.0
   */
  protected class AdvisorListener implements MembershipListener {
    Set members = new HashSet(); // package-private to avoid synthetic accessor

    protected boolean destroyed = false;

    synchronized void addMembers(Set newMembers) {
      this.members.addAll(newMembers);
    }

    protected synchronized Set getInitialMembers() {
      Set initMembers = this.members;
      this.members = null;
      return initMembers;
    }

    @Override
    public void quorumLost(Set<InternalDistributedMember> failures,
        List<InternalDistributedMember> remaining) {
      // do nothing
    }

    @Override
    public void memberSuspect(InternalDistributedMember id, InternalDistributedMember whoSuspected,
        String reason) {
      // do nothing
    }

    /** called when membership listeners are added after region creation */
    synchronized void initRMLWrappers() {
      Set<InternalDistributedMember> membersWithThisRegion =
          DistributedRegion.this.distAdvisor.adviseGeneric();
      initPostCreateRegionMembershipListeners(membersWithThisRegion);
    }

    @Override
    public synchronized void memberJoined(InternalDistributedMember id) {
      if (this.destroyed) {
        return;
      }
      if (this.members != null) {
        this.members.add(id);
      }
      // bug #44684 - do not notify listener of create until remote member is initialized
      if (getMembershipAttributes().hasRequiredRoles()) {
        // newlyAcquiredRoles is used for intersection and RoleEvent
        Set newlyAcquiredRoles = Collections.emptySet();
        synchronized (missingRequiredRoles) {
          if (isMissingRequiredRoles) {
            Set roles = id.getRoles();
            newlyAcquiredRoles = new HashSet(missingRequiredRoles);
            newlyAcquiredRoles.retainAll(roles); // find the intersection
            if (!newlyAcquiredRoles.isEmpty()) {
              missingRequiredRoles.removeAll(newlyAcquiredRoles);
              if (this.members == null && missingRequiredRoles.isEmpty()) {
                isMissingRequiredRoles = false;
                getCachePerfStats().incReliableRegionsMissing(-1);
                if (getMembershipAttributes().getLossAction().isAllAccess())
                  getCachePerfStats().incReliableRegionsMissingFullAccess(-1); // rahul
                else if (getMembershipAttributes().getLossAction().isLimitedAccess())
                  getCachePerfStats().incReliableRegionsMissingLimitedAccess(-1);
                else if (getMembershipAttributes().getLossAction().isNoAccess())
                  getCachePerfStats().incReliableRegionsMissingNoAccess(-1);

                boolean async = resumeReliability(id, newlyAcquiredRoles);
                if (async) {
                  this.destroyed = true;
                }
              }
            }
          }
          if (!this.destroyed) {
            // any number of threads may be waiting on missingRequiredRoles
            missingRequiredRoles.notifyAll();
          }
        }
        if (!this.destroyed && this.members == null && hasListener()) {
          if (!newlyAcquiredRoles.isEmpty()) {
            // fire afterRoleGain event
            RoleEventImpl relEvent = new RoleEventImpl(DistributedRegion.this,
                Operation.REGION_CREATE, null, true, id, newlyAcquiredRoles);
            dispatchListenerEvent(EnumListenerEvent.AFTER_ROLE_GAIN, relEvent);
          }
        }
      }
    }

    public synchronized void memberDeparted(InternalDistributedMember id, boolean crashed) {
      if (this.destroyed) {
        return;
      }
      if (this.members != null) {
        this.members.remove(id);
      }
      if (this.members == null && hasListener()) {
        RegionEventImpl event =
            new RegionEventImpl(DistributedRegion.this, Operation.REGION_CLOSE, null, true, id);
        if (crashed) {
          dispatchListenerEvent(EnumListenerEvent.AFTER_REMOTE_REGION_CRASH, event);
        } else {
          // @todo darrel: it would be nice to know if what actual op was done
          // could be close, local destroy, or destroy (or load snap?)
          if (DestroyRegionOperation.isRegionDepartureNotificationOk()) {
            dispatchListenerEvent(EnumListenerEvent.AFTER_REMOTE_REGION_DEPARTURE, event);
          }
        }
      }
      if (getMembershipAttributes().hasRequiredRoles()) {
        Set newlyMissingRoles = Collections.emptySet();
        synchronized (missingRequiredRoles) {
          Set roles = id.getRoles();
          for (Iterator iter = roles.iterator(); iter.hasNext();) {
            Role role = (Role) iter.next();
            if (getMembershipAttributes().getRequiredRoles().contains(role)
                && !basicIsRoleInRegionMembership(role)) {
              if (newlyMissingRoles == Collections.emptySet()) {
                newlyMissingRoles = new HashSet();
              }
              newlyMissingRoles.add(role);
              if (this.members == null && !isMissingRequiredRoles) {
                isMissingRequiredRoles = true;
                getCachePerfStats().incReliableRegionsMissing(1);
                if (getMembershipAttributes().getLossAction().isAllAccess())
                  getCachePerfStats().incReliableRegionsMissingFullAccess(1); // rahul
                else if (getMembershipAttributes().getLossAction().isLimitedAccess())
                  getCachePerfStats().incReliableRegionsMissingLimitedAccess(1);
                else if (getMembershipAttributes().getLossAction().isNoAccess())
                  getCachePerfStats().incReliableRegionsMissingNoAccess(1);

                boolean async = lostReliability(id, newlyMissingRoles);
                if (async) {
                  this.destroyed = true;
                }
              }
            }
          }
          if (!this.destroyed) {
            missingRequiredRoles.addAll(newlyMissingRoles);
            // any number of threads may be waiting on missingRequiredRoles...
            missingRequiredRoles.notifyAll();
          }
        }
        if (!this.destroyed && this.members == null && hasListener()) {
          if (!newlyMissingRoles.isEmpty()) {
            // fire afterRoleLoss event
            RoleEventImpl relEvent = new RoleEventImpl(DistributedRegion.this,
                Operation.REGION_CLOSE, null, true, id, newlyMissingRoles);
            dispatchListenerEvent(EnumListenerEvent.AFTER_ROLE_LOSS, relEvent);
          }
        }
      }
    }
  }

  /**
   * Used to bootstrap txState.
   * 
   * @return member with primary bucket for partitionedRegions
   */
  @Override
  public DistributedMember getOwnerForKey(KeyInfo key) {
    assert !this.isInternalRegion() || this.isMetaRegionWithTransactions();
    if (!this.getAttributes().getDataPolicy().withStorage() || (this.concurrencyChecksEnabled
        && this.getAttributes().getDataPolicy() == DataPolicy.NORMAL)) {
      // execute on random replicate
      return getRandomReplicate();
    }
    // if we are non-persistent, forward transactions to
    // a persistent member
    if (this.concurrencyChecksEnabled && !generateVersionTag) {
      return getRandomPersistentReplicate();
    }
    return super.getOwnerForKey(key);
  }


  /**
   * Returns the size in this region.
   *
   * This is used in a transaction to find the size of the region on the transaction hosting node.
   *
   * @param target the host of the transaction TXState
   * @return the number of entries in this region
   */
  public int getRegionSize(DistributedMember target) {
    try {
      RemoteSizeMessage.SizeResponse response =
          RemoteSizeMessage.send(Collections.singleton(target), this);
      return response.waitForSize();
    } catch (RegionDestroyedException rde) {
      throw new TransactionDataNotColocatedException(
          LocalizedStrings.RemoteMessage_REGION_0_NOT_COLOCATED_WITH_TRANSACTION
              .toLocalizedString(rde.getRegionFullPath()),
          rde);
    } catch (Exception e) {
      throw new TransactionException(e);
    }
  }

  /**
   * Execute the provided named function in all locations that contain the given keys. So function
   * can be executed on just one fabric node, executed in parallel on a subset of nodes in parallel
   * across all the nodes.
   * 
   * @since GemFire 5.8
   */
  @Override
  public ResultCollector executeFunction(final DistributedRegionFunctionExecutor execution,
      final Function function, final Object args, final ResultCollector rc, final Set filter,
      final ServerToClientFunctionResultSender sender) {
    DistributedMember target = getTransactionalNode();
    if (target != null) {
      if (target.equals(getMyId())) {
        return executeLocally(execution, function, args, 0, rc, filter, sender);
      }
      return executeOnReplicate(execution, function, args, rc, filter, target);
    } else if (this.getAttributes().getDataPolicy().withReplication()
        || this.getAttributes().getDataPolicy().withPreloaded()) {
      // execute locally
      final Set<InternalDistributedMember> singleMember = Collections.singleton(getMyId());
      execution.validateExecution(function, singleMember);
      execution.setExecutionNodes(singleMember);
      return executeLocally(execution, function, args, 0, rc, filter, sender);
    } else {
      // select a random replicate
      target = getRandomReplicate();
      if (target == null) {
        throw new FunctionException(
            LocalizedStrings.DistributedRegion_NO_REPLICATED_REGION_FOUND_FOR_EXECUTING_FUNCTION_0
                .toLocalizedString(function.getId()));
      }
    }
    final LocalResultCollector<?, ?> localRC = execution.getLocalResultCollector(function, rc);
    return executeOnReplicate(execution, function, args, localRC, filter, target);
  }

  private ResultCollector executeOnReplicate(final DistributedRegionFunctionExecutor execution,
      final Function function, final Object args, ResultCollector rc, final Set filter,
      final DistributedMember target) {
    final Set singleMember = Collections.singleton(target);
    execution.validateExecution(function, singleMember);
    execution.setExecutionNodes(singleMember);

    HashMap<InternalDistributedMember, Object> memberArgs =
        new HashMap<InternalDistributedMember, Object>();
    memberArgs.put((InternalDistributedMember) target,
        execution.getArgumentsForMember(target.getId()));

    ResultSender resultSender = new DistributedRegionFunctionResultSender(null, rc, function,
        execution.getServerResultSender());

    DistributedRegionFunctionResultWaiter waiter =
        new DistributedRegionFunctionResultWaiter(this.getSystem(), this.getFullPath(), rc,
            function, filter, Collections.singleton(target), memberArgs, resultSender);

    rc = waiter.getFunctionResultFrom(Collections.singleton(target), function, execution);
    return rc;
  }

  /**
   * @return the node which a transaction is already is progress, null otherwise
   */
  private DistributedMember getTransactionalNode() {
    if (cache.getTxManager().getTXState() != null) {
      return cache.getTxManager().getTXState().getTarget();
    }
    return null;
  }

  /**
   * Implementation of {@link ProfileVisitor} that selects a random replicated member from the
   * available ones for this region.
   */
  static class GetRandomReplicate implements ProfileVisitor<DistributedMember> {

    private boolean onlyPersistent = false;

    InternalDistributedMember member = null;

    private int randIndex = -1;

    public GetRandomReplicate() {}

    public GetRandomReplicate(boolean onlyPersistent) {
      this.onlyPersistent = onlyPersistent;
    }

    public boolean visit(DistributionAdvisor advisor, Profile profile, int profileIndex,
        int numProfiles, DistributedMember member) {
      final CacheProfile cp = (CacheProfile) profile;
      if (this.randIndex < 0) {
        this.randIndex = PartitionedRegion.RANDOM.nextInt(numProfiles);
      }
      if (cp.dataPolicy.withReplication() && cp.regionInitialized) {
        if (onlyPersistent && !cp.dataPolicy.withPersistence()) {
          return true;
        }
        // store the last replicated member in any case since in the worst case
        // there may be no replicated node after "randIndex" in which case the
        // last visited member will be used
        this.member = cp.getDistributedMember();
        if (profileIndex >= this.randIndex) {
          return false;
        }
      }
      return true;
    }
  }

  /**
   * @return a random replicate, null if there are none
   */
  public InternalDistributedMember getRandomReplicate() {
    /*
     * [sumedh] The old code causes creation of a unnecessary HashSet and population with all
     * replicates (which may be large), then copy into an array and then selection of a random one
     * from that. The new approach uses a much more efficient visitor instead. Set replicates =
     * this.getCacheDistributionAdvisor().adviseReplicates(); if (replicates.isEmpty()) { return
     * null; } return (InternalDistributedMember)(replicates .toArray()[new
     * Random().nextInt(replicates.size())]);
     */
    final GetRandomReplicate getReplicate = new GetRandomReplicate();
    this.getCacheDistributionAdvisor().accept(getReplicate, null);
    return getReplicate.member;
  }

  /**
   * @return a random persistent replicate, null if there is none
   */
  public InternalDistributedMember getRandomPersistentReplicate() {
    final GetRandomReplicate getPersistentReplicate = new GetRandomReplicate(true);
    this.getCacheDistributionAdvisor().accept(getPersistentReplicate, null);
    return getPersistentReplicate.member;
  }

  void executeOnRegion(DistributedRegionFunctionStreamingMessage msg, final Function function,
      final Object args, int prid, final Set filter, boolean isReExecute) throws IOException {
    final DM dm = getDistributionManager();
    ResultSender resultSender = new DistributedRegionFunctionResultSender(dm, msg, function);
    final RegionFunctionContextImpl context = new RegionFunctionContextImpl(cache, function.getId(),
        this, args, filter, null, null, resultSender, isReExecute);
    FunctionStats stats = FunctionStats.getFunctionStats(function.getId(), dm.getSystem());
    try {
      long start = stats.startTime();
      stats.startFunctionExecution(function.hasResult());
      function.execute(context);
      stats.endFunctionExecution(start, function.hasResult());
    } catch (FunctionException functionException) {
      if (logger.isDebugEnabled()) {
        logger.debug("FunctionException occurred on remote node  while executing Function: {}",
            function.getId(), functionException);
      }
      stats.endFunctionExecutionWithException(function.hasResult());
      throw functionException;
    } catch (CacheClosedException cacheClosedexception) {
      if (logger.isDebugEnabled()) {
        logger.debug("CacheClosedException occurred on remote node  while executing Function: {}",
            function.getId(), cacheClosedexception);
      }
      throw cacheClosedexception;
    } catch (Exception exception) {
      if (logger.isDebugEnabled()) {
        logger.debug("Exception occurred on remote node  while executing Function: {}",
            function.getId(), exception);
      }
      stats.endFunctionExecutionWithException(function.hasResult());
      throw new FunctionException(exception);
    }
  }

  ResultCollector executeLocally(final DistributedRegionFunctionExecutor execution,
      final Function function, final Object args, int prid, final ResultCollector rc,
      final Set filter, final ServerToClientFunctionResultSender sender) {
    final LocalResultCollector<?, ?> localRC = execution.getLocalResultCollector(function, rc);
    final DM dm = getDistributionManager();
    final DistributedRegionFunctionResultSender resultSender =
        new DistributedRegionFunctionResultSender(dm, localRC, function, sender);
    final RegionFunctionContextImpl context = new RegionFunctionContextImpl(cache, function.getId(),
        DistributedRegion.this, args, filter, null, null, resultSender, execution.isReExecute());
    execution.executeFunctionOnLocalNode(function, context, resultSender, dm, isTX());
    return localRC;
  }

  @Override
  protected void setMemoryThresholdFlag(MemoryEvent event) {
    Set<InternalDistributedMember> others = getCacheDistributionAdvisor().adviseGeneric();

    if (event.isLocal() || others.contains(event.getMember())) {
      if (event.getState().isCritical() && !event.getPreviousState().isCritical()
          && (event.getType() == ResourceType.HEAP_MEMORY
              || (event.getType() == ResourceType.OFFHEAP_MEMORY && getOffHeap()))) {
        setMemoryThresholdReachedCounterTrue(event.getMember());
      } else if (!event.getState().isCritical() && event.getPreviousState().isCritical()
          && (event.getType() == ResourceType.HEAP_MEMORY
              || (event.getType() == ResourceType.OFFHEAP_MEMORY && getOffHeap()))) {
        removeMemberFromCriticalList(event.getMember());
      }
    }
  }

  @Override
  public void removeMemberFromCriticalList(DistributedMember member) {
    if (logger.isDebugEnabled()) {
      logger.debug("DR: removing member {} from critical member list", member);
    }
    synchronized (this.memoryThresholdReachedMembers) {
      this.memoryThresholdReachedMembers.remove(member);
      if (this.memoryThresholdReachedMembers.size() == 0) {
        memoryThresholdReached.set(false);
      }
    }
  }

  @Override
  public Set<DistributedMember> getMemoryThresholdReachedMembers() {
    synchronized (this.memoryThresholdReachedMembers) {
      return Collections.unmodifiableSet(this.memoryThresholdReachedMembers);
    }
  }

  @Override
  public void initialCriticalMembers(boolean localMemoryIsCritical,
      Set<InternalDistributedMember> criticalMembers) {
    Set<InternalDistributedMember> others = getCacheDistributionAdvisor().adviseGeneric();
    for (InternalDistributedMember idm : criticalMembers) {
      if (others.contains(idm)) {
        setMemoryThresholdReachedCounterTrue(idm);
      }
    }
  }

  /**
   * @param idm member whose threshold has been exceeded
   */
  private void setMemoryThresholdReachedCounterTrue(final DistributedMember idm) {
    synchronized (this.memoryThresholdReachedMembers) {
      this.memoryThresholdReachedMembers.add(idm);
      if (this.memoryThresholdReachedMembers.size() > 0) {
        memoryThresholdReached.set(true);
      }
    }
  }

  /**
   * Fetch Version for the given key from a remote replicate member.
   * 
   * @throws EntryNotFoundException if the entry is not found on replicate member
   * @return VersionTag for the key
   */
  protected VersionTag fetchRemoteVersionTag(Object key) {
    VersionTag tag = null;
    assert this.dataPolicy != DataPolicy.REPLICATE;
    final TXStateProxy tx = cache.getTXMgr().pauseTransaction();
    try {
      boolean retry = true;
      InternalDistributedMember member = getRandomReplicate();
      while (retry) {
        try {
          if (member == null) {
            break;
          }
          FetchVersionResponse response = RemoteFetchVersionMessage.send(member, this, key);
          tag = response.waitForResponse();
          retry = false;
        } catch (RemoteOperationException e) {
          member = getRandomReplicate();
          if (member != null) {
            if (logger.isDebugEnabled()) {
              logger.debug("Retrying RemoteFetchVersionMessage on member:{}", member);
            }
          }
        }
      }
    } finally {
      if (tx != null) {
        cache.getTXMgr().unpauseTransaction(tx);
      }
    }
    return tag;
  }

  /**
   * Test hook for bug 48578. Returns true if it sees a net loader. Returns false if it does not
   * have one.
   */
  public boolean hasNetLoader() {
    return this.hasNetLoader(getCacheDistributionAdvisor());
  }

  @Override
  public long getLatestLastAccessTimeFromOthers(Object key) {
    LatestLastAccessTimeOperation op = new LatestLastAccessTimeOperation(this, key);
    return op.getLatestLastAccessTime();
  }

}

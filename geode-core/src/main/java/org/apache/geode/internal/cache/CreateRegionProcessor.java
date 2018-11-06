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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.DataSerializer;
import org.apache.geode.SystemFailure;
import org.apache.geode.cache.DynamicRegionFactory;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.Scope;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionAdvisee;
import org.apache.geode.distributed.internal.DistributionAdvisor;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.HighPriorityDistributionMessage;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.MessageWithReply;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.ReplyMessage;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.cache.CacheDistributionAdvisor.CacheProfile;
import org.apache.geode.internal.cache.CacheDistributionAdvisor.InitialImageAdvice;
import org.apache.geode.internal.cache.event.EventSequenceNumberHolder;
import org.apache.geode.internal.cache.ha.ThreadIdentifier;
import org.apache.geode.internal.cache.partitioned.Bucket;
import org.apache.geode.internal.cache.partitioned.PRLocallyDestroyedException;
import org.apache.geode.internal.cache.partitioned.RegionAdvisor;
import org.apache.geode.internal.cache.partitioned.RegionAdvisor.PartitionProfile;
import org.apache.geode.internal.cache.persistence.PersistentMemberID;
import org.apache.geode.internal.logging.LogService;

/**
 * This message processor handles creation and initial exchange of
 * org.apache.geode.internal.cache.CacheDistributionAdvisor.Profiles. It represents creation of a
 * {@link CacheDistributionAdvisee}. Name remains CreateRegion to avoid merge conflicts.
 */
public class CreateRegionProcessor implements ProfileExchangeProcessor {
  private static final Logger logger = LogService.getLogger();

  protected CacheDistributionAdvisee newRegion;

  /** Creates a new instance of CreateRegionProcessor */
  public CreateRegionProcessor(CacheDistributionAdvisee newRegion) {
    this.newRegion = newRegion;
  }

  /** this method tells other members that the region is being created */
  public void initializeRegion() {
    InternalDistributedSystem system = this.newRegion.getSystem();
    // try 5 times, see CreateRegionMessage#skipDuringInitialization
    for (int retry = 0; retry < 5; retry++) {
      Set recps = getRecipients();

      if (logger.isDebugEnabled()) {
        logger.debug("Creating region {}", this.newRegion);
      }

      if (recps.isEmpty()) {
        if (logger.isDebugEnabled()) {
          logger.debug("CreateRegionProcessor.initializeRegion, no recipients, msg not sent");
        }
        this.newRegion.getDistributionAdvisor().setInitialized();

        ((LocalRegion) this.newRegion).getEventTracker().setInitialized();
        return;
      }

      CreateRegionReplyProcessor replyProc = new CreateRegionReplyProcessor(recps);
      newRegion.registerCreateRegionReplyProcessor(replyProc);

      boolean useMcast = false; // multicast is disabled for this message for now
      CreateRegionMessage msg = getCreateRegionMessage(recps, replyProc, useMcast);

      // since PR buckets can be created during cache entry operations, enable
      // severe alert processing if we're creating one of them
      if (((LocalRegion) newRegion).isUsedForPartitionedRegionBucket()) {
        replyProc.enableSevereAlertProcessing();
        msg.severeAlertCompatible = true;
      }

      this.newRegion.getDistributionManager().putOutgoing(msg);
      // this was in a while() loop, which is incorrect use of a reply processor.
      // Reply procs are deregistered when they return from waitForReplies
      try {
        // Don't allow a region to be created if the distributed system is
        // disconnecting
        this.newRegion.getCache().getCancelCriterion().checkCancelInProgress(null);

        // This isn't right. We should disable region creation in general, not just
        // the remote case here...
        // // Similarly, don't allow new regions to be created if the cache is closing
        try {
          replyProc.waitForRepliesUninterruptibly();
          if (!replyProc.needRetry()) {
            break;
          }
        } catch (ReplyException e) {
          Throwable t = e.getCause();
          if (t instanceof IllegalStateException) {
            // region is incompatible with region in another cache
            throw (IllegalStateException) t;
          }
          e.handleCause();
          break;
        }
      } finally {
        replyProc.cleanup();
        ((LocalRegion) this.newRegion).getEventTracker().setInitialized();
        if (((LocalRegion) this.newRegion).isUsedForPartitionedRegionBucket()) {
          if (logger.isDebugEnabled()) {
            logger.debug("initialized bucket event tracker: {}",
                ((LocalRegion) this.newRegion).getEventTracker());
          }
        }
      }
    } // while
    // tell advisor that it has been initialized since a profile exchange occurred
    this.newRegion.getDistributionAdvisor().setInitialized();
  }

  protected Set getRecipients() {
    DistributionAdvisee parent = this.newRegion.getParentAdvisee();
    Set recps = null;
    if (parent == null) { // root region, all recipients
      InternalDistributedSystem system = this.newRegion.getSystem();
      recps = system.getDistributionManager().getOtherDistributionManagerIds();
    } else {
      // get recipients that have the parent region defined as distributed.
      recps = getAdvice();
    }
    return recps;
  }

  public InitialImageAdvice getInitialImageAdvice(InitialImageAdvice previousAdvice) {
    return newRegion.getCacheDistributionAdvisor().adviseInitialImage(previousAdvice);
  }

  private Set getAdvice() {
    if (this.newRegion instanceof BucketRegion) {
      return ((Bucket) this.newRegion).getBucketAdvisor().adviseProfileExchange();
    } else {
      DistributionAdvisee rgn = this.newRegion.getParentAdvisee();
      DistributionAdvisor advisor = rgn.getDistributionAdvisor();
      return advisor.adviseGeneric();
    }
  }

  protected CreateRegionMessage getCreateRegionMessage(Set recps, ReplyProcessor21 proc,
      boolean useMcast) {
    CreateRegionMessage msg = new CreateRegionMessage();
    msg.regionPath = this.newRegion.getFullPath();
    msg.profile = (CacheProfile) this.newRegion.getProfile();
    msg.processorId = proc.getProcessorId();
    msg.concurrencyChecksEnabled = this.newRegion.getAttributes().getConcurrencyChecksEnabled();
    msg.setMulticast(useMcast);
    msg.setRecipients(recps);
    return msg;
  }

  public void setOnline(InternalDistributedMember target) {
    // nothing
  }

  class CreateRegionReplyProcessor extends ReplyProcessor21 {

    CreateRegionReplyProcessor(Set members) {
      super((InternalDistributedSystem) CreateRegionProcessor.this.newRegion.getCache()
          .getDistributedSystem(), members);
    }

    private final Map<DistributedMember, Map<ThreadIdentifier, EventSequenceNumberHolder>> remoteEventStates =
        new ConcurrentHashMap<>();

    private boolean allMembersSkippedChecks = true;

    public Map<ThreadIdentifier, EventSequenceNumberHolder> getEventState(
        InternalDistributedMember provider) {
      return this.remoteEventStates.get(provider);
    }

    /**
     * true if all members skipped CreateRegionMessage#checkCompatibility(), in which case
     * CreateRegionMessage should be retried.
     */
    public boolean needRetry() {
      return this.allMembersSkippedChecks;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void process(DistributionMessage msg) {
      Assert.assertTrue(msg instanceof CreateRegionReplyMessage,
          "CreateRegionProcessor is unable to process message of type " + msg.getClass());
      CreateRegionReplyMessage reply = (CreateRegionReplyMessage) msg;
      LocalRegion lr = (LocalRegion) newRegion;
      if (logger.isDebugEnabled()) {
        logger.debug("CreateRegionProcessor processing {}", msg);
      }
      try {
        if (reply.profile != null) {
          if (newRegion instanceof DistributedRegion) {
            DistributedRegion dr = (DistributedRegion) newRegion;
            if (!dr.getDataPolicy().withPersistence() && reply.profile.isPersistent) {
              dr.setGeneratedVersionTag(false);
            }
          }
          if (CreateRegionMessage.isLocalAccessor(newRegion) && reply.profile.isPersistent) {
            lr.enableConcurrencyChecks();
          }

          CacheDistributionAdvisor cda =
              CreateRegionProcessor.this.newRegion.getCacheDistributionAdvisor();
          cda.putProfile(reply.profile);
          if (reply.bucketProfiles != null) {
            RegionAdvisor ra = (RegionAdvisor) cda;
            ra.putBucketRegionProfiles(reply.bucketProfiles);
          }

          // Save all event states, need to initiate the event tracker from the GII provider
          if (reply.eventState != null) {
            remoteEventStates.put(reply.getSender(),
                (Map<ThreadIdentifier, EventSequenceNumberHolder>) reply.eventState);
          }

          if (lr.isUsedForPartitionedRegionBucket()) {
            ((BucketRegion) lr).updateEventSeqNum(reply.seqKeyForWan);
          }
          // Process any delta filter-profile messages received during profile
          // exchange.
          // The pending messages are queued in the local profile.
          FilterProfile remoteFP = reply.profile.filterProfile;
          if (remoteFP != null) {
            FilterProfile localFP = ((LocalRegion) newRegion).filterProfile;
            // localFP can be null and remoteFP not null when upgrading from 7.0.1.14 to 7.0.1.15
            if (localFP != null) {
              List messages = localFP.getQueuedFilterProfileMsgs(reply.getSender());
              // Thread init level is set since region is used during CQ registration.
              int oldLevel = LocalRegion.setThreadInitLevelRequirement(LocalRegion.ANY_INIT);
              try {
                remoteFP.processQueuedFilterProfileMsgs(messages);
              } finally {
                LocalRegion.setThreadInitLevelRequirement(oldLevel);
                localFP.removeQueuedFilterProfileMsgs(reply.getSender());
              }
            }
          }
        }
        if (reply.destroyedId != null && newRegion instanceof DistributedRegion) {
          DistributedRegion dr = (DistributedRegion) newRegion;
          dr.getPersistenceAdvisor().removeMember(reply.destroyedId);
        }
        if (!reply.skippedCompatibilityChecks) {
          allMembersSkippedChecks = false;
        }
      } finally {
        // invoke super.process() even in case of exceptions (bug #41556)
        if (logger.isDebugEnabled()) {
          logger.debug("CreateRegionProcessor invoking super.process()");
        }
        super.process(msg);
      }
    }

    /**
     * IllegalStateException is an anticipated reply exception. Receiving multiple replies with this
     * exception is normal.
     */
    @Override
    protected boolean logMultipleExceptions() {
      return false;
    }
  }

  public static class CreateRegionMessage extends HighPriorityDistributionMessage
      implements MessageWithReply {

    public boolean concurrencyChecksEnabled;
    protected String regionPath;
    protected CacheProfile profile;
    protected int processorId;

    private transient boolean incompatible = false;
    private transient ReplyException replyException;
    private transient CacheProfile replyProfile;
    private transient ArrayList replyBucketProfiles;
    private transient Object eventState;
    protected transient boolean severeAlertCompatible;
    private transient boolean skippedCompatibilityChecks;

    @Override
    public int getProcessorId() {
      return this.processorId;
    }

    @Override
    public boolean isSevereAlertCompatible() {
      return severeAlertCompatible;
    }

    @Override
    public boolean sendViaUDP() {
      return true;
    }

    @Override
    protected void process(ClusterDistributionManager dm) {
      int oldLevel = // Set thread local flag to allow entrance through initialization Latch
          LocalRegion.setThreadInitLevelRequirement(LocalRegion.ANY_INIT);
      LocalRegion lclRgn = null;

      PersistentMemberID destroyedId = null;
      try {
        // get the region from the path, but do NOT wait on initialization,
        // otherwise we could have a distributed deadlock

        InternalCache cache = dm.getExistingCache();

        // Fix for bug 42051 - Discover any regions that are in the process
        // of being destroyed
        DistributedRegion destroyingRegion = cache.getRegionInDestroy(this.regionPath);
        if (destroyingRegion != null) {
          destroyedId = destroyingRegion.getPersistentID();
        }

        lclRgn = (LocalRegion) cache.getRegion(this.regionPath);

        if (lclRgn instanceof CacheDistributionAdvisee) {
          // bug 37604 - don't return a profile if this is a bucket and the owner
          // has been locally destroyed
          if (lclRgn.isUsedForPartitionedRegionBucket()) {
            if (!((BucketRegion) lclRgn).isPartitionedRegionOpen()) {
              if (logger.isDebugEnabled()) {
                logger.debug("<Partitioned Region Closed or Locally Destroyed> {}", this);
              }
              return;
            }
          }
          handleCacheDistributionAdvisee((CacheDistributionAdvisee) lclRgn, true);
        } else {
          if (lclRgn == null) {
            // check to see if a ProxyBucketRegion (not a true region) exists
            handleCacheDistributionAdvisee(
                PartitionedRegionHelper.getProxyBucketRegion(cache, this.regionPath, false), false);
          } else {
            if (logger.isDebugEnabled()) {
              logger.debug("<lclRgn scope is not distributed. Scope={}> {}",
                  lclRgn.getAttributes().getScope(), this);
            }
          }
        }
      } catch (PRLocallyDestroyedException ignore) {
        if (logger.isDebugEnabled()) {
          logger.debug("<Region Locally Destroyed> {}", this);
        }
      } catch (RegionDestroyedException ignore) {
        if (logger.isDebugEnabled()) {
          logger.debug("<RegionDestroyed> {}", this);
        }
      } catch (CancelException ignore) {
        if (logger.isDebugEnabled()) {
          logger.debug("<CancelException> {}", this);
        }
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
        if (replyException == null) {
          replyException = new ReplyException(t);
        } else {
          logger.warn(String.format("More than one exception thrown in %s", this),
              t);
        }
      } finally {
        LocalRegion.setThreadInitLevelRequirement(oldLevel);
        CreateRegionReplyMessage replyMsg = new CreateRegionReplyMessage();
        replyMsg.profile = replyProfile;
        replyMsg.bucketProfiles = replyBucketProfiles;
        replyMsg.eventState = this.eventState;
        replyMsg.destroyedId = destroyedId;
        replyMsg.setProcessorId(this.processorId);
        replyMsg.setSender(dm.getId()); // for EventStateHelper.dataSerialize
        replyMsg.setRecipient(this.getSender());
        replyMsg.skippedCompatibilityChecks = this.skippedCompatibilityChecks;

        if (lclRgn != null && lclRgn.isUsedForPartitionedRegionBucket()) {
          replyMsg.seqKeyForWan = ((BucketRegion) lclRgn).getEventSeqNum().get();
        }
        if (replyException != null && !this.incompatible) {
          // no need to log the exception if it was caused by compatibility check
          if (logger.isDebugEnabled()) {
            logger.debug("While processing '{}', got exception, returning to sender", this,
                replyException);
          }
        }
        replyMsg.setException(replyException);
        dm.putOutgoing(replyMsg);
        if (lclRgn instanceof PartitionedRegion)
          ((PartitionedRegion) lclRgn).sendIndexCreationMsg(this.getSender());
      }
    }

    /**
     * Attempts to process this message with the specified <code>CacheDistributionAdvisee</code>.
     *
     * @param cda the CacheDistributionAdvisee to apply this profile to
     * @param isRealRegion true if CacheDistributionAdvisee is a real region
     */
    private void handleCacheDistributionAdvisee(CacheDistributionAdvisee cda,
        boolean isRealRegion) {
      if (cda == null) {
        // local region or proxy bucket region not found
        if (logger.isDebugEnabled()) {
          logger.debug("<lclRgn is null> {}", this); // matches old logging
        }
        return;
      }
      String errorMsg = null;
      if (isRealRegion) {
        // only check compatibility if this advisee is a real region
        errorMsg = checkCompatibility(cda, this.profile);
      }
      if (errorMsg != null) {
        this.incompatible = true;
        if (logger.isDebugEnabled()) {
          logger.debug("{} <replyProfile not set because errorMsg={}", this, errorMsg);
        }
        this.replyException = new ReplyException(new IllegalStateException(errorMsg));
      } else {
        if (isRealRegion) { // TODO do we need this if clause??
          // if the new member is persistent, turn on concurrency checks
          // fixes bug 45208
          if (isLocalAccessor(cda) && this.profile.isPersistent) {
            // #45934 need to set the generateVersionTag flag
            if (cda instanceof DistributedRegion) {
              DistributedRegion dr = (DistributedRegion) cda;
              if (!dr.getDataPolicy().withPersistence()) {
                dr.setGeneratedVersionTag(false);
              }
            }

            assert cda instanceof LocalRegion;
            LocalRegion lr = (LocalRegion) cda;
            lr.enableConcurrencyChecks();
          }
        }

        // #45934 don't add profile until the attributes are set correctly,
        // in particular enableConcurrencyChecks and generateVersionTag
        cda.getDistributionAdvisor().putProfile(this.profile);

        if (isRealRegion) {
          // only exchange profile if this advisee is a real region
          this.replyProfile = (CacheProfile) cda.getProfile();
          if (cda instanceof PartitionedRegion) {
            // partitioned region needs to also answer back all real bucket profiles
            PartitionedRegion pr = (PartitionedRegion) cda;
            this.replyBucketProfiles = pr.getRegionAdvisor().getBucketRegionProfiles();
          } else if (((LocalRegion) cda).isUsedForPartitionedRegionBucket()) {
            this.eventState = ((LocalRegion) cda).getEventState();
          }
        }
      }
    }

    protected String checkCompatibility(CacheDistributionAdvisee rgn, CacheProfile profile) {
      Scope otherScope = rgn.getAttributes().getScope();
      String result = null;

      // Verify both VMs are gateway-enabled or neither are. Note that since
      // this string is sent back to the caller, the 'other' and the 'my'
      // below are from the caller's point of view.
      final DistributedMember myId = rgn.getDistributionManager().getId();
      boolean otherCCEnabled = rgn.getAttributes().getConcurrencyChecksEnabled();
      boolean skipCheckForAccessor = skipCheckForAccessor(rgn, profile);
      boolean skipConcurrencyChecks = skipChecksForInternalRegion(rgn);
      boolean initializing = skipDuringInitialization(rgn);

      if (initializing) {
        this.skippedCompatibilityChecks = true;
      }

      if (!initializing && !skipCheckForAccessor && (rgn.getAttributes().getDataPolicy()
          .withPersistence() != profile.dataPolicy.withPersistence())) {
        // 45186: Do not allow a persistent replicate to be started if a
        // non-persistent replicate is running
        if (!rgn.getAttributes().getDataPolicy().withPersistence()) {
          result =
              String.format(
                  "Cannot create region %s DataPolicy withPersistence=true because another cache (%s) has the same region DataPolicy withPersistence=false. Persistent members must be started before non-persistent members",
                  regionPath, myId);
          skipConcurrencyChecks = true;
        } else {
          // make the new member turn on concurrency checks
          skipConcurrencyChecks = true;
        }
      }

      if (!initializing && !skipCheckForAccessor && !skipConcurrencyChecks
          && this.concurrencyChecksEnabled != otherCCEnabled) {
        result =
            String.format(
                "Cannot create region %s concurrency-checks-enabled=%s because another cache (%s) has the same region concurrency-checks-enabled=%s",
                regionPath, this.concurrencyChecksEnabled, myId, otherCCEnabled);
      }

      Set<String> otherGatewaySenderIds = ((LocalRegion) rgn).getGatewaySenderIds();
      Set<String> myGatewaySenderIds = profile.gatewaySenderIds;
      if (!otherGatewaySenderIds.equals(myGatewaySenderIds)) {
        if (!rgn.getFullPath().contains(DynamicRegionFactory.dynamicRegionListName)) {
          result =
              String.format(
                  "Cannot create Region %s with %s gateway sender ids because another cache has the same region defined with %s gateway sender ids",
                  this.regionPath, myGatewaySenderIds, otherGatewaySenderIds);
        }
      }

      Set<String> otherAsynEventQueueIds = ((LocalRegion) rgn).getVisibleAsyncEventQueueIds();
      Set<String> myAsyncEventQueueIds = profile.asyncEventQueueIds;
      if (!isLocalOrRemoteAccessor(rgn, profile)
          && !otherAsynEventQueueIds.equals(myAsyncEventQueueIds)) {
        result =
            String.format(
                "Cannot create Region %s with %s async event ids because another cache has the same region defined with %s async event ids",
                this.regionPath, myAsyncEventQueueIds, otherAsynEventQueueIds);
      }

      final PartitionAttributes pa = rgn.getAttributes().getPartitionAttributes();
      if (pa == null && profile.isPartitioned) {
        result =
            String.format(
                "Cannot create PartitionedRegion %s because another cache (%s) has the same region defined as a non PartitionedRegion.",
                this.regionPath, myId);
      } else if (pa != null && !profile.isPartitioned) {
        result =
            String.format(
                "Cannot create the non PartitionedRegion %s because another cache (%s) has a Partitioned Region defined with the same name.",
                this.regionPath, myId);
      } else if (profile.scope.isDistributed() && otherScope.isDistributed()) {
        // This check is somewhat unnecessary as all Partitioned Regions should have the same scope
        // due to the fact that Partitioned Regions do no support scope.
        if (profile.scope != otherScope) {
          result =
              String.format(
                  "Cannot create region %s with %s scope because another cache (%s) has same region with %s scope.",
                  this.regionPath, profile.scope, myId, otherScope);
        }
      }

      final boolean otherIsOffHeap = rgn.getAttributes().getOffHeap();

      boolean thisIsRemoteAccessor = false;
      if (!rgn.getAttributes().getDataPolicy().withStorage()
          || (pa != null && pa.getLocalMaxMemory() == 0)) {
        thisIsRemoteAccessor = true;
      }

      if (!isRemoteAccessor(profile) && !thisIsRemoteAccessor
          && profile.isOffHeap != otherIsOffHeap) {
        result =
            String.format(
                "Cannot create region %s with off-heap=%s because another cache (%s) has the same region with off-heap=%s.",
                this.regionPath, profile.isOffHeap, myId, otherIsOffHeap);
      }

      String cspResult = null;
      Map<String, CacheServiceProfile> myProfiles = ((LocalRegion) rgn).getCacheServiceProfiles();
      // Iterate and compare the remote CacheServiceProfiles to the local ones
      for (CacheServiceProfile remoteProfile : profile.cacheServiceProfiles) {
        CacheServiceProfile localProfile = myProfiles.get(remoteProfile.getId());
        if (localProfile == null) {
          cspResult = getMissingProfileMessage(remoteProfile, true);
        } else {
          cspResult = remoteProfile.checkCompatibility(rgn.getFullPath(), localProfile);
        }
        if (cspResult != null) {
          break;
        }
      }

      // If the comparison result is null, compare the local profiles to the remote ones. If there
      // are more local profiles than remote ones (meaning there are ones defined locally that are
      // not defined remotely), then compare those. This should produce an informative error message
      // (as opposed to returning something like 'the profiles don't match').
      if (cspResult == null) {
        if (myProfiles.size() > profile.cacheServiceProfiles.size()) {
          for (CacheServiceProfile localProfile : myProfiles.values()) {
            if (!profile.cacheServiceProfiles.stream()
                .anyMatch(remoteProfile -> remoteProfile.getId().equals(localProfile.getId()))) {
              cspResult = getMissingProfileMessage(localProfile, false);
              break;
            }
          }
        }
      }

      // If the comparison result is not null, set the final result.
      // Note: Be careful not to overwrite the final result with null in case it has already been
      // set in a previous compatibility check.
      if (cspResult != null) {
        result = cspResult;
      }

      if (logger.isDebugEnabled()) {
        logger.debug("CreateRegionProcessor.checkCompatibility: this={}; other={}; result={}", rgn,
            profile, result);
      }

      return result;
    }

    protected String getMissingProfileMessage(CacheServiceProfile profile,
        boolean existsInThisMember) {
      return profile.getMissingProfileMessage(existsInThisMember);
    }

    /**
     * When many members are started concurrently, it is possible that an accessor or non-version
     * generating replicate receives CreateRegionMessage before it is initialized, thus preventing
     * persistent members from starting. We skip compatibilityChecks if the region is not
     * initialized, and let other members check compatibility. If all members skipCompatabilit
     * checks, then the CreateRegionMessage should be retried. fixes #45186
     */
    private boolean skipDuringInitialization(CacheDistributionAdvisee rgn) {
      boolean skip = false;
      if (rgn instanceof LocalRegion) {
        LocalRegion lr = (LocalRegion) rgn;
        if (!lr.isInitialized()) {
          Set recipients = new CreateRegionProcessor(rgn).getRecipients();
          recipients.remove(getSender());
          if (!recipients.isEmpty()) {
            skip = true;
          }
        }
      }
      return skip;
    }

    /**
     * For internal regions skip concurrency-checks-enabled checks, since we will set it to true
     * after profile exchange if required.
     */
    private boolean skipChecksForInternalRegion(CacheDistributionAdvisee rgn) {
      boolean skip = false;
      if (rgn instanceof LocalRegion) {
        LocalRegion lr = (LocalRegion) rgn;
        skip = lr.isInternalRegion();
      }
      return skip;
    }

    /**
     * check for isLocalOrRemoteAccessor(CacheDistributionAdvisee, CacheProfile) and check if
     * DistributedRegion does not generate entry versions.
     */
    private boolean skipCheckForAccessor(CacheDistributionAdvisee rgn, CacheProfile profile) {
      boolean skip = false;
      if (rgn instanceof DistributedRegion) {
        DistributedRegion dr = (DistributedRegion) rgn;
        skip = !dr.getGenerateVersionTag();
      }
      return skip || isLocalOrRemoteAccessor(rgn, profile);
    }

    /**
     * @return true if profile being exchanged or region is an accessor i.e has no storage
     */
    protected static boolean isLocalOrRemoteAccessor(CacheDistributionAdvisee region,
        CacheProfile profile) {
      return isLocalAccessor(region) || isRemoteAccessor(profile);
    }

    protected static boolean isLocalAccessor(CacheDistributionAdvisee region) {
      if (!region.getAttributes().getDataPolicy().withStorage()) {
        return true;
      }
      if (region.getAttributes().getPartitionAttributes() != null
          && region.getAttributes().getPartitionAttributes().getLocalMaxMemory() == 0) {
        return true;
      }
      return false;
    }

    protected static boolean isRemoteAccessor(CacheProfile profile) {
      if (!profile.dataPolicy.withStorage()) {
        return true;
      }
      if (profile.isPartitioned) {
        PartitionProfile prProfile = (PartitionProfile) profile;
        if (prProfile.localMaxMemory == 0) {
          return true;
        }
      }
      return false;
    }

    @Override
    public void reset() {
      super.reset();
      this.regionPath = null;
      this.profile = null;
      this.processorId = -1;
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      super.fromData(in);
      this.regionPath = DataSerializer.readString(in);
      this.profile = (CacheProfile) DataSerializer.readObject(in);
      this.processorId = in.readInt();
      this.concurrencyChecksEnabled = in.readBoolean();
    }

    public int getDSFID() {
      return CREATE_REGION_MESSAGE;
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      super.toData(out);
      DataSerializer.writeString(this.regionPath, out);
      DataSerializer.writeObject(this.profile, out);
      out.writeInt(this.processorId);
      out.writeBoolean(this.concurrencyChecksEnabled);
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("CreateRegionMessage (region='");
      sb.append(this.regionPath);
      sb.append("'; processorId=");
      sb.append(this.processorId);
      sb.append("; concurrencyChecksEnabled=").append(this.concurrencyChecksEnabled);
      sb.append("; profile=");
      sb.append(this.profile);
      sb.append(")");
      return sb.toString();
    }
  }

  public static class CreateRegionReplyMessage extends ReplyMessage {
    protected CacheProfile profile;
    protected ArrayList bucketProfiles;
    protected Object eventState;
    /**
     * Added to fix 42051. If the region is in the middle of being destroyed, return the destroyed
     * profile
     */
    protected PersistentMemberID destroyedId;
    protected boolean skippedCompatibilityChecks;

    long seqKeyForWan = -1;

    @Override
    public int getDSFID() {
      return CREATE_REGION_REPLY_MESSAGE;
    }

    @Override
    public boolean sendViaUDP() {
      return true;
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      super.fromData(in);
      if (in.readBoolean()) {
        this.profile = (CacheProfile) DataSerializer.readObject(in);
      }
      int size = in.readInt();
      if (size == 0) {
        this.bucketProfiles = null;
      } else {
        this.bucketProfiles = new ArrayList(size);
        for (int i = 0; i < size; i++) {
          RegionAdvisor.BucketProfileAndId bp = new RegionAdvisor.BucketProfileAndId();
          InternalDataSerializer.invokeFromData(bp, in);
          this.bucketProfiles.add(bp);
        }
      }
      if (in.readBoolean()) {
        this.eventState = EventStateHelper.deDataSerialize(in, false);
      }
      if (in.readBoolean()) {
        this.destroyedId = new PersistentMemberID();
        InternalDataSerializer.invokeFromData(this.destroyedId, in);
      }
      this.skippedCompatibilityChecks = in.readBoolean();
      this.seqKeyForWan = in.readLong();
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      super.toData(out);
      out.writeBoolean(this.profile != null);
      if (this.profile != null) {
        DataSerializer.writeObject(this.profile, out);
      }
      if (this.bucketProfiles == null) {
        out.writeInt(0);
      } else {
        int size = this.bucketProfiles.size();
        out.writeInt(size);
        for (int i = 0; i < size; i++) {
          RegionAdvisor.BucketProfileAndId bp =
              (RegionAdvisor.BucketProfileAndId) this.bucketProfiles.get(i);
          InternalDataSerializer.invokeToData(bp, out);
        }
      }
      if (this.eventState != null) {
        out.writeBoolean(true);
        // The isHARegion flag is false here because
        // we currently only include the event state in the profile
        // for bucket regions.
        EventStateHelper.dataSerialize(out, (Map) eventState, false, getSender());
      } else {
        out.writeBoolean(false);
      }
      if (this.destroyedId != null) {
        out.writeBoolean(true);
        InternalDataSerializer.invokeToData(destroyedId, out);
      } else {
        out.writeBoolean(false);
      }
      out.writeBoolean(this.skippedCompatibilityChecks);
      out.writeLong(seqKeyForWan);
    }

    @Override
    public String toString() {
      StringBuilder buff = new StringBuilder();
      buff.append("CreateRegionReplyMessage");
      buff.append("(sender=").append(getSender());
      buff.append("; processorId=");
      buff.append(super.processorId);
      buff.append("; profile=");
      buff.append(this.profile);
      if (this.bucketProfiles != null) {
        buff.append("; bucketProfiles=");
        buff.append(this.bucketProfiles);
      }
      if (this.eventState != null) {
        buff.append("; eventState=<not null>");
      }
      buff.append("; skippedCompatibilityChecks=");
      buff.append(this.skippedCompatibilityChecks);
      buff.append("; seqKeyForWan=");
      buff.append(this.seqKeyForWan);
      if (this.getException() != null) {
        buff.append("; with exception {").append(getException().getMessage()).append("}");
      }
      buff.append(")");
      return buff.toString();
    }
  }
}

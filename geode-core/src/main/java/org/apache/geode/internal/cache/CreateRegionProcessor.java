/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.gemstone.gemfire.internal.cache;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.DistributionAdvisee;
import com.gemstone.gemfire.distributed.internal.DistributionAdvisor;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.DistributionMessage;
import com.gemstone.gemfire.distributed.internal.HighPriorityDistributionMessage;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.MessageWithReply;
import com.gemstone.gemfire.distributed.internal.ReplyException;
import com.gemstone.gemfire.distributed.internal.ReplyMessage;
import com.gemstone.gemfire.distributed.internal.ReplyProcessor21;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.InternalDataSerializer;
import com.gemstone.gemfire.internal.cache.CacheDistributionAdvisor.CacheProfile;
import com.gemstone.gemfire.internal.cache.CacheDistributionAdvisor.InitialImageAdvice;
import com.gemstone.gemfire.internal.cache.partitioned.PRLocallyDestroyedException;
import com.gemstone.gemfire.internal.cache.partitioned.RegionAdvisor;
import com.gemstone.gemfire.internal.cache.partitioned.RegionAdvisor.PartitionProfile;
import com.gemstone.gemfire.internal.cache.persistence.PersistentMemberID;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;

/**
 * This message processor handles creation and initial exchange of
 * com.gemstone.gemfire.internal.cache.CacheDistributionAdvisor.Profiles. It 
 * represents creation of
 * a {@link CacheDistributionAdvisee}. Name remains CreateRegion to avoid
 * merge conflicts.
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
    for (int retry=0; retry<5; retry++) {
    Set recps = getRecipients();
    
    if (logger.isDebugEnabled()) {
      logger.debug("Creating region {}", this.newRegion);
    }

    if (recps.isEmpty()) {
      if (logger.isDebugEnabled()) {
        logger.debug("CreateRegionProcessor.initializeRegion, no recipients, msg not sent");
      }
      this.newRegion.getDistributionAdvisor().setInitialized();
      EventTracker tracker = ((LocalRegion)this.newRegion).getEventTracker();
      if (tracker != null) {
        tracker.setInitialized();
      }
      return;
    }

    CreateRegionReplyProcessor replyProc = new CreateRegionReplyProcessor(recps);
    

    boolean useMcast = false; // multicast is disabled for this message for now
    CreateRegionMessage msg = getCreateRegionMessage(recps, replyProc, useMcast);

    // since PR buckets can be created during cache entry operations, enable
    // severe alert processing if we're creating one of them
    if (((LocalRegion)newRegion).isUsedForPartitionedRegionBucket()) {
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
      
      // This isn't right.  We should disable region creation in general, not just
      // the remote case here...
//      // Similarly, don't allow new regions to be created if the cache is closing
//      GemFireCache cache = (GemFireCache)this.newRegion.getCache();
//      if (cache.isClosing()) {
//        throw new CacheClosedException("Cannot create a region when the cache is closing");
//      }
      try {
        replyProc.waitForRepliesUninterruptibly();
        if (!replyProc.needRetry()) {
          break;
        }
      }
      catch (ReplyException e) {
        Throwable t = e.getCause();
        if (t instanceof IllegalStateException) {
          // region is incompatible with region in another cache
          throw (IllegalStateException)t;
        }
        e.handleAsUnexpected();
        break;
      }
    } finally {
      replyProc.cleanup();
      EventTracker tracker = ((LocalRegion)this.newRegion).getEventTracker();
      if (tracker != null) {
        tracker.setInitialized();
      }
      if (((LocalRegion)this.newRegion).isUsedForPartitionedRegionBucket()) {
        if (logger.isDebugEnabled()) {
          logger.debug("initialized bucket event tracker: {}", tracker);
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
    }
    else {
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
      return ((BucketRegion)this.newRegion).getBucketAdvisor().adviseProfileExchange();
    } else {
      DistributionAdvisee rgn = this.newRegion.getParentAdvisee();
      DistributionAdvisor advisor = rgn.getDistributionAdvisor();
      return advisor.adviseGeneric();
    }
  }

  protected CreateRegionMessage getCreateRegionMessage(Set recps,
                                 ReplyProcessor21 proc, boolean useMcast) {
    CreateRegionMessage msg = new CreateRegionMessage();
    msg.regionPath = this.newRegion.getFullPath();
    msg.profile = (CacheProfile)this.newRegion.getProfile();
    msg.processorId = proc.getProcessorId();
    msg.concurrencyChecksEnabled = this.newRegion.getAttributes().getConcurrencyChecksEnabled();
    msg.setMulticast(useMcast);
    msg.setRecipients(recps);
    return msg;
  }
  
  public void setOnline(InternalDistributedMember target) {
    
  }
  
  class CreateRegionReplyProcessor extends ReplyProcessor21  {

    CreateRegionReplyProcessor(Set members) {
      super((InternalDistributedSystem)CreateRegionProcessor.this.newRegion.
               getCache().getDistributedSystem(),
             members);
    }
    
    /**
     * guards application of event state to the region so that we deserialize
     * and apply event state only once
     */
    private Object eventStateLock = new Object();
    
    /** whether event state has been recorded in the region */
    private boolean eventStateRecorded = false;

    private boolean allMembersSkippedChecks = true;

    /**
     * true if all members skipped CreateRegionMessage#checkCompatibility(),
     * in which case CreateRegionMessage should be retried.
     */
    public boolean needRetry() {
      return this.allMembersSkippedChecks;
    }

    @Override
    public void process(DistributionMessage msg) {
      Assert.assertTrue(msg instanceof CreateRegionReplyMessage,
                        "CreateRegionProcessor is unable to process message of type " +
                        msg.getClass());
      CreateRegionReplyMessage reply = (CreateRegionReplyMessage)msg;
      LocalRegion lr = (LocalRegion)newRegion;
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

       CacheDistributionAdvisor cda = CreateRegionProcessor.this.newRegion.getCacheDistributionAdvisor();
       cda.putProfile(reply.profile);
       if (reply.bucketProfiles != null) {
         RegionAdvisor ra = (RegionAdvisor)cda;
         ra.putBucketRegionProfiles(reply.bucketProfiles);
       }
       if (reply.eventState != null && lr.hasEventTracker()) {
         synchronized(eventStateLock) {
           if (!this.eventStateRecorded) {
             this.eventStateRecorded = true;
             Object eventState = null;
             eventState = reply.eventState;
             lr.recordEventState(reply.getSender(), (Map)eventState);
           }
         }
       }
       reply.eventState = null;
       if (lr.isUsedForPartitionedRegionBucket()) {
         ((BucketRegion)lr).updateEventSeqNum(reply.seqKeyForWan);
       }
       // Process any delta filter-profile messages received during profile
       // exchange.
       // The pending messages are queued in the local profile.
       FilterProfile remoteFP = reply.profile.filterProfile;
       if (remoteFP != null) {
         FilterProfile localFP = ((LocalRegion)newRegion).filterProfile;
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
      if(reply.destroyedId != null && newRegion instanceof DistributedRegion) {
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
     * IllegalStateException is an anticipated reply exception.  Receiving
     * multiple replies with this exception is normal.
     */
    @Override
    protected boolean logMultipleExceptions() {
      return false;
    }
  }

  public static final class CreateRegionMessage extends HighPriorityDistributionMessage
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
    protected void process(DistributionManager dm) {
      int oldLevel =         // Set thread local flag to allow entrance through initialization Latch
        LocalRegion.setThreadInitLevelRequirement(LocalRegion.ANY_INIT);
      LocalRegion lclRgn = null;

      PersistentMemberID destroyedId = null;
      try {
        // get the region from the path, but do NOT wait on initialization,
        // otherwise we could have a distributed deadlock

        GemFireCacheImpl cache = (GemFireCacheImpl) CacheFactory.getInstance(dm.getSystem());
        
        //Fix for bug 42051 - Discover any regions that are in the process
        //of being destroyed
        DistributedRegion destroyingRegion = cache.getRegionInDestroy(this.regionPath);
        if(destroyingRegion != null) {
          destroyedId = destroyingRegion.getPersistentID();
        }

        lclRgn = (LocalRegion)cache.getRegion(this.regionPath);
        
        if (lclRgn instanceof CacheDistributionAdvisee) {
          // bug 37604 - don't return a profile if this is a bucket and the owner
          // has been locally destroyed
          if (lclRgn.isUsedForPartitionedRegionBucket()) {
            if (!((BucketRegion)lclRgn).isPartitionedRegionOpen()) {
              if (logger.isDebugEnabled()) {
                logger.debug("<Partitioned Region Closed or Locally Destroyed> {}", this);
              }
              return;
            }
          }
          handleCacheDistributionAdvisee((CacheDistributionAdvisee)lclRgn,true);
        }
        else {
          if (lclRgn == null) {
            // check to see if a ProxyBucketRegion (not a true region) exists
            handleCacheDistributionAdvisee(
                PartitionedRegionHelper.getProxyBucketRegion(cache, this.regionPath, false),
                false);
          }
          else {
            if (logger.isDebugEnabled()) {
              logger.debug("<lclRgn scope is not distributed. Scope={}> {}", lclRgn.getAttributes().getScope(), this);
            }
          }
        }
      }
      catch (PRLocallyDestroyedException fre) {
        if (logger.isDebugEnabled()) {
          logger.debug("<Region Locally Destroyed> {}", this);
        }
      }
      catch (RegionDestroyedException e) {
        if (logger.isDebugEnabled()) {
          logger.debug("<RegionDestroyed> {}", this);
        }
      }
      catch (CancelException e) {
        if (logger.isDebugEnabled()) {
          logger.debug("<CancelException> {}", this);
        }
      }
      catch (VirtualMachineError err) {
        SystemFailure.initiateFailure(err);
        // If this ever returns, rethrow the error.  We're poisoned
        // now, so don't let this thread continue.
        throw err;
      }
      catch (Throwable t) {
        // Whenever you catch Error or Throwable, you must also
        // catch VirtualMachineError (see above).  However, there is
        // _still_ a possibility that you are dealing with a cascading
        // error condition, so you also need to check to see if the JVM
        // is still usable:
        SystemFailure.checkFailure();
        if (replyException == null) {
          replyException = new ReplyException(t);
        }
        else {
          logger.warn(LocalizedMessage.create(LocalizedStrings.CreateRegionProcessor_MORE_THAN_ONE_EXCEPTION_THROWN_IN__0, this), t);
        }
      }
      finally {
        LocalRegion.setThreadInitLevelRequirement(oldLevel);
        CreateRegionReplyMessage replyMsg = new CreateRegionReplyMessage();
        replyMsg.profile = replyProfile;
        replyMsg.bucketProfiles = replyBucketProfiles;
        replyMsg.eventState = this.eventState;
        replyMsg.destroyedId = destroyedId;
        replyMsg.setProcessorId(this.processorId);
        replyMsg.setRecipient(this.getSender());
        replyMsg.skippedCompatibilityChecks = this.skippedCompatibilityChecks;
        
        if (lclRgn != null && lclRgn.isUsedForPartitionedRegionBucket()) {
          replyMsg.seqKeyForWan = ((BucketRegion)lclRgn).getEventSeqNum().get();
        }
        if (replyException != null && !this.incompatible) {
          // no need to log the exception if it was caused by compatibility check
          if (logger.isDebugEnabled()) {
            logger.debug("While processing '{}', got exception, returning to sender", this, replyException);
          }
        }
        replyMsg.setException(replyException);
        dm.putOutgoing(replyMsg);
        if(lclRgn instanceof PartitionedRegion)
          ((PartitionedRegion)lclRgn).sendIndexCreationMsg(this.getSender());
     
        
      }
    }
    
    /**
     * Attempts to process this message with the specified 
     * <code>CacheDistributionAdvisee</code>.
     * @param cda the CacheDistributionAdvisee to apply this profile to
     * @param isRealRegion true if CacheDistributionAdvisee is a real region
     */
    private void handleCacheDistributionAdvisee(
        CacheDistributionAdvisee cda, boolean isRealRegion) {
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
      }
      else {
        if (isRealRegion) {  // TODO do we need this if clause??
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
          this.replyProfile = (CacheProfile)cda.getProfile();
          if (cda instanceof PartitionedRegion) {
            // partitioned region needs to also answer back all real bucket profiles
            PartitionedRegion pr = (PartitionedRegion)cda;
            this.replyBucketProfiles = pr.getRegionAdvisor().getBucketRegionProfiles();
          } else if (((LocalRegion)cda).isUsedForPartitionedRegionBucket()) {
            this.eventState = ((LocalRegion)cda).getEventState();
          }
        }
      }
    }

    protected String checkCompatibility(CacheDistributionAdvisee rgn,
                                        CacheProfile profile) {
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
      
      if (!initializing && !skipCheckForAccessor && (rgn.getAttributes().getDataPolicy().withPersistence() 
          != profile.dataPolicy.withPersistence())) {
        // 45186: Do not allow a persistent replicate to be started if a
        // non-persistent replicate is running
        if (!rgn.getAttributes().getDataPolicy().withPersistence()) {
          result = LocalizedStrings.CreateRegionProcessor_CANNOT_CREATE_REGION_0_WITH_PERSISTANCE_TRUE_PERSISTENT_MEMBERS_B4_NON_PERSISTENT
              .toLocalizedString(regionPath, myId);
          skipConcurrencyChecks = true;
        } else {
          // make the new member turn on concurrency checks
          skipConcurrencyChecks = true;
        }
      }
      
      if (!initializing && !skipCheckForAccessor && !skipConcurrencyChecks && this.concurrencyChecksEnabled != otherCCEnabled) {
        result = LocalizedStrings.CreateRegionProcessor_CANNOT_CREATE_REGION_0_CCENABLED_1_BECAUSE_ANOTHER_CACHE_HAS_THE_SAME_REGION_CCENABLED_2
          .toLocalizedString( new Object[] { regionPath,
              Boolean.valueOf(this.concurrencyChecksEnabled), myId, Boolean.valueOf(otherCCEnabled) } );
      }
      
      Set<String> otherGatewaySenderIds = ((LocalRegion)rgn).getGatewaySenderIds();
      Set<String> myGatewaySenderIds = profile.gatewaySenderIds;
      if (!otherGatewaySenderIds
          .equals(myGatewaySenderIds)) {
        if (!rgn.getFullPath().contains( 
            DynamicRegionFactoryImpl.dynamicRegionListName)) { 
          result = LocalizedStrings.CreateRegionProcessor_CANNOT_CREATE_REGION_0_WITH_1_GATEWAY_SENDER_IDS_BECAUSE_ANOTHER_CACHE_HAS_THE_SAME_REGION_WITH_2_GATEWAY_SENDER_IDS
            .toLocalizedString(this.regionPath, myGatewaySenderIds, otherGatewaySenderIds);
        }
      }
      
      Set<String> otherAsynEventQueueIds = ((LocalRegion)rgn).getAsyncEventQueueIds();
      Set<String> myAsyncEventQueueIds = profile.asyncEventQueueIds;
      if (!isLocalOrRemoteAccessor(rgn, profile) && !otherAsynEventQueueIds
          .equals(myAsyncEventQueueIds)) {
        result = LocalizedStrings.CreateRegionProcessor_CANNOT_CREATE_REGION_0_WITH_1_ASYNC_EVENT_IDS_BECAUSE_ANOTHER_CACHE_HAS_THE_SAME_REGION_WITH_2_ASYNC_EVENT_IDS
            .toLocalizedString(this.regionPath, myAsyncEventQueueIds, otherAsynEventQueueIds);
      }
      
      final PartitionAttributes pa = rgn.getAttributes()
          .getPartitionAttributes();      
      if (pa == null &&  profile.isPartitioned) {
        result = LocalizedStrings.CreateRegionProcessor_CANNOT_CREATE_PARTITIONEDREGION_0_BECAUSE_ANOTHER_CACHE_HAS_THE_SAME_REGION_DEFINED_AS_A_NON_PARTITIONEDREGION.toLocalizedString( this.regionPath, myId );
      } else if (pa != null &&  ! profile.isPartitioned) {
        result = LocalizedStrings.CreateRegionProcessor_CANNOT_CREATE_THE_NON_PARTITIONEDREGION_0_BECAUSE_ANOTHER_CACHE_HAS_A_PARTITIONED_REGION_DEFINED_WITH_THE_SAME_NAME.toLocalizedString(this.regionPath, myId);
      } else if (profile.scope.isDistributed() && otherScope.isDistributed()) {
        // This check is somewhat unnecessary as all Partitioned Regions should have the same scope
        // due to the fact that Partitioned Regions do no support scope.
        if (profile.scope != otherScope) {
          result = LocalizedStrings.CreateRegionProcessor_CANNOT_CREATE_REGION_0_WITH_1_SCOPE_BECAUSE_ANOTHER_CACHE_HAS_SAME_REGION_WITH_2_SCOPE.toLocalizedString( new Object[] {this.regionPath, profile.scope, myId, otherScope});
        }
      }
        
      final boolean otherIsOffHeap = rgn.getAttributes().getOffHeap();
      
      boolean thisIsRemoteAccessor = false;
      if (!rgn.getAttributes().getDataPolicy().withStorage() || (pa != null && pa.getLocalMaxMemory() == 0)) {
        thisIsRemoteAccessor = true;
      }
          
      if (!isRemoteAccessor(profile) && !thisIsRemoteAccessor && profile.isOffHeap != otherIsOffHeap) {
        result = LocalizedStrings.CreateRegionProcessor_CANNOT_CREATE_REGION_0_WITH_OFF_HEAP_EQUALS_1_BECAUSE_ANOTHER_CACHE_2_HAS_SAME_THE_REGION_WITH_OFF_HEAP_EQUALS_3
            .toLocalizedString(new Object[] { this.regionPath, profile.isOffHeap, myId, otherIsOffHeap });
      }

      String cspResult = null;
      // TODO Compares set sizes and equivalent entries.
      if (profile.cacheServiceProfiles != null) {
        for (CacheServiceProfile remoteProfile : profile.cacheServiceProfiles) {
          CacheServiceProfile localProfile = ((LocalRegion) rgn).getCacheServiceProfile(remoteProfile.getId());
          cspResult = remoteProfile.checkCompatibility(rgn.getFullPath(), localProfile);
          if (cspResult != null) {
            break;
          }
        }
        // Don't overwrite result with null in case it has already been set in a previous compatibility check.
        if (cspResult != null) {
          result = cspResult;
        }
      }
      if (logger.isDebugEnabled()) {
        logger.debug("CreateRegionProcessor.checkCompatibility: this={}; other={}; result={}", rgn, profile, result);
      }

//       if (profile.membershipAttributes != null) {
//         // check to see if:
//         // 1. we do not have DataPolicy that will take queued msgs
//         // 2. the profile has queuing turned on
//         // 3. we are playing one of the queued roles
//         if (!rgn.getAttributes().getDataPolicy().withQueuedMessages()) {
//           if (profile.membershipAttributes.getLossAction().isAllAccessWithQueuing()) {
//             Set myRoles = rgn.getSystem().getDistributedMember().getRoles();
//             if (!myRoles.isEmpty()) {
//               Set intersection = new HashSet(myRoles);
//               intersection.retainAll(profile.membershipAttributes.getRequiredRoles());
//               if (!intersection.isEmpty()) {
//                 result = "Cannot create region " + regionPath
//                   + " with queuing because the region already exists"
//                   + " with a data-policy " + rgn.getAttributes().getDataPolicy()
//                   + " that does not allow queued messages with the roles "
//                   + intersection;
//               }
//             }
//           }
//         }
//       } else {
//         // see if we are queuing on this region
//         MembershipAttributes ra = rgn.getMembershipAttributes();
//         if (ra != null && ra.hasRequiredRoles()
//             && ra.getLossAction().isAllAccessWithQueuing()) {
//           // we are queuing so make sure this other guy allows queued messages
//           // if he is playing a role we queue for.
//           if (!profile.dataPolicy.withQueuedMessages()) {
//             Set intersection = new HashSet(ra.getRequiredRoles());
//             intersection.retainAll(profile.getDistributedMember().getRoles());
//             if (!intersection.isEmpty()) {
//               result = "Cannot create region " + regionPath
//                 + " with a data-policy " + profile.dataPolicy
//                 + " that does not allow queued messages because the region"
//                 + " already exists with queuing enabled for roles " + intersection;
//             }
//           }
//         }
//       }

      return result;
    }

    /**
     * When many members are started concurrently, it is possible that an
     * accessor or non-version generating replicate receives CreateRegionMessage
     * before it is initialized, thus preventing persistent members from
     * starting. We skip compatibilityChecks if the region is not initialized,
     * and let other members check compatibility. If all members
     * skipCompatabilit checks, then the CreateRegionMessage should be retried.
     * fixes #45186
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
     * For internal regions skip concurrency-checks-enabled checks, since we will
     * set it to true after profile exchange if required.
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
     * check for isLocalOrRemoteAccessor(CacheDistributionAdvisee, CacheProfile)
     * and check if DistributedRegion does not generate entry versions.
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
     * @return true if profile being exchanged or region is an accessor
     * i.e has no storage
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
          && region.getAttributes().getPartitionAttributes()
              .getLocalMaxMemory() == 0) {
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
    public void fromData(DataInput in)
    throws IOException, ClassNotFoundException {
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
      StringBuffer buff = new StringBuffer();
      buff.append("CreateRegionMessage (region='");
      buff.append(this.regionPath);
      buff.append("'; processorId=");
      buff.append(this.processorId);
      buff.append("; concurrencyChecksEnabled=").append(this.concurrencyChecksEnabled);
      buff.append("; profile=");
      buff.append(this.profile);
      buff.append(")");
      return buff.toString();
    }
   }

  public static final class CreateRegionReplyMessage extends ReplyMessage {
    protected CacheProfile profile;
    protected ArrayList bucketProfiles;
    protected Object eventState;
    /** Added to fix 42051. If the region is in the middle of being destroyed, return the destroyed profile */
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
    public void fromData(DataInput in)
    throws IOException, ClassNotFoundException {
      super.fromData(in);
      if (in.readBoolean()) {
        // this.profile = new CacheProfile();
        // this.profile.fromData(in);
        this.profile = (CacheProfile) DataSerializer.readObject(in);
      }
      int size = in.readInt();
      if (size == 0) {
        this.bucketProfiles = null;
      }
      else {
        this.bucketProfiles = new ArrayList(size);
        for (int i=0; i < size; i++) {
          RegionAdvisor.BucketProfileAndId bp =
            new RegionAdvisor.BucketProfileAndId();
          InternalDataSerializer.invokeFromData(bp, in);
          this.bucketProfiles.add(bp);
        }
      }
      if (in.readBoolean()) {
        this.eventState = EventStateHelper.fromData(in, false);
      }
      if(in.readBoolean()) {
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
        // this.profile.toData(out);
        DataSerializer.writeObject(this.profile, out);
      }
      if (this.bucketProfiles == null) {
        out.writeInt(0);
      }
      else {
        int size = this.bucketProfiles.size();
        out.writeInt(size);
        for (int i = 0; i < size; i++) {
          RegionAdvisor.BucketProfileAndId bp = (RegionAdvisor.BucketProfileAndId)
            this.bucketProfiles.get(i);
          InternalDataSerializer.invokeToData(bp, out);
        }
      }
      if (this.eventState != null) {
        out.writeBoolean(true);
        //The isHARegion flag is false here because
        //we currently only include the event state in the profile 
        //for bucket regions.
        EventStateHelper.toData(out, (Map) eventState, false);
      } else {
        out.writeBoolean(false);
      }
      if(this.destroyedId != null) {
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
      StringBuffer buff = new StringBuffer();
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
        buff.append("; with exception {")
          .append(getException().getMessage()).append("}");
      }
      buff.append(")");
      return buff.toString();
    }
  }
}

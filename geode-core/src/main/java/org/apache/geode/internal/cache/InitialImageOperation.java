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

import static org.apache.geode.internal.cache.LocalRegion.InitializationLevel.AFTER_INITIAL_IMAGE;
import static org.apache.geode.internal.cache.LocalRegion.InitializationLevel.ANY_INIT;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.InternalGemFireError;
import org.apache.geode.InternalGemFireException;
import org.apache.geode.SystemFailure;
import org.apache.geode.annotations.Immutable;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.annotations.internal.MutableForTesting;
import org.apache.geode.cache.DiskAccessException;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.query.internal.CqStateImpl;
import org.apache.geode.cache.query.internal.DefaultQueryService;
import org.apache.geode.cache.query.internal.cq.CqService;
import org.apache.geode.cache.query.internal.cq.ServerCQ;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.HighPriorityDistributionMessage;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.MessageWithReply;
import org.apache.geode.distributed.internal.OperationExecutors;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.ReplyMessage;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.NullDataOutputStream;
import org.apache.geode.internal.cache.InitialImageFlowControl.FlowControlPermitMessage;
import org.apache.geode.internal.cache.LocalRegion.InitializationLevel;
import org.apache.geode.internal.cache.entries.DiskEntry;
import org.apache.geode.internal.cache.ha.HAContainerWrapper;
import org.apache.geode.internal.cache.persistence.DiskStoreID;
import org.apache.geode.internal.cache.persistence.PersistenceAdvisor;
import org.apache.geode.internal.cache.tier.InterestType;
import org.apache.geode.internal.cache.tier.sockets.CacheClientNotifier;
import org.apache.geode.internal.cache.tier.sockets.CacheClientProxy;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.versions.DiskRegionVersionVector;
import org.apache.geode.internal.cache.versions.DiskVersionTag;
import org.apache.geode.internal.cache.versions.RegionVersionHolder;
import org.apache.geode.internal.cache.versions.RegionVersionVector;
import org.apache.geode.internal.cache.versions.VersionSource;
import org.apache.geode.internal.cache.versions.VersionStamp;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.internal.cache.vmotion.VMotionObserverHolder;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.internal.sequencelog.EntryLogger;
import org.apache.geode.internal.sequencelog.RegionLogger;
import org.apache.geode.internal.serialization.ByteArrayDataInput;
import org.apache.geode.internal.serialization.DataSerializableFixedID;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.internal.serialization.StaticSerialization;
import org.apache.geode.internal.serialization.Version;
import org.apache.geode.internal.util.ObjectIntProcedure;
import org.apache.geode.logging.internal.executors.LoggingThread;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.util.internal.GeodeGlossary;

/**
 * Handles requests for an initial image from a cache peer
 *
 */
public class InitialImageOperation {
  private static final Logger logger = LogService.getLogger();

  /**
   * internal flag used by unit tests to test early disconnect from distributed system
   */
  @MutableForTesting
  public static volatile boolean abortTest = false;

  /**
   * maximum number of bytes to put in a single message
   */
  @MutableForTesting
  public static int CHUNK_SIZE_IN_BYTES =
      Integer.getInteger("GetInitialImage.chunkSize", 500 * 1024).intValue();

  /**
   * Allowed number of in flight GII chunks
   */
  @MutableForTesting
  public static int CHUNK_PERMITS =
      Integer.getInteger(GeodeGlossary.GEMFIRE_PREFIX + "GetInitialImage.CHUNK_PERMITS", 16)
          .intValue();

  /**
   * maximum number of unfinished operations to be supported by delta GII
   */
  @MutableForTesting
  public static int MAXIMUM_UNFINISHED_OPERATIONS = Integer.getInteger(
      GeodeGlossary.GEMFIRE_PREFIX + "GetInitialImage.MAXIMUM_UNFINISHED_OPERATIONS", 10000)
      .intValue();

  /**
   * Allowed number GIIs in parallel
   */
  @MutableForTesting
  public static final int MAX_PARALLEL_GIIS =
      Integer.getInteger(GeodeGlossary.GEMFIRE_PREFIX + "GetInitialImage.MAX_PARALLEL_GIIS", 5)
          .intValue();

  /**
   * the region we are fetching
   */
  protected final DistributedRegion region;

  /**
   * the underlying Map to receive our values
   */
  private final RegionMap entries;

  /**
   * true if we have received the last piece of the image
   */
  protected volatile boolean gotImage = false;

  /**
   * received region version holder for lost member, used by synchronizeWith only
   */
  protected RegionVersionHolder rcvd_holderToSync;

  /**
   * received GC versions from the GCC source
   */
  protected Map<VersionSource, Long> gcVersions;

  /**
   * true if this is delta gii
   */
  @MutableForTesting("Seems to be unused?")
  protected volatile boolean isDeltaGII = false;

  /**
   * for testing purposes
   */
  @MutableForTesting
  public static volatile int slowImageProcessing = 0;

  /**
   * for testing purposes
   */
  @MutableForTesting
  public static volatile int slowImageSleeps = 0;

  /**
   * for testing purposes
   */
  @MutableForTesting
  public static boolean VMOTION_DURING_GII = false;

  private boolean isSynchronizing;

  /** Creates a new instance of InitalImageOperation */
  InitialImageOperation(DistributedRegion region, RegionMap entries) {
    this.region = region;
    this.entries = entries;
  }

  /** a flag for inhibiting the use of StateFlushOperation before gii */
  private static final ThreadLocal inhibitStateFlush = new ThreadLocal() {
    @Override
    protected Object initialValue() {
      return Boolean.valueOf(false);
    }
  };


  /** inhibit use of StateFlush for the current thread */
  public static void setInhibitStateFlush(boolean inhibitIt) {
    inhibitStateFlush.set(Boolean.valueOf(inhibitIt));
  }

  public enum GIIStatus {
    NO_GII, GOTIMAGE_BY_FULLGII, GOTIMAGE_BY_DELTAGII;

    public static boolean didGII(GIIStatus giiStatus) {
      return (giiStatus == GIIStatus.GOTIMAGE_BY_FULLGII
          || giiStatus == GIIStatus.GOTIMAGE_BY_DELTAGII);
    }

    public static boolean didDeltaGII(GIIStatus giiStatus) {
      return giiStatus == GIIStatus.GOTIMAGE_BY_DELTAGII;
    }

    public static boolean didFullGII(GIIStatus giiStatus) {
      return giiStatus == GIIStatus.GOTIMAGE_BY_FULLGII;
    }
  }

  private GIIStatus reportGIIStatus() {
    if (!this.gotImage) {
      return GIIStatus.NO_GII;
    } else {
      // got image
      if (this.isDeltaGII) {
        return GIIStatus.GOTIMAGE_BY_DELTAGII;
      } else {
        return GIIStatus.GOTIMAGE_BY_FULLGII;
      }
    }
  }

  /**
   * Fetch an initial image from a single recipient
   *
   * @param recipientSet list of candidates to fetch from
   * @param targetReinitialized true if candidate should wait until initialized before responding
   * @param recoveredRVV recovered rvv
   * @return true if succeeded to get image
   * @throws org.apache.geode.cache.TimeoutException when it is unable to get a reply within the
   *         limit.
   */
  GIIStatus getFromOne(Set recipientSet, boolean targetReinitialized,
      CacheDistributionAdvisor.InitialImageAdvice advice, boolean recoveredFromDisk,
      RegionVersionVector recoveredRVV) throws org.apache.geode.cache.TimeoutException {
    final boolean isDebugEnabled = logger.isDebugEnabled();

    if (VMOTION_DURING_GII) {
      /*
       * TODO (ashetkar): recipientSet may contain more than one member. Ensure only the gii-source
       * member is vMotioned. The test hook may need to be placed at another point.
       */
      VMotionObserverHolder.getInstance().vMotionDuringGII(recipientSet, this.region);
    }
    // Make sure that candidates are regarded in random order
    ArrayList recipients = new ArrayList(recipientSet);

    if (this.region.isUsedForSerialGatewaySenderQueue()) {
      AbstractGatewaySender sender = this.region.getSerialGatewaySender();
      if (sender != null) {
        InternalDistributedMember primary = sender.getSenderAdvisor().advisePrimaryGatewaySender();
        if (primary != null) {
          recipients.remove(primary);
          recipients.add(0, primary);
        }
      }
    } else {
      if (recipients.size() > 1) {
        Collections.shuffle(recipients);
      }
    }
    long giiStart = this.region.getCachePerfStats().startGetInitialImage();
    InternalDistributedMember provider = null;

    for (Iterator itr = recipients.iterator(); !this.gotImage && itr.hasNext();) {
      // if we got a partial image from the previous recipient, then clear it

      InternalDistributedMember recipient = (InternalDistributedMember) itr.next();
      provider = recipient;

      // In case of HARegion, before getting the region snapshot(image) get the filters
      // registered by the associated client and apply them.
      // As part of bug fix 39014, while creating the secondary HARegion/Queue, the
      // filters registered by the client is applied first and then the HARegion
      // initial image is applied. This is to process any events thats arriving while
      // GII is happening and is not part of the GII result.
      if (region instanceof HARegion) {
        try {
          // HARegion r = (HARegion)region;
          // if (!r.isPrimaryQueue()) {
          if (!this.requestFilterInfo(recipient)) {
            if (isDebugEnabled) {
              logger.debug("Failed to receive interest and CQ information from {}", recipient);
            }
          }
          // }
        } catch (Exception ex) {
          if (!itr.hasNext()) {
            if (isDebugEnabled) {
              logger.info("Failed while getting interest and CQ information from {}", recipient,
                  ex);
            }
          }
          continue;
        }
      }

      PersistenceAdvisor persistenceAdvisor = this.region.getPersistenceAdvisor();
      if (persistenceAdvisor != null) {
        try {
          persistenceAdvisor.updateMembershipView(recipient, targetReinitialized);
          persistenceAdvisor.setInitializing(this.region.getPersistentID());
        } catch (ReplyException e) {
          if (isDebugEnabled) {
            logger.debug("Failed to get membership view", e);
          }
          continue;
        }
      }
      final ClusterDistributionManager dm =
          (ClusterDistributionManager) this.region.getDistributionManager();
      boolean allowDeltaGII = true;
      if (FORCE_FULL_GII || recipient.getVersionObject().compareTo(Version.GFE_80) < 0) {
        allowDeltaGII = false;
      }
      Set keysOfUnfinishedOps = null;
      RegionVersionVector received_rvv = null;
      RegionVersionVector remote_rvv = null;
      if (this.region.getConcurrencyChecksEnabled()
          && recipient.getVersionObject().compareTo(Version.GFE_80) >= 0) {
        if (internalBeforeRequestRVV != null
            && internalBeforeRequestRVV.getRegionName().equals(this.region.getName())) {
          internalBeforeRequestRVV.run();
        }
        // Request the RVV from the provider and discover any operations on this
        // member that have not been performed on the provider.
        //
        // It is important that this happens *before* the state flush. An operation
        // maybe unfinished because a member crashed during distribution, or because
        // it is in flight right now. If it is in flight right now, we need to make
        // sure the provider receives the latest value for the operation before the
        // GII really starts.
        received_rvv = getRVVFromProvider(dm, recipient, targetReinitialized);
        if (received_rvv == null) {
          continue;
        }
        // remote_rvv will be filled with the versions of unfinished keys
        // then if recoveredRVV is still newer than the filled remote_rvv, do fullGII
        remote_rvv = received_rvv.getCloneForTransmission();
        keysOfUnfinishedOps = processReceivedRVV(remote_rvv, recoveredRVV);
        if (internalAfterCalculatedUnfinishedOps != null
            && internalAfterCalculatedUnfinishedOps.getRegionName().equals(this.region.getName())) {
          internalAfterCalculatedUnfinishedOps.run();
        }
        if (keysOfUnfinishedOps == null) {
          // if got rvv, keysOfUnfinishedOps at least will be empty
          continue;
        }
      }

      Boolean inhibitFlush = (Boolean) inhibitStateFlush.get();
      if (!inhibitFlush.booleanValue() && !this.region.doesNotDistribute()) {
        if (region instanceof BucketRegionQueue) {
          // get the corresponding userPRs and do state flush on all of them
          // TODO we should be able to do this state flush with a single
          // message, but that will require changing the messaging layer,
          // which has implications for a rolling upgrade.
          Collection<BucketRegion> userPRBuckets =
              ((BucketRegionQueue) (this.region)).getCorrespondingUserPRBuckets();
          if (isDebugEnabled) {
            logger.debug("The parent buckets of this shadowPR region are {}", userPRBuckets);
          }

          for (BucketRegion parentBucket : userPRBuckets) {
            if (isDebugEnabled) {
              logger.debug("Going to do state flush operation on the parent bucket.");
            }
            final StateFlushOperation sf;
            sf = new StateFlushOperation(parentBucket);
            final Set<InternalDistributedMember> r = new HashSet<InternalDistributedMember>();
            r.addAll(advice.replicates);
            r.addAll(advice.preloaded);
            r.addAll(advice.others);
            r.addAll(advice.empties);
            r.addAll(advice.uninitialized);
            int processorType =
                targetReinitialized ? OperationExecutors.WAITING_POOL_EXECUTOR
                    : OperationExecutors.HIGH_PRIORITY_EXECUTOR;
            try {
              boolean success = sf.flush(r, recipient, processorType, true);
              if (!success) {
                continue;
              }
            } catch (InterruptedException ie) {
              Thread.currentThread().interrupt();
              region.getCancelCriterion().checkCancelInProgress(ie);
              return GIIStatus.NO_GII;
            }
            if (isDebugEnabled) {
              logger.debug("Completed state flush operation on the parent bucket.");
            }
          }
        }
        final StateFlushOperation sf;
        sf = new StateFlushOperation(this.region);
        final Set<InternalDistributedMember> r = new HashSet<InternalDistributedMember>();
        r.addAll(advice.replicates);
        r.addAll(advice.preloaded);
        r.addAll(advice.others);
        r.addAll(advice.empties);
        r.addAll(advice.uninitialized);
        int processorType = targetReinitialized ? OperationExecutors.WAITING_POOL_EXECUTOR
            : OperationExecutors.HIGH_PRIORITY_EXECUTOR;
        try {
          boolean success = sf.flush(r, recipient, processorType, false);
          if (!success) {
            continue;
          }
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          region.getCancelCriterion().checkCancelInProgress(ie);
          this.region.getCachePerfStats().endNoGIIDone(giiStart);
          return GIIStatus.NO_GII;
        }
      }

      RequestImageMessage m = new RequestImageMessage();
      m.regionPath = this.region.getFullPath();
      m.keysOnly = false;
      m.targetReinitialized = targetReinitialized;
      m.setRecipient(recipient);

      if (this.region.getConcurrencyChecksEnabled()) {
        if (allowDeltaGII && recoveredFromDisk) {
          if (!this.region.getDiskRegion().getRVVTrusted()) {
            if (isDebugEnabled) {
              logger.debug("Region {} recovered without EndGII flag, do full GII",
                  this.region.getFullPath());
            }
            m.versionVector = null;
          } else if (keysOfUnfinishedOps.size() > MAXIMUM_UNFINISHED_OPERATIONS) {
            if (isDebugEnabled) {
              logger.debug(
                  "Region {} has {} unfinished operations, which exceeded threshold {}, do full GII instead",
                  this.region.getFullPath(), keysOfUnfinishedOps.size(),
                  MAXIMUM_UNFINISHED_OPERATIONS);
            }
            m.versionVector = null;
          } else {
            if (recoveredRVV.isNewerThanOrCanFillExceptionsFor(remote_rvv)) {
              m.versionVector = null;
              if (isDebugEnabled) {
                logger.debug(
                    "Region {}: after filled versions of unfinished keys, recovered rvv is still newer than remote rvv:{}. recovered rvv is {}. Do full GII",
                    this.region.getFullPath(), remote_rvv, recoveredRVV);
              }
            } else {
              m.versionVector = recoveredRVV;
              m.unfinishedKeys = keysOfUnfinishedOps;
              if (isDebugEnabled) {
                logger.debug(
                    "Region {} recovered with EndGII flag, rvv is {}. recovered rvv is {}. Do delta GII",
                    this.region.getFullPath(), m.versionVector, recoveredRVV);
              }
            }
          }
          m.checkTombstoneVersions = true;
        }
        if (received_rvv != null) {
          // pack the original RVV, then save the received one
          if (internalBeforeSavedReceivedRVV != null
              && internalBeforeSavedReceivedRVV.getRegionName().equals(this.region.getName())) {
            internalBeforeSavedReceivedRVV.run();
          }
          saveReceivedRVV(received_rvv);
          if (internalAfterSavedReceivedRVV != null
              && internalAfterSavedReceivedRVV.getRegionName().equals(this.region.getName())) {
            internalAfterSavedReceivedRVV.run();
          }
        }
      }

      ImageProcessor processor = new ImageProcessor(this.region.getSystem(), recipient);
      dm.acquireGIIPermitUninterruptibly();
      try {
        m.processorId = processor.getProcessorId();
        if (region.isUsedForPartitionedRegionBucket()
            && region.getDistributionConfig().getAckSevereAlertThreshold() > 0) {
          processor.enableSevereAlertProcessing();
          m.severeAlertEnabled = true;
        }

        // do not remove the following log statement
        logger.info("Region {} requesting initial image from {}",
            new Object[] {this.region.getName(), recipient});

        dm.putOutgoing(m);
        this.region.cache.getCancelCriterion().checkCancelInProgress(null);
        if (internalAfterSentRequestImage != null
            && internalAfterSentRequestImage.getRegionName().equals(this.region.getName())) {
          internalAfterSentRequestImage.run();
        }
        try {
          processor.waitForRepliesUninterruptibly();

          // review unfinished keys and remove untouched entries
          if (this.region.getDataPolicy().withPersistence() && keysOfUnfinishedOps != null
              && !keysOfUnfinishedOps.isEmpty()) {
            final DiskRegion dr = this.region.getDiskRegion();
            assert dr != null;
            for (Object key : keysOfUnfinishedOps) {
              RegionEntry re = this.entries.getEntry(key);
              if (re == null) {
                continue;
              }
              if (logger.isTraceEnabled(LogMarker.INITIAL_IMAGE_VERBOSE)) {
                logger.trace(LogMarker.INITIAL_IMAGE_VERBOSE,
                    "Processing unfinished operation:entry={}", re);
              }
              DiskEntry de = (DiskEntry) re;
              synchronized (de) {
                DiskId id = de.getDiskId();
                if (id != null && EntryBits.isRecoveredFromDisk(id.getUserBits())) {
                  this.region.destroyRecoveredEntry(key);
                  if (isDebugEnabled) {
                    logger.debug("Deleted unfinished keys:key={}", key);
                  }
                }
              }
            }
          }

          continue;
        } catch (InternalGemFireException ex) {
          Throwable cause = ex.getCause();
          if (cause instanceof org.apache.geode.cache.TimeoutException) {
            throw (org.apache.geode.cache.TimeoutException) cause;
          }
          throw ex;
        } catch (ReplyException e) {
          if (!region.isDestroyed()) {
            e.handleCause();
          }
        } finally {
          ImageState imgState = region.getImageState();

          if (imgState.getClearRegionFlag()) {
            // Asif : Since the operation has been completed clear flag
            imgState.setClearRegionFlag(false, null);
          }

          // Make sure we have applied the tombstone GC as seen on the GII
          // source
          if (this.gcVersions != null) {
            region.getGemFireCache().getTombstoneService().gcTombstones(region, this.gcVersions,
                false);
          }

          if (this.gotImage) {
            RegionLogger.logGII(this.region.getFullPath(), recipient,
                region.getDistributionManager().getDistributionManagerId(),
                region.getPersistentID());
          }
          if (this.gotImage) {
            // TODO add localizedString
            logger.info("{} is done getting image from {}. isDeltaGII is {}", this.region.getName(),
                recipient, this.isDeltaGII);
          } else {
            // TODO add localizedString
            logger.info("{} failed to get image from {}", this.region.getName(), recipient);
          }
          if (this.region.getDataPolicy().withPersistence()) {
            logger.info("Region {} initialized persistent id: {} with data from {}.",
                new Object[] {this.region.getName(), this.region.getPersistentID(),
                    recipient});
          }
          // bug 39050 - no partial images after GII when network partition
          // detection is enabled
          if (!this.gotImage) {
            this.region.cleanUpAfterFailedGII(recoveredFromDisk);
          } else if (received_rvv != null) {
            checkForUnrecordedOperations(recipient);
          }
        }
      } finally {
        dm.releaseGIIPermit();
        processor.cleanup();
      }
    } // for

    if (this.gotImage) {
      this.region.recordEventStateFromImageProvider(provider);
      this.region.getCachePerfStats().endGetInitialImage(giiStart);
      if (this.isDeltaGII) {
        this.region.getCachePerfStats().incDeltaGIICompleted();
      }
    } else {
      this.region.getCachePerfStats().endNoGIIDone(giiStart);
    }
    return reportGIIStatus();
  }

  /**
   * synchronize with another member (delta GII from it). If lostMember is not null, then only
   * changes that it made to the image provider will be sent back. Otherwise all changes made to the
   * image provider will be compared with those made to this cache and a full delta will be sent.
   */
  public void synchronizeWith(InternalDistributedMember target, VersionSource lostMemberVersionID,
      InternalDistributedMember lostMember) {
    final ClusterDistributionManager dm =
        (ClusterDistributionManager) this.region.getDistributionManager();

    this.isSynchronizing = true;
    RequestImageMessage m = new RequestImageMessage();
    m.regionPath = this.region.getFullPath();
    m.keysOnly = false;

    if (lostMemberVersionID != null) {
      m.versionVector = this.region.getVersionVector().getCloneForTransmission(lostMemberVersionID);
      m.lostMemberVersionID = lostMemberVersionID;
      m.lostMemberID = lostMember;
    } else {
      m.versionVector = this.region.getVersionVector().getCloneForTransmission();
    }
    m.setRecipient(target);
    ImageProcessor processor = new ImageProcessor(this.region.getSystem(), target);
    dm.acquireGIIPermitUninterruptibly();
    try {
      m.processorId = processor.getProcessorId();
      if (region.isUsedForPartitionedRegionBucket()
          && region.getDistributionConfig().getAckSevereAlertThreshold() > 0) {
        processor.enableSevereAlertProcessing();
        m.severeAlertEnabled = true;
      }

      logger.info("Region {} is requesting synchronization with {} for {}", this.region.getName(),
          target, lostMember);

      long hisVersion = this.region.getVersionVector().getVersionForMember(lostMemberVersionID);

      dm.putOutgoing(m);
      this.region.cache.getCancelCriterion().checkCancelInProgress(null);
      try {
        processor.waitForRepliesUninterruptibly();
        ImageState imgState = region.getImageState();
        if (imgState.getClearRegionFlag()) {
          imgState.setClearRegionFlag(false, null);
        }
      } catch (InternalGemFireException ex) {
        Throwable cause = ex.getCause();
        if (cause instanceof org.apache.geode.cache.TimeoutException) {
          throw (org.apache.geode.cache.TimeoutException) cause;
        }
        throw ex;
      } catch (ReplyException e) {
        if (!region.isDestroyed()) {
          e.handleCause();
        }
      } finally {
        if (this.gotImage) {
          this.region.getVersionVector().removeExceptionsFor(target, hisVersion);
          RegionVersionHolder holder =
              this.region.getVersionVector().getHolderForMember(lostMemberVersionID);
          if (this.rcvd_holderToSync != null
              && this.rcvd_holderToSync.isNewerThanOrCanFillExceptionsFor(holder)) {
            logger.info(
                "synchronizeWith detected mismatch region version holder for lost member {}. Old is {}, new is {}",
                lostMemberVersionID, holder, this.rcvd_holderToSync);
            this.region.getVersionVector().initializeVersionHolder(lostMemberVersionID,
                this.rcvd_holderToSync);
          }
          RegionLogger.logGII(this.region.getFullPath(), target,
              region.getDistributionManager().getDistributionManagerId(), region.getPersistentID());
        }
        if (this.gotImage) {
          if (logger.isDebugEnabled()) {
            logger.debug("{} is done synchronizing with {}", this.region.getName(), target);
          }
        } else {
          if (logger.isDebugEnabled()) {
            logger.debug(
                "{} received no synchronization data from {} which could mean that we are already synchronized",
                this.region.getName(), target);
          }
        }
      }
    } finally {
      dm.releaseGIIPermit();
      processor.cleanup();
    }
  }

  private void checkForUnrecordedOperations(InternalDistributedMember imageProvider) {
    final boolean isTraceEnabled = logger.isTraceEnabled();

    // bug #48962 - a change could have been received from a member
    // that the image provider didn't see. This can happen if the
    // image provider is creating the region in parallel with this member.
    // We have to check all of the received versions for members that
    // left during GII to see if the RVV contains them.
    RegionVersionVector rvv = this.region.getVersionVector();
    if (this.region.getConcurrencyChecksEnabled() && rvv != null) {
      ImageState state = this.region.getImageState();
      if (state.hasLeftMembers()) {
        Set<VersionSource> needsSync = null;
        Set<VersionSource> leftMembers = state.getLeftMembers();
        Iterator<ImageState.VersionTagEntry> tags = state.getVersionTags();
        while (tags.hasNext()) {
          ImageState.VersionTagEntry tag = tags.next();
          if (isTraceEnabled) {
            logger.trace("checkForUnrecordedOperations: processing tag {}", tag);
          }
          if (leftMembers.contains(tag.getMemberID())
              && !rvv.contains(tag.getMemberID(), tag.getRegionVersion())) {
            if (needsSync == null) {
              needsSync = new HashSet<VersionSource>();
            }
            needsSync.add(tag.getMemberID());
            rvv.recordVersion(tag.getMemberID(), tag.getRegionVersion());
          }
        }
        if (needsSync != null) {
          // we need to tell the image provider to request syncs on the given
          // member(s) These will either be DistributedMember IDs or DiskStore IDs
          RequestSyncMessage msg = new RequestSyncMessage();
          msg.regionPath = this.region.getFullPath();
          msg.lostVersionSources = needsSync.toArray(new VersionSource[0]);
          Set recipients = this.region.getCacheDistributionAdvisor().adviseReplicates();
          for (Iterator it = recipients.iterator(); it.hasNext();) {
            InternalDistributedMember mbr = (InternalDistributedMember) it.next();
            if (mbr.getVersionObject().compareTo(Version.GFE_80) < 0) {
              it.remove();
            }
          }
          if (!recipients.isEmpty()) {
            msg.setRecipients(recipients);
            if (logger.isDebugEnabled()) {
              logger.debug("Local versions were found that the image provider has not seen for {}",
                  needsSync);
            }
            this.region.getDistributionManager().putOutgoing(msg);
          }
        }
      }
    }
  }

  /**
   * test hook invokation
   */
  public static void beforeGetInitialImage(DistributedRegion region) {
    if (internalBeforeGetInitialImage != null
        && internalBeforeGetInitialImage.getRegionName().equals(region.getName())) {
      internalBeforeGetInitialImage.run();
    }

  }


  /**
   * transfer interest/cq registrations from the image provider to this VM
   *
   * @return whether the operation succeeded in transferring anything
   */
  private boolean requestFilterInfo(InternalDistributedMember recipient) {
    // Request for Filter Information before getting the
    // HARegion snapshot.
    final DistributionManager dm = this.region.getDistributionManager();
    RequestFilterInfoMessage filterInfoMsg = new RequestFilterInfoMessage();
    filterInfoMsg.regionPath = this.region.getFullPath();
    filterInfoMsg.setRecipient(recipient);
    FilterInfoProcessor processor = new FilterInfoProcessor(this.region.getSystem(), recipient);
    filterInfoMsg.processorId = processor.getProcessorId();
    dm.putOutgoing(filterInfoMsg);

    try {
      processor.waitForRepliesUninterruptibly();
      return processor.filtersReceived;
    } catch (InternalGemFireException ex) {
      Throwable cause = ex.getCause();
      if (cause instanceof org.apache.geode.cache.TimeoutException) {
        throw (org.apache.geode.cache.TimeoutException) cause;
      }
      throw ex;
    } catch (ReplyException e) {
      if (!region.isDestroyed()) {
        e.handleCause();
      }
    }
    return false;
  }


  /**
   * Called from separate thread when reply is processed.
   *
   * @param entries entries to add to the region
   * @return false if should abort (region was destroyed or cache was closed)
   */
  boolean processChunk(List entries, InternalDistributedMember sender, Version remoteVersion)
      throws IOException, ClassNotFoundException {
    final boolean isDebugEnabled = logger.isDebugEnabled();
    final boolean isTraceEnabled = logger.isTraceEnabled();

    // one volatile read of test flag
    int slow = slowImageProcessing;
    final CachePerfStats stats = this.region.getCachePerfStats();
    ImageState imgState = this.region.getImageState();
    // Asif : Can the image state be null here. Don't think so
    // Assert.assertTrue(imgState != null, "processChunk :ImageState should not have been null ");
    // Asif: Set the Htree Reference in Thread Local before the iteration begins so as
    // to detect a clear operation occurring while the put operation is in progress
    // It is Ok to set it every time the loop is executed, because a clear can happen
    // only once during GII life cycle & so it does not matter if the HTree ref changes after the
    // clear
    // whenever a conflict is detected in DiskRegion it is Ok to abort the operation
    final DiskRegion diskRegion = this.region.getDiskRegion();
    if (diskRegion != null) {
      diskRegion.setClearCountReference();
    }
    try {
      int entryCount = entries.size();
      Set keys = null;
      if (entryCount <= 1000 && isDebugEnabled) {
        keys = new HashSet();
      }
      final ByteArrayDataInput in = new ByteArrayDataInput();
      List<Entry> entriesToSynchronize = null;
      if (this.isSynchronizing) {
        entriesToSynchronize = new ArrayList<>();
      }
      for (int i = 0; i < entryCount; i++) {
        // stream is null-terminated
        if (internalDuringApplyDelta != null && !internalDuringApplyDelta.isRunning
            && internalDuringApplyDelta.getRegionName().equals(this.region.getName())) {
          internalDuringApplyDelta.run();
        }
        if (slow > 0) {
          // make sure we are still slow
          slow = slowImageProcessing;
          if (slow > 0) {
            boolean interrupted = Thread.interrupted();
            try {
              if (isDebugEnabled) {
                logger.debug("processChunk: Sleeping for {} ms for rgn {}", slow,
                    this.region.getFullPath());
              }
              Thread.sleep(slow);
              slowImageSleeps++;
            } catch (InterruptedException e) {
              interrupted = true;
              region.getCancelCriterion().checkCancelInProgress(e);
            } finally {
              if (interrupted) {
                Thread.currentThread().interrupt();
              }
            }
          }
        }
        try {
          if (this.region.isDestroyed() || imgState.getClearRegionFlag()) {
            return false;
          }
        } catch (CancelException e) {
          return false;
        }

        Entry entry = (Entry) entries.get(i);

        stats.incGetInitialImageKeysReceived();

        final long lastModified = entry.getLastModified(this.region.getDistributionManager());

        Object tmpValue = entry.value;
        byte[] tmpBytes = null;

        {
          if (tmpValue instanceof byte[]) {
            tmpBytes = (byte[]) tmpValue;
          }
        }

        boolean didIIP = false;
        boolean wasRecovered = false;

        VersionTag tag = entry.getVersionTag();

        if (diskRegion != null) {
          // verify if entry from GII is the same as the one from recovery
          RegionEntry regionEntry = this.entries.getEntry(entry.key);
          if (isTraceEnabled) {
            logger.trace("processChunk:entry={},tag={},re={}", entry, tag, regionEntry);
          }
          // re will be null if the gii chunk gives us a create
          if (regionEntry != null) {
            synchronized (regionEntry) { // fixes bug 41409
              if (diskRegion.testIsRecoveredAndClear(regionEntry)) {
                wasRecovered = true;
                if (tmpValue == null) {
                  tmpValue = entry.isLocalInvalid() ? Token.LOCAL_INVALID : Token.INVALID;
                }

                // Compare the version stamps, and if they are equal
                // we can skip adding the entry we receive as part of GII.
                VersionStamp stamp = regionEntry.getVersionStamp();
                boolean entriesEqual = stamp != null && stamp.asVersionTag().equals(tag);

                // If the received entry and what we have in the cache
                // actually are equal, keep don't put the received
                // entry into the cache (this avoids writing a record to disk)
                if (entriesEqual) {
                  continue;
                }
                if (entry.isSerialized() && !Token.isInvalidOrRemoved(tmpValue)) {
                  tmpValue =
                      CachedDeserializableFactory.create((byte[]) tmpValue, region.getCache());
                }
                try {
                  if (tag != null) {
                    tag.replaceNullIDs(sender);
                  }
                  boolean record;
                  if (this.region.getVersionVector() != null) {
                    this.region.getVersionVector().recordVersion(tag.getMemberID(), tag);
                    record = true;
                  } else {
                    // bug #50992
                    record = (tmpValue != Token.TOMBSTONE);
                  }
                  if (record) {
                    this.entries.initialImagePut(entry.key, lastModified, tmpValue, wasRecovered,
                        true, tag, sender, this.isSynchronizing);
                    if (this.isSynchronizing) {
                      entriesToSynchronize.add(entry);
                    }
                  }
                } catch (RegionDestroyedException e) {
                  return false;
                } catch (CancelException e) {
                  return false;
                }
                didIIP = true;
              }
            }
            // fix for 41814, java level deadlock
            this.entries.lruUpdateCallback();
          }
        }

        if (keys != null) {
          if (tag == null) {
            keys.add(entry.key);
          } else {
            keys.add(String.valueOf(entry.key) + ",v=" + tag);
          }
        }
        if (!didIIP) {
          if (tmpValue == null) {
            tmpValue = entry.isLocalInvalid() ? Token.LOCAL_INVALID : Token.INVALID;
          } else if (entry.isSerialized()) {
            tmpValue = CachedDeserializableFactory.create((byte[]) tmpValue, region.getCache());
          }
          try {
            // null IDs in a version tag are meant to mean "this member", so
            // we need to change them to refer to the image provider.
            if (tag != null) {
              tag.replaceNullIDs(sender);
            }
            if (isTraceEnabled) {
              logger.trace(
                  "processChunk:initialImagePut:key={},lastModified={},tmpValue={},wasRecovered={},tag={}",
                  entry.key, lastModified, tmpValue, wasRecovered, tag);
            }
            if (this.region.getVersionVector() != null) {
              this.region.getVersionVector().recordVersion(tag.getMemberID(), tag);
            }
            this.entries.initialImagePut(entry.key, lastModified, tmpValue, wasRecovered, false,
                tag, sender, this.isSynchronizing);
            if (this.isSynchronizing) {
              entriesToSynchronize.add(entry);
            }
          } catch (RegionDestroyedException e) {
            return false;
          } catch (CancelException e) {
            return false;
          }
          didIIP = true;
        }
      }
      if (this.isSynchronizing && !entriesToSynchronize.isEmpty()) {
        LocalRegion owner = ((AbstractRegionMap) this.entries)._getOwner();
        LocalRegion region = owner instanceof BucketRegion ? owner.getPartitionedRegion() : owner;
        owner.getCache().invokeRegionEntrySynchronizationListenersAfterSynchronization(sender,
            region, entriesToSynchronize);
      }
      if (keys != null) {
        if (isDebugEnabled) {
          logger.debug("processed these initial image keys: {}", keys);
        }
      }
      if (internalBeforeCleanExpiredTombstones != null
          && internalBeforeCleanExpiredTombstones.getRegionName().equals(this.region.getName())) {
        internalBeforeCleanExpiredTombstones.run();
      }
      if (internalAfterSavedRVVEnd != null
          && internalAfterSavedRVVEnd.getRegionName().equals(this.region.getName())) {
        internalAfterSavedRVVEnd.run();
      }
      return true;
    } finally {
      if (diskRegion != null) {
        diskRegion.removeClearCountReference();
      }
    }
  }

  protected RegionVersionVector getRVVFromProvider(final ClusterDistributionManager dm,
      InternalDistributedMember recipient, boolean targetReinitialized) {
    RegionVersionVector received_rvv = null;
    // RequestRVVMessage is to send rvv of gii provider for both persistent and non-persistent
    // region
    RequestRVVMessage rrm = new RequestRVVMessage();
    rrm.regionPath = this.region.getFullPath();
    rrm.targetReinitialized = targetReinitialized;
    rrm.setRecipient(recipient);

    RequestRVVProcessor rvv_processor = new RequestRVVProcessor(this.region.getSystem(), recipient);
    rrm.processorId = rvv_processor.getProcessorId();
    dm.putOutgoing(rrm);
    if (internalAfterRequestRVV != null
        && internalAfterRequestRVV.getRegionName().equals(this.region.getName())) {
      internalAfterRequestRVV.run();
    }

    try {
      rvv_processor.waitForRepliesUninterruptibly();
      received_rvv = rvv_processor.received_rvv;
    } catch (InternalGemFireException ex) {
      Throwable cause = ex.getCause();
      if (cause instanceof org.apache.geode.cache.TimeoutException) {
        throw (org.apache.geode.cache.TimeoutException) cause;
      }
      throw ex;
    } catch (ReplyException e) {
      if (!region.isDestroyed()) {
        e.handleCause();
      }
    }
    return received_rvv;
  }

  /**
   * Compare the received RVV with local RVV and return a set of keys for unfinished operations.
   *
   * @param remoteRVV RVV from provider
   * @param localRVV RVV recovered from disk
   * @return set for keys of unfinished operations.
   */
  protected Set processReceivedRVV(RegionVersionVector remoteRVV, RegionVersionVector localRVV) {
    if (remoteRVV == null) {
      return null;
    }
    // calculate keys for unfinished ops
    HashSet keys = new HashSet();
    if (this.region.getDataPolicy().withPersistence()
        && localRVV.isNewerThanOrCanFillExceptionsFor(remoteRVV)) {
      // only search for unfinished keys when localRVV has something newer
      // and the region is persistent region
      Iterator it = this.region.getBestIterator(false);
      int count = 0;
      VersionSource<?> myId = this.region.getVersionMember();
      while (it.hasNext()) {
        RegionEntry mapEntry = (RegionEntry) it.next();
        VersionStamp<?> stamp = mapEntry.getVersionStamp();
        VersionSource<?> id = stamp.getMemberID();
        if (id == null) {
          id = myId;
        }
        if (!remoteRVV.contains(id, stamp.getRegionVersion())) {
          // found an unfinished operation
          keys.add(mapEntry.getKey());
          remoteRVV.recordVersion(id, stamp.getRegionVersion());

          if (count < 10) {
            if (logger.isTraceEnabled(LogMarker.INITIAL_IMAGE_VERBOSE)) {
              logger.trace(LogMarker.INITIAL_IMAGE_VERBOSE,
                  "Region:{} found unfinished operation key={},member={},region version={}",
                  region.getFullPath(), mapEntry.getKey(), stamp.getMemberID(),
                  stamp.getRegionVersion());
            }
          }
          count++;
        }
      }
      if (!keys.isEmpty()) {
        if (logger.isTraceEnabled(LogMarker.INITIAL_IMAGE_VERBOSE)) {
          logger.trace(LogMarker.INITIAL_IMAGE_VERBOSE, "Region:{} found {} unfinished operations",
              region.getFullPath(), keys.size());
        }
      }
    }
    return keys;
  }

  protected void saveReceivedRVV(RegionVersionVector rvv) {
    assert rvv != null;

    // Make sure the RVV is at least as current as
    // the provider's was when the GII began. This ensures that a
    // concurrent clear() doesn't prevent the new region's RVV from being
    // initialized and that any vector entries that are no longer represented
    // by stamps in the region are not lost
    if (logger.isTraceEnabled(LogMarker.INITIAL_IMAGE_VERBOSE)) {
      logger.trace(LogMarker.INITIAL_IMAGE_VERBOSE, "Applying received version vector {} to {}",
          rvv.fullToString(), region.getName());
    }
    // TODO - RVV - Our current RVV might reflect some operations
    // that are concurrent updates. We want to keep those updates. However
    // it might also reflect things that we recovered from disk that we are going
    // to remove. We'll need to remove those from the RVV somehow.
    region.getVersionVector().recordVersions(rvv);
    if (region.getDataPolicy().withPersistence()) {
      region.getDiskRegion().writeRVV(region, false);
      region.getDiskRegion().writeRVVGC(region);
    }
    if (logger.isTraceEnabled(LogMarker.INITIAL_IMAGE_VERBOSE)) {
      logger.trace(LogMarker.INITIAL_IMAGE_VERBOSE, "version vector is now {}",
          region.getVersionVector().fullToString());
    }
  }

  /**
   * This is the processor that handles {@link ImageReplyMessage}s that arrive
   */
  class ImageProcessor extends ReplyProcessor21 {
    /**
     * true if this image has been rendered moot, esp. by a region destroy, a clear, or a shutdown
     */
    private volatile boolean abort = false;

    /**
     * to know whether chunk received or not, since last checkpoint
     */
    private volatile boolean receivedChunk = false;

    /**
     * Tracks the status of this operation.
     * <p>
     * Keys are the senders (@link {@link InternalDistributedMember}), and values are instances of
     * {@link Status}.
     */
    private final Map statusMap = new HashMap();

    /**
     * number of outstanding executors currently in-flight on this request
     */
    private final AtomicInteger msgsBeingProcessed = new AtomicInteger();

    @Override
    public boolean isSevereAlertProcessingEnabled() {
      return isSevereAlertProcessingForced();
    }

    /**
     * process the memberid:threadid -> sequence# information transmitted along with an initial
     * image from another cache
     */
    void processRegionStateMessage(RegionStateMessage msg) {
      if (msg.eventState != null) {
        logger.debug("Applying event state to region {} from {}", region.getName(),
            msg.getSender());
        region.recordEventState(msg.getSender(), msg.eventState);
      }
      if (msg.versionVector != null
          && msg.getSender().getVersionObject().compareTo(Version.GFE_80) < 0
          && region.getConcurrencyChecksEnabled()) {
        // for older version, save received rvv from RegionStateMessage
        logger.debug("Applying version vector to {}: {}", region.getName(), msg.versionVector);
        // pack the original RVV, then save the received one
        if (internalBeforeSavedReceivedRVV != null
            && internalBeforeSavedReceivedRVV.getRegionName().equals(region.getName())) {
          internalBeforeSavedReceivedRVV.run();
        }
        saveReceivedRVV(msg.versionVector);
        if (internalAfterSavedReceivedRVV != null
            && internalAfterSavedReceivedRVV.getRegionName().equals(region.getName())) {
          internalAfterSavedReceivedRVV.run();
        }
      }
    }

    /**
     * Track the status of this request from (a given sender)
     */
    class Status {
      /**
       * number of chunks we have received from this sender
       * <p>
       * Indexed by seriesNum, always 0)
       */
      int[] msgsProcessed = null;

      /**
       * Number of chunks total we need before we are done.
       * <p>
       * This is not set until the last chunk is received, so while it is zero we know we are not
       * done.
       * <p>
       * Indexed by seriesNum, always 0.
       */
      int[] numInSeries = null;

      /**
       * Have we received event state from the provider?
       */
      boolean eventStateReceived;

      /**
       * Have we received all of the chunked messages from the provider?
       */
      boolean allChunksReceived;

      /** Return true if this is the very last reply for this member */
      protected synchronized boolean trackMessage(ImageReplyMessage m) {
        if (this.msgsProcessed == null) {
          this.msgsProcessed = new int[m.numSeries];
        }
        if (this.numInSeries == null) {
          this.numInSeries = new int[m.numSeries];
        }
        this.msgsProcessed[m.seriesNum]++;

        if (m.lastInSeries) {
          this.numInSeries[m.seriesNum] = m.msgNum + 1;
        }
        if (logger.isDebugEnabled()) {
          logger.debug(
              "InitialImage Message Tracking Status: Processor id: {}; Sender: {}; Messages Processed: {}; NumInSeries:{}",
              getProcessorId(), m.getSender(), arrayToString(this.msgsProcessed),
              arrayToString(this.numInSeries));
        }

        // this.numInSeries starts out as zeros and gets initialized
        // for a series only when we get a lastInSeries true.
        // Since we increment msgsProcessed, the following condition
        // cannot be true until sometime after we've received the
        // lastInSeries for a given series.
        this.allChunksReceived = Arrays.equals(this.msgsProcessed, this.numInSeries);
        return (this.allChunksReceived);
      }
    }


    public ImageProcessor(final InternalDistributedSystem system,
        InternalDistributedMember member) {
      super(system, member);
    }

    public ImageProcessor(InternalDistributedSystem system, Set members) {
      super(system, members);
    }

    @Override
    protected boolean processTimeout() {
      // if chunk received then no need to process timeout
      boolean ret = this.receivedChunk;
      this.receivedChunk = false;
      return !ret;
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.apache.geode.distributed.internal.ReplyProcessor21#process(org.apache.geode.distributed.
     * internal.DistributionMessage)
     */
    @Override
    public void process(DistributionMessage msg) {
      // ignore messages from members not in the wait list
      if (!waitingOnMember(msg.getSender())) {
        return;
      }
      Status status = getStatus(msg.getSender());
      this.msgsBeingProcessed.incrementAndGet();
      EntryLogger.setSource(msg.getSender(), "gii");
      try {
        boolean isDone;
        if (msg instanceof RegionStateMessage) {
          isDone = false;
          status.eventStateReceived = true;
          processRegionStateMessage((RegionStateMessage) msg);
        } else {
          isDone = true;
          ImageReplyMessage m = (ImageReplyMessage) msg;

          boolean isLast = true; // is last message for this member?
          if (m.entries != null) {
            try {
              if (internalAfterReceivedImageReply != null
                  && internalAfterReceivedImageReply.getRegionName().equals(region.getName())) {
                internalAfterReceivedImageReply.run();
              }
              // bug 37461: don't allow abort flag to be reset
              boolean isAborted = this.abort; // volatile fetch
              if (!isAborted) {
                isAborted = !processChunk(m.entries, m.getSender(), m.remoteVersion);
                if (isAborted) {
                  this.abort = true; // volatile store
                } else {
                  this.receivedChunk = true;
                }
              }
              isLast = trackMessage(m); // interpret series/msgNum
              isDone = isAborted || isLast;

              // @todo ericz send an abort message to image provider if
              // !doContinue (region was destroyed or cache closed)
              if (isDone) {
                if (this.abort) {
                  // Bug 48578: In deltaGII, if abort in processChunk, we should mark trustRVV=false
                  // to force full GII next time.
                  InitialImageOperation.this.gotImage = false;
                  logger.debug(
                      "processChunk is aborted for region {}, rvv is {}. Do full gii next time.",
                      InitialImageOperation.this.region.getFullPath(),
                      InitialImageOperation.this.region.getVersionVector());
                } else {
                  InitialImageOperation.this.gotImage = true;
                }
                if (m.isDeltaGII) {
                  InitialImageOperation.this.isDeltaGII = true;
                }
              }
            } catch (DiskAccessException dae) {
              ReplyException ex = new ReplyException("while processing entries", dae);
              ex.setSenderIfNull(region.getCache().getMyId());
              processException(ex);
            }
            // save exceptions so they can be thrown from waitForReplies
            catch (VirtualMachineError err) {
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
              ReplyException ex = new ReplyException("while processing entries", t);
              ex.setSenderIfNull(region.getCache().getMyId());
              processException(ex);
            }
          } else {
            // if a null entries was received (no image was found), then
            // we're done with that member
            if (isDone && m.isDeltaGII) {
              InitialImageOperation.this.gotImage = true;
              InitialImageOperation.this.isDeltaGII = true;
            }
          }
          if (m.holderToSend != null) {
            InitialImageOperation.this.rcvd_holderToSync = m.holderToSend;
          }

          if (m.gcVersions != null) {
            InitialImageOperation.this.gcVersions = m.gcVersions;
          }
        }
        if (isDone) {
          super.process(msg, false); // removes from members and cause us to
                                     // ignore future messages received from that member
        }
      } catch (RegionDestroyedException e) {
        // bug #46135 - disk store can throw this exception
        InitialImageOperation.this.region.getCancelCriterion().checkCancelInProgress(e);
      } finally {
        this.msgsBeingProcessed.decrementAndGet();
        checkIfDone(); // check to see if decrementing msgsBeingProcessed requires signaling to
                       // proceed
        EntryLogger.clearSource();
      }
    }

    /**
     * True if we have signalled to stop waiting
     * <p>
     * Contract of {@link ReplyProcessor21#stillWaiting()} is that it must never return true after
     * having returned false.
     */
    private volatile boolean finishedWaiting = false;

    /**
     * Overridden to wait for messages being currently processed: This situation can come about if a
     * member departs while we are still processing data from that member
     */
    @Override
    protected boolean stillWaiting() {
      if (finishedWaiting) { // volatile fetch
        return false;
      }
      if (this.msgsBeingProcessed.get() > 0) {
        // to fix bug 37391 always wait for msgsBeingProcessed to go to 0;
        // even if abort is true.
        return true;
      }
      // Volatile fetches and volatile store:
      if (this.abort || !super.stillWaiting()) {
        finishedWaiting = true;
        return false;
      } else {
        return true;
      }
    }


    @Override
    public String toString() {
      // bug 37189 These strings are a work-around for an escaped reference
      // in ReplyProcessor21 constructor
      String msgsBeingProcessedStr = String.valueOf(this.msgsBeingProcessed.get());
      String regionStr = (InitialImageOperation.this.region == null) ? "nullRef"
          : InitialImageOperation.this.region.getFullPath();
      String numMembersStr = (this.members == null) ? "nullRef" : String.valueOf(numMembers());
      // String membersToStr = (this.members == null) ? "nullRef" : membersToString();

      return "<" + this.getClass().getName() + " " + this.getProcessorId() + " waiting for "
          + numMembersStr + " replies" + (exception == null ? "" : (" exception: " + exception))
          + " from " + membersToString() + "; waiting for " + msgsBeingProcessedStr
          + " messages in-flight; " + "region=" + regionStr + "; abort=" + this.abort + ">";
    }

    private Status getStatus(InternalDistributedMember sender) {
      Status status;
      synchronized (this) {
        status = (Status) this.statusMap.get(sender);
        if (status == null) {
          status = new Status();
          this.statusMap.put(sender, status);
        }
      }
      return status;
    }

    private boolean trackMessage(ImageReplyMessage m) {
      return getStatus(m.getSender()).trackMessage(m);
    }

  }

  protected static String arrayToString(int[] a) {
    StringBuffer buf = new StringBuffer();
    buf.append("[");
    for (int i = 0; i < a.length; i++) {
      buf.append(String.valueOf(a[i]));
      if (i < (a.length - 1))
        buf.append(",");
    }
    buf.append("]");
    return buf.toString();
  }

  protected static LocalRegion getGIIRegion(final ClusterDistributionManager dm,
      final String regionPath, final boolean targetReinitialized) {

    final boolean isDebugEnabled = logger.isDebugEnabled();

    LocalRegion lclRgn = null;
    final InitializationLevel initLevel = targetReinitialized ? AFTER_INITIAL_IMAGE : ANY_INIT;
    final InitializationLevel oldLevel = LocalRegion.setThreadInitLevelRequirement(initLevel);
    try {
      if (isDebugEnabled) {
        logger.debug("RequestImageMessage: attempting to get region reference for {}, initLevel={}",
            regionPath, initLevel);
      }
      InternalCache cache = dm.getExistingCache();
      lclRgn = cache == null ? null : (LocalRegion) cache.getRegion(regionPath);
      // if this is a targeted getInitialImage after a region was initialized,
      // make sure this is the region that was reinitialized.
      if (lclRgn != null && !lclRgn.isUsedForPartitionedRegionBucket() && targetReinitialized
          && !lclRgn.reinitialized_new()) {
        lclRgn = null; // got a region that wasn't reinitialized, so must not be the right one
        if (isDebugEnabled) {
          logger.debug(
              "GII message process: Found region, but wasn't reinitialized, so assuming region destroyed and recreated");
        }
      }
    } finally {
      LocalRegion.setThreadInitLevelRequirement(oldLevel);
    }
    if (lclRgn == null || !lclRgn.isInitialized()) {
      if (isDebugEnabled) {
        logger.debug("{}, nothing to do",
            (lclRgn == null ? "region not found" : "region not initialized yet"));
      }
      // allow finally block to send a failure message
      return null;
    }

    if (lclRgn.getScope().isLocal()) {
      if (isDebugEnabled) {
        logger.debug("local scope region, nothing to do");
      }
      // allow finally block to send a failure message
      return null;
    }
    return lclRgn;
  }

  /**
   * This is the message that initiates a request for an image
   */
  public static class RequestImageMessage extends DistributionMessage implements MessageWithReply {

    /**
     * a version vector is transmitted with the request if we are merely synchronizing with an
     * existing region, or providing missed updates for a recreated region
     */
    public RegionVersionVector versionVector;

    /**
     * if a version vector is transmitted, this will be sent along with it to tell the image
     * provider that only changes made by this ID should be sent back
     */
    public VersionSource lostMemberVersionID;

    /**
     * the distribution ID of the lost member (see above)
     */
    public InternalDistributedMember lostMemberID;

    /**
     * Name of the region we want This field is public for test code.
     */
    public String regionPath;

    /**
     * Id of the {@link InitialImageOperation.ImageProcessor} that will handle the replies
     */
    protected int processorId;

    /**
     * True if we only want keys for the region (no values)
     */
    protected boolean keysOnly;

    /**
     * true if we want to get a full GII if there are tombstone version problems
     */
    protected boolean checkTombstoneVersions;

    /**
     * If true, recipient should wait until fully initialized before returning data.
     */
    protected boolean targetReinitialized;

    /**
     * whether severe alert processing should be performed in the reply processor for this message
     */
    protected transient boolean severeAlertEnabled;

    /* key list for unfinished operations */
    protected Set unfinishedKeys;

    /** The versions in which this message was modified */
    @Immutable
    private static final Version[] dsfidVersions = null;

    @Override
    public int getProcessorId() {
      return this.processorId;
    }

    @Override
    public int getProcessorType() {
      return this.targetReinitialized ? OperationExecutors.WAITING_POOL_EXECUTOR
          : OperationExecutors.HIGH_PRIORITY_EXECUTOR;
    }

    public boolean goWithFullGII(DistributedRegion rgn, RegionVersionVector requesterRVV) {
      if (getSender().getVersionObject().compareTo(Version.GFE_80) < 0) {
        // pre-8.0 could not handle a delta-GII
        return true;
      }
      if (!rgn.getDataPolicy().withPersistence()) {
        // non-persistent regions always do full GII
        if (logger.isDebugEnabled()) {
          logger.debug("Region {} is not a persistent region, do full GII", rgn.getFullPath());
        }
        return true;
      }
      if (!rgn.getVersionVector().isRVVGCDominatedBy(requesterRVV)) {
        if (logger.isDebugEnabled()) {
          logger.debug("Region {}'s local RVVGC is not dominated by remote RVV={}, do full GII",
              rgn.getFullPath(), requesterRVV);
        }
        return true;
      }
      return false;
    }

    @VisibleForTesting
    public String getRegionPath() {
      return regionPath;
    }

    @Override
    protected void process(final ClusterDistributionManager dm) {
      final boolean isGiiDebugEnabled = logger.isTraceEnabled(LogMarker.INITIAL_IMAGE_VERBOSE);

      Throwable thr = null;
      final boolean lclAbortTest = abortTest;
      if (lclAbortTest)
        abortTest = false;
      DistributedRegion targetRegion = null;
      boolean sendFailureMessage = true;
      try {
        Assert.assertTrue(this.regionPath != null, "Region path is null.");
        final DistributedRegion rgn =
            (DistributedRegion) getGIIRegion(dm, this.regionPath, this.targetReinitialized);
        if (lostMemberID != null) {
          targetRegion = rgn;
        }
        if (rgn == null) {
          return;
        }

        // can simulate gc tombstone in middle of packing
        if (internalAfterReceivedRequestImage != null
            && internalAfterReceivedRequestImage.getRegionName().equals(rgn.getName())) {
          internalAfterReceivedRequestImage.run();
        }

        if (this.versionVector != null) {
          if (this.versionVector.isForSynchronization() && !rgn.getConcurrencyChecksEnabled()) {
            if (isGiiDebugEnabled) {
              logger.trace(LogMarker.INITIAL_IMAGE_VERBOSE,
                  "ignoring synchronization request as this region has no version vector");
            }
            replyNoData(dm, true, Collections.EMPTY_MAP);
            sendFailureMessage = false;
            return;
          }
          if (isGiiDebugEnabled) {
            logger.debug("checking version vector against region's ({})",
                rgn.getVersionVector().fullToString());
          }
          // [bruce] I suppose it's possible to have this check return a list of
          // specific versions that the sender is missing. The current check
          // just stops when it finds the first inconsistency
          if (!rgn.getVersionVector().isNewerThanOrCanFillExceptionsFor(this.versionVector)) {
            // Delta GII might have unfinished operations to send. Otherwise,
            // no need to send any data. This is a synchronization request and this region's
            // vector doesn't have anything that the other region needs
            if (this.unfinishedKeys == null || this.unfinishedKeys.isEmpty()) {
              if (isGiiDebugEnabled) {
                logger.trace(LogMarker.INITIAL_IMAGE_VERBOSE,
                    "version vector reports that I have nothing that the requester hasn't already seen");
              }
              replyNoData(dm, true, rgn.getVersionVector().getMemberToGCVersion());
              sendFailureMessage = false;
              return;
            }
          } else {
            if (isGiiDebugEnabled) {
              logger.trace(LogMarker.INITIAL_IMAGE_VERBOSE,
                  "version vector reports that I have updates the requester hasn't seen, remote rvv is {}",
                  this.versionVector);
            }
          }
        }

        final int numSeries = 1; // @todo ericz parallelize using series
        final int seriesNum = 0;

        // chunkEntries returns false if didn't finish
        if (isGiiDebugEnabled) {
          logger.trace(LogMarker.INITIAL_IMAGE_VERBOSE,
              "RequestImageMessage: Starting chunkEntries for {}", rgn.getFullPath());
        }

        final InitialImageFlowControl flowControl =
            InitialImageFlowControl.register(dm, getSender());

        if (rgn instanceof HARegion) {
          ((HARegion) rgn).startServingGIIRequest();
        }
        boolean markedOngoingGII = false;
        try {
          boolean recoveringForLostMember = (this.lostMemberVersionID != null);
          RegionVersionHolder holderToSync = null;
          if (recoveringForLostMember && this.lostMemberID != null) {
            // wait for the lost member to be gone from this VM's membership and all ops applied to
            // the cache
            try {
              dm.getDistribution().waitForDeparture(this.lostMemberID);
              RegionVersionHolder rvh =
                  rgn.getVersionVector().getHolderForMember(this.lostMemberVersionID);
              if (rvh != null) {
                holderToSync = rvh.clone();
              }
              if (isGiiDebugEnabled) {
                RegionVersionHolder holderOfRequest =
                    this.versionVector.getHolderForMember(this.lostMemberVersionID);
                if (holderToSync.isNewerThanOrCanFillExceptionsFor(holderOfRequest)) {
                  logger.trace(LogMarker.INITIAL_IMAGE_VERBOSE,
                      "synchronizeWith detected mismatch region version holder for lost member {}. Old is {}, new is {}",
                      lostMemberVersionID, holderOfRequest, holderToSync);
                }
              }
            } catch (TimeoutException e) {
              if (isGiiDebugEnabled) {
                logger.trace(LogMarker.INITIAL_IMAGE_VERBOSE,
                    "timed out waiting for the departure of {} before processing delta GII request",
                    this.lostMemberID);
              }
            }
          }
          if (rgn instanceof HARegion) {
            // long eventXferStart = System.currentTimeMillis();
            Map<? extends DataSerializable, ? extends DataSerializable> eventState =
                rgn.getEventState();
            if (eventState != null && eventState.size() > 0) {
              RegionStateMessage.send(dm, getSender(), this.processorId, eventState, true);
            }
          } else if (getSender().getVersionObject().compareTo(Version.GFE_80) < 0) {
            // older versions of the product expect a RegionStateMessage at this point
            if (rgn.getConcurrencyChecksEnabled() && this.versionVector == null
                && !recoveringForLostMember) {
              RegionVersionVector rvv = rgn.getVersionVector().getCloneForTransmission();
              RegionStateMessage.send(dm, getSender(), this.processorId, rvv, false);
            }
          }
          if (this.checkTombstoneVersions && this.versionVector != null
              && rgn.getConcurrencyChecksEnabled()) {
            synchronized (rgn.getCache().getTombstoneService().getBlockGCLock()) {
              if (goWithFullGII(rgn, this.versionVector)) {
                if (isGiiDebugEnabled) {
                  logger.trace(LogMarker.INITIAL_IMAGE_VERBOSE, "have to do fullGII");
                }
                this.versionVector = null; // full GII
              } else {
                // lock GIILock only for deltaGII
                int count = rgn.getCache().getTombstoneService().incrementGCBlockCount();
                markedOngoingGII = true;
                if (isGiiDebugEnabled) {
                  logger.trace(LogMarker.INITIAL_IMAGE_VERBOSE, "There're {} Delta GII on going",
                      count);
                }
              }
            }
          }
          final RegionVersionHolder holderToSend = holderToSync;
          boolean finished = chunkEntries(rgn, CHUNK_SIZE_IN_BYTES, !keysOnly, versionVector,
              (HashSet) this.unfinishedKeys, flowControl, new ObjectIntProcedure() {
                int msgNum = 0;

                boolean last = false;

                /**
                 * @param entList ArrayList of entries
                 * @param b positive if last chunk
                 * @return true to continue to next chunk
                 */
                @Override
                public boolean executeWith(Object entList, int b) {
                  if (rgn.getCache().isClosed()) {
                    return false;
                  }

                  if (this.last) {
                    throw new InternalGemFireError(
                        "Already processed last chunk");
                  }

                  List entries = (List) entList;
                  this.last = b > 0 && !lclAbortTest; // if abortTest, then never send last flag set
                                                      // to true
                  try {
                    boolean abort = rgn.isDestroyed();
                    if (!abort) {
                      int flowControlId = flowControl.getId();
                      Map<VersionSource, Long> gcVersions = null;
                      if (this.last && rgn.getVersionVector() != null) {
                        gcVersions = rgn.getVersionVector().getMemberToGCVersion();
                      }
                      replyWithData(dm, entries, seriesNum, msgNum++, numSeries, this.last,
                          flowControlId,
                          versionVector != null, holderToSend, gcVersions);
                    }
                    return !abort;
                  } catch (CancelException e) {
                    return false;
                  }
                }
              });


          if (isGiiDebugEnabled) {
            logger.trace(LogMarker.INITIAL_IMAGE_VERBOSE,
                "RequestImageMessage: ended chunkEntries for {}; finished = {}", rgn.getFullPath(),
                finished);
          }

          // Call to chunkEntries above will have sent at least one
          // reply with last==true for the last message. (unless doing abortTest or
          // region is destroyed or cache closed)
          if (finished && !lclAbortTest) {
            sendFailureMessage = false;
            return; // sent msg with last indicated
          }
          // One more chance to discover region or cache destruction...
          rgn.checkReadiness();
        } finally {
          if (markedOngoingGII) {
            int count = rgn.getCache().getTombstoneService().decrementGCBlockCount();
            assert count >= 0;
            if (count == 0) {
              markedOngoingGII = false;
              if (isGiiDebugEnabled) {
                logger.trace(LogMarker.INITIAL_IMAGE_VERBOSE, "Delta GII count is reset");
              }
            }
          }
          if (rgn instanceof HARegion) {
            ((HARegion) rgn).endServingGIIRequest();
          }
          flowControl.unregister();
        }
        // This should never happen in production code!!!!

        // Code specific to abortTest... :-(
        Assert.assertTrue(lclAbortTest,
            this + ": Did not finish sending image, but region, cache, and DS are alive.");

        initiateLocalAbortForTest(dm);
      } catch (RegionDestroyedException e) {
        // thr = e; Don't marshal an exception here; just return null
        if (isGiiDebugEnabled) {
          logger.trace(LogMarker.INITIAL_IMAGE_VERBOSE,
              "{}; Region destroyed: aborting image provision", this);
        }
      } catch (IllegalStateException e) {
        // thr = e; Don't marshal an exception here; just return null
        logger.trace(LogMarker.INITIAL_IMAGE_VERBOSE,
            "{}; disk region deleted? aborting image provision", this, e);
      } catch (CancelException e) {
        // thr = e; Don't marshal an exception here; just return null
        if (isGiiDebugEnabled) {
          logger.trace(LogMarker.INITIAL_IMAGE_VERBOSE,
              "{}; Cache Closed: aborting image provision", this);
        }
      } catch (VirtualMachineError err) {
        sendFailureMessage = false; // Don't try to respond!
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
        thr = t;
      } finally {
        if (sendFailureMessage) {
          // if we get here then send reply possibly with an exception
          ReplyException rex = null;
          if (thr != null) {
            rex = new ReplyException(thr);
          }
          sendFailureMessage(dm, rex);
        } // !success

        if (lostMemberID != null && targetRegion != null) {
          if (lostMemberVersionID == null) {
            lostMemberVersionID = lostMemberID;
          }
          // check to see if the region in this cache needs to synchronize with others
          // it is possible that the cache is recover/restart of a member and not
          // scheduled to synchronize with others
          synchronizeIfNotScheduled(targetRegion, lostMemberID, lostMemberVersionID);
        }

        if (internalAfterSentImageReply != null
            && regionPath.endsWith(internalAfterSentImageReply.getRegionName())) {
          internalAfterSentImageReply.run();
        }
      }
    }

    void sendFailureMessage(ClusterDistributionManager dm, ReplyException rex) {
      // null chunk signals receiver that we are aborting
      ImageReplyMessage.send(getSender(), processorId, rex, dm, null, 0, 0, 1, true, 0, false,
          null, null);
    }

    /**
     * If there is no region sync scheduled after checking region version holder holding the
     * lost member. Region sync requests are sent to members hosting the region.
     * This is only executed when processing region sync requests from other members hosting the
     * region.
     * Region sync is triggered by a member departed event. If this member exists during the
     * event, region sync would be scheduled. This method is only handles the case when
     * node is recently joining the cluster or restarted, and does not get the member departed
     * event.
     */
    void synchronizeIfNotScheduled(DistributedRegion region,
        InternalDistributedMember lostMember, VersionSource lostVersionSource) {
      if (region.setRegionSynchronizedWithIfNotScheduled(lostVersionSource)) {
        // if region synchronization has not been scheduled or performed,
        // we do synchronization with no delay as we received the synchronization request
        // indicating timed task has been triggered on other nodes
        if (logger.isDebugEnabled()) {
          logger.debug("Newly joined member is triggered to schedule SynchronizeForLostMember");
        }
        region.scheduleSynchronizeForLostMember(lostMember, lostVersionSource, 0);
      } else {
        if (logger.isDebugEnabled()) {
          logger.debug(
              "Live member has been scheduled SynchronizeForLostMember by membership listener.");
        }
      }
    }

    /**
     * Serialize the entries into byte[] chunks, calling proc for each one. proc args: the byte[]
     * chunk and an int indicating whether it is the last chunk (positive means last chunk, zero
     * otherwise). The return value of proc indicates whether to continue to the next chunk (true)
     * or abort (false).
     *
     * @param versionVector requester's region version vector
     * @param unfinishedKeys keys of unfinished operation (persistent region only)
     * @return true if finished all chunks, false if stopped early
     */
    protected boolean chunkEntries(DistributedRegion rgn, int chunkSizeInBytes,
        boolean includeValues, RegionVersionVector versionVector, HashSet unfinishedKeys,
        InitialImageFlowControl flowControl, ObjectIntProcedure proc) throws IOException {
      boolean keepGoing = true;
      boolean sentLastChunk = false;
      int MAX_ENTRIES_PER_CHUNK = chunkSizeInBytes / 100;
      if (MAX_ENTRIES_PER_CHUNK < 1000) {
        MAX_ENTRIES_PER_CHUNK = 1000;
      }

      ByteArrayDataInput in = null;
      ClusterDistributionManager dm = (ClusterDistributionManager) rgn.getDistributionManager();

      List chunkEntries = null;
      chunkEntries = new InitialImageVersionedEntryList(rgn.getConcurrencyChecksEnabled(),
          MAX_ENTRIES_PER_CHUNK);
      DiskRegion dr = rgn.getDiskRegion();
      if (dr != null) {
        dr.setClearCountReference();
        in = new ByteArrayDataInput();
      }
      VersionSource myId = rgn.getVersionMember();
      Set<VersionSource> foundIds = new HashSet<VersionSource>();
      if (internalDuringPackingImage != null
          && this.regionPath.endsWith(internalDuringPackingImage.getRegionName())) {
        internalDuringPackingImage.run();
      }

      try {
        Iterator it = null;
        if (versionVector != null) {
          // deltaGII
          it = rgn.entries.regionEntries().iterator();
        } else {
          it = rgn.getBestIterator(includeValues);
        }
        do {
          flowControl.acquirePermit();
          int currentChunkSize = 0;

          while (chunkEntries.size() < MAX_ENTRIES_PER_CHUNK && currentChunkSize < chunkSizeInBytes
              && it.hasNext()) {
            RegionEntry mapEntry = (RegionEntry) it.next();
            Object key = mapEntry.getKey();
            if (rgn.checkEntryNotValid(mapEntry)) { // entry was just removed
              continue;
            }
            if (logger.isDebugEnabled()) {
              Object v = mapEntry.getValueInVM(rgn); // OFFHEAP: noop
              if (v instanceof Conflatable) {
                if (((Conflatable) v).getEventId() == null) {
                  logger.debug("bug 44959: chunkEntries found conflatable with no eventID: {}", v);
                }
              }
            }
            InitialImageOperation.Entry entry = null;
            if (includeValues) {
              boolean fillRes = false;
              try {
                // also fills in lastModifiedTime
                VersionStamp<?> stamp = mapEntry.getVersionStamp();
                if (stamp != null) {
                  synchronized (mapEntry) { // bug #46042 must sync to make sure the tag goes with
                                            // the value
                    VersionSource<?> id = stamp.getMemberID();
                    if (id == null) {
                      id = myId;
                    }
                    foundIds.add(id);
                    // if the recipient passed a version vector, use it to filter out
                    // entries the recipient already has
                    // For keys in unfinishedKeys, not to filter them out
                    if ((unfinishedKeys == null || !unfinishedKeys.contains(key))
                        && versionVector != null) {
                      if (versionVector.contains(id, stamp.getRegionVersion())) {
                        continue;
                      }
                    }
                    entry = new InitialImageOperation.Entry();
                    entry.key = key;
                    entry.setVersionTag(stamp.asVersionTag());
                    fillRes = mapEntry.fillInValue(rgn, entry, in, rgn.getDistributionManager(),
                        sender.getVersionObject());
                    if (versionVector != null) {
                      if (logger.isTraceEnabled(LogMarker.INITIAL_IMAGE_VERBOSE)) {
                        logger.trace(LogMarker.INITIAL_IMAGE_VERBOSE,
                            "chunkEntries:entry={},stamp={}", entry, stamp);
                      }
                    }
                  }
                } else {
                  entry = new InitialImageOperation.Entry();
                  entry.key = key;
                  fillRes = mapEntry.fillInValue(rgn, entry, in, rgn.getDistributionManager(),
                      sender.getVersionObject());
                }
              } catch (DiskAccessException dae) {
                rgn.handleDiskAccessException(dae);
                throw dae;
              }
              if (!fillRes) {
                // map entry went away
                continue;
              }
            } else {
              entry = new InitialImageOperation.Entry();
              entry.key = key;
              entry.setLocalInvalid();
              entry.setLastModified(rgn.getDistributionManager(), mapEntry.getLastModified());
            }

            chunkEntries.add(entry);
            currentChunkSize += entry.calcSerializedSize();
          }

          // send 1 for last message if no more data
          int lastMsg = it.hasNext() ? 0 : 1;
          keepGoing = proc.executeWith(chunkEntries, lastMsg);
          sentLastChunk = lastMsg == 1 && keepGoing;
          chunkEntries.clear();

          // if this region is destroyed while we are sending data, then abort.
        } while (keepGoing && it.hasNext());

        if (foundIds.size() > 0) {
          RegionVersionVector vv = rgn.getVersionVector();
          if (vv != null) {
            vv.removeOldMembers(foundIds);
          }
        }
        // return false if we were told to abort
        return sentLastChunk;
      } finally {
        if (dr != null) {
          dr.removeClearCountReference();
        }
      }
    }

    private void replyNoData(ClusterDistributionManager dm, boolean isDeltaGII,
        Map<VersionSource, Long> gcVersions) {
      ImageReplyMessage.send(getSender(), this.processorId, null, dm, null, 0, 0, 1, true, 0,
          isDeltaGII, null, gcVersions);
    }

    protected void replyWithData(ClusterDistributionManager dm, List entries, int seriesNum,
        int msgNum, int numSeries, boolean lastInSeries, int flowControlId, boolean isDeltaGII,
        RegionVersionHolder holderToSend, Map<VersionSource, Long> gcVersions) {
      ImageReplyMessage.send(getSender(), this.processorId, null, dm, entries, seriesNum, msgNum,
          numSeries, lastInSeries, flowControlId, isDeltaGII, holderToSend, gcVersions);
    }


    // test hook
    private void initiateLocalAbortForTest(final DistributionManager dm) {
      if (!dm.getSystem().isDisconnecting()) {
        if (logger.isDebugEnabled()) {
          logger.debug(
              "abortTest: Disconnecting from distributed system and sending null chunk to abort");
        }
        // can't disconnect the distributed system in a thread owned by the ds,
        // so start a new thread to do the work
        Thread disconnectThread =
            new LoggingThread("InitialImageOperation abortTest Thread",
                () -> dm.getSystem().disconnect());
        disconnectThread.start();
      } // !isDisconnecting
      // ...end of abortTest code
    }

    @Override
    public int getDSFID() {
      return REQUEST_IMAGE_MESSAGE;
    }

    @Override
    public void fromData(DataInput in,
        DeserializationContext context) throws IOException, ClassNotFoundException {
      super.fromData(in, context);
      this.regionPath = DataSerializer.readString(in);
      this.processorId = in.readInt();
      this.keysOnly = in.readBoolean();
      this.targetReinitialized = in.readBoolean();
      this.checkTombstoneVersions = in.readBoolean();
      this.lostMemberVersionID = (VersionSource) context.getDeserializer().readObject(in);
      this.versionVector = (RegionVersionVector) context.getDeserializer().readObject(in);
      this.lostMemberID = (InternalDistributedMember) context.getDeserializer().readObject(in);
      this.unfinishedKeys = (Set) context.getDeserializer().readObject(in);
    }

    @Override
    public void toData(DataOutput out,
        SerializationContext context) throws IOException {
      super.toData(out, context);
      DataSerializer.writeString(this.regionPath, out);
      out.writeInt(this.processorId);
      out.writeBoolean(this.keysOnly);
      out.writeBoolean(this.targetReinitialized);
      out.writeBoolean(this.checkTombstoneVersions);
      context.getSerializer().writeObject(this.lostMemberVersionID, out);
      context.getSerializer().writeObject(this.versionVector, out);
      context.getSerializer().writeObject(this.lostMemberID, out);
      context.getSerializer().writeObject(this.unfinishedKeys, out);
    }

    @Override
    public Version[] getSerializationVersions() {
      return dsfidVersions;
    }

    @Override
    public String toString() {
      StringBuffer buff = new StringBuffer();
      String cname = getClass().getName().substring(getClass().getPackage().getName().length() + 1);
      buff.append(cname);
      buff.append("(region path='"); // make sure this is the first one
      buff.append(this.regionPath);
      buff.append("'; sender=");
      buff.append(getSender());
      buff.append("; keysOnly=");
      buff.append(this.keysOnly);
      buff.append("; processorId=");
      buff.append(this.processorId);
      buff.append("; waitForInit=");
      buff.append(this.targetReinitialized);
      buff.append("; checkTombstoneVersions=");
      buff.append(this.checkTombstoneVersions);
      if (this.lostMemberVersionID != null) {
        buff.append("; lostMember=").append(lostMemberVersionID);
      }
      buff.append("; versionVector=").append(versionVector);
      buff.append("; unfinished keys=").append(unfinishedKeys);
      buff.append(")");
      return buff.toString();
    }

    @Override
    public boolean isSevereAlertCompatible() {
      return severeAlertEnabled;
    }
  }

  /**
   * FilterInfo message processor.
   */
  class FilterInfoProcessor extends ReplyProcessor21 {
    boolean filtersReceived;

    public FilterInfoProcessor(final InternalDistributedSystem system,
        InternalDistributedMember member) {
      super(system, member);
    }

    public FilterInfoProcessor(InternalDistributedSystem system, Set members) {
      super(system, members);
    }

    @Override
    public void process(DistributionMessage msg) {
      // ignore messages from members not in the wait list
      if (!waitingOnMember(msg.getSender())) {
        return;
      }
      try {
        if (!(msg instanceof FilterInfoMessage)) {
          return;
        }
        FilterInfoMessage m = (FilterInfoMessage) msg;
        if (m.getException() != null) {
          return;
        }

        if (logger.isDebugEnabled()) {
          try {
            CacheClientNotifier ccn = CacheClientNotifier.getInstance();
            if (ccn != null && ccn.getHaContainer() != null) {
              CacheClientProxy proxy =
                  ((HAContainerWrapper) ccn.getHaContainer()).getProxy(region.getName());
              logger.debug("Processing FilterInfo for proxy: {} : {}", proxy, msg);
            }
          } catch (Exception ex) {
            // Ignore.
          }
        }

        try {
          m.registerFilters(region);
        } catch (Exception ex) {
          logger.info("Exception while registering filters during GII: {}", ex.getMessage(), ex);
        }

        this.filtersReceived = true;

      } finally {
        super.process(msg);
      }
    }

    @Override
    public String toString() {
      String cname = getClass().getName().substring(getClass().getPackage().getName().length() + 1);
      return "<" + cname + " " + this.getProcessorId() + " replies"
          + (exception == null ? "" : (" exception: " + exception)) + " from " + membersToString()
          + ">";
    }

    @Override
    protected boolean logMultipleExceptions() {
      return false;
    }
  }

  /**
   * This is the message thats sent to get Filter information.
   */
  public static class RequestFilterInfoMessage extends DistributionMessage
      implements MessageWithReply {

    /**
     * Name of the region.
     */
    protected String regionPath;

    /**
     * Id of the {@link InitialImageOperation.ImageProcessor} that will handle the replies
     */
    protected int processorId;

    @Override
    public int getProcessorId() {
      return this.processorId;
    }

    @Override
    public int getProcessorType() {
      return OperationExecutors.HIGH_PRIORITY_EXECUTOR;
    }

    @Override
    protected void process(final ClusterDistributionManager dm) {
      Throwable thr = null;
      boolean sendFailureMessage = true;
      InternalRegion lclRgn = null;
      ReplyException rex = null;
      try {
        Assert.assertTrue(this.regionPath != null, "Region path is null.");
        InternalCache cache = dm.getCache();
        lclRgn = cache == null ? null : cache.getInternalRegionByPath(regionPath);

        if (lclRgn == null) {
          if (logger.isDebugEnabled()) {
            logger.debug("{}; Failed to process filter info request. Region not found.", this);
          }
          return;
        }
        if (!lclRgn.isInitialized()) {
          if (logger.isDebugEnabled()) {
            logger.debug("{}; Failed to process filter info request. Region not yet initialized.",
                this);
          }
          return;
        }

        final DistributedRegion rgn = (DistributedRegion) lclRgn;
        FilterInfoMessage.send(dm, getSender(), this.processorId, rgn, null);
        sendFailureMessage = false;
      } catch (CancelException e) {
        if (logger.isDebugEnabled()) {
          logger.debug("{}; Cache Closed: aborting filter info request.", this);
        }
        rex = new ReplyException("Cache Closed: filter info request aborted.");
      } catch (VirtualMachineError err) {
        sendFailureMessage = false; // Don't try to respond!
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
        thr = t;
      } finally {
        if (sendFailureMessage) {
          // if we get here then send reply possibly with an exception
          if (thr != null) {
            rex = new ReplyException(thr);
          }

          if (rex == null) {
            rex = new ReplyException("Failed to process filter info request.");
          }
          FilterInfoMessage.send(dm, getSender(), this.processorId, (LocalRegion) lclRgn, rex);
        } // !success
      }
    }

    @Override
    public int getDSFID() {
      return REQUEST_FILTERINFO_MESSAGE;
    }

    @Override
    public void fromData(DataInput in,
        DeserializationContext context) throws IOException, ClassNotFoundException {
      super.fromData(in, context);
      this.regionPath = DataSerializer.readString(in);
      this.processorId = in.readInt();
    }

    @Override
    public void toData(DataOutput out,
        SerializationContext context) throws IOException {
      super.toData(out, context);
      DataSerializer.writeString(this.regionPath, out);
      out.writeInt(this.processorId);
    }

    @Override
    public String toString() {
      StringBuffer buff = new StringBuffer();
      String cname = getClass().getName().substring(getClass().getPackage().getName().length() + 1);
      buff.append(cname);
      buff.append("(region path='");
      buff.append(this.regionPath);
      buff.append("'; sender=");
      buff.append(getSender());
      buff.append("; processorId=");
      buff.append(this.processorId);
      buff.append(")");
      return buff.toString();
    }

  }

  /**
   * RequestRVV message processor.
   */
  class RequestRVVProcessor extends ReplyProcessor21 {
    // Set keysOfUnfinishedOps;
    RegionVersionVector received_rvv;

    public RequestRVVProcessor(final InternalDistributedSystem system,
        InternalDistributedMember member) {
      super(system, member);
    }

    public RequestRVVProcessor(InternalDistributedSystem system, Set members) {
      super(system, members);
    }

    @Override
    public void process(DistributionMessage msg) {
      final boolean isGiiDebugEnabled = logger.isTraceEnabled(LogMarker.INITIAL_IMAGE_VERBOSE);

      ReplyMessage reply = (ReplyMessage) msg;
      try {
        // if remote member has exception or shutdown, just try next recipient
        if (reply == null) {
          // if remote member is shutting down, the reply will be null
          if (isGiiDebugEnabled) {
            logger.trace(LogMarker.INITIAL_IMAGE_VERBOSE,
                "Did not received RVVReply from {}. Remote member might be down.",
                Arrays.toString(getMembers()));
          }
          return;
        }
        if (reply.getException() != null) {
          if (isGiiDebugEnabled) {
            logger.trace(LogMarker.INITIAL_IMAGE_VERBOSE, "Failed to get RVV from {} due to {}",
                reply.getSender(), reply.getException());
          }
          return;
        }
        if (reply instanceof RVVReplyMessage) {
          RVVReplyMessage rvv_reply = (RVVReplyMessage) reply;
          received_rvv = rvv_reply.versionVector;
        }
      } finally {
        if (received_rvv == null) {
          if (isGiiDebugEnabled) {
            logger.trace(LogMarker.INITIAL_IMAGE_VERBOSE,
                "{} did not send back rvv. Maybe it's non-persistent proxy region or remote region {} not found or not initialized. Nothing to do.",
                reply.getSender(), region.getFullPath());
          }
        }
        super.process(msg);
      }
    }

    @Override
    public String toString() {
      String cname = getClass().getName().substring(getClass().getPackage().getName().length() + 1);
      StringBuffer sb = new StringBuffer();
      sb.append("<" + cname + " " + this.getProcessorId());
      sb.append(" ,from " + membersToString() + ">");
      return sb.toString();
    }

    @Override
    protected boolean logMultipleExceptions() {
      return false;
    }
  }

  /**
   * RVVReplyMessage transmits the GII provider's RVV to requester
   *
   */
  public static class RVVReplyMessage extends ReplyMessage {

    @Override
    public boolean getInlineProcess() {
      return false;
    }

    RegionVersionVector versionVector;

    public RVVReplyMessage() {}

    private RVVReplyMessage(InternalDistributedMember mbr, int processorId,
        RegionVersionVector rvv) {
      setRecipient(mbr);
      setProcessorId(processorId);
      this.versionVector = rvv;
    }

    public static void send(DistributionManager dm, InternalDistributedMember dest, int processorId,
        RegionVersionVector rvv, ReplyException ex) {
      RVVReplyMessage msg = new RVVReplyMessage(dest, processorId, rvv);
      if (ex != null) {
        msg.setException(ex);
      }
      dm.putOutgoing(msg);
    }

    @Override
    public void toData(DataOutput dop,
        SerializationContext context) throws IOException {
      super.toData(dop, context);
      if (versionVector != null) {
        dop.writeBoolean(true);
        dop.writeBoolean(versionVector instanceof DiskRegionVersionVector);
        versionVector.toData(dop, context);
      } else {
        dop.writeBoolean(false);
      }
    }

    @Override
    public String toString() {
      String descr = super.toString();
      if (versionVector != null) {
        descr += "; versionVector="
            + (RegionVersionVector.DEBUG ? versionVector.fullToString() : versionVector);
      }
      return descr;
    }

    @Override
    public void fromData(DataInput dip,
        DeserializationContext context) throws IOException, ClassNotFoundException {
      super.fromData(dip, context);
      boolean has = dip.readBoolean();
      if (has) {
        boolean persistent = dip.readBoolean();
        versionVector = RegionVersionVector.create(persistent, dip);
      }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.geode.internal.serialization.DataSerializableFixedID#getDSFID()
     */
    @Override
    public int getDSFID() {
      return RVV_REPLY_MESSAGE;
    }
  }

  /**
   * This is the message thats sent to get RVV from GII provider.
   */
  public static class RequestRVVMessage extends DistributionMessage implements MessageWithReply {

    /**
     * Name of the region.
     */
    protected String regionPath;

    /**
     * Id of the {@link InitialImageOperation.ImageProcessor} that will handle the replies
     */
    protected int processorId;

    /**
     * If true, recipient should wait until fully initialized before returning data.
     */
    protected boolean targetReinitialized;

    @Override
    public int getProcessorId() {
      return this.processorId;
    }

    @Override
    public int getProcessorType() {
      return this.targetReinitialized ? OperationExecutors.WAITING_POOL_EXECUTOR
          : OperationExecutors.HIGH_PRIORITY_EXECUTOR;
    }

    @Override
    protected void process(final ClusterDistributionManager dm) {
      Throwable thr = null;
      boolean sendFailureMessage = true;
      LocalRegion lclRgn = null;
      ReplyException rex = null;
      try {
        Assert.assertTrue(this.regionPath != null, "Region path is null.");
        final DistributedRegion rgn =
            (DistributedRegion) getGIIRegion(dm, this.regionPath, this.targetReinitialized);
        if (rgn == null) {
          return;
        }
        if (!rgn.getGenerateVersionTag()) {
          if (logger.isDebugEnabled()) {
            logger.debug("{} non-persistent proxy region, nothing to do. Just reply", this);
          }
          // allow finally block to send a failure message
          RVVReplyMessage.send(dm, getSender(), processorId, null, null);
          sendFailureMessage = false;
          return;
        } else {
          RegionVersionVector rvv = rgn.getVersionVector().getCloneForTransmission();
          RVVReplyMessage.send(dm, getSender(), processorId, rvv, null);
          sendFailureMessage = false;
        }
      } catch (RegionDestroyedException e) {
        if (logger.isDebugEnabled()) {
          logger.debug("{}; Region destroyed: Request RVV aborting.", this);
        }
      } catch (CancelException e) {
        if (logger.isDebugEnabled()) {
          logger.debug("{}; Cache Closed: Request RVV aborting.", this);
        }
      } catch (VirtualMachineError err) {
        sendFailureMessage = false; // Don't try to respond!
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
        thr = t;
      } finally {
        if (sendFailureMessage) {
          // if we get here then send reply possibly with an exception
          if (thr != null) {
            rex = new ReplyException(thr);
          }

          RVVReplyMessage.send(dm, getSender(), processorId, null, rex);
        } // !success
      }
    }

    @Override
    public int getDSFID() {
      return REQUEST_RVV_MESSAGE;
    }

    @Override
    public void fromData(DataInput in,
        DeserializationContext context) throws IOException, ClassNotFoundException {
      super.fromData(in, context);
      this.regionPath = DataSerializer.readString(in);
      this.processorId = in.readInt();
      this.targetReinitialized = in.readBoolean();
    }

    @Override
    public void toData(DataOutput out,
        SerializationContext context) throws IOException {
      super.toData(out, context);
      DataSerializer.writeString(this.regionPath, out);
      out.writeInt(this.processorId);
      out.writeBoolean(this.targetReinitialized);
    }

    @Override
    public String toString() {
      StringBuffer buff = new StringBuffer();
      String cname = getClass().getName().substring(getClass().getPackage().getName().length() + 1);
      buff.append(cname);
      buff.append("(region path='");
      buff.append(this.regionPath);
      buff.append("'; sender=");
      buff.append(getSender());
      buff.append("; processorId=");
      buff.append(this.processorId);
      buff.append("; targetReinitalized=");
      buff.append(this.targetReinitialized);
      buff.append(")");
      return buff.toString();
    }

  }

  /**
   * This is the message thats sent to get RVV from GII provider.
   */
  public static class RequestSyncMessage extends HighPriorityDistributionMessage {

    /**
     * Name of the region.
     */
    protected String regionPath;

    /**
     * IDs that destroyed the region or crashed during GII that the GII recipient got events from
     * that weren't sent to this member
     */
    protected VersionSource[] lostVersionSources;


    @Override
    protected void process(final ClusterDistributionManager dm) {
      LocalRegion lclRgn = null;
      try {
        Assert.assertTrue(this.regionPath != null, "Region path is null.");
        final DistributedRegion rgn = (DistributedRegion) getGIIRegion(dm, this.regionPath, false);
        if (rgn != null) {
          if (logger.isDebugEnabled()) {
            logger.debug("synchronizing region with {}", Arrays.toString(lostVersionSources));
          }
          for (VersionSource lostSource : this.lostVersionSources) {
            InternalDistributedMember mbr = null;
            if (lostSource instanceof InternalDistributedMember) {
              mbr = (InternalDistributedMember) lostSource;
            }
            InitialImageOperation op = new InitialImageOperation(rgn, rgn.entries);
            op.synchronizeWith(getSender(), lostSource, mbr);
          }
        }
      } catch (RegionDestroyedException e) {
        if (logger.isDebugEnabled()) {
          logger.debug("{}; Region destroyed, nothing to do.", this);
        }
      } catch (CancelException e) {
        if (logger.isDebugEnabled()) {
          logger.debug("{}; Cache Closed, nothing to do.", this);
        }
      } catch (VirtualMachineError err) {
        SystemFailure.initiateFailure(err);
        throw err;
      } catch (Throwable t) {
        SystemFailure.checkFailure();
      }
    }

    @Override
    public int getDSFID() {
      return REQUEST_SYNC_MESSAGE;
    }

    @Override
    public void toData(DataOutput out,
        SerializationContext context) throws IOException {
      super.toData(out, context);
      DataSerializer.writeString(this.regionPath, out);
      out.writeBoolean(this.lostVersionSources[0] instanceof DiskStoreID);
      out.writeInt(this.lostVersionSources.length);
      for (VersionSource id : this.lostVersionSources) {
        id.writeEssentialData(out);
      }
    }

    @Override
    public void fromData(DataInput in,
        DeserializationContext context) throws IOException, ClassNotFoundException {
      super.fromData(in, context);
      this.regionPath = DataSerializer.readString(in);
      boolean persistentIDs = in.readBoolean();
      int len = in.readInt();
      this.lostVersionSources = new VersionSource[len];
      for (int i = 0; i < len; i++) {
        this.lostVersionSources[i] = (persistentIDs ? DiskStoreID.readEssentialData(in)
            : InternalDistributedMember.readEssentialData(in));
      }
    }

    @Override
    public String toString() {
      StringBuffer buff = new StringBuffer();
      String cname = getClass().getName().substring(getClass().getPackage().getName().length() + 1);
      buff.append(cname);
      buff.append("(region path='");
      buff.append(this.regionPath);
      buff.append("'; sender=");
      buff.append(getSender());
      buff.append("; sources=").append(Arrays.toString(this.lostVersionSources));
      buff.append(")");
      return buff.toString();
    }

  }

  public static class ImageReplyMessage extends ReplyMessage {
    /** the next entries in this chunk. Null means abort. */
    protected List entries;

    /** total number of series, duplicated in each message */
    protected int numSeries;

    /** the series this message belongs to (0-based) */
    protected int seriesNum;

    /** the number of this message within this series */
    protected int msgNum;

    /** whether this message is the last one in this series */
    protected boolean lastInSeries;

    private int flowControlId;

    private boolean isDeltaGII;

    /* The region version holder for the lost member. It's used for synchronizeWith() only */
    private boolean hasHolderToSend;
    private RegionVersionHolder holderToSend;

    /**
     * A map of the final GC versions. This sent with the last GII chunk to ensure that the GII
     * recipient's GC version matches that of the sender.
     */
    private Map<VersionSource, Long> gcVersions;

    /** the {@link Version} of the remote peer */
    private transient Version remoteVersion;

    /** The versions in which this message was modified */
    @Immutable
    private static final Version[] dsfidVersions = null;

    @Override
    public boolean getInlineProcess() {
      return false;
    }

    /**
     * @param entries the data to send back, if null then all the following parameters are ignored
     *        and any future replies from this member will be ignored, and the streaming of chunks
     *        is considered aborted by the receiver.
     * @param seriesNum series number for this message (0-based)
     * @param msgNum message number in this series (0-based)
     * @param numSeries total number of series
     * @param lastInSeries if this is the last message in this series
     * @param isDeltaGII if this message is for deltaGII
     * @param holderToSend higher version holder to sync for the lost member
     */
    public static void send(InternalDistributedMember recipient, int processorId,
        ReplyException exception, ClusterDistributionManager dm, List entries, int seriesNum,
        int msgNum, int numSeries, boolean lastInSeries, int flowControlId, boolean isDeltaGII,
        RegionVersionHolder holderToSend, Map<VersionSource, Long> gcVersions) {
      ImageReplyMessage m = new ImageReplyMessage();

      m.processorId = processorId;
      if (exception != null) {
        m.setException(exception);
        if (logger.isDebugEnabled()) {
          logger.debug("Replying with exception: {}", m, exception);
        }
      }
      m.setRecipient(recipient);
      m.entries = entries;
      m.seriesNum = seriesNum;
      m.msgNum = msgNum;
      m.numSeries = numSeries;
      m.lastInSeries = lastInSeries;
      m.flowControlId = flowControlId;
      m.isDeltaGII = isDeltaGII;
      m.holderToSend = holderToSend;
      m.hasHolderToSend = (holderToSend != null);
      m.gcVersions = gcVersions;
      dm.putOutgoing(m);
    }



    @Override
    public void process(DistributionManager dm, ReplyProcessor21 processor) {
      // We have to do this here, rather than in the reply processor code,
      // because the reply processor may be null.
      try {
        super.process(dm, processor);
      } finally {
        // TODO we probably should send an abort message to the sender
        // if we have aborted, but at the very least we need to keep
        // the permits going.
        if (this.flowControlId != 0) {
          FlowControlPermitMessage.send(dm, getSender(), this.flowControlId);
        }
      }
    }

    @Override
    protected Object clone() throws CloneNotSupportedException {
      // TODO Auto-generated method stub
      return super.clone();
    }

    @Override
    public int getDSFID() {
      return IMAGE_REPLY_MESSAGE;
    }

    @Override
    public void fromData(DataInput in,
        DeserializationContext context) throws IOException, ClassNotFoundException {
      super.fromData(in, context);
      ArrayList list = DataSerializer.readArrayList(in);
      Object listData = null;
      if (list != null /* fix bug 46874 */ && list.size() > 0) {
        listData = list.get(0);
      }
      if (listData instanceof InitialImageVersionedEntryList) {
        this.entries = (List) listData;
      } else {
        this.entries = list;
      }
      this.seriesNum = in.readInt();
      this.msgNum = in.readInt();
      this.numSeries = in.readInt();
      this.lastInSeries = in.readBoolean();
      this.flowControlId = in.readInt();
      this.remoteVersion = StaticSerialization.getVersionForDataStreamOrNull(in);
      this.isDeltaGII = in.readBoolean();
      this.hasHolderToSend = in.readBoolean();
      if (this.hasHolderToSend) {
        this.holderToSend = new RegionVersionHolder(in);
      }

      int gcVersionsLength = in.readShort();
      if (gcVersionsLength >= 0) {
        gcVersions = new HashMap<VersionSource, Long>(gcVersionsLength);
      }
      for (int i = 0; i < gcVersionsLength; i++) {
        VersionSource key = context.getDeserializer().readObject(in);
        long value = InternalDataSerializer.readUnsignedVL(in);
        gcVersions.put(key, value);
      }
    }

    @Override
    public void toData(DataOutput out,
        SerializationContext context) throws IOException {
      super.toData(out, context);
      if (this.entries instanceof InitialImageVersionedEntryList) {
        ArrayList list = new ArrayList(1);
        list.add(this.entries);
        DataSerializer.writeArrayList(list, out);
      } else {
        DataSerializer.writeArrayList((ArrayList) this.entries, out);
      }
      out.writeInt(this.seriesNum);
      out.writeInt(this.msgNum);
      out.writeInt(this.numSeries);
      out.writeBoolean(this.lastInSeries);
      out.writeInt(this.flowControlId);
      out.writeBoolean(this.isDeltaGII);
      out.writeBoolean(this.hasHolderToSend);
      if (this.hasHolderToSend) {
        InternalDataSerializer.invokeToData(this.holderToSend, out);
      }
      out.writeShort(gcVersions == null ? -1 : gcVersions.size());
      if (gcVersions != null) {
        for (Map.Entry<VersionSource, Long> entry : gcVersions.entrySet()) {
          context.getSerializer().writeObject(entry.getKey(), out);
          InternalDataSerializer.writeUnsignedVL(entry.getValue(), out);
        }
      }
    }

    @Override
    public String toString() {
      StringBuffer buff = new StringBuffer();
      String cname = getClass().getName().substring(getClass().getPackage().getName().length() + 1);
      buff.append(cname);
      buff.append("(processorId=");
      buff.append(this.processorId);
      buff.append(" from ");
      buff.append(this.getSender());
      ReplyException ex = this.getException();
      if (ex != null) {
        buff.append(" with exception ");
        buff.append(ex);
      }
      if (entries == null) {
        buff.append("; with no data - abort");
      } else {
        buff.append("; entryCount=");
        buff.append(this.entries.size());
        buff.append("; msgNum=");
        buff.append(this.msgNum);
        buff.append("; Series=");
        buff.append(this.seriesNum);
        buff.append("/");
        buff.append(this.numSeries);
        buff.append("; lastInSeries=");
        buff.append(this.lastInSeries);
        buff.append("; flowControlId=");
        buff.append(this.flowControlId);
        buff.append("; isDeltaGII=");
        buff.append(this.isDeltaGII);
      }
      if (this.remoteVersion != null) {
        buff.append("; remoteVersion=").append(this.remoteVersion);
      }
      if (this.holderToSend != null) {
        buff.append("; holderToSend=").append(this.holderToSend);
      }
      buff.append(")");
      return buff.toString();
    }

    @Override
    public Version[] getSerializationVersions() {
      return dsfidVersions;
    }
  }

  /**
   * Represents a key/value pair returned from a peer as part of an {@link InitialImageOperation}
   */
  public static class Entry implements DataSerializableFixedID {
    /**
     * key for this entry. Null if "end of chunk" marker entry
     */
    Object key;

    /**
     * value of this entry. Null when invalid or local invalid
     */
    Object value = null;

    /**
     * Characterizes this entry
     * <p>
     * Defaults to invalid, not serialized, not local invalid. The "invalid" flag is not used. When
     * invalid, localInvalid is false and the values is null.
     *
     * @see EntryBits
     */
    private byte entryBits = 0;

    /** lastModified is stored as "cache time milliseconds" */
    private long lastModified;

    /**
     * if the region has versioning enabled, we need to transfer the version with the entry
     */
    private VersionTag versionTag;

    /** Given local milliseconds, store as cache milliseconds */
    public void setLastModified(DistributionManager dm, long localMillis) {
      this.lastModified = localMillis;
    }

    /** Return lastModified as local milliseconds */
    public long getLastModified(DistributionManager dm) {
      return this.lastModified;
    }

    public boolean isSerialized() {
      return EntryBits.isSerialized(this.entryBits);
    }

    public void setSerialized(boolean isSerialized) {
      this.entryBits = EntryBits.setSerialized(this.entryBits, isSerialized);
    }

    public boolean isInvalid() {
      return (this.value == null) && !EntryBits.isLocalInvalid(this.entryBits);
    }

    public void setInvalid() {
      this.entryBits = EntryBits.setLocalInvalid(this.entryBits, false);
      this.value = null;
    }

    public boolean isLocalInvalid() {
      return EntryBits.isLocalInvalid(this.entryBits);
    }

    public void setLocalInvalid() {
      this.entryBits = EntryBits.setLocalInvalid(this.entryBits, true);
      this.value = null;
    }

    public void setTombstone() {
      this.entryBits = EntryBits.setTombstone(this.entryBits, true);
    }

    public Object getKey() {
      return key;
    }

    public VersionTag getVersionTag() {
      return versionTag;
    }

    public void setVersionTag(VersionTag tag) {
      this.versionTag = tag;
    }

    @Override
    public int getDSFID() {
      return IMAGE_ENTRY;
    }

    @Override
    public void toData(DataOutput out,
        SerializationContext context) throws IOException {
      out.writeByte(this.entryBits);
      byte flags = (this.versionTag != null) ? HAS_VERSION : 0;
      flags |= (this.versionTag instanceof DiskVersionTag) ? PERSISTENT_VERSION : 0;
      out.writeByte(flags);
      context.getSerializer().writeObject(this.key, out);
      if (!EntryBits.isTombstone(this.entryBits)) {
        DataSerializer.writeObjectAsByteArray(this.value, out);
      }
      out.writeLong(this.lastModified);
      if (this.versionTag != null) {
        InternalDataSerializer.invokeToData(this.versionTag, out);
      }
    }

    static final byte HAS_VERSION = 0x01;
    static final byte PERSISTENT_VERSION = 0x02;

    @Override
    public void fromData(DataInput in,
        DeserializationContext context) throws IOException, ClassNotFoundException {
      this.entryBits = in.readByte();
      byte flags = in.readByte();
      this.key = context.getDeserializer().readObject(in);

      if (EntryBits.isTombstone(this.entryBits)) {
        this.value = Token.TOMBSTONE;
      } else {
        this.value = DataSerializer.readByteArray(in);
      }
      this.lastModified = in.readLong();
      if ((flags & HAS_VERSION) != 0) {
        // note that null IDs must be later replaced with the image provider's ID
        this.versionTag = VersionTag.create((flags & PERSISTENT_VERSION) != 0, in);
      }
    }

    public int calcSerializedSize() {
      NullDataOutputStream dos = new NullDataOutputStream();
      try {
        toData(dos, InternalDataSerializer.createSerializationContext(dos));
        return dos.size();
      } catch (IOException ex) {
        RuntimeException ex2 = new IllegalArgumentException(
            "Could not calculate size of object");
        ex2.initCause(ex);
        throw ex2;
      }
    }

    @Override
    public String toString() {
      return "GIIEntry[key=" + this.key + "]";
    }

    @Override
    public Version[] getSerializationVersions() {
      return null;
    }

    public Object getValue() {
      return value;
    }

    public void setValue(Object value) {
      this.value = value;
    }
  }

  /**
   * List to hold versioned entries for InitialImageOperation requested from a member.
   *
   */
  public static class InitialImageVersionedEntryList extends ArrayList<Entry>
      implements DataSerializableFixedID, Externalizable {

    /**
     * if the region has versioning enabled, we need to transfer the version with the entry
     */
    List<VersionTag> versionTags;

    /**
     * InitialImageOperation.Entry list
     *
     */
    // List<Entry> entries;

    boolean isRegionVersioned = false;

    public InitialImageVersionedEntryList() {
      super();
      this.versionTags = new ArrayList();
    }

    public InitialImageVersionedEntryList(boolean isRegionVersioned, int size) {
      super(size);
      this.isRegionVersioned = isRegionVersioned;
      if (isRegionVersioned) {
        this.versionTags = new ArrayList(size);
      } else {
        this.versionTags = Collections.EMPTY_LIST;
      }
    }

    public static InitialImageVersionedEntryList create(DataInput in)
        throws IOException, ClassNotFoundException {
      InitialImageVersionedEntryList newList = new InitialImageVersionedEntryList();
      InternalDataSerializer.invokeFromData(newList, in);
      return newList;
    }

    @Override
    public boolean add(Entry entry) {
      VersionTag tag = entry.getVersionTag();
      // Remove duplicate before serialization
      entry.setVersionTag(null);
      return addEntryAndVersion(entry, tag);
    }

    private boolean addEntryAndVersion(Entry entry, VersionTag versionTag) {

      // version tag can be null if only keys are sent in InitialImage.
      if (this.isRegionVersioned && versionTag != null) {
        int tagsSize = this.versionTags.size();
        if (tagsSize != super.size()) {
          // this should not happen - either all or none of the entries should have tags
          throw new InternalGemFireException();
        }
        this.versionTags.add(versionTag);
      }

      // Add entry without version tag in top-level ArrayList.
      return super.add(entry);
    }

    /*
     * This should be called only on receiving side only as this call resets the
     * InitialImageOperation.Entry with version tag.
     */
    @Override
    public Entry get(int index) {
      Entry entry = super.get(index);

      VersionTag tag = getVersionTag(index);
      entry.setVersionTag(tag);

      return entry;
    }

    private VersionTag<VersionSource> getVersionTag(int index) {
      VersionTag tag = null;
      if (isRegionVersioned && this.versionTags != null) {
        tag = versionTags.get(index);
      }
      return tag;
    }

    @Override
    public int size() {
      // Sanity check for entries size and versions size.
      if (isRegionVersioned) {
        if (super.size() != versionTags.size()) {
          throw new InternalGemFireException();
        }
      }
      return super.size();
    }

    @Override
    public void clear() {
      super.clear();
      this.versionTags.clear();
    }

    /**
     *
     * @return whether the source region had concurrency checks enabled
     */
    public boolean isRegionVersioned() {
      return this.isRegionVersioned;
    }

    /**
     * replace null membership IDs in version tags with the given member ID. VersionTags received
     * from a server may have null IDs because they were operations performed by that server. We
     * transmit them as nulls to cut costs, but have to do the swap on the receiving end (in the
     * client)
     *
     */
    public void replaceNullIDs(DistributedMember sender) {
      for (VersionTag versionTag : versionTags) {
        if (versionTag != null) {
          versionTag.replaceNullIDs((InternalDistributedMember) sender);
        }
      }
    }

    @Override
    public int getDSFID() {
      return DataSerializableFixedID.INITIAL_IMAGE_VERSIONED_OBJECT_LIST;
    }

    static final byte FLAG_NULL_TAG = 0;
    static final byte FLAG_FULL_TAG = 1;
    static final byte FLAG_TAG_WITH_NEW_ID = 2;
    static final byte FLAG_TAG_WITH_NUMBER_ID = 3;

    @Override
    public void toData(DataOutput out,
        SerializationContext context) throws IOException {
      int flags = 0;
      boolean hasEntries = false;
      boolean hasTags = false;

      if (!super.isEmpty()) {
        flags |= 0x02;
        hasEntries = true;
      }
      if (this.versionTags.size() > 0) {
        flags |= 0x04;
        hasTags = true;
        for (VersionTag tag : this.versionTags) {
          if (tag != null) {
            if (tag instanceof DiskVersionTag) {
              flags |= 0x20;
            }
            break;
          }
        }
      }
      if (this.isRegionVersioned) {
        flags |= 0x08;
      }

      if (logger.isTraceEnabled(LogMarker.INITIAL_IMAGE_VERSIONED_VERBOSE)) {
        logger.trace(LogMarker.INITIAL_IMAGE_VERSIONED_VERBOSE, "serializing {} with flags 0x{}",
            this, Integer.toHexString(flags));
      }

      out.writeByte(flags);

      if (hasEntries) {
        InternalDataSerializer.writeUnsignedVL(super.size(), out);
        for (int i = 0; i < super.size(); i++) {
          context.getSerializer().writeObject(super.get(i), out);
        }
      }
      if (hasTags) {
        InternalDataSerializer.writeUnsignedVL(this.versionTags.size(), out);
        Map<VersionSource, Integer> ids = new HashMap<VersionSource, Integer>(versionTags.size());
        int idCount = 0;
        for (VersionTag tag : this.versionTags) {
          if (tag == null) {
            out.writeByte(FLAG_NULL_TAG);
          } else {
            VersionSource id = tag.getMemberID();
            if (id == null) {
              out.writeByte(FLAG_FULL_TAG);
              InternalDataSerializer.invokeToData(tag, out);
            } else {
              Integer idNumber = ids.get(id);
              if (idNumber == null) {
                out.writeByte(FLAG_TAG_WITH_NEW_ID);
                idNumber = Integer.valueOf(idCount++);
                ids.put(id, idNumber);
                InternalDataSerializer.invokeToData(tag, out);
              } else {
                out.writeByte(FLAG_TAG_WITH_NUMBER_ID);
                tag.toData(out, false);
                tag.setMemberID(id);
                InternalDataSerializer.writeUnsignedVL(idNumber, out);
              }
            }
          }
        }
      }
    }

    @Override
    public void fromData(DataInput in,
        DeserializationContext context) throws IOException, ClassNotFoundException {
      final boolean isGiiVersionEntryDebugEnabled =
          logger.isTraceEnabled(LogMarker.INITIAL_IMAGE_VERSIONED_VERBOSE);

      int flags = in.readByte();
      boolean hasEntries = (flags & 0x02) == 0x02;
      boolean hasTags = (flags & 0x04) == 0x04;
      this.isRegionVersioned = (flags & 0x08) == 0x08;
      boolean persistent = (flags & 0x20) == 0x20;

      if (isGiiVersionEntryDebugEnabled) {
        logger.trace(LogMarker.INITIAL_IMAGE_VERSIONED_VERBOSE,
            "deserializing a InitialImageVersionedObjectList with flags 0x{}",
            Integer.toHexString(flags));
      }
      if (hasEntries) {
        int size = (int) InternalDataSerializer.readUnsignedVL(in);
        if (isGiiVersionEntryDebugEnabled) {
          logger.trace(LogMarker.INITIAL_IMAGE_VERSIONED_VERBOSE, "reading {} keys", size);
        }
        for (int i = 0; i < size; i++) {
          super.add((Entry) context.getDeserializer().readObject(in));
        }
      }

      if (hasTags) {
        int size = (int) InternalDataSerializer.readUnsignedVL(in);
        if (isGiiVersionEntryDebugEnabled) {
          logger.trace(LogMarker.INITIAL_IMAGE_VERSIONED_VERBOSE, "reading {} version tags", size);
        }
        this.versionTags = new ArrayList<VersionTag>(size);
        List<VersionSource> ids = new ArrayList<VersionSource>(size);
        for (int i = 0; i < size; i++) {
          byte entryType = in.readByte();
          switch (entryType) {
            case FLAG_NULL_TAG:
              this.versionTags.add(null);
              break;
            case FLAG_FULL_TAG:
              this.versionTags.add(VersionTag.create(persistent, in));
              break;
            case FLAG_TAG_WITH_NEW_ID:
              VersionTag tag = VersionTag.create(persistent, in);
              ids.add(tag.getMemberID());
              this.versionTags.add(tag);
              break;
            case FLAG_TAG_WITH_NUMBER_ID:
              tag = VersionTag.create(persistent, in);
              int idNumber = (int) InternalDataSerializer.readUnsignedVL(in);
              tag.setMemberID(ids.get(idNumber));
              this.versionTags.add(tag);
              break;
          }
        }
      } else {
        this.versionTags = new ArrayList<VersionTag>();
      }
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
      toData(out, InternalDataSerializer.createSerializationContext(out));
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      fromData(in, InternalDataSerializer.createDeserializationContext(in));
    }

    @Override
    public Version[] getSerializationVersions() {
      return null;
    }
  }

  /**
   * EventStateMessage transmits the cache's memberId:threadId sequence# information so that a cache
   * receiving an initial image will know what events that image represents.
   *
   */
  public static class RegionStateMessage extends ReplyMessage {

    // event state is processed in-line to ensure it is applied before
    // the initial image state is received
    @Override
    public boolean getInlineProcess() {
      return true;
    }

    Map eventState;
    private boolean isHARegion;
    RegionVersionVector versionVector;

    public RegionStateMessage() {}

    private RegionStateMessage(InternalDistributedMember mbr, int processorId, Map eventState,
        boolean isHARegion) {
      setRecipient(mbr);
      setProcessorId(processorId);
      this.eventState = eventState;
      this.isHARegion = isHARegion;
    }

    private RegionStateMessage(InternalDistributedMember mbr, int processorId,
        RegionVersionVector rvv, boolean isHARegion) {
      setRecipient(mbr);
      setProcessorId(processorId);
      this.versionVector = rvv;
      this.isHARegion = isHARegion;
    }

    public static void send(DistributionManager dm, InternalDistributedMember dest, int processorId,
        Map<? extends DataSerializable, ? extends DataSerializable> eventState,
        boolean isHARegion) {
      RegionStateMessage msg = new RegionStateMessage(dest, processorId, eventState, isHARegion);
      msg.setSender(dm.getId()); // for EventStateHelper.dataSerialize
      dm.putOutgoing(msg);
    }

    public static void send(DistributionManager dm, InternalDistributedMember dest, int processorId,
        RegionVersionVector rvv, boolean isHARegion) {
      RegionStateMessage msg = new RegionStateMessage(dest, processorId, rvv, isHARegion);
      msg.setSender(dm.getId()); // for EventStateHelper.dataSerialize
      dm.putOutgoing(msg);
    }

    @Override
    public void toData(DataOutput dop,
        SerializationContext context) throws IOException {
      super.toData(dop, context);
      dop.writeBoolean(isHARegion);
      if (eventState != null) {
        dop.writeBoolean(true);
        EventStateHelper.dataSerialize(dop, eventState, isHARegion, getSender());
      } else {
        dop.writeBoolean(false);
      }
      if (versionVector != null) {
        dop.writeBoolean(true);
        dop.writeBoolean(versionVector instanceof DiskRegionVersionVector);
        InternalDataSerializer.invokeToData(versionVector, dop);
      } else {
        dop.writeBoolean(false);
      }
    }


    @Override
    public String toString() {
      String descr = super.toString();
      if (eventState != null) {
        descr += "; eventCount=" + eventState.size();
      }
      if (versionVector != null) {
        descr += "; versionVector="
            + (RegionVersionVector.DEBUG ? versionVector.fullToString() : versionVector);
      }
      return descr;
    }

    @Override
    public void fromData(DataInput dip,
        DeserializationContext context) throws IOException, ClassNotFoundException {
      super.fromData(dip, context);
      isHARegion = dip.readBoolean();
      boolean has = dip.readBoolean();
      if (has) {
        eventState = EventStateHelper.deDataSerialize(dip, isHARegion);
      }
      has = dip.readBoolean();
      if (has) {
        boolean persistent = dip.readBoolean();
        versionVector = RegionVersionVector.create(persistent, dip);
      }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.geode.internal.serialization.DataSerializableFixedID#getDSFID()
     */
    @Override
    public int getDSFID() {
      return REGION_STATE_MESSAGE;
    }
  }

  /**
   * This Message is sent as response to RequestFilterInfo. The filters registered by the client
   * owning the HARegion is sent as part of this message.
   */
  public static class FilterInfoMessage extends ReplyMessage {

    private LocalRegion haRegion;

    private Map emptyRegionMap;

    static class InterestMaps {
      Map<String, String> allKeys;

      Map<String, String> allKeysInv;

      Map<String, Set> keysOfInterest;

      Map<String, Set> keysOfInterestInv;

      Map<String, Set> patternsOfInterest;

      Map<String, Set> patternsOfInterestInv;

      Map<String, Set> filtersOfInterest;

      Map<String, Set> filtersOfInterestInv;
    }

    private final InterestMaps interestMaps[] =
        new InterestMaps[] {new InterestMaps(), new InterestMaps()};

    /** index values for interestMaps[] */

    static final int NON_DURABLE = 0;
    static final int DURABLE = 1;

    private Map<String, ServerCQ> cqs;


    @Override
    public boolean getInlineProcess() {
      return false;
    }

    public FilterInfoMessage() {}

    private FilterInfoMessage(InternalDistributedMember mbr, int processorId,
        LocalRegion haRegion) {
      setRecipient(mbr);
      setProcessorId(processorId);
      this.haRegion = haRegion;
    }

    /**
     * Collects all the filters registered by this client on regions.
     */
    public void fillInFilterInfo() {
      LocalRegion haReg = this.haRegion;

      if (haReg == null || haReg.getName() == null) {
        throw new ReplyException("HARegion for the proxy is Null.");
      }
      InternalCache cache = haReg.getCache();

      CacheClientNotifier ccn = CacheClientNotifier.getInstance();
      if (ccn == null || ccn.getHaContainer() == null) {
        logger.info("HA Container not found during HA Region GII for {}", haReg);
        return;
      }

      CacheClientProxy clientProxy = null;
      ClientProxyMembershipID clientID =
          ((HAContainerWrapper) ccn.getHaContainer()).getProxyID(haReg.getName());
      if (clientID == null) {
        throw new ReplyException("Client proxy ID not found for queue " + haReg.getName());
      }
      clientProxy = ccn.getClientProxy(clientID);
      if (clientProxy == null) {
        throw new ReplyException("Client proxy not found for queue " + haReg.getName());
      }

      if (logger.isDebugEnabled()) {
        logger.debug("Gathering interest information for {}", clientProxy);
      }

      this.emptyRegionMap = clientProxy.getRegionsWithEmptyDataPolicy();
      Set<String> regions = clientProxy.getInterestRegisteredRegions();

      // Get Filter Info from all regions.
      for (String rName : regions) {
        LocalRegion r = (LocalRegion) cache.getRegion(rName);
        if (r == null) {
          continue;
        }
        if (logger.isDebugEnabled()) {
          logger.debug("Finding interest on region :{} for Client(ID) :{}", r.getName(), clientID);
        }
        FilterProfile pf = r.getFilterProfile();

        getInterestMaps(pf, rName, NON_DURABLE, clientID);

        if (clientID.isDurable()) {
          getInterestMaps(pf, rName, DURABLE, clientID.getDurableId());
        }


      }

      // COllect CQ info.
      CqService cqService = cache.getCqService(); // fix for bug 43139
      if (cqService != null) {
        try {
          List<ServerCQ> cqsList = cqService.getAllClientCqs(clientID);
          if (!cqsList.isEmpty()) {
            this.cqs = new HashMap<String, ServerCQ>();
            for (ServerCQ cq : cqsList) {
              this.cqs.put(cq.getName(), cq);
            }
          }
        } catch (Exception ex) {
          if (logger.isDebugEnabled()) {
            logger.debug("{}: Failed to get CQ info. {}", this, ex.getMessage(), ex);
          }
        }
      }
      if (logger.isDebugEnabled()) {
        logger.debug("Number of filters filled : {}", this);
      }
    }

    private void getInterestMaps(FilterProfile pf, String rName, int mapIndex, Object interestID) {
      try {
        // Check if interested in all keys.
        boolean all = pf.isInterestedInAllKeys(interestID);
        if (all) {
          if (this.interestMaps[mapIndex].allKeys == null) {
            this.interestMaps[mapIndex].allKeys = new HashMap<String, String>();
          }
          this.interestMaps[mapIndex].allKeys.put(rName, ".*");
        }

        // Check if interested in all keys, for which updates are sent as invalidates.
        all = pf.isInterestedInAllKeysInv(interestID);
        if (all) {
          if (this.interestMaps[mapIndex].allKeysInv == null) {
            this.interestMaps[mapIndex].allKeysInv = new HashMap<String, String>();
          }
          this.interestMaps[mapIndex].allKeysInv.put(rName, ".*");
        }

        // Collect interest of type keys.
        Set keys = pf.getKeysOfInterest(interestID);
        if (keys != null) {
          if (this.interestMaps[mapIndex].keysOfInterest == null) {
            this.interestMaps[mapIndex].keysOfInterest = new HashMap<String, Set>();
          }
          this.interestMaps[mapIndex].keysOfInterest.put(rName, keys);
        }

        // Collect interest of type keys, for which updates are sent as invalidates.
        keys = pf.getKeysOfInterestInv(interestID);
        if (keys != null) {
          if (this.interestMaps[mapIndex].keysOfInterestInv == null) {
            this.interestMaps[mapIndex].keysOfInterestInv = new HashMap<String, Set>();
          }
          this.interestMaps[mapIndex].keysOfInterestInv.put(rName, keys);
        }

        // Collect interest of type expression.
        keys = pf.getPatternsOfInterest(interestID);
        if (keys != null) {
          if (this.interestMaps[mapIndex].patternsOfInterest == null) {
            this.interestMaps[mapIndex].patternsOfInterest = new HashMap<String, Set>();
          }
          this.interestMaps[mapIndex].patternsOfInterest.put(rName, keys);
        }

        // Collect interest of type expression, for which updates are sent as invalidates.
        keys = pf.getPatternsOfInterestInv(interestID);
        if (keys != null) {
          if (this.interestMaps[mapIndex].patternsOfInterestInv == null) {
            this.interestMaps[mapIndex].patternsOfInterestInv = new HashMap<String, Set>();
          }
          this.interestMaps[mapIndex].patternsOfInterestInv.put(rName, keys);
        }

        // Collect interest of type filter.
        keys = pf.getFiltersOfInterest(interestID);
        if (keys != null) {
          if (this.interestMaps[mapIndex].filtersOfInterest == null) {
            this.interestMaps[mapIndex].filtersOfInterest = new HashMap<String, Set>();
          }
          this.interestMaps[mapIndex].filtersOfInterest.put(rName, keys);
        }

        // Collect interest of type filter, for which updates are sent as invalidates.
        keys = pf.getFiltersOfInterestInv(interestID);
        if (keys != null) {
          if (this.interestMaps[mapIndex].filtersOfInterestInv == null) {
            this.interestMaps[mapIndex].filtersOfInterestInv = new HashMap<String, Set>();
          }
          this.interestMaps[mapIndex].filtersOfInterestInv.put(rName, keys);
        }
      } catch (Exception ex) {
        if (logger.isDebugEnabled()) {
          logger.debug("{}: Failed to get Register interest info for region : {}", this, rName, ex);
        }
      }
    }

    /**
     * Registers the filters associated with this client on current cache region.
     *
     */
    public void registerFilters(LocalRegion region) {
      CacheClientNotifier ccn = CacheClientNotifier.getInstance();

      CacheClientProxy proxy;
      try {
        if (ccn == null || ccn.getHaContainer() == null) {
          logger.info(
              "Found null cache client notifier. Failed to register Filters during HARegion GII. Region :{}",
              region.getName());
          return;
        }
        proxy = ((HAContainerWrapper) ccn.getHaContainer()).getProxy(region.getName());
      } catch (Exception ex) {
        logger.info(
            "Unable to obtain the client proxy. Failed to register Filters during HARegion GII. Region :{}, {}",
            region.getName(), ex.getMessage(), ex);
        return;
      }

      if (proxy == null) {
        logger.info(
            "Found null client proxy. Failed to register Filters during HARegion GII. Region :{}, HaContainer :{}",
            region.getName(), ccn.getHaContainer());
        return;
      }

      registerFilters(region, proxy, false);
      if (proxy.getProxyID().isDurable()) {
        registerFilters(region, proxy, true);
      }

      // Register CQs.
      if (this.cqs != null && !this.cqs.isEmpty()) {
        try {
          CqService cqService =
              ((DefaultQueryService) (region.getCache().getQueryService())).getCqService();

          for (Map.Entry<String, ServerCQ> e : this.cqs.entrySet()) {
            ServerCQ cq = e.getValue();
            try {
              // Passing regionDataPolicy as -1, the actual value is
              // obtained in executeCQ once the CQs base region name is
              // found.
              cqService.executeCq(e.getKey(), cq.getQueryString(),
                  ((CqStateImpl) cq.getState()).getState(), proxy.getProxyID(), ccn, cq.isDurable(),
                  true, -1, this.emptyRegionMap);
            } catch (Exception ex) {
              logger.info("Failed to register CQ during HARegion GII. CQ: {} {}", e.getKey(),
                  ex.getMessage(), ex);
            }
          }
        } catch (Exception ex) {
          logger.info("Failed to get CqService for CQ registration during HARegion GII. {}",
              ex.getMessage(), ex);
        }
      }
    }

    private void registerFilters(LocalRegion region, CacheClientProxy proxy, boolean durable) {
      CacheClientNotifier ccn = CacheClientNotifier.getInstance();
      Set<String> regionsWithInterest = new HashSet<String>();

      int mapIndex = durable ? DURABLE : NON_DURABLE;

      // Register interest for all keys.
      try {
        registerInterestKeys(this.interestMaps[mapIndex].allKeys, true, region, ccn, proxy, durable,
            false, InterestType.REGULAR_EXPRESSION, regionsWithInterest);
      } catch (Exception ex) {
        logger.info("Failed to register interest of type keys during HARegion GII. {}",
            ex.getMessage(), ex);
      }

      // Register interest for all keys, for which updates are sent as invalidates.
      try {
        registerInterestKeys(this.interestMaps[mapIndex].allKeysInv, true, region, ccn, proxy,
            durable, false, InterestType.REGULAR_EXPRESSION, regionsWithInterest);
      } catch (Exception ex) {
        logger.info("Failed to register interest of type keys during HARegion GII. {}",
            ex.getMessage(), ex);
      }

      // Register interest of type keys.
      try {
        registerInterestKeys(this.interestMaps[mapIndex].keysOfInterest, false, region, ccn, proxy,
            durable, false, InterestType.KEY, regionsWithInterest);
      } catch (Exception ex) {
        logger.info("Failed to register interest of type keys during HARegion GII. {}",
            ex.getMessage(), ex);
      }

      // Register interest of type keys, for which updates are sent as invalidates.
      try {
        registerInterestKeys(this.interestMaps[mapIndex].keysOfInterestInv, false, region, ccn,
            proxy, durable, true, InterestType.KEY, regionsWithInterest);
      } catch (Exception ex) {
        logger.info(
            "Failed to register interest of type keys for invalidates during HARegion GII. {}",
            ex.getMessage(), ex);
      }

      // Register interest of type expression.
      try {
        registerInterestKeys(this.interestMaps[mapIndex].patternsOfInterest, false, region, ccn,
            proxy, durable, false, InterestType.REGULAR_EXPRESSION, regionsWithInterest);
      } catch (Exception ex) {
        logger.info("Failed to register interest of type expression during HARegion GII. {}",
            ex.getMessage(), ex);
      }

      // Register interest of type expression, for which updates are sent as invalidates.
      try {
        registerInterestKeys(this.interestMaps[mapIndex].patternsOfInterestInv, false, region, ccn,
            proxy, durable, true, InterestType.REGULAR_EXPRESSION, regionsWithInterest);
      } catch (Exception ex) {
        logger.info(
            "Failed to register interest of type expression for invalidates during HARegion GII. {}",
            ex.getMessage(), ex);
      }

      // Register interest of type expression.
      try {
        registerInterestKeys(this.interestMaps[mapIndex].filtersOfInterest, false, region, ccn,
            proxy, durable, false, InterestType.FILTER_CLASS, regionsWithInterest);
      } catch (Exception ex) {
        logger.info("Failed to register interest of type filter during HARegion GII. {}",
            ex.getMessage(), ex);
      }

      // Register interest of type expression, for which updates are sent as invalidates.
      try {
        registerInterestKeys(this.interestMaps[mapIndex].filtersOfInterestInv, false, region, ccn,
            proxy, durable, true, InterestType.FILTER_CLASS, regionsWithInterest);
      } catch (Exception ex) {
        logger.info(
            "Failed to register interest of type filter for invalidates during HARegion GII. {}",
            ex.getMessage(), ex);
      }

      /*
       * now that interest is in place we need to flush operations to the image provider
       */
      for (String regionName : regionsWithInterest) {
        proxy.flushForInterestRegistration(regionName, getSender());
      }
    }

    /**
     * Helper method to register the filters.
     */
    private void registerInterestKeys(Map regionKeys, boolean allKey, LocalRegion region,
        CacheClientNotifier ccn, CacheClientProxy proxy, boolean isDurable,
        boolean updatesAsInvalidates, int interestType, Set<String> regionsWithInterest)
        throws IOException {

      final boolean isDebugEnabled = logger.isDebugEnabled();

      if (regionKeys != null) {
        Iterator iter = regionKeys.entrySet().iterator();
        while (iter.hasNext()) {
          Map.Entry e = (Map.Entry) iter.next();
          String regionName = (String) e.getKey();
          if (region.getCache().getRegion(regionName) == null) {
            if (isDebugEnabled) {
              logger.debug("Unable to register interests. Region not found :{}" + regionName);
            }
          } else {
            boolean manageEmptyRegions = false;
            if (this.emptyRegionMap != null) {
              manageEmptyRegions = this.emptyRegionMap.containsKey(regionName);
            }
            regionsWithInterest.add(regionName);
            if (allKey) {
              ccn.registerClientInterest(regionName, e.getValue(), proxy.getProxyID(), interestType,
                  isDurable, updatesAsInvalidates, manageEmptyRegions, 0, false);
            } else if (InterestType.REGULAR_EXPRESSION == interestType) {
              for (Iterator i = ((Set) e.getValue()).iterator(); i.hasNext();) {
                ccn.registerClientInterest(regionName, (String) i.next(), proxy.getProxyID(),
                    interestType, isDurable, updatesAsInvalidates, manageEmptyRegions, 0, false);
              }
            } else {
              ccn.registerClientInterest(regionName, new ArrayList((Set) e.getValue()),
                  proxy.getProxyID(), isDurable, updatesAsInvalidates, manageEmptyRegions,
                  interestType, false);
            }
          }
        }
      }
    }

    public static void send(DistributionManager dm, InternalDistributedMember dest, int processorId,
        LocalRegion rgn, ReplyException ex) {
      FilterInfoMessage msg = new FilterInfoMessage(dest, processorId, rgn);
      if (ex != null) {
        msg.setException(ex);
      } else {
        try {
          msg.fillInFilterInfo();
        } catch (ReplyException e) {
          msg.setException(e);
        }
      }
      dm.putOutgoing(msg);
    }

    @Override
    public String toString() {
      String descr = super.toString();
      descr += "; NON_DURABLE allKeys="
          + (this.interestMaps[NON_DURABLE].allKeys != null
              ? this.interestMaps[NON_DURABLE].allKeys.size() : 0)
          + "; allKeysInv="
          + (this.interestMaps[NON_DURABLE].allKeysInv != null
              ? this.interestMaps[NON_DURABLE].allKeysInv.size() : 0)
          + "; keysOfInterest="
          + (this.interestMaps[NON_DURABLE].keysOfInterest != null
              ? this.interestMaps[NON_DURABLE].keysOfInterest.size() : 0)
          + "; keysOfInterestInv="
          + (this.interestMaps[NON_DURABLE].keysOfInterestInv != null
              ? this.interestMaps[NON_DURABLE].keysOfInterestInv.size() : 0)
          + "; patternsOfInterest="
          + (this.interestMaps[NON_DURABLE].patternsOfInterest != null
              ? this.interestMaps[NON_DURABLE].patternsOfInterest.size() : 0)
          + "; patternsOfInterestInv="
          + (this.interestMaps[NON_DURABLE].patternsOfInterestInv != null
              ? this.interestMaps[NON_DURABLE].patternsOfInterestInv.size() : 0)
          + "; filtersOfInterest="
          + (this.interestMaps[NON_DURABLE].filtersOfInterest != null
              ? this.interestMaps[NON_DURABLE].filtersOfInterest.size() : 0)
          + "; filtersOfInterestInv=" + (this.interestMaps[NON_DURABLE].filtersOfInterestInv != null
              ? this.interestMaps[NON_DURABLE].filtersOfInterestInv.size() : 0);
      descr += "; DURABLE allKeys="
          + (this.interestMaps[DURABLE].allKeys != null ? this.interestMaps[DURABLE].allKeys.size()
              : 0)
          + "; allKeysInv="
          + (this.interestMaps[DURABLE].allKeysInv != null
              ? this.interestMaps[DURABLE].allKeysInv.size() : 0)
          + "; keysOfInterest="
          + (this.interestMaps[DURABLE].keysOfInterest != null
              ? this.interestMaps[DURABLE].keysOfInterest.size() : 0)
          + "; keysOfInterestInv="
          + (this.interestMaps[DURABLE].keysOfInterestInv != null
              ? this.interestMaps[DURABLE].keysOfInterestInv.size() : 0)
          + "; patternsOfInterest="
          + (this.interestMaps[DURABLE].patternsOfInterest != null
              ? this.interestMaps[DURABLE].patternsOfInterest.size() : 0)
          + "; patternsOfInterestInv="
          + (this.interestMaps[DURABLE].patternsOfInterestInv != null
              ? this.interestMaps[DURABLE].patternsOfInterestInv.size() : 0)
          + "; filtersOfInterest="
          + (this.interestMaps[DURABLE].filtersOfInterest != null
              ? this.interestMaps[DURABLE].filtersOfInterest.size() : 0)
          + "; filtersOfInterestInv=" + (this.interestMaps[DURABLE].filtersOfInterestInv != null
              ? this.interestMaps[DURABLE].filtersOfInterestInv.size() : 0);
      descr += "; cqs=" + (this.cqs != null ? this.cqs.size() : 0);
      return descr;
    }

    public boolean isEmpty() {
      if (this.interestMaps[NON_DURABLE].keysOfInterest != null
          || this.interestMaps[NON_DURABLE].keysOfInterestInv != null
          || this.interestMaps[NON_DURABLE].patternsOfInterest != null
          || this.interestMaps[NON_DURABLE].patternsOfInterestInv != null
          || this.interestMaps[NON_DURABLE].filtersOfInterest != null
          || this.interestMaps[NON_DURABLE].filtersOfInterestInv != null
          || this.interestMaps[DURABLE].patternsOfInterest != null
          || this.interestMaps[DURABLE].patternsOfInterestInv != null
          || this.interestMaps[DURABLE].filtersOfInterest != null
          || this.interestMaps[DURABLE].filtersOfInterestInv != null || this.cqs != null) {
        return false;
      }
      return true;
    }

    @Override
    public void toData(DataOutput dop,
        SerializationContext context) throws IOException {
      super.toData(dop, context);
      // DataSerializer.writeString(this.haRegion.getName(), dop);
      DataSerializer.writeHashMap((HashMap) this.emptyRegionMap, dop);
      // Write interest info.
      DataSerializer.writeHashMap((HashMap) this.interestMaps[NON_DURABLE].allKeys, dop);
      DataSerializer.writeHashMap((HashMap) this.interestMaps[NON_DURABLE].allKeysInv, dop);
      DataSerializer.writeHashMap((HashMap) this.interestMaps[NON_DURABLE].keysOfInterest, dop);
      DataSerializer.writeHashMap((HashMap) this.interestMaps[NON_DURABLE].keysOfInterestInv, dop);
      DataSerializer.writeHashMap((HashMap) this.interestMaps[NON_DURABLE].patternsOfInterest, dop);
      DataSerializer.writeHashMap((HashMap) this.interestMaps[NON_DURABLE].patternsOfInterestInv,
          dop);
      DataSerializer.writeHashMap((HashMap) this.interestMaps[NON_DURABLE].filtersOfInterest, dop);
      DataSerializer.writeHashMap((HashMap) this.interestMaps[NON_DURABLE].filtersOfInterestInv,
          dop);

      DataSerializer.writeHashMap((HashMap) this.interestMaps[DURABLE].allKeys, dop);
      DataSerializer.writeHashMap((HashMap) this.interestMaps[DURABLE].allKeysInv, dop);
      DataSerializer.writeHashMap((HashMap) this.interestMaps[DURABLE].keysOfInterest, dop);
      DataSerializer.writeHashMap((HashMap) this.interestMaps[DURABLE].keysOfInterestInv, dop);
      DataSerializer.writeHashMap((HashMap) this.interestMaps[DURABLE].patternsOfInterest, dop);
      DataSerializer.writeHashMap((HashMap) this.interestMaps[DURABLE].patternsOfInterestInv, dop);
      DataSerializer.writeHashMap((HashMap) this.interestMaps[DURABLE].filtersOfInterest, dop);
      DataSerializer.writeHashMap((HashMap) this.interestMaps[DURABLE].filtersOfInterestInv, dop);

      // Write CQ info.
      DataSerializer.writeHashMap((HashMap) this.cqs, dop);
    }

    @Override
    public void fromData(DataInput dip,
        DeserializationContext context) throws IOException, ClassNotFoundException {
      super.fromData(dip, context);
      // String regionName = DataSerializer.readString(dip);
      this.emptyRegionMap = DataSerializer.readHashMap(dip);
      // Read interest info.
      this.interestMaps[NON_DURABLE].allKeys = DataSerializer.readHashMap(dip);
      this.interestMaps[NON_DURABLE].allKeysInv = DataSerializer.readHashMap(dip);
      this.interestMaps[NON_DURABLE].keysOfInterest = DataSerializer.readHashMap(dip);
      this.interestMaps[NON_DURABLE].keysOfInterestInv = DataSerializer.readHashMap(dip);
      this.interestMaps[NON_DURABLE].patternsOfInterest = DataSerializer.readHashMap(dip);
      this.interestMaps[NON_DURABLE].patternsOfInterestInv = DataSerializer.readHashMap(dip);
      this.interestMaps[NON_DURABLE].filtersOfInterest = DataSerializer.readHashMap(dip);
      this.interestMaps[NON_DURABLE].filtersOfInterestInv = DataSerializer.readHashMap(dip);

      this.interestMaps[DURABLE].allKeys = DataSerializer.readHashMap(dip);
      this.interestMaps[DURABLE].allKeysInv = DataSerializer.readHashMap(dip);
      this.interestMaps[DURABLE].keysOfInterest = DataSerializer.readHashMap(dip);
      this.interestMaps[DURABLE].keysOfInterestInv = DataSerializer.readHashMap(dip);
      this.interestMaps[DURABLE].patternsOfInterest = DataSerializer.readHashMap(dip);
      this.interestMaps[DURABLE].patternsOfInterestInv = DataSerializer.readHashMap(dip);
      this.interestMaps[DURABLE].filtersOfInterest = DataSerializer.readHashMap(dip);
      this.interestMaps[DURABLE].filtersOfInterestInv = DataSerializer.readHashMap(dip);

      // read CQ info.
      this.cqs = DataSerializer.readHashMap(dip);
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.geode.internal.serialization.DataSerializableFixedID#getDSFID()
     */
    @Override
    public int getDSFID() {
      return FILTER_INFO_MESSAGE;
    }
  }

  public abstract static class GIITestHook implements Runnable, Serializable {
    private final GIITestHookType type;
    private final String region_name;
    public volatile boolean isRunning;

    public GIITestHook(GIITestHookType type, String region_name) {
      this.type = type;
      this.region_name = region_name;
      this.isRunning = false;
    }

    public GIITestHookType getType() {
      return this.type;
    }

    public String getRegionName() {
      return this.region_name;
    }

    public String toString() {
      return type + ":" + region_name + ":" + isRunning;
    }

    public abstract void reset();

    @Override
    public abstract void run();

  }

  public static final boolean TRACE_GII =
      Boolean.getBoolean(GeodeGlossary.GEMFIRE_PREFIX + "GetInitialImage.TRACE_GII");

  @MutableForTesting
  public static boolean FORCE_FULL_GII =
      Boolean.getBoolean(GeodeGlossary.GEMFIRE_PREFIX + "GetInitialImage.FORCE_FULL_GII");

  // test hooks should be applied and waited in strict order as following

  // test hooks should be applied and waited in strict order as following

  // internal test hooks at requester for sending request
  @MutableForTesting
  private static GIITestHook internalBeforeGetInitialImage;
  @MutableForTesting
  private static GIITestHook internalBeforeRequestRVV;
  @MutableForTesting
  private static GIITestHook internalAfterRequestRVV;
  @MutableForTesting
  private static GIITestHook internalAfterCalculatedUnfinishedOps;
  @MutableForTesting
  private static GIITestHook internalBeforeSavedReceivedRVV;
  @MutableForTesting
  private static GIITestHook internalAfterSavedReceivedRVV;
  @MutableForTesting
  private static GIITestHook internalAfterSentRequestImage;

  // internal test hooks at provider
  @MutableForTesting
  private static GIITestHook internalAfterReceivedRequestImage;
  @MutableForTesting
  private static GIITestHook internalDuringPackingImage;
  @MutableForTesting
  private static GIITestHook internalAfterSentImageReply;

  // internal test hooks at requester for processing ImageReply
  @MutableForTesting
  private static GIITestHook internalAfterReceivedImageReply;
  @MutableForTesting
  private static GIITestHook internalDuringApplyDelta;
  @MutableForTesting
  private static GIITestHook internalBeforeCleanExpiredTombstones;
  @MutableForTesting
  private static GIITestHook internalAfterSavedRVVEnd;

  public enum GIITestHookType {
    BeforeGetInitialImage,
    BeforeRequestRVV,
    AfterRequestRVV,
    AfterCalculatedUnfinishedOps,
    BeforeSavedReceivedRVV,
    AfterSavedReceivedRVV,
    AfterSentRequestImage,

    AfterReceivedRequestImage,
    DuringPackingImage,
    AfterSentImageReply,

    AfterReceivedImageReply,
    DuringApplyDelta,
    BeforeCleanExpiredTombstones,
    AfterSavedRVVEnd
  }

  public static GIITestHook getGIITestHookForCheckingPurpose(final GIITestHookType type) {
    switch (type) {
      case BeforeGetInitialImage: // 0
        return internalBeforeGetInitialImage;
      case BeforeRequestRVV: // 1
        return internalBeforeRequestRVV;
      case AfterRequestRVV: // 2
        return internalAfterRequestRVV;
      case AfterCalculatedUnfinishedOps: // 3
        return internalAfterCalculatedUnfinishedOps;
      case BeforeSavedReceivedRVV: // 4
        return internalBeforeSavedReceivedRVV;
      case AfterSavedReceivedRVV: // 5
        return internalAfterSavedReceivedRVV;
      case AfterSentRequestImage: // 6
        return internalAfterSentRequestImage;

      case AfterReceivedRequestImage: // 7
        return internalAfterReceivedRequestImage;
      case DuringPackingImage: // 8
        return internalDuringPackingImage;
      case AfterSentImageReply: // 9
        return internalAfterSentImageReply;

      case AfterReceivedImageReply: // 10
        return internalAfterReceivedImageReply;
      case DuringApplyDelta: // 11
        return internalDuringApplyDelta;
      case BeforeCleanExpiredTombstones: // 12
        return internalBeforeCleanExpiredTombstones;
      case AfterSavedRVVEnd: // 13
        return internalAfterSavedRVVEnd;
      default:
        throw new RuntimeException("Illegal test hook type");
    }
  }

  public static void setGIITestHook(final GIITestHook callback) {
    switch (callback.type) {
      case BeforeGetInitialImage: // 0
        assert internalBeforeGetInitialImage == null;
        internalBeforeGetInitialImage = callback;
        break;
      case BeforeRequestRVV: // 1
        assert internalBeforeRequestRVV == null;
        internalBeforeRequestRVV = callback;
        break;
      case AfterRequestRVV: // 2
        assert internalAfterRequestRVV == null;
        internalAfterRequestRVV = callback;
        break;
      case AfterCalculatedUnfinishedOps: // 3
        assert internalAfterCalculatedUnfinishedOps == null;
        internalAfterCalculatedUnfinishedOps = callback;
        break;
      case BeforeSavedReceivedRVV: // 4
        assert internalBeforeSavedReceivedRVV == null;
        internalBeforeSavedReceivedRVV = callback;
        break;
      case AfterSavedReceivedRVV: // 5
        internalAfterSavedReceivedRVV = callback;
        break;
      case AfterSentRequestImage: // 6
        internalAfterSentRequestImage = callback;
        break;

      case AfterReceivedRequestImage: // 7
        assert internalAfterReceivedRequestImage == null;
        internalAfterReceivedRequestImage = callback;
        break;
      case DuringPackingImage: // 8
        assert internalDuringPackingImage == null;
        internalDuringPackingImage = callback;
        break;
      case AfterSentImageReply: // 9
        assert internalAfterSentImageReply == null;
        internalAfterSentImageReply = callback;
        break;

      case AfterReceivedImageReply: // 10
        assert internalAfterReceivedImageReply == null;
        internalAfterReceivedImageReply = callback;
        break;
      case DuringApplyDelta: // 11
        assert internalDuringApplyDelta == null;
        internalDuringApplyDelta = callback;
        break;
      case BeforeCleanExpiredTombstones: // 12
        assert internalBeforeCleanExpiredTombstones == null;
        internalBeforeCleanExpiredTombstones = callback;
        break;
      case AfterSavedRVVEnd: // 13
        assert internalAfterSavedRVVEnd == null;
        internalAfterSavedRVVEnd = callback;
        break;
      default:
        throw new RuntimeException("Illegal test hook type");
    }
  }

  public static void resetGIITestHook(final GIITestHookType type, final boolean setNull) {
    switch (type) {
      case BeforeGetInitialImage: // 0
        if (internalBeforeGetInitialImage != null) {
          internalBeforeGetInitialImage.reset();
          if (setNull) {
            internalBeforeGetInitialImage = null;
          }
        }
        break;
      case BeforeRequestRVV: // 1
        if (internalBeforeRequestRVV != null) {
          internalBeforeRequestRVV.reset();
          if (setNull) {
            internalBeforeRequestRVV = null;
          }
        }
        break;
      case AfterRequestRVV: // 2
        if (internalAfterRequestRVV != null) {
          internalAfterRequestRVV.reset();
          if (setNull) {
            internalAfterRequestRVV = null;
          }
        }
        break;
      case AfterCalculatedUnfinishedOps: // 3
        if (internalAfterCalculatedUnfinishedOps != null) {
          internalAfterCalculatedUnfinishedOps.reset();
          if (setNull) {
            internalAfterCalculatedUnfinishedOps = null;
          }
        }
        break;
      case BeforeSavedReceivedRVV: // 4
        if (internalBeforeSavedReceivedRVV != null) {
          internalBeforeSavedReceivedRVV.reset();
          if (setNull) {
            internalBeforeSavedReceivedRVV = null;
          }
        }
        break;
      case AfterSavedReceivedRVV: // 5
        if (internalAfterSavedReceivedRVV != null) {
          internalAfterSavedReceivedRVV.reset();
          if (setNull) {
            internalAfterSavedReceivedRVV = null;
          }
        }
        break;
      case AfterSentRequestImage: // 6
        if (internalAfterSentRequestImage != null) {
          internalAfterSentRequestImage.reset();
          if (setNull) {
            internalAfterSentRequestImage = null;
          }
        }
        break;

      case AfterReceivedRequestImage: // 7
        if (internalAfterReceivedRequestImage != null) {
          internalAfterReceivedRequestImage.reset();
          if (setNull) {
            internalAfterReceivedRequestImage = null;
          }
        }
        break;
      case DuringPackingImage: // 8
        if (internalDuringPackingImage != null) {
          internalDuringPackingImage.reset();
          if (setNull) {
            internalDuringPackingImage = null;
          }
        }
        break;
      case AfterSentImageReply: // 9
        if (internalAfterSentImageReply != null) {
          internalAfterSentImageReply.reset();
          if (setNull) {
            internalAfterSentImageReply = null;
          }
        }
        break;

      case AfterReceivedImageReply: // 10
        if (internalAfterReceivedImageReply != null) {
          internalAfterReceivedImageReply.reset();
          if (setNull) {
            internalAfterReceivedImageReply = null;
          }
        }
        break;
      case DuringApplyDelta: // 11
        if (internalDuringApplyDelta != null) {
          internalDuringApplyDelta.reset();
          if (setNull) {
            internalDuringApplyDelta = null;
          }
        }
        break;
      case BeforeCleanExpiredTombstones: // 12
        if (internalBeforeCleanExpiredTombstones != null) {
          internalBeforeCleanExpiredTombstones.reset();
          if (setNull) {
            internalBeforeCleanExpiredTombstones = null;
          }
        }
        break;
      case AfterSavedRVVEnd: // 13
        if (internalAfterSavedRVVEnd != null) {
          internalAfterSavedRVVEnd.reset();
          if (setNull) {
            internalAfterSavedRVVEnd = null;
          }
        }
        break;
      default:
        throw new RuntimeException("Illegal test hook type");
    }
  }

  public static void resetAllGIITestHooks() {
    internalBeforeGetInitialImage = null;
    internalBeforeRequestRVV = null;
    internalAfterRequestRVV = null;
    internalAfterCalculatedUnfinishedOps = null;
    internalBeforeSavedReceivedRVV = null;
    internalAfterSavedReceivedRVV = null;
    internalAfterSentRequestImage = null;

    internalAfterReceivedRequestImage = null;
    internalDuringPackingImage = null;
    internalAfterSentImageReply = null;

    internalAfterReceivedImageReply = null;
    internalDuringApplyDelta = null;
    internalBeforeCleanExpiredTombstones = null;
    internalAfterSavedRVVEnd = null;
  }
}

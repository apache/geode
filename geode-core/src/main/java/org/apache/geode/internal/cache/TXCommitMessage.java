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

import static org.apache.geode.internal.cache.LocalRegion.InitializationLevel.BEFORE_INITIAL_IMAGE;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ServiceConfigurationError;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.DataSerializer;
import org.apache.geode.SystemFailure;
import org.apache.geode.annotations.Immutable;
import org.apache.geode.annotations.internal.MakeNotStatic;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.CacheRuntimeException;
import org.apache.geode.cache.CommitDistributionException;
import org.apache.geode.cache.CommitIncompleteException;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.RegionDistributionException;
import org.apache.geode.cache.TransactionId;
import org.apache.geode.cache.TransactionListener;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.MembershipListener;
import org.apache.geode.distributed.internal.MessageWithReply;
import org.apache.geode.distributed.internal.PooledDistributionMessage;
import org.apache.geode.distributed.internal.ReliableReplyProcessor21;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.ReplyMessage;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.cache.LocalRegion.InitializationLevel;
import org.apache.geode.internal.cache.locks.TXLockId;
import org.apache.geode.internal.cache.locks.TXLockIdImpl;
import org.apache.geode.internal.cache.locks.TXLockService;
import org.apache.geode.internal.cache.partitioned.Bucket;
import org.apache.geode.internal.cache.persistence.PersistentMemberID;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.versions.VersionSource;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.internal.offheap.annotations.Released;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.internal.serialization.StaticSerialization;
import org.apache.geode.logging.internal.executors.LoggingThread;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * TXCommitMessage is the message that contains all the information that needs to be distributed, on
 * commit, to other cache members.
 *
 * @since GemFire 4.0
 */
public class TXCommitMessage extends PooledDistributionMessage
    implements MembershipListener, MessageWithReply {

  private static final Logger logger = LogService.getLogger();

  // Keep a 60 second history @ an estimated 1092 transactions/second ~= 16^4
  @MakeNotStatic
  protected static final TXFarSideCMTracker txTracker = new TXFarSideCMTracker((60 * 1092));

  private ArrayList<RegionCommit> regions; // list of RegionCommit instances
  protected TXId txIdent;
  protected int processorId; // 0 unless needsAck is true
  protected TXLockIdImpl lockId;
  protected HashSet<InternalDistributedMember> farSiders;
  protected transient DistributionManager dm; // Used on the sending side of this message
  private transient int sequenceNum = 0;

  // Maps receiver Serializables to RegionCommitList instances
  private transient HashMap<InternalDistributedMember, RegionCommitList> msgMap = null;

  private transient RegionCommit currentRegion;
  protected transient TXState txState = null;
  private transient boolean wasProcessed;
  private transient boolean isProcessing;
  private transient boolean dontProcess;
  private transient boolean departureNoticed = false;
  private transient boolean lockNeedsUpdate = false;
  private transient boolean ackRequired = true;
  /**
   * List of operations to do when processing this tx. Valid on farside only.
   */
  protected transient ArrayList<RegionCommit.FarSideEntryOp> farSideEntryOps;
  private byte[] farsideBaseMembershipId; // only available on farside
  private long farsideBaseThreadId; // only available on farside
  private long farsideBaseSequenceId; // only available on farside

  /**
   * (Nearside) true of any regions in this TX have required roles
   */
  private transient boolean hasReliableRegions = false;

  /**
   * Set of all caching exceptions produced while processing this tx
   */
  private transient Set<Exception> processingExceptions = Collections.emptySet();

  private ClientProxyMembershipID bridgeContext = null;

  /**
   * Version of the client that this TXCommitMessage is being sent to. Used for backwards
   * compatibility
   */
  private transient KnownVersion clientVersion;

  /**
   * A token to be put in TXManagerImpl#failoverMap to represent a CommitConflictException while
   * committing a transaction
   */
  @Immutable
  public static final TXCommitMessage CMT_CONFLICT_MSG = new TXCommitMessage();
  /**
   * A token to be put in TXManagerImpl#failoverMap to represent a
   * TransactionDataNodeHasDepartedException
   * while committing a transaction
   */
  @Immutable
  public static final TXCommitMessage REBALANCE_MSG = new TXCommitMessage();
  /**
   * A token to be put in TXManagerImpl#failoverMap to represent an exception while committing a
   * transaction
   */
  @Immutable
  public static final TXCommitMessage EXCEPTION_MSG = new TXCommitMessage();
  /**
   * A token to be put in TXManagerImpl#failoverMap to represent a rolled back transaction
   */
  @Immutable
  public static final TXCommitMessage ROLLBACK_MSG = new TXCommitMessage();

  public TXCommitMessage(TXId txIdent, DistributionManager dm, TXState txState) {
    this.dm = dm;
    this.txIdent = txIdent;
    lockId = null;
    regions = null;
    this.txState = txState;
    wasProcessed = false;
    isProcessing = false;
    dontProcess = false;
    farSiders = null;
    bridgeContext = txState.bridgeContext;
  }

  public TXCommitMessage() {
    // zero arg constructor for DataSerializer
  }

  public static TXFarSideCMTracker getTracker() {
    return TXCommitMessage.txTracker;
  }

  /**
   * Create and return an eventId given its offset.
   *
   * @since GemFire 5.7
   */
  protected EventID getEventId(int eventOffset) {
    return new EventID(farsideBaseMembershipId, farsideBaseThreadId,
        farsideBaseSequenceId + eventOffset);
  }

  /**
   * Return the TXCommitMessage we have already received that is associated with id. Note because of
   * bug 37657 we may need to wait for it to show up.
   */
  public static TXCommitMessage waitForMessage(Object id, DistributionManager dm) {
    TXFarSideCMTracker map = getTracker();
    return map.waitForMessage(id, dm);
  }

  void startRegion(InternalRegion r, int maxSize) {
    currentRegion = new RegionCommit(this, r, maxSize);
    if (r.requiresReliabilityCheck()) {
      hasReliableRegions = true;
    }
  }

  void finishRegion(Set<InternalDistributedMember> s) {
    // make sure we have some changes and someone to send them to
    if (!currentRegion.isEmpty() && s != null && !s.isEmpty()) {
      // Get the persistent ids for the current region and save them
      currentRegion.persistentIds = getPersistentIds(currentRegion.internalRegion);

      if (msgMap == null) {
        msgMap = new HashMap<>();
      }
      {
        RegionCommitList newRCL = null;
        for (final InternalDistributedMember recipient : s) {
          if (!dm.getDistributionManagerIds().contains(recipient)) {
            if (logger.isDebugEnabled()) {
              logger.debug("Skipping member {} due to dist list absence", recipient);
            }
            // skip this member since the dm no longer knows about it
            continue;
          }
          RegionCommitList rcl = msgMap.get(recipient);
          if (rcl == null) {
            if (newRCL == null) {
              rcl = new RegionCommitList();
              rcl.add(currentRegion);
              newRCL = rcl;
            } else {
              rcl = newRCL;
            }
            msgMap.put(recipient, rcl);
          } else if (rcl.get(rcl.size() - 1) != currentRegion) {
            rcl.add(currentRegion);
          }
        }
      }

      // Now deal with each existing recipient that does not care
      // about this region
      for (final Map.Entry<InternalDistributedMember, RegionCommitList> me : msgMap.entrySet()) {
        if (!s.contains(me.getKey())) {
          RegionCommitList rcl = me.getValue();
          RegionCommitList trimmedRcl = rcl.trim(currentRegion);
          if (trimmedRcl != rcl) {
            me.setValue(trimmedRcl);
          }
        }
      }
    }
    currentRegion = null;
  }

  private Map<InternalDistributedMember, PersistentMemberID> getPersistentIds(InternalRegion r) {
    if (r instanceof DistributedRegion) {
      return ((CacheDistributionAdvisee) r).getCacheDistributionAdvisor().advisePersistentMembers();
    } else {
      return Collections.emptyMap();
    }
  }

  void finishRegionComplete() {
    // make sure we have some changes and someone to send them to
    if (!currentRegion.isEmpty()) {
      {
        if (regions == null) {
          regions = new RegionCommitList();
        }
        regions.add(currentRegion);
      }
    }
    currentRegion = null;
  }

  Map<DistributedRegion, Long> viewVersions = new HashMap<>();

  private Boolean needsLargeModCount;

  private transient boolean disableListeners = false;

  /**
   * record CacheDistributionAdvisor.startOperation versions for later cleanup
   */
  protected void addViewVersion(DistributedRegion dr, long version) {
    viewVersions.put(dr, version);
  }

  protected void releaseViewVersions() {
    RuntimeException rte = null;
    for (final Map.Entry<DistributedRegion, Long> e : viewVersions.entrySet()) {
      final DistributedRegion dr = e.getKey();
      final Long viewVersion = e.getValue();
      // need to continue the iteration if one of the regions is destroyed
      // since others may still be okay
      try {
        dr.getDistributionAdvisor().endOperation(viewVersion);
      } catch (RuntimeException ex) {
        rte = ex;
      }
    }
    if (rte != null) {
      throw rte;
    }
  }

  private boolean isEmpty() {
    return msgMap == null || msgMap.isEmpty();
  }

  void addOp(InternalRegion r, Object key, TXEntryState entry) {
    currentRegion.addOp(key, entry);
  }

  void send(TXLockId lockId) {
    if (isEmpty()) {
      if (logger.isDebugEnabled()) {
        logger.debug("empty transaction - nothing to distribute");
      }
      return;
    }
    Assert.assertTrue(txState != null, "Send must have transaction state.");
    this.lockId = (TXLockIdImpl) lockId;
    updateLockMembers();

    // Map of RegionCommitList keys to Sets of receivers
    IdentityHashMap<RegionCommitList, Set<InternalDistributedMember>> distMap =
        new IdentityHashMap<>();
    HashSet<InternalDistributedMember> ackReceivers = null;
    for (final Map.Entry<InternalDistributedMember, RegionCommitList> entry : msgMap.entrySet()) {
      final RegionCommitList rcl = entry.getValue();
      if (rcl.getNeedsAck()) {
        if (ackReceivers == null) {
          ackReceivers = new HashSet<>();
        }
        ackReceivers.add(entry.getKey());
      }
      final Set<InternalDistributedMember> receivers =
          distMap.computeIfAbsent(rcl, k -> new HashSet<>());
      receivers.add(entry.getKey());
    }

    CommitReplyProcessor processor = null;
    {
      if (ackReceivers != null) {
        processor = new CommitReplyProcessor(dm, ackReceivers, msgMap);
        if (ackReceivers.size() > 1) {
          farSiders = ackReceivers;
        }
        processor.enableSevereAlertProcessing();
      }
      {
        for (final Map.Entry<RegionCommitList, Set<InternalDistributedMember>> me : distMap
            .entrySet()) {
          RegionCommitList rcl = me.getKey();
          Set<InternalDistributedMember> recipients = me.getValue();
          // now remove from the recipients any recipients that the dm no
          // longer knows about
          recipients.retainAll(dm.getDistributionManagerIds());
          if (!recipients.isEmpty()) {
            if (txState.internalDuringIndividualSend != null) {
              // Run in test mode, splitting out individual recipients,
              // so we can control who gets what
              for (final InternalDistributedMember recipient : recipients) {
                txState.internalDuringIndividualSend.run();
                setRecipientsSendData(Collections.singleton(recipient), processor, rcl);
              }
            } else {
              // Run in normal mode sending to multiple recipients in one shot
              setRecipientsSendData(recipients, processor, rcl);
            }
          }
        }
      }
    }
    if (txState.internalAfterIndividualSend != null) {
      txState.internalAfterIndividualSend.run();
    }

    if (processor != null) {
      // Send the CommitProcessMessage
      final CommitProcessMessage cpMsg;
      if (this.lockId != null) {
        cpMsg = new CommitProcessForLockIdMessage(this.lockId);
      } else {
        cpMsg = new CommitProcessForTXIdMessage(txIdent);
      }
      if (txState.internalDuringIndividualCommitProcess != null) {
        // Run in test mode
        for (final InternalDistributedMember ackReceiver : ackReceivers) {
          txState.internalDuringIndividualCommitProcess.run();
          cpMsg.setRecipients(Collections.singleton(ackReceiver));
          dm.putOutgoing(cpMsg);
          cpMsg.resetRecipients();
        }
      } else {
        // Run in normal mode
        cpMsg.setRecipients(ackReceivers);
        dm.putOutgoing(cpMsg);
      }

      if (txState.internalAfterIndividualCommitProcess != null) {
        // Testing callback
        txState.internalAfterIndividualCommitProcess.run();
      }

      // for() loop removed for bug 36983 - you can't loop on waitForReplies()
      dm.getCancelCriterion().checkCancelInProgress(null);
      processor.waitForCommitCompletion();
      dm.getStats().incCommitWaits();
    }
    if (hasReliableRegions) {
      checkDistributionReliability(distMap, processor);
    }
  }

  @Override
  public boolean containsRegionContentChange() {
    return true;
  }

  /**
   * Checks reliable regions and throws CommitDistributionException if any required roles may not
   * have received the commit message.
   *
   * @param distMap map of RegionCommitList keys to Sets of receivers
   * @param processor the reply processor
   * @throws CommitDistributionException if any required roles may not have received the commit
   *         message
   */
  private void checkDistributionReliability(
      IdentityHashMap<RegionCommitList, Set<InternalDistributedMember>> distMap,
      CommitReplyProcessor processor) {
    // key=RegionCommit, value=Set of recipients
    Map<RegionCommit, Set<InternalDistributedMember>> regionToRecipients = new IdentityHashMap<>();

    // build up the keys in regionToRecipients and add all receivers
    for (final Map.Entry<RegionCommitList, Set<InternalDistributedMember>> me : distMap
        .entrySet()) {
      RegionCommitList rcl = me.getKey();
      Set<InternalDistributedMember> recipients = me.getValue();

      for (RegionCommit rc : rcl) {
        // skip region if no required roles
        if (!rc.internalRegion.requiresReliabilityCheck()) {
          continue;
        }

        final Set<InternalDistributedMember> recipientsForRegion =
            regionToRecipients.computeIfAbsent(rc, k -> new HashSet<>());
        // get the receiver Set for rcl and perform addAll
        if (recipients != null) {
          recipientsForRegion.addAll(recipients);
        }
      }
    }

    Set<InternalDistributedMember> cacheClosedMembers =
        (processor == null) ? Collections.emptySet() : processor.getCacheClosedMembers();
    Set<InternalDistributedMember> departedMembers =
        (processor == null) ? Collections.emptySet() : processor.getDepartedMembers();

    // check reliability on each region
    @SuppressWarnings("deprecation")
    Set<RegionDistributionException> regionDistributionExceptions = Collections.emptySet();
    Set<String> failedRegionNames = Collections.emptySet();
    for (final Map.Entry<RegionCommit, Set<InternalDistributedMember>> me : regionToRecipients
        .entrySet()) {
      final RegionCommit rc = me.getKey();
      final Set<InternalDistributedMember> successfulRecipients = new HashSet<>(msgMap.keySet());
      successfulRecipients.removeAll(departedMembers);

      // remove members who destroyed that region or closed their cache
      Set<InternalDistributedMember> regionDestroyedMembers =
          (processor == null) ? Collections.emptySet()
              : processor.getRegionDestroyedMembers(rc.internalRegion.getFullPath());

      successfulRecipients.removeAll(cacheClosedMembers);
      successfulRecipients.removeAll(regionDestroyedMembers);

      try {
        rc.internalRegion.handleReliableDistribution(successfulRecipients);
      } catch (@SuppressWarnings("deprecation") RegionDistributionException e) {
        if (regionDistributionExceptions.isEmpty()) {
          regionDistributionExceptions = new HashSet<>();
          failedRegionNames = new HashSet<>();
        }
        regionDistributionExceptions.add(e);
        failedRegionNames.add(rc.internalRegion.getFullPath());
      }
    }

    if (!regionDistributionExceptions.isEmpty()) {
      throw new CommitDistributionException(
          String.format(
              "These regions experienced reliability failure during distribution of the operation: %s",
              failedRegionNames),
          regionDistributionExceptions);
    }
  }

  /**
   * Helper method for send
   */
  private void setRecipientsSendData(Set<InternalDistributedMember> recipients,
      ReplyProcessor21 processor,
      RegionCommitList rcl) {
    setRecipients(recipients);
    regions = rcl;
    if (rcl.getNeedsAck()) {
      processorId = processor.getProcessorId();
    } else {
      processorId = 0;
    }
    dm.getStats().incSentCommitMessages(1L);
    sequenceNum++;
    dm.putOutgoing(this);
    resetRecipients();
  }

  @Override
  protected void process(ClusterDistributionManager dm) {
    this.dm = dm;
    // Remove this node from the set of recipients
    if (farSiders != null) {
      farSiders.remove(dm.getId());
    }

    if (processorId != 0) {
      TXLockService.createDTLS(this.dm.getSystem()); // fix bug 38843; no-op if already created
      synchronized (this) {
        // Handle potential origin departure
        this.dm.addMembershipListener(this);
        // Assume ACK mode, defer processing until we receive a
        // CommitProcess message
        if (logger.isDebugEnabled()) {
          final Object key = getTrackerKey();
          logger.debug("Adding key:{} class{} to tracker list", key, key.getClass().getName());
        }
        txTracker.add(this);
      }
      if (!this.dm.getDistributionManagerIds().contains(getSender())) {
        memberDeparted(this.dm, getSender(), false /* don't care */);
      }

    } else {
      basicProcess();
    }
  }

  /**
   * Adds an entry op for this tx to do on the far side
   */
  void addFarSideEntryOp(RegionCommit.FarSideEntryOp entryOp) {
    farSideEntryOps.add(entryOp);
  }

  protected void addProcessingException(Exception e) {
    // clear all previous exceptions if e is a CacheClosedException
    if (processingExceptions.isEmpty() || e instanceof CancelException) {
      processingExceptions = new HashSet<>();
    }
    processingExceptions.add(e);
  }

  public void setDM(DistributionManager dm) {
    this.dm = dm;
  }

  public void basicProcess() {
    final DistributionManager dm = this.dm;

    synchronized (this) {
      if (isProcessing()) {
        if (logger.isDebugEnabled()) {
          logger.debug("TXCommitMessage {} is already in process, returning", this);
        }
        return;
      } else {
        setIsProcessing(true);
      }
    }

    if (logger.isDebugEnabled()) {
      logger.debug("begin processing TXCommitMessage for {}", txIdent);
    }
    final InitializationLevel oldLevel =
        LocalRegion.setThreadInitLevelRequirement(BEFORE_INITIAL_IMAGE);
    boolean forceListener = false; // this gets flipped if we need to fire tx listener
    // it needs to default to false because we don't want to fire listeners on pr replicates
    try {
      TXRmtEvent txEvent = null;
      final Cache cache = dm.getExistingCache();
      if (cache == null) {
        addProcessingException(new CacheClosedException());
        // return ... this cache is closed so we can't do anything.
        return;
      }
      final TransactionListener[] tls = cache.getCacheTransactionManager().getListeners();
      if (tls.length > 0) {
        txEvent = new TXRmtEvent(txIdent, cache);
      }
      try {
        // Pre-process each Region in the tx
        try {
          Iterator<RegionCommit> it = regions.iterator();
          while (it.hasNext()) {
            boolean failedBeginProcess = true;
            RegionCommit rc = it.next();
            try {
              failedBeginProcess = !rc.beginProcess(dm, txIdent, txEvent);
            } catch (CacheRuntimeException problem) {
              processCacheRuntimeException(problem);
            } finally {
              if (failedBeginProcess) {
                rc.internalRegion = null; // Cause related FarSideEntryOps to skip processing
                it.remove(); // Skip endProcessing as well
              }
            }
          }
          basicProcessOps();
        } finally {
          // post-process each Region in the tx
          Iterator<RegionCommit> it = regions.iterator();
          while (it.hasNext()) {
            try {
              RegionCommit rc = it.next();
              rc.endProcess();
              if (rc.isForceFireEvent(dm)) {
                forceListener = true;
              }
            } catch (CacheRuntimeException problem) {
              processCacheRuntimeException(problem);
            }
          }
        }

        /*
         * We need to make sure that we should fire a TX afterCommit event.
         */
        boolean internalEvent = (txEvent != null && txEvent.hasOnlyInternalEvents());
        if (!disableListeners && !internalEvent
            && (forceListener || (txEvent != null && !txEvent.isEmpty()))) {
          for (final TransactionListener tl : tls) {
            try {
              tl.afterCommit(txEvent);
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
              logger.error("Exception occurred in TransactionListener",
                  t);
            }
          }
        }
      } catch (CancelException e) {
        processCacheRuntimeException(e);
      } finally {
        if (txEvent != null) {
          txEvent.freeOffHeapResources();
        }
      }
    } finally {
      LocalRegion.setThreadInitLevelRequirement(oldLevel);
      if (isAckRequired()) {
        ack();
      }
      if (!dm.getExistingCache().isClient() && bridgeContext != null) {
        getTracker().saveTXForClientFailover(txIdent, this);
      }
      if (logger.isDebugEnabled()) {
        logger.debug("completed processing TXCommitMessage for {}", txIdent);
      }
    }
  }

  public void basicProcessOps() {
    List<EntryEventImpl> pendingCallbacks = new ArrayList<>(farSideEntryOps.size());
    farSideEntryOps.sort(null);
    Iterator<RegionCommit.FarSideEntryOp> it = farSideEntryOps.iterator();
    while (it.hasNext()) {
      try {
        RegionCommit.FarSideEntryOp entryOp = it.next();
        entryOp.process(pendingCallbacks);
      } catch (CacheRuntimeException problem) {
        processCacheRuntimeException(problem);
      } catch (Exception e) {
        addProcessingException(e);
      }
    }
    firePendingCallbacks(pendingCallbacks);
  }

  void firePendingCallbacks(List<EntryEventImpl> callbacks) {
    boolean isConfigError = false;
    EntryEventImpl lastTransactionEvent = null;
    try {
      lastTransactionEvent = getLastTransactionEvent(callbacks);
    } catch (ServiceConfigurationError ex) {
      logger.error(ex.getMessage());
      isConfigError = true;
    }

    for (EntryEventImpl ee : callbacks) {
      boolean isLastTransactionEvent = isConfigError || ee.equals(lastTransactionEvent);
      try {
        if (ee.getOperation().isDestroy()) {
          ee.getRegion().invokeTXCallbacks(EnumListenerEvent.AFTER_DESTROY, ee, true,
              isLastTransactionEvent);
        } else if (ee.getOperation().isInvalidate()) {
          ee.getRegion().invokeTXCallbacks(EnumListenerEvent.AFTER_INVALIDATE, ee, true,
              isLastTransactionEvent);
        } else if (ee.getOperation().isCreate()) {
          ee.getRegion().invokeTXCallbacks(EnumListenerEvent.AFTER_CREATE, ee, true,
              isLastTransactionEvent);
        } else {
          if (!ee.hasNewValue()) { // GEODE-8964, fixes GII and TX create conflict that
            ee.getRegion(). // produces an Update with null value
                invokeTXCallbacks(EnumListenerEvent.AFTER_CREATE, ee, true, isLastTransactionEvent);
          } else {
            ee.getRegion().invokeTXCallbacks(EnumListenerEvent.AFTER_UPDATE, ee, true,
                isLastTransactionEvent);
          }
        }
      } finally {
        ee.release();
      }
    }
  }

  EntryEventImpl getLastTransactionEvent(List<EntryEventImpl> callbacks) {
    return TXLastEventInTransactionUtils.getLastTransactionEvent(callbacks, dm.getCache());
  }

  protected void processCacheRuntimeException(CacheRuntimeException problem) {
    if (problem instanceof RegionDestroyedException) { // catch RegionDestroyedException
      addProcessingException(problem);
    } else if (problem instanceof CancelException) { // catch CacheClosedException
      addProcessingException(problem);
      throw problem;
    } else { // catch CacheRuntimeException
      addProcessingException(problem);
      logger.error(
          "Transaction message {} from sender {} failed processing, unknown transaction state: {}",
          new Object[] {this, getSender(), problem});
    }
  }

  private void ack() {
    if (processorId != 0) {
      CommitReplyException replyEx = null;
      if (!processingExceptions.isEmpty()) {
        replyEx = new CommitReplyException(
            String.format("Commit operation generated one or more exceptions from %s",
                getSender()),
            processingExceptions);
      }
      ReplyMessage.send(getSender(), processorId, replyEx, dm);
    }
  }

  @Override
  public int getDSFID() {
    return TX_COMMIT_MESSAGE;
  }

  @Override
  public void fromData(DataInput in, DeserializationContext context)
      throws IOException, ClassNotFoundException {
    int pId = in.readInt();

    if (isAckRequired()) {
      processorId = pId;
      ReplyProcessor21.setMessageRPId(processorId);
    } else {
      processorId = -1;
    }

    txIdent = TXId.createFromData(in);
    if (in.readBoolean()) {
      lockId = TXLockIdImpl.createFromData(in);
    }
    int totalMaxSize = in.readInt();

    farsideBaseMembershipId = DataSerializer.readByteArray(in);
    farsideBaseThreadId = in.readLong();
    farsideBaseSequenceId = in.readLong();

    needsLargeModCount = in.readBoolean();

    final boolean hasShadowKeys = hasFlagsField(in) ? in.readBoolean() : useShadowKey();

    int regionsSize = in.readInt();
    regions = new ArrayList<>(regionsSize);
    farSideEntryOps = new ArrayList<>(totalMaxSize);
    for (int i = 0; i < regionsSize; i++) {
      RegionCommit rc = new RegionCommit(this);
      try {
        rc.fromData(in, hasShadowKeys);
      } catch (CacheClosedException cce) {
        addProcessingException(cce);
        // return to avoid serialization error being sent in reply
        return;
      }
      regions.add(rc);
    }

    bridgeContext = ClientProxyMembershipID.readCanonicalized(in);
    farSiders = DataSerializer.readHashSet(in);
  }

  /**
   * Return true if a distributed ack message is required. On the client side of a transaction, this
   * returns false, while returning true elsewhere.
   *
   * @return requires ack message or not
   */
  private boolean isAckRequired() {
    return ackRequired;
  }


  /**
   * Indicate whether an ack is required. Defaults to true.
   *
   * @param a true if we require an ack. false if not. false on clients.
   */
  public void setAckRequired(boolean a) {
    ackRequired = a;
    if (!a) {
      processorId = -1;
    }
  }

  @Override
  public void toData(DataOutput out, SerializationContext context) throws IOException {
    out.writeInt(processorId);
    InternalDataSerializer.invokeToData(txIdent, out);
    {
      boolean hasLockId = lockId != null;
      out.writeBoolean(hasLockId);
      if (hasLockId) {
        InternalDataSerializer.invokeToData(lockId, out);
      }
    }
    int regionsSize = 0;
    {
      int totalMaxSize = 0;
      if (regions != null) {
        regionsSize = regions.size();
        for (RegionCommit region : regions) {
          totalMaxSize += region.maxSize;
        }
      }
      out.writeInt(totalMaxSize);
    }

    if (txState != null) {
      DataSerializer.writeByteArray(txState.getBaseMembershipId(), out);
      out.writeLong(txState.getBaseThreadId());
      out.writeLong(txState.getBaseSequenceId());
    } else {
      DataSerializer.writeByteArray(farsideBaseMembershipId, out);
      out.writeLong(farsideBaseThreadId);
      out.writeLong(farsideBaseSequenceId);
    }

    if (txState != null) {
      DataSerializer.writeBoolean(txState.needsLargeModCount(), out);
    } else {
      DataSerializer.writeBoolean(needsLargeModCount, out);
    }

    final boolean useShadowKey = useShadowKey();
    if (hasFlagsField(out)) {
      out.writeBoolean(useShadowKey);
    }

    out.writeInt(regionsSize);
    {
      if (regionsSize > 0) {
        for (RegionCommit region : regions) {
          region.toData(out, context, useShadowKey);
        }
      }
    }

    DataSerializer.writeObject(bridgeContext, out);

    DataSerializer.writeHashSet(farSiders, out);
  }

  private boolean hasFlagsField(final DataOutput out) {
    return hasFlagsField(StaticSerialization.getVersionForDataStream(out));
  }

  private boolean hasFlagsField(final DataInput in) {
    return hasFlagsField(StaticSerialization.getVersionForDataStream(in));
  }

  private boolean hasFlagsField(final KnownVersion version) {
    return version.isNotOlderThan(KnownVersion.GEODE_1_7_0);
  }

  private boolean useShadowKey() {
    return null == clientVersion;
  }

  @Override
  public String toString() {
    StringBuilder result = new StringBuilder(256);
    result.append("TXCommitMessage@").append(System.identityHashCode(this)).append("#")
        .append(sequenceNum).append(" processorId=").append(processorId).append(" txId=")
        .append(txIdent);

    if (farSiders != null) {
      Iterator<InternalDistributedMember> fs = farSiders.iterator();
      result.append(" farSiders=");
      while (fs.hasNext()) {
        result.append(fs.next());
        if (fs.hasNext()) {
          result.append(' ');
        }
      }
    } else {
      result.append(" farSiders=<null>");
    }
    if (regions != null) {
      for (final RegionCommit region : regions) {
        result.append(' ').append(region);
      }
    }
    return result.toString();
  }

  /**
   * Combines a set of small TXCommitMessages that belong to one transaction into a txCommitMessage
   * that represents an entire transaction. At commit time the txCommitMessage sent to each node can
   * be a subset of the transaction, this method will combine those subsets into a complete
   * message.
   *
   * @return the complete txCommitMessage
   */
  public static TXCommitMessage combine(Set<TXCommitMessage> msgSet) {
    assert msgSet != null;
    TXCommitMessage firstPart = null;
    Iterator<TXCommitMessage> it = msgSet.iterator();
    while (it.hasNext()) {
      if (firstPart == null) {
        firstPart = it.next();
        continue;
      }
      firstPart.combine(it.next());
    }
    return firstPart;
  }

  /**
   * Combines the other TXCommitMessage into this message. Used to compute complete TXCommitMessage
   * from parts.
   */
  public void combine(TXCommitMessage other) {
    assert other != null;
    final Map<String, RegionCommit> regionCommits = new HashMap<>();
    for (RegionCommit commit : regions) {
      regionCommits.put(commit.getRegionPath(), commit);
    }
    for (RegionCommit commit : other.regions) {
      if (!regionCommits.containsKey(commit.getRegionPath())) {
        commit.msg = this;
        regions.add(commit);
        regionCommits.put(commit.getRegionPath(), commit);
      }
    }
  }

  public static class RegionCommitList extends ArrayList<RegionCommit> {
    private static final long serialVersionUID = -8910813949027683641L;
    private transient boolean needsAck = false;
    private transient RegionCommit trimRC = null;
    private transient RegionCommitList trimChild = null;

    public RegionCommitList() {
      super();
    }

    public RegionCommitList(RegionCommitList c) {
      super(c);
    }

    public boolean getNeedsAck() {
      return needsAck;
    }

    @Override
    public boolean add(RegionCommit regionCommit) {
      regionCommit.incRefCount();
      if (!needsAck && regionCommit.needsAck()) {
        needsAck = true;
      }
      return super.add(regionCommit);
    }

    /**
     * Creates a new list, if needed, that contains all the elements of the specified old list
     * except the last one if it is 'rc'. Also recomputes needsAck field.
     */
    public RegionCommitList trim(RegionCommit rc) {
      if (get(size() - 1) != rc) {
        // no need to trim because it does not contain rc
        return this;
      }
      if (trimRC == rc) {
        return trimChild;
      }
      RegionCommitList result = new RegionCommitList(this);
      trimRC = rc;
      trimChild = result;
      result.remove(result.size() - 1);
      {
        for (final RegionCommit itrc : result) {
          itrc.incRefCount();
          if (itrc.needsAck()) {
            result.needsAck = true;
          }
        }
      }
      return result;
    }

    @Override
    public String toString() {
      return "@" + System.identityHashCode(this) + ' ' + super.toString();
    }
  }

  public static class RegionCommit {
    private final TxCallbackEventFactory txCallbackEventFactory = new TxCallbackEventFactoryImpl();
    /**
     * The region that this commit represents. Valid on both nearside and farside.
     */
    protected transient InternalRegion internalRegion;
    /**
     * Valid only on farside.
     */
    private String regionPath;
    private String parentRegionPath;
    /**
     * The message this region commit is a part of. Valid on both farside and nearside.
     */
    private transient TXCommitMessage msg;
    /**
     * Number of RegionCommitList instances that have this RegionCommit in them Valid only on
     * nearside.
     */
    private transient int refCount = 0;
    /**
     * Valid only on nearside.
     */
    private transient HeapDataOutputStream preserializedBuffer = null;
    /**
     * Upperbound on the number of operations this region could possibly have Valid only on
     * nearside.
     */
    transient int maxSize;
    /**
     * A list of Object; each one is the entry key for a distributed operation done by this
     * transaction. The list must be kept in sync with opKeys. Valid only on nearside.
     */
    private transient ArrayList<Object> opKeys;
    /**
     * A list of TXEntryState; each one is the entry info for a distributed operation done by this
     * transaction. The list must be kept in sync with opKeys. Valid only on nearside.
     */
    private transient ArrayList<Object> opEntries;

    private transient VersionSource memberId;

    /**
     * The persistent ids of the peers for this region. Used to mark peers as offline if they do not
     * apply the commit due to a cache close.
     */
    public Map<InternalDistributedMember, PersistentMemberID> persistentIds;

    /**
     * Used on nearside
     */
    RegionCommit(TXCommitMessage msg, InternalRegion r, int maxSize) {
      this.msg = msg;
      internalRegion = r;
      this.maxSize = maxSize;
    }

    /**
     * Used on farside who inits r later and never sets maxSize
     */
    RegionCommit(TXCommitMessage msg) {
      this.msg = msg;
    }

    public String getRegionPath() {
      return regionPath;
    }

    public void incRefCount() {
      refCount++;
    }

    /**
     * Valid on farside after beginProcess. Used to remember what to do at region cleanup time
     */
    private boolean needsUnlock;
    /**
     * Valid on farside after beginProcess. Used to remember what to do at region cleanup time
     */
    private boolean needsLRUEnd;
    /**
     * Valid on farside after beginProcess This is the txEvent that should be used by this
     * RegionCommit
     */
    private TXRmtEvent txEvent;

    /**
     * Called to setup a region commit so its entryOps can be processed
     *
     * @return true if region should be processed; false if it can be ignored
     * @throws CacheClosedException if the cache has been closed
     */
    boolean beginProcess(DistributionManager dm, TransactionId txIdent, TXRmtEvent txEvent)
        throws CacheClosedException {
      if (logger.isDebugEnabled()) {
        logger.debug("begin processing TXCommitMessage {} for region {}", txIdent, regionPath);
      }
      try {
        if (!hookupRegion(dm)) {
          return false;
        }
        if (msg.isAckRequired()
            && (internalRegion == null || !internalRegion.getScope().isDistributed())) {
          if (logger.isDebugEnabled()) {
            logger.debug("Received unneeded commit data for region {}", regionPath);
          }
          msg.addProcessingException(new RegionDestroyedException(
              "Region not found",
              regionPath));
          internalRegion = null;
          return false;
        }
        needsUnlock = internalRegion.lockGII();
        internalRegion.txLRUStart();
        needsLRUEnd = true;
        if (internalRegion.isInitialized()) {
          // We don't want the txEvent to know anything about our regions
          // that are still doing gii.
          this.txEvent = txEvent;
        }
      } catch (RegionDestroyedException e) {
        msg.addProcessingException(e);
        // Region destroyed: Update cancelled
        if (logger.isDebugEnabled()) {
          logger.debug(
              "Received unneeded commit data for region {} because the region was destroyed.",
              regionPath, e);
        }
        internalRegion = null;
      }
      return internalRegion != null;
    }

    private boolean hookupRegion(DistributionManager dm) {
      internalRegion = getRegionByPath(dm, regionPath);
      if (internalRegion == null && parentRegionPath != null) {
        internalRegion = getRegionByPath(dm, parentRegionPath);
        regionPath = parentRegionPath;
      }
      // If there are additional regions that the server enlisted in the tx,
      // which the client does not have, the client can just ignore the region
      return internalRegion != null || !dm.getSystem().isLoner();
    }

    LocalRegion getRegionByPath(DistributionManager dm, String regionPath) {
      InternalCache cache = dm.getCache();
      return cache == null ? null : (LocalRegion) cache.getInternalRegionByPath(regionPath);
    }

    /**
     * Called when processing is complete; only needs to be called if beginProcess returned true.
     */
    void endProcess() {
      if (internalRegion != null) {
        try {
          if (needsLRUEnd) {
            needsLRUEnd = false;
            internalRegion.txLRUEnd();
          }
        } finally {
          if (needsUnlock) {
            needsUnlock = false;
            internalRegion.unlockGII();
          }
        }
      }
    }

    /**
     * Returns the eventId to use for the give farside entry op.
     *
     * @since GemFire 5.7
     */
    private EventID getEventId(FarSideEntryOp entryOp) {
      return msg.getEventId(entryOp.eventOffset);
    }


    /**
     * Apply a single tx entry op on the far side
     */
    @SuppressWarnings("synthetic-access")
    protected void txApplyEntryOp(FarSideEntryOp entryOp, List<EntryEventImpl> pendingCallbacks) {
      if (internalRegion == null) {
        return;
      }
      EventID eventID = getEventId(entryOp);
      boolean isDuplicate = internalRegion.hasSeenEvent(eventID);
      if (internalRegion instanceof PartitionedRegion) {
        /*
         * This happens when we don't have the bucket and are getting adjunct notification
         */
        // No need to release because it is added to pendingCallbacks and they will be released
        // later
        EntryEventImpl eei =
            txCallbackEventFactory.createCallbackEvent(internalRegion, entryOp.op,
                entryOp.key,
                entryOp.value, msg.txIdent, txEvent, getEventId(entryOp), entryOp.callbackArg,
                entryOp.filterRoutingInfo, msg.bridgeContext, null, entryOp.versionTag,
                entryOp.tailKey);
        if (entryOp.filterRoutingInfo != null) {
          eei.setLocalFilterInfo(
              entryOp.filterRoutingInfo.getFilterInfo(internalRegion.getCache().getMyId()));
        }
        if (isDuplicate) {
          eei.setPossibleDuplicate(true);
        }
        if (logger.isDebugEnabled()) {
          logger.debug("invoking transactional callbacks for {} key={} needsUnlock={} event={}",
              entryOp.op, entryOp.key, needsUnlock, eei);
        }
        // we reach this spot because the event is either delivered to this member
        // as an "adjunct" message or because the bucket was being created when
        // the message was sent and already reflects the change caused by this event.
        // In the latter case we need to invoke listeners
        final boolean skipListeners = !isDuplicate;
        eei.setInvokePRCallbacks(!skipListeners);
        pendingCallbacks.add(eei);
        return;
      }
      if (logger.isDebugEnabled()) {
        logger.debug("applying transactional {} key={} needsUnlock={} eventId {} with routing {}",
            entryOp.op, entryOp.key, needsUnlock, getEventId(entryOp),
            entryOp.filterRoutingInfo);
      }
      if (entryOp.versionTag != null) {
        entryOp.versionTag.replaceNullIDs(msg.getSender());
      }
      if (entryOp.op.isDestroy()) {
        internalRegion.txApplyDestroy(entryOp.key, msg.txIdent, txEvent,
            needsUnlock,
            entryOp.op, getEventId(entryOp), entryOp.callbackArg, pendingCallbacks,
            entryOp.filterRoutingInfo, msg.bridgeContext, false /* origin remote */,
            null/* txEntryState */, entryOp.versionTag, entryOp.tailKey);
      } else if (entryOp.op.isInvalidate()) {
        internalRegion.txApplyInvalidate(entryOp.key, Token.INVALID, entryOp.didDestroy,
            msg.txIdent,
            txEvent, false /* localOp */, getEventId(entryOp), entryOp.callbackArg,
            pendingCallbacks, entryOp.filterRoutingInfo, msg.bridgeContext,
            null/* txEntryState */, entryOp.versionTag, entryOp.tailKey);
      } else {
        internalRegion.txApplyPut(entryOp.op, entryOp.key, entryOp.value, entryOp.didDestroy,
            msg.txIdent, txEvent, getEventId(entryOp), entryOp.callbackArg,
            pendingCallbacks, entryOp.filterRoutingInfo, msg.bridgeContext,
            null/* txEntryState */, entryOp.versionTag, entryOp.tailKey);
      }
    }

    /**
     * Apply a single tx entry op on the far side
     */
    @SuppressWarnings("synthetic-access")
    protected void txApplyEntryOpAdjunctOnly(FarSideEntryOp entryOp) {
      if (internalRegion == null) {
        return;
      }
      EventID eventID = getEventId(entryOp);
      boolean isDuplicate = internalRegion.hasSeenEvent(eventID);
      if (internalRegion instanceof PartitionedRegion) {

        PartitionedRegion pr = (PartitionedRegion) internalRegion;
        BucketRegion br = pr.getBucketRegion(entryOp.key);
        Set<InternalDistributedMember> bucketOwners = br.getBucketOwners();
        InternalDistributedMember thisMember = internalRegion.getDistributionManager().getId();
        if (bucketOwners.contains(thisMember)) {
          return;
        }

        /*
         * This happens when we don't have the bucket and are getting adjunct notification
         */
        @Released
        EntryEventImpl eei =
            txCallbackEventFactory.createCallbackEvent(internalRegion, entryOp.op,
                entryOp.key,
                entryOp.value, msg.txIdent, txEvent, getEventId(entryOp), entryOp.callbackArg,
                entryOp.filterRoutingInfo, msg.bridgeContext, null, entryOp.versionTag,
                entryOp.tailKey);
        try {
          if (entryOp.filterRoutingInfo != null) {
            eei.setLocalFilterInfo(
                entryOp.filterRoutingInfo.getFilterInfo(internalRegion.getCache().getMyId()));
          }
          if (isDuplicate) {
            eei.setPossibleDuplicate(true);
          }
          if (logger.isDebugEnabled()) {
            logger.debug("invoking transactional callbacks for {} key={} needsUnlock={} event={}",
                entryOp.op, entryOp.key, needsUnlock, eei);
          }
          // we reach this spot because the event is either delivered to this member
          // as an "adjunct" message or because the bucket was being created when
          // the message was sent and already reflects the change caused by this event.
          // In the latter case we need to invoke listeners
          final boolean skipListeners = !isDuplicate;
          eei.invokeCallbacks(internalRegion, skipListeners, true);
        } finally {
          eei.release();
        }
      }
    }

    boolean isEmpty() {
      return opKeys == null;
    }

    boolean needsAck() {
      return internalRegion.getScope().isDistributedAck();
    }

    void addOp(Object key, TXEntryState entry) {
      if (opKeys == null) {
        opKeys = new ArrayList<>(maxSize);
        opEntries = new ArrayList<>(maxSize);
      }
      opKeys.add(key);
      opEntries.add(entry);
    }


    public boolean isForceFireEvent(DistributionManager dm) {
      LocalRegion r = getRegionByPath(dm, regionPath);
      return !(r instanceof PartitionedRegion) && (r == null || !r
          .isUsedForPartitionedRegionBucket());
    }

    public void fromData(DataInput in, boolean hasShadowKey)
        throws IOException, ClassNotFoundException {
      regionPath = DataSerializer.readString(in);
      parentRegionPath = DataSerializer.readString(in);

      int size = in.readInt();
      if (size > 0) {
        opKeys = new ArrayList<>(size);
        opEntries = new ArrayList<>(size);
        final boolean largeModCount = in.readBoolean();
        memberId = DataSerializer.readObject(in);
        for (int i = 0; i < size; i++) {
          FarSideEntryOp entryOp = new FarSideEntryOp();
          // shadowkey is not being sent to clients
          entryOp.fromData(in, largeModCount, hasShadowKey);
          if (entryOp.versionTag != null && memberId != null) {
            entryOp.versionTag.setMemberID(memberId);
          }
          msg.addFarSideEntryOp(entryOp);
          opKeys.add(entryOp.key);
          opEntries.add(entryOp);
        }
      }
    }

    @Override
    public String toString() {
      StringBuilder result = new StringBuilder(64);
      if (regionPath != null) {
        result.append(regionPath);
      } else {
        result.append(internalRegion.getFullPath());
      }
      if (refCount > 0) {
        result.append(" refCount=").append(refCount);
      }
      return result.toString();
    }

    private void basicToData(DataOutput out,
        SerializationContext context,
        boolean useShadowKey) throws IOException {
      if (internalRegion != null) {
        DataSerializer.writeString(internalRegion.getFullPath(), out);
        if (internalRegion instanceof BucketRegion) {
          DataSerializer.writeString(
              ((Bucket) internalRegion).getPartitionedRegion().getFullPath(), out);
        } else {
          DataSerializer.writeString(null, out);
        }
      } else {
        DataSerializer.writeString(regionPath, out);
        DataSerializer.writeString(parentRegionPath, out);
      }

      if (isEmpty() || opKeys.size() == 0) {
        out.writeInt(0);
      } else {
        int size = opKeys.size();
        out.writeInt(size);

        final boolean largeModCount;
        if (msg.txState != null) {
          largeModCount = msg.txState.needsLargeModCount();
        } else {
          largeModCount = msg.needsLargeModCount;
        }
        out.writeBoolean(largeModCount);

        VersionSource member = memberId;
        if (member == null) {
          if (internalRegion == null) {
            Assert.assertTrue(msg.txState == null);
          } else {
            member = internalRegion.getVersionMember();
          }
        }
        DataSerializer.writeObject(member, out);
        for (int i = 0; i < size; i++) {
          DataSerializer.writeObject(opKeys.get(i), out);
          if (msg.txState != null) {
            /* we are still on tx node and have the entry state */
            ((TXEntryState) opEntries.get(i)).toFarSideData(out, context, largeModCount,
                true, useShadowKey);
          } else {
            ((FarSideEntryOp) opEntries.get(i)).toData(out, largeModCount, true,
                useShadowKey);
          }
        }
      }
    }


    public void toData(DataOutput out, SerializationContext context, boolean useShadowKey)
        throws IOException {
      if (preserializedBuffer != null) {
        preserializedBuffer.rewind();
        preserializedBuffer.sendTo(out);
      } else if (refCount > 1) {
        KnownVersion v = StaticSerialization.getVersionForDataStream(out);
        HeapDataOutputStream hdos = new HeapDataOutputStream(1024, v);
        basicToData(hdos, context, useShadowKey);
        preserializedBuffer = hdos;
        preserializedBuffer.sendTo(out);
      } else {
        basicToData(out, context, useShadowKey);
      }
    }

    /**
     * Holds data that describes a tx entry op on the far side.
     *
     * @since GemFire 5.0
     */
    public class FarSideEntryOp implements Comparable<FarSideEntryOp> {
      public Operation op;
      public int modSerialNum;
      public int eventOffset;
      public Object key;
      public Object value;
      public boolean didDestroy;
      public Object callbackArg;
      private FilterRoutingInfo filterRoutingInfo;
      private VersionTag versionTag;
      private long tailKey;

      /**
       * Create a new representation of a tx entry op on the far side. All init will be done by a
       * call to fromData
       */
      public FarSideEntryOp() {}

      /**
       * Creates and returns a new instance of a tx entry op on the far side. The "toData" that this
       * should match is {@link TXEntryState#toFarSideData}.
       *
       * @param in the data input that is used to read the data for this entry op
       * @param largeModCount true if the mod count is a int instead of a byte.
       * @param readShadowKey true if a long shadowKey should be read
       */
      public void fromData(DataInput in, boolean largeModCount, boolean readShadowKey)
          throws IOException, ClassNotFoundException {
        key = DataSerializer.readObject(in);
        op = Operation.fromOrdinal(in.readByte());
        if (largeModCount) {
          modSerialNum = in.readInt();
        } else {
          modSerialNum = in.readByte();
        }
        callbackArg = DataSerializer.readObject(in);
        filterRoutingInfo = DataSerializer.readObject(in);
        versionTag = DataSerializer.readObject(in);
        if (readShadowKey) {
          tailKey = in.readLong();
        }
        eventOffset = in.readInt();
        if (!op.isDestroy()) {
          didDestroy = in.readBoolean();
          if (!op.isInvalidate()) {
            boolean isTokenOrByteArray = in.readBoolean();
            if (isTokenOrByteArray) {
              // token or byte[]
              value = DataSerializer.readObject(in);
            } else {
              // CachedDeserializable, Object, or PDX
              value = CachedDeserializableFactory.create(DataSerializer.readByteArray(in),
                  GemFireCacheImpl.getInstance());
            }
          }
        }
      }

      public void toData(DataOutput out, boolean largeModCount, boolean sendVersionTag,
          boolean sendShadowKey) throws IOException {
        // DataSerializer.writeObject(this.key,out);
        /* Don't serialize key because caller did that already */

        out.writeByte(op.ordinal);
        if (largeModCount) {
          out.writeInt(modSerialNum);
        } else {
          out.writeByte(modSerialNum);
        }
        DataSerializer.writeObject(callbackArg, out);
        DataSerializer.writeObject(filterRoutingInfo, out);
        if (sendVersionTag) {
          DataSerializer.writeObject(versionTag, out);
        }
        if (sendShadowKey) {
          out.writeLong(tailKey);
        }
        out.writeInt(eventOffset);
        if (!op.isDestroy()) {
          out.writeBoolean(didDestroy);
          if (!op.isInvalidate()) {
            boolean sendObject = Token.isInvalidOrRemoved(value);
            sendObject = sendObject || value instanceof byte[];
            out.writeBoolean(sendObject);
            if (sendObject) {
              DataSerializer.writeObject(value, out);
            } else {
              DataSerializer.writeObjectAsByteArray(value, out);
            }
          }
        }
      }


      /**
       * Performs this entryOp on the farside of a tx commit.
       */
      public void process(List<EntryEventImpl> pendingCallbacks) {
        txApplyEntryOp(this, pendingCallbacks);
      }

      public void processAdjunctOnly() {
        txApplyEntryOpAdjunctOnly(this);
      }

      public RegionCommit getRegionCommit() {
        return RegionCommit.this;
      }

      /**
       * Returns the value to use to sort us
       */
      private int getSortValue() {
        return modSerialNum;
      }

      @Override
      public int compareTo(FarSideEntryOp o) {
        return getSortValue() - o.getSortValue();
      }

      @Override
      public boolean equals(Object o) {
        if (!(o instanceof FarSideEntryOp)) {
          return false;
        }
        return compareTo((FarSideEntryOp) o) == 0;
      }

      @Override
      public int hashCode() {
        return getSortValue();
      }
    }
  }

  Object getTrackerKey() {
    if (lockId != null) {
      return lockId;
    } else {
      return txIdent;
    }
  }

  /**
   * Used to prevent processing of the message if we have reported to other FarSiders that we did
   * not received the CommitProcessMessage
   */
  boolean dontProcess() {
    return dontProcess;
  }

  /**
   * Indicate that this message should not be processed if we receive CommitProcessMessage (late)
   */
  void setDontProcess() {
    dontProcess = true;
  }

  boolean isProcessing() {
    return isProcessing;
  }

  private void setIsProcessing(boolean isProcessing) {
    this.isProcessing = isProcessing;
  }

  boolean wasProcessed() {
    return wasProcessed;
  }

  void setProcessed(boolean wasProcessed) {
    this.wasProcessed = wasProcessed;
  }

  /**
   * The CommitProcessForLockIDMessaage is sent by the Distributed ACK TX origin to the recipients
   * (aka FarSiders) to indicate that a previously received RegionCommit that contained a lockId
   * should commence processing.
   */
  public static class CommitProcessForLockIdMessage extends CommitProcessMessage {
    private TXLockId lockId;

    public CommitProcessForLockIdMessage() {
      // Zero arg constructor for DataSerializer
    }

    public CommitProcessForLockIdMessage(TXLockId lockId) {
      this.lockId = lockId;
      Assert.assertTrue(this.lockId != null,
          "CommitProcessForLockIdMessage must have a non-null lockid!");
    }

    @Override
    protected void process(ClusterDistributionManager dm) {
      final TXCommitMessage mess = waitForMessage(lockId, dm);
      Assert.assertTrue(mess != null, "Commit data for TXLockId: " + lockId + " not found");
      basicProcess(mess, dm);
    }

    @Override
    public int getDSFID() {
      return COMMIT_PROCESS_FOR_LOCKID_MESSAGE;
    }

    @Override
    public void toData(DataOutput out,
        SerializationContext context) throws IOException {
      InternalDataSerializer.invokeToData(lockId, out);
    }

    @Override
    public void fromData(DataInput in,
        DeserializationContext context) throws IOException, ClassNotFoundException {
      lockId = TXLockIdImpl.createFromData(in);
      Assert.assertTrue(lockId != null,
          "CommitProcessForLockIdMessage must have a non-null lockid!");
    }

    @Override
    public String toString() {
      return "CommitProcessForLockIdMessage@" + System.identityHashCode(this)
          + " lockId=" + lockId;
    }
  }

  /**
   * The CommitProcessForTXIdMessaage is sent by the Distributed ACK TX origin to the recipients
   * (aka FarSiders) to indicate that a previously received RegionCommit that contained a TXId
   * should commence processing. RegionCommit messages that contain a TXId (and no TXLockId) are
   * typically sent if all the TX changes are a result of load/netsearch/netload values (thus no
   * lockid)
   */
  public static class CommitProcessForTXIdMessage extends CommitProcessMessage {
    private TXId txId;

    public CommitProcessForTXIdMessage() {
      // Zero arg constructor for DataSerializer
    }

    public CommitProcessForTXIdMessage(TXId txId) {
      this.txId = txId;
      Assert.assertTrue(this.txId != null,
          "CommitProcessMessageForTXId must have a non-null txid!");
    }

    @Override
    protected void process(ClusterDistributionManager dm) {
      final TXCommitMessage mess = waitForMessage(txId, dm);
      Assert.assertTrue(mess != null, "Commit data for TXId: " + txId + " not found");
      basicProcess(mess, dm);
    }

    @Override
    public int getDSFID() {
      return COMMIT_PROCESS_FOR_TXID_MESSAGE;
    }

    @Override
    public void toData(DataOutput out,
        SerializationContext context) throws IOException {
      InternalDataSerializer.invokeToData(txId, out);
    }

    @Override
    public void fromData(DataInput in,
        DeserializationContext context) throws IOException, ClassNotFoundException {
      txId = TXId.createFromData(in);
      Assert.assertTrue(txId != null,
          "CommitProcessMessageForTXId must have a non-null txid!");
    }

    @Override
    public String toString() {
      return "CommitProcessForTXIdMessage@" + System.identityHashCode(this)
          + " txId=" + txId;
    }
  }

  public abstract static class CommitProcessMessage extends PooledDistributionMessage {
    protected void basicProcess(final TXCommitMessage mess, final ClusterDistributionManager dm) {
      dm.removeMembershipListener(mess);
      synchronized (mess) {
        if (mess.dontProcess()) {
          return;
        }
      }
      try {
        mess.basicProcess();
      } finally {
        txTracker.processed(mess);
      }
    }
  }

  /**
   * The CommitProcessQueryMessage is used to attempt to recover - in the Distributed ACK TXs - when
   * the origin of the CommitProcess messages departed from the distributed system. The sender of
   * this message is attempting to query other potential fellow FarSiders (aka recipients) who may
   * have received the CommitProcess message.
   *
   * Since the occurance of this message will be rare (hopefully), it was decided to be general
   * about the the tracker key - opting not to have specific messages for each type like
   * CommitProcessFor<Lock/TX>Id - and take the performance penalty of an extra call to
   * DataSerializer
   */
  public static class CommitProcessQueryMessage extends PooledDistributionMessage {
    private Object trackerKey; // Either a TXLockId or a TXId
    private int processorId;

    public CommitProcessQueryMessage() {
      // Zero arg constructor for DataSerializer
    }

    public CommitProcessQueryMessage(Object trackerKey, int processorId) {
      this.trackerKey = trackerKey;
      this.processorId = processorId;
    }

    @Override
    protected void process(ClusterDistributionManager dm) {
      final boolean processMsgReceived = txTracker.commitProcessReceived(trackerKey);
      if (!processMsgReceived) {
        if (logger.isDebugEnabled()) {
          logger.debug("CommitProcessQuery did not find {} in the history", trackerKey);
        }
      }

      // Reply to the fellow FarSider as to whether the
      // CommitProcess message was received
      CommitProcessQueryReplyMessage resp = new CommitProcessQueryReplyMessage(processMsgReceived);
      resp.setProcessorId(processorId);
      resp.setRecipient(getSender());
      dm.putOutgoing(resp);
    }

    @Override
    public void toData(DataOutput out,
        SerializationContext context) throws IOException {
      DataSerializer.writeObject(trackerKey, out);
      out.writeInt(processorId);
    }

    @Override
    public int getDSFID() {
      return COMMIT_PROCESS_QUERY_MESSAGE;
    }

    @Override
    public void fromData(DataInput in,
        DeserializationContext context) throws IOException, ClassNotFoundException {
      trackerKey = DataSerializer.readObject(in);
      processorId = in.readInt();
    }

    @Override
    public String toString() {
      return "CommitProcessQueryMessage@" + System.identityHashCode(this)
          + " trackerKeyClass=" + trackerKey.getClass().getName()
          + " trackerKey=" + trackerKey + " processorId="
          + processorId;
    }
  }

  /********************* Commit Process Query Response Message **********************************/
  public static class CommitProcessQueryReplyMessage extends ReplyMessage {
    private boolean wasReceived;

    public CommitProcessQueryReplyMessage(boolean wasReceived) {
      this.wasReceived = wasReceived;
    }

    public CommitProcessQueryReplyMessage() {
      // zero arg constructor for DataSerializer
    }

    public boolean wasReceived() {
      return wasReceived;
    }

    @Override
    public int getDSFID() {
      return COMMIT_PROCESS_QUERY_REPLY_MESSAGE;
    }

    @Override
    public void fromData(DataInput in,
        DeserializationContext context) throws IOException, ClassNotFoundException {
      super.fromData(in, context);
      wasReceived = in.readBoolean();
    }

    @Override
    public void toData(DataOutput out,
        SerializationContext context) throws IOException {
      super.toData(out, context);
      out.writeBoolean(wasReceived);
    }

    @Override
    public String toString() {
      return "CommitProcessQueryReplyMessage@" + System.identityHashCode(this)
          + " wasReceived=" + wasReceived + " processorId="
          + processorId + " from " + getSender();
    }
  }

  /********************* Commit Process Query Response Processor *********************************/
  public static class CommitProcessQueryReplyProcessor extends ReplyProcessor21 {
    public boolean receivedOnePositive;

    CommitProcessQueryReplyProcessor(DistributionManager dm,
        Set<InternalDistributedMember> members) {
      super(dm, members);
      receivedOnePositive = false;
    }

    @Override
    public void process(DistributionMessage msg) {
      CommitProcessQueryReplyMessage ccMess = (CommitProcessQueryReplyMessage) msg;
      if (ccMess.wasReceived()) {
        receivedOnePositive = true;
      }
      super.process(msg);
    }

    @Override
    protected boolean canStopWaiting() {
      return receivedOnePositive;
    }

    public boolean receivedACommitProcessMessage() {
      return receivedOnePositive;
    }
  }

  /********************* MembershipListener Implementation ***************************************/
  @Override
  public void memberJoined(DistributionManager distributionManager, InternalDistributedMember id) {
    // do nothing
  }

  @Override
  public void memberSuspect(DistributionManager distributionManager, InternalDistributedMember id,
      InternalDistributedMember whoSuspected, String reason) {}

  @Override
  public void quorumLost(DistributionManager distributionManager,
      Set<InternalDistributedMember> failures,
      List<InternalDistributedMember> remaining) {}

  /**
   * return true if the member initiating this transaction has left the cluster
   */
  public boolean isDepartureNoticed() {
    return departureNoticed;
  }

  private void doOriginDepartedCommit() {
    try {
      // Set processor to zero to avoid the ack to the now departed origin
      processorId = 0;
      basicProcess();
    } finally {
      txTracker.processed(this);
    }
  }

  @Override
  public void memberDeparted(DistributionManager distributionManager,
      final InternalDistributedMember id, boolean crashed) {

    if (!getSender().equals(id)) {
      return;
    }
    distributionManager.removeMembershipListener(this);

    synchronized (this) {
      if (isProcessing() || departureNoticed) {
        if (logger.isDebugEnabled()) {
          logger.debug("Member departed: Commit data is already being processed for lockid: {}",
              lockId);
        }
        return;
      }
      departureNoticed = true;
    }

    // Send message to fellow FarSiders (aka recipients), if any, to
    // determine if any one of them have received a CommitProcessMessage
    if (getFarSiders() != null && !getFarSiders().isEmpty()) {
      if (logger.isDebugEnabled()) {
        logger.debug(
            "Member departed: {} sending query for CommitProcess message to other recipients.", id);
      }

      // Create a new thread, send the CommitProcessQuery, wait for a response and potentially
      // process
      // Should I use a thread pool?, Darrel suggests look in DM somewhere or introduce a zero
      // sized thread pool
      Thread fellowFarSidersQuery = new LoggingThread("CommitProcessQuery Thread",
          () -> doCommitProcessQuery(id));
      fellowFarSidersQuery.start();
    } else {
      if (logger.isDebugEnabled()) {
        logger.debug("Member departed: {}. Processing commit data.", getSender());
      }

      // Optimimal case where we are the only FarSider, assume we
      // will never get the CommitProcess message, but it
      // doesn't matter since we can commit anyway.
      // Start a new thread to process the commit
      Thread originDepartedCommit = new LoggingThread("Origin Departed Commit",
          this::doOriginDepartedCommit);
      originDepartedCommit.start();
    }
  }

  HashSet<InternalDistributedMember> getFarSiders() {
    return farSiders;
  }

  DistributionManager getDistributionManager() {
    return dm;
  }

  void doCommitProcessQuery(final InternalDistributedMember id) {
    CommitProcessQueryReplyProcessor replyProcessor = createReplyProcessor();
    CommitProcessQueryMessage queryMessage = createQueryMessage(replyProcessor);
    queryMessage.setRecipients(farSiders);
    getDistributionManager().putOutgoing(queryMessage);
    // Wait for any one positive response or all negative responses.
    // (while() loop removed for bug 36983 - you can't loop on waitForReplies()
    getDistributionManager().getCancelCriterion().checkCancelInProgress(null);
    try {
      replyProcessor.waitForRepliesUninterruptibly();
    } catch (ReplyException e) {
      e.handleCause();
    }
    if (replyProcessor.receivedACommitProcessMessage()) {
      if (logger.isDebugEnabled()) {
        logger.debug(
            "Transaction associated with lockID: {} from orign {} is processing due to a received \"commit process\" message",
            lockId, id);
      }

      try {
        // Set processor to zero to avoid the ack to the now departed origin
        processorId = 0;
        basicProcess();
      } finally {
        txTracker.processed(this);
      }
    } else {
      if (logger.isDebugEnabled()) {
        logger.debug(
            "Transaction associated with lockID: {} from origin {} ignored.  No other recipients received \"commit process\" message",
            lockId, id);
      }
      txTracker.removeMessage(this);
    }
  }

  CommitProcessQueryReplyProcessor createReplyProcessor() {
    return new CommitProcessQueryReplyProcessor(dm, farSiders);
  }

  CommitProcessQueryMessage createQueryMessage(CommitProcessQueryReplyProcessor replyProcessor) {
    return new CommitProcessQueryMessage(getTrackerKey(), replyProcessor.getProcessorId());
  }

  void setUpdateLockMembers() {
    lockNeedsUpdate = true;
  }

  /**
   * Intended to be called after TXState.applyChanges when the potential for a different set of TX
   * members has been determined and it is safe to ignore any new members because the changes have
   * been applied to committed state. This was added as the solution to bug 32999 and the recovery
   * when the TXLock Lessor (the sending VM) crashes/departs before or while sending the
   * TXCommitMessage.
   *
   * @see TXState#commit()
   * @see org.apache.geode.internal.cache.locks.TXLockBatch#getBatchId()
   */
  private void updateLockMembers() {
    if (lockNeedsUpdate && lockId != null) {
      TXLockService.createDTLS(dm.getSystem()).updateParticipants(lockId,
          msgMap.keySet());
    }
  }

  /**
   * Reply processor which collects all CommitReplyExceptions and emits a detailed failure exception
   * if problems occur
   *
   * @since GemFire 5.7
   */
  private class CommitReplyProcessor extends ReliableReplyProcessor21 {
    private final Map<InternalDistributedMember, RegionCommitList> msgMap;

    public CommitReplyProcessor(DistributionManager dm, Set<InternalDistributedMember> initMembers,
        Map<InternalDistributedMember, RegionCommitList> msgMap) {
      super(dm, initMembers);
      this.msgMap = msgMap;
    }

    public void waitForCommitCompletion() {
      try {
        waitForRepliesUninterruptibly();
      } catch (CommitExceptionCollectingException e) {
        e.handlePotentialCommitFailure(msgMap);
      }
    }

    @Override
    protected synchronized void processException(DistributionMessage msg, ReplyException ex) {
      if (msg instanceof ReplyMessage) {
        synchronized (this) {
          if (exception == null) {
            // Exception Container
            exception = new CommitExceptionCollectingException(txIdent);
          }
          CommitExceptionCollectingException cce =
              (CommitExceptionCollectingException) exception;
          if (ex instanceof CommitReplyException) {
            CommitReplyException cre = (CommitReplyException) ex;
            cce.addExceptionsFromMember(msg.getSender(), cre.getExceptions());
          } else {
            cce.addExceptionsFromMember(msg.getSender(), Collections.singleton(ex));
          }
        }
      }
    }

    @Override
    protected boolean stopBecauseOfExceptions() {
      return false;
    }

    public Set<InternalDistributedMember> getCacheClosedMembers() {
      if (exception != null) {
        CommitExceptionCollectingException cce =
            (CommitExceptionCollectingException) exception;
        return cce.getCacheClosedMembers();
      } else {
        return Collections.emptySet();
      }
    }

    public Set<InternalDistributedMember> getRegionDestroyedMembers(String regionFullPath) {
      if (exception != null) {
        CommitExceptionCollectingException cce =
            (CommitExceptionCollectingException) exception;
        return cce.getRegionDestroyedMembers(regionFullPath);
      } else {
        return Collections.emptySet();
      }
    }
  }

  /**
   * An Exception that collects many remote CommitExceptions
   *
   * @since GemFire 5.7
   */
  public static class CommitExceptionCollectingException extends ReplyException {
    private static final long serialVersionUID = 589384721273797822L;
    /**
     * Set of members that threw CacheClosedExceptions
     */
    private final Set<InternalDistributedMember> cacheExceptions = new HashSet<>();
    /**
     * key=region path, value=Set of members
     */
    private final Map<String, Set<InternalDistributedMember>> regionExceptions = new HashMap<>();
    /**
     * List of exceptions that were unexpected and caused the tx to fail
     */
    private final Map<InternalDistributedMember, List<Exception>> fatalExceptions = new HashMap<>();

    private final TXId id;

    public CommitExceptionCollectingException(TXId txIdent) {
      id = txIdent;
    }

    /**
     * Determine if the commit processing was incomplete, if so throw a detailed exception
     * indicating the source of the problem
     */
    public void handlePotentialCommitFailure(
        Map<InternalDistributedMember, RegionCommitList> msgMap) {
      if (fatalExceptions.size() > 0) {
        StringBuilder errorMessage = new StringBuilder("Incomplete commit of transaction ")
            .append(id).append(".  Caused by the following exceptions: ");
        for (final Map.Entry<InternalDistributedMember, List<Exception>> me : fatalExceptions
            .entrySet()) {
          InternalDistributedMember mem = me.getKey();
          errorMessage.append(" From member: ").append(mem).append(" ");
          List<Exception> exceptions = me.getValue();
          for (Iterator<Exception> ei = exceptions.iterator(); ei.hasNext();) {
            Exception e = ei.next();
            errorMessage.append(e);
            for (StackTraceElement ste : e.getStackTrace()) {
              errorMessage.append("\n\tat ").append(ste);
            }
            if (ei.hasNext()) {
              errorMessage.append("\nAND\n");
            }
          }
          errorMessage.append(".");
        }
        throw new CommitIncompleteException(errorMessage.toString());
      }

      // Mark any persistent members as offline
      handleClosedMembers(msgMap);
      handleRegionDestroyed(msgMap);
    }

    /**
     * Mark peers as offline for regions that the peer returned a RegionDestroyedException
     */
    private void handleRegionDestroyed(Map<InternalDistributedMember, RegionCommitList> msgMap) {
      if (regionExceptions.isEmpty()) {
        return;
      }

      for (Map.Entry<InternalDistributedMember, RegionCommitList> memberMap : msgMap.entrySet()) {
        InternalDistributedMember member = memberMap.getKey();
        RegionCommitList rcl = memberMap.getValue();
        for (RegionCommit region : rcl) {
          Set<InternalDistributedMember> failedMembers =
              regionExceptions.get(region.internalRegion.getFullPath());
          if (failedMembers != null && failedMembers.contains(member)) {
            markMemberOffline(member, region);
          }
        }
      }

    }

    /**
     * Mark peers as offline that returned a cache closed exception
     */
    private void handleClosedMembers(Map<InternalDistributedMember, RegionCommitList> msgMap) {
      for (InternalDistributedMember member : getCacheClosedMembers()) {
        RegionCommitList rcl = msgMap.get(member);

        for (RegionCommit region : rcl) {
          markMemberOffline(member, region);
        }
      }
    }

    private void markMemberOffline(InternalDistributedMember member, RegionCommit region) {
      if (region.persistentIds == null) {
        return;
      }

      PersistentMemberID persistentId = region.persistentIds.get(member);
      /// iterate over the list and mark the members offline
      if (persistentId != null) {
        // Fix for bug 42142 - In order for recovery to work,
        // we must either
        // 1) persistent the region operation successfully on the peer
        // 2) record that the peer is offline
        // or
        // 3) fail the operation

        // if we have started to shutdown, we don't want to mark the peer
        // as offline, or we will think we have newer data when in fact we don't
        region.internalRegion.getCancelCriterion().checkCancelInProgress(null);

        // Otherwise, mark the peer as offline, because it didn't complete
        // the operation.
        ((DistributedRegion) region.internalRegion).getPersistenceAdvisor().markMemberOffline(
            member,
            persistentId);
      }
    }

    public Set<InternalDistributedMember> getCacheClosedMembers() {
      return cacheExceptions;
    }

    public Set<InternalDistributedMember> getRegionDestroyedMembers(String regionFullPath) {
      Set<InternalDistributedMember> members = regionExceptions.get(regionFullPath);
      if (members == null) {
        members = Collections.emptySet();
      }
      return members;
    }

    /**
     * Protected by (this)
     */
    public void addExceptionsFromMember(InternalDistributedMember member,
        Set<Exception> exceptions) {
      for (final Exception ex : exceptions) {
        if (ex instanceof CancelException) {
          cacheExceptions.add(member);
        } else if (ex instanceof RegionDestroyedException) {
          final String r = ((RegionDestroyedException) ex).getRegionFullPath();
          regionExceptions.computeIfAbsent(r, k -> new HashSet<>()).add(member);
        } else {
          fatalExceptions.computeIfAbsent(member, k -> new ArrayList<>(2)).add(ex);
        }
      }
    }
  }

  public void hookupRegions(DistributionManager dm) {
    if (regions != null) {
      for (final RegionCommit rc : regions) {
        rc.hookupRegion(dm);
      }
    }

  }


  /**
   * Disable firing of TX Listeners. Currently on used on clients.
   *
   * @param disableListeners disable the listeners
   */
  public void setDisableListeners(boolean disableListeners) {
    this.disableListeners = disableListeners;
  }

  public KnownVersion getClientVersion() {
    return clientVersion;
  }

  public void setClientVersion(KnownVersion clientVersion) {
    this.clientVersion = clientVersion;
  }

}

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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.DataSerializer;
import org.apache.geode.SystemFailure;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.CacheRuntimeException;
import org.apache.geode.cache.CommitDistributionException;
import org.apache.geode.cache.CommitIncompleteException;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.RegionDistributionException;
import org.apache.geode.cache.TransactionId;
import org.apache.geode.cache.TransactionListener;
import org.apache.geode.distributed.DistributedMember;
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
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.locks.TXLockId;
import org.apache.geode.internal.cache.locks.TXLockIdImpl;
import org.apache.geode.internal.cache.locks.TXLockService;
import org.apache.geode.internal.cache.partitioned.Bucket;
import org.apache.geode.internal.cache.persistence.PersistentMemberID;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.versions.VersionSource;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.LoggingThreadGroup;
import org.apache.geode.internal.logging.log4j.LocalizedMessage;
import org.apache.geode.internal.offheap.annotations.Released;

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
  protected static final TXFarSideCMTracker txTracker = new TXFarSideCMTracker((60 * 1092));

  private ArrayList regions; // list of RegionCommit instances
  protected TXId txIdent;
  protected int processorId; // 0 unless needsAck is true
  protected TXLockIdImpl lockId;
  protected HashSet farSiders;

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
  protected transient ArrayList farSideEntryOps;
  private transient byte[] farsideBaseMembershipId; // only available on farside
  private transient long farsideBaseThreadId; // only available on farside
  private transient long farsideBaseSequenceId; // only available on farside

  /**
   * (Nearside) true of any regions in this TX have required roles
   */
  private transient boolean hasReliableRegions = false;

  /**
   * Set of all caching exceptions produced hile processing this tx
   */
  private transient Set processingExceptions = Collections.emptySet();

  private transient ClientProxyMembershipID bridgeContext = null;

  /**
   * Version of the client that this TXCommitMessage is being sent to. Used for backwards
   * compatibility
   */
  private transient Version clientVersion;

  /**
   * A token to be put in TXManagerImpl#failoverMap to represent a CommitConflictException while
   * committing a transaction
   */
  public static final TXCommitMessage CMT_CONFLICT_MSG = new TXCommitMessage();
  /**
   * A token to be put in TXManagerImpl#failoverMap to represent a
   * TransactionDataNodeHasDepartedException
   * while committing a transaction
   */
  public static final TXCommitMessage REBALANCE_MSG = new TXCommitMessage();
  /**
   * A token to be put in TXManagerImpl#failoverMap to represent an exception while committing a
   * transaction
   */
  public static final TXCommitMessage EXCEPTION_MSG = new TXCommitMessage();
  /**
   * A token to be put in TXManagerImpl#failoverMap to represent a rolled back transaction
   */
  public static final TXCommitMessage ROLLBACK_MSG = new TXCommitMessage();

  public TXCommitMessage(TXId txIdent, DistributionManager dm, TXState txState) {
    this.dm = dm;
    this.txIdent = txIdent;
    this.lockId = null;
    this.regions = null;
    this.txState = txState;
    this.wasProcessed = false;
    this.isProcessing = false;
    this.dontProcess = false;
    this.farSiders = null;
    this.bridgeContext = txState.bridgeContext;
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
    return new EventID(this.farsideBaseMembershipId, this.farsideBaseThreadId,
        this.farsideBaseSequenceId + eventOffset);
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
    this.currentRegion = new RegionCommit(this, r, maxSize);
    if (r.requiresReliabilityCheck()) {
      this.hasReliableRegions = true;
    }
  }

  void finishRegion(Set<InternalDistributedMember> s) {
    // make sure we have some changes and someone to send them to
    if (!this.currentRegion.isEmpty() && s != null && !s.isEmpty()) {
      // Get the persistent ids for the current region and save them
      this.currentRegion.persistentIds = getPersistentIds(this.currentRegion.internalRegion);

      if (this.msgMap == null) {
        this.msgMap = new HashMap<>();
      }
      {
        RegionCommitList newRCL = null;
        Iterator<InternalDistributedMember> it = s.iterator();
        while (it.hasNext()) {
          InternalDistributedMember recipient = it.next();

          if (!this.dm.getDistributionManagerIds().contains(recipient)) {
            if (logger.isDebugEnabled()) {
              logger.debug("Skipping member {} due to dist list absence", recipient);
            }
            // skip this guy since the dm no longer knows about him
            continue;
          }
          RegionCommitList rcl = this.msgMap.get(recipient);
          if (rcl == null) {
            if (newRCL == null) {
              rcl = new RegionCommitList();
              rcl.add(this.currentRegion);
              newRCL = rcl;
            } else {
              rcl = newRCL;
            }
            this.msgMap.put(recipient, rcl);
          } else if (rcl.get(rcl.size() - 1) != this.currentRegion) {
            rcl.add(this.currentRegion);
          }
        }
      }

      // Now deal with each existing recipient that does not care
      // about this region
      Iterator<Map.Entry<InternalDistributedMember, RegionCommitList>> it =
          this.msgMap.entrySet().iterator();
      while (it.hasNext()) {
        Map.Entry<InternalDistributedMember, RegionCommitList> me = it.next();
        if (!s.contains(me.getKey())) {
          RegionCommitList rcl = me.getValue();
          RegionCommitList trimmedRcl = rcl.trim(this.currentRegion);
          if (trimmedRcl != rcl) {
            me.setValue(trimmedRcl);
          }
        }
      }
    }
    this.currentRegion = null;
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
    if (!this.currentRegion.isEmpty()) {
      {
        if (this.regions == null) {
          this.regions = new RegionCommitList();
        }
        this.regions.add(this.currentRegion);
      }
    }
    this.currentRegion = null;
  }

  Map viewVersions = new HashMap();

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
    for (Iterator it = viewVersions.entrySet().iterator(); it.hasNext();) {
      Map.Entry e = (Map.Entry) it.next();
      DistributedRegion dr = (DistributedRegion) e.getKey();
      Long viewVersion = (Long) e.getValue();
      // need to continue the iteration if one of the regions is destroyed
      // since others may still be okay
      try {
        long newv = dr.getDistributionAdvisor().endOperation(viewVersion);
      } catch (RuntimeException ex) {
        rte = ex;
      }
    }
    if (rte != null) {
      throw rte;
    }
  }

  private boolean isEmpty() {
    return this.msgMap == null || this.msgMap.isEmpty();
  }

  void addOp(InternalRegion r, Object key, TXEntryState entry, Set otherRecipients) {
    this.currentRegion.addOp(key, entry);
  }

  void send(TXLockId lockId) {
    if (isEmpty()) {
      if (logger.isDebugEnabled()) {
        logger.debug("empty transaction - nothing to distribute");
      }
      return;
    }
    Assert.assertTrue(this.txState != null, "Send must have transaction state.");
    this.lockId = (TXLockIdImpl) lockId;
    updateLockMembers();

    IdentityHashMap distMap = new IdentityHashMap(); // Map of RegionCommitList keys to Sets of
    // receivers
    HashSet ackReceivers = null;
    {
      Iterator it = this.msgMap.entrySet().iterator();
      while (it.hasNext()) {
        Map.Entry me = (Map.Entry) it.next();
        RegionCommitList rcl = (RegionCommitList) me.getValue();
        if (rcl.getNeedsAck()) {
          if (ackReceivers == null) {
            ackReceivers = new HashSet();
          }
          ackReceivers.add(me.getKey());
        }
        HashSet receivers = (HashSet) distMap.get(rcl);
        if (receivers == null) {
          receivers = new HashSet();
          distMap.put(rcl, receivers);
        }
        receivers.add(me.getKey());
      }
    }

    CommitReplyProcessor processor = null;
    {
      if (ackReceivers != null) {
        processor = new CommitReplyProcessor(this.dm, ackReceivers, msgMap);
        if (ackReceivers.size() > 1) {
          this.farSiders = ackReceivers;
        }
        processor.enableSevereAlertProcessing();
      }
      {
        Iterator it = distMap.entrySet().iterator();
        while (it.hasNext()) {
          Map.Entry me = (Map.Entry) it.next();
          RegionCommitList rcl = (RegionCommitList) me.getKey();
          HashSet recipients = (HashSet) me.getValue();
          // now remove from the recipients any guys that the dm no
          // longer knows about
          recipients.retainAll(this.dm.getDistributionManagerIds());
          if (!recipients.isEmpty()) {
            if (this.txState.internalDuringIndividualSend != null) {
              // Run in test mode, splitting out individaual recipients,
              // so we can control who gets what
              Iterator indivRecip = recipients.iterator();
              while (indivRecip.hasNext()) {
                this.txState.internalDuringIndividualSend.run();
                setRecipientsSendData(Collections.singleton(indivRecip.next()), processor, rcl);
              }
            } else {
              // Run in normal mode sending to mulitiple recipients in
              // one shot
              setRecipientsSendData(recipients, processor, rcl);
            }
          }
        }
      }
      if (this.txState.internalAfterIndividualSend != null) {
        this.txState.internalAfterIndividualSend.run();
      }
    }

    if (processor != null) {
      // Send the CommitProcessMessage
      final CommitProcessMessage cpMsg;
      if (this.lockId != null) {
        cpMsg = new CommitProcessForLockIdMessage(this.lockId);
      } else {
        cpMsg = new CommitProcessForTXIdMessage(this.txIdent);
      }
      if (this.txState.internalDuringIndividualCommitProcess != null) {
        // Run in test mode
        Iterator<InternalDistributedMember> indivRecip = ackReceivers.iterator();
        while (indivRecip.hasNext()) {
          this.txState.internalDuringIndividualCommitProcess.run();
          cpMsg.setRecipients(Collections.<InternalDistributedMember>singleton(indivRecip.next()));
          this.dm.putOutgoing(cpMsg);
          cpMsg.resetRecipients();
        }
      } else {
        // Run in normal mode
        cpMsg.setRecipients(ackReceivers);
        this.dm.putOutgoing(cpMsg);
      }

      if (this.txState.internalAfterIndividualCommitProcess != null) {
        // Testing callback
        this.txState.internalAfterIndividualCommitProcess.run();
      }

      // for() loop removed for bug 36983 - you can't loop on waitForReplies()
      dm.getCancelCriterion().checkCancelInProgress(null);
      processor.waitForCommitCompletion();
      this.dm.getStats().incCommitWaits();
    }
    if (this.hasReliableRegions) {
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
  private void checkDistributionReliability(Map distMap, CommitReplyProcessor processor) {
    // key=RegionCommit, value=Set of recipients
    Map regionToRecipients = new IdentityHashMap();

    // build up the keys in regionToRecipients and add all receivers
    for (Iterator distIter = distMap.entrySet().iterator(); distIter.hasNext();) {
      Map.Entry me = (Map.Entry) distIter.next();
      RegionCommitList rcl = (RegionCommitList) me.getKey();
      Set recipients = (Set) me.getValue();

      for (Iterator rclIter = rcl.iterator(); rclIter.hasNext();) {
        RegionCommit rc = (RegionCommit) rclIter.next();
        // skip region if no required roles
        if (!rc.internalRegion.requiresReliabilityCheck()) {
          continue;
        }

        Set recipientsForRegion = (Set) regionToRecipients.get(rc);
        if (recipientsForRegion == null) {
          recipientsForRegion = new HashSet();
          regionToRecipients.put(rc, recipientsForRegion);
        }

        // get the receiver Set for rcl and perform addAll
        if (recipients != null) {
          recipientsForRegion.addAll(recipients);
        }
      }
    }

    Set cacheClosedMembers =
        (processor == null) ? Collections.emptySet() : processor.getCacheClosedMembers();
    Set departedMembers =
        (processor == null) ? Collections.emptySet() : processor.getDepartedMembers();

    // check reliability on each region
    Set regionDistributionExceptions = Collections.emptySet();
    Set failedRegionNames = Collections.emptySet();
    for (Iterator iter = regionToRecipients.entrySet().iterator(); iter.hasNext();) {
      Map.Entry me = (Map.Entry) iter.next();
      final RegionCommit rc = (RegionCommit) me.getKey();

      final Set successfulRecipients = new HashSet(msgMap.keySet());
      successfulRecipients.removeAll(departedMembers);

      // remove members who destroyed that region or closed their cache
      Set regionDestroyedMembers = (processor == null) ? Collections.emptySet()
          : processor.getRegionDestroyedMembers(rc.internalRegion.getFullPath());

      successfulRecipients.removeAll(cacheClosedMembers);
      successfulRecipients.removeAll(regionDestroyedMembers);

      try {
        rc.internalRegion.handleReliableDistribution(successfulRecipients);
      } catch (RegionDistributionException e) {
        if (regionDistributionExceptions == Collections.emptySet()) {
          regionDistributionExceptions = new HashSet();
          failedRegionNames = new HashSet();
        }
        regionDistributionExceptions.add(e);
        failedRegionNames.add(rc.internalRegion.getFullPath());
      }
    }

    if (!regionDistributionExceptions.isEmpty()) {
      throw new CommitDistributionException(
          LocalizedStrings.TXCommitMessage_THESE_REGIONS_EXPERIENCED_RELIABILITY_FAILURE_DURING_DISTRIBUTION_OF_THE_OPERATION_0
              .toLocalizedString(failedRegionNames),
          regionDistributionExceptions);
    }
  }

  /**
   * Helper method for send
   */
  private void setRecipientsSendData(Set recipients, ReplyProcessor21 processor,
      RegionCommitList rcl) {
    setRecipients(recipients);
    this.regions = rcl;
    if (rcl.getNeedsAck()) {
      this.processorId = processor.getProcessorId();
    } else {
      this.processorId = 0;
    }
    this.dm.getStats().incSentCommitMessages(1L);
    this.sequenceNum++;
    this.dm.putOutgoing(this);
    resetRecipients();
  }

  @Override
  protected void process(ClusterDistributionManager dm) {
    this.dm = dm;
    // Remove this node from the set of recipients
    if (this.farSiders != null) {
      this.farSiders.remove(dm.getId());
    }

    if (this.processorId != 0) {
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
    this.farSideEntryOps.add(entryOp);
  }

  protected void addProcessingException(Exception e) {
    // clear all previous exceptions if e is a CacheClosedException
    if (this.processingExceptions == Collections.emptySet() || e instanceof CancelException) {
      this.processingExceptions = new HashSet();
    }
    this.processingExceptions.add(e);
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
      logger.debug("begin processing TXCommitMessage for {}", this.txIdent);
    }
    final int oldLevel =
        LocalRegion.setThreadInitLevelRequirement(LocalRegion.BEFORE_INITIAL_IMAGE);
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
        txEvent = new TXRmtEvent(this.txIdent, cache);
      }
      try {
        // Pre-process each Region in the tx
        try {
          Iterator it = this.regions.iterator();
          while (it.hasNext()) {
            boolean failedBeginProcess = true;
            RegionCommit rc = (RegionCommit) it.next();
            try {
              failedBeginProcess = !rc.beginProcess(dm, this.txIdent, txEvent);
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
        } finally { // fix for bug 40001
          // post-process each Region in the tx
          Iterator it = this.regions.iterator();
          while (it.hasNext()) {
            try {
              RegionCommit rc = (RegionCommit) it.next();
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
          for (int i = 0; i < tls.length; i++) {
            try {
              tls[i].afterCommit(txEvent);
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
                      LocalizedStrings.TXCommitMessage_EXCEPTION_OCCURRED_IN_TRANSACTIONLISTENER),
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
      if (!dm.getExistingCache().isClient()) {
        getTracker().saveTXForClientFailover(txIdent, this);
      }
      if (logger.isDebugEnabled()) {
        logger.debug("completed processing TXCommitMessage for {}", this.txIdent);
      }
    }
  }

  public void basicProcessOps() {
    List<EntryEventImpl> pendingCallbacks = new ArrayList<>(this.farSideEntryOps.size());
    Collections.sort(this.farSideEntryOps);
    Iterator it = this.farSideEntryOps.iterator();
    while (it.hasNext()) {
      try {
        RegionCommit.FarSideEntryOp entryOp = (RegionCommit.FarSideEntryOp) it.next();
        entryOp.process(pendingCallbacks);
      } catch (CacheRuntimeException problem) {
        processCacheRuntimeException(problem);
      } catch (Exception e) {
        addProcessingException(e);
      }
    }
    firePendingCallbacks(pendingCallbacks);
  }

  private void firePendingCallbacks(List<EntryEventImpl> callbacks) {
    Iterator<EntryEventImpl> ci = callbacks.iterator();
    while (ci.hasNext()) {
      EntryEventImpl ee = ci.next();
      try {
        if (ee.getOperation().isDestroy()) {
          ee.getRegion().invokeTXCallbacks(EnumListenerEvent.AFTER_DESTROY, ee, true);
        } else if (ee.getOperation().isInvalidate()) {
          ee.getRegion().invokeTXCallbacks(EnumListenerEvent.AFTER_INVALIDATE, ee, true);
        } else if (ee.getOperation().isCreate()) {
          ee.getRegion().invokeTXCallbacks(EnumListenerEvent.AFTER_CREATE, ee, true);
        } else {
          ee.getRegion().invokeTXCallbacks(EnumListenerEvent.AFTER_UPDATE, ee, true);
        }
      } finally {
        ee.release();
      }
    }
  }

  protected void processCacheRuntimeException(CacheRuntimeException problem) {
    if (problem instanceof RegionDestroyedException) { // catch RegionDestroyedException
      addProcessingException(problem);
    } else if (problem instanceof CancelException) { // catch CacheClosedException
      addProcessingException(problem);
      throw problem;
    } else { // catch CacheRuntimeException
      addProcessingException(problem);
      logger.error(LocalizedMessage.create(
          LocalizedStrings.TXCommitMessage_TRANSACTION_MESSAGE_0_FROM_SENDER_1_FAILED_PROCESSING_UNKNOWN_TRANSACTION_STATE_2,
          new Object[] {this, getSender(), problem}));
    }
  }

  private void ack() {
    if (this.processorId != 0) {
      CommitReplyException replyEx = null;
      if (!this.processingExceptions.isEmpty()) {
        replyEx = new CommitReplyException(
            LocalizedStrings.TXCommitMessage_COMMIT_OPERATION_GENERATED_ONE_OR_MORE_EXCEPTIONS_FROM_0
                .toLocalizedString(this.getSender()),
            this.processingExceptions);
      }
      ReplyMessage.send(getSender(), this.processorId, replyEx, this.dm);
    }
  }

  public int getDSFID() {
    // on near side send old TX_COMMIT_MESSAGE if there is at least one 7.0
    // member in the system, otherwise send the new 7.0.1 message.
    // 7.0.1 members will be able to deserialize either
    // if (shouldSend701Message()) {
    // this.shouldWriteShadowKey = true;
    // return TX_COMMIT_MESSAGE_701;
    return TX_COMMIT_MESSAGE;
    /*
     * } this.shouldWriteShadowKey = false; return TX_COMMIT_MESSAGE;
     */
  }

  /*
   * /** Do not send shadowKey to clients or when there are member(s) older than 7.0.1.
   *
   * private boolean shouldSend701Message() { if (this.clientVersion == null &&
   * this.getDM().getMembersWithOlderVersion("7.0.1").isEmpty()) { return true; } return false; }
   *
   * public boolean shouldReadShadowKey() { return this.shouldReadShadowKey; }
   *
   * public void setShouldReadShadowKey(boolean shouldReadShadowKey) { this.shouldReadShadowKey =
   * shouldReadShadowKey; }
   *
   * public boolean shouldWriteShadowKey() { return this.shouldWriteShadowKey; }
   */

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    int pId = in.readInt();

    if (isAckRequired()) {
      this.processorId = pId;
      ReplyProcessor21.setMessageRPId(this.processorId);
    } else {
      this.processorId = -1;
    }

    this.txIdent = TXId.createFromData(in);
    if (in.readBoolean()) {
      this.lockId = TXLockIdImpl.createFromData(in);
    }
    int totalMaxSize = in.readInt();

    this.farsideBaseMembershipId = DataSerializer.readByteArray(in);
    this.farsideBaseThreadId = in.readLong();
    this.farsideBaseSequenceId = in.readLong();

    this.needsLargeModCount = in.readBoolean();

    final boolean hasShadowKeys = hasFlagsField(in) ? in.readBoolean() : useShadowKey();

    int regionsSize = in.readInt();
    this.regions = new ArrayList(regionsSize);
    this.farSideEntryOps = new ArrayList(totalMaxSize);
    for (int i = 0; i < regionsSize; i++) {
      RegionCommit rc = new RegionCommit(this);
      try {
        rc.fromData(in, hasShadowKeys);
      } catch (CacheClosedException cce) {
        addProcessingException(cce);
        // return to avoid serialization error being sent in reply
        return;
      }
      this.regions.add(rc);
    }

    this.bridgeContext = ClientProxyMembershipID.readCanonicalized(in);
    this.farSiders = DataSerializer.readHashSet(in);
  }

  /**
   * Return true if a distributed ack message is required. On the client side of a transaction, this
   * returns false, while returning true elsewhere.
   *
   * @return requires ack message or not
   */
  private boolean isAckRequired() {
    return this.ackRequired;
  }


  /**
   * Indicate whether an ack is required. Defaults to true.
   *
   * @param a true if we require an ack. false if not. false on clients.
   */
  public void setAckRequired(boolean a) {
    this.ackRequired = a;
    if (!a) {
      this.processorId = -1;
    }
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    out.writeInt(this.processorId);
    InternalDataSerializer.invokeToData(this.txIdent, out);
    {
      boolean hasLockId = this.lockId != null;
      out.writeBoolean(hasLockId);
      if (hasLockId) {
        InternalDataSerializer.invokeToData(this.lockId, out);
      }
    }
    int regionsSize = 0;
    {
      int totalMaxSize = 0;
      if (this.regions != null) {
        regionsSize = this.regions.size();
        for (int i = 0; i < this.regions.size(); i++) {
          RegionCommit rc = (RegionCommit) this.regions.get(i);
          totalMaxSize += rc.maxSize;
        }
      }
      out.writeInt(totalMaxSize);
    }

    if (this.txState != null) {
      DataSerializer.writeByteArray(this.txState.getBaseMembershipId(), out);
      out.writeLong(this.txState.getBaseThreadId());
      out.writeLong(this.txState.getBaseSequenceId());
    } else {
      DataSerializer.writeByteArray(this.farsideBaseMembershipId, out);
      out.writeLong(this.farsideBaseThreadId);
      out.writeLong(this.farsideBaseSequenceId);
    }

    if (this.txState != null) {
      DataSerializer.writeBoolean(this.txState.needsLargeModCount(), out);
    } else {
      DataSerializer.writeBoolean(this.needsLargeModCount, out);
    }

    final boolean useShadowKey = useShadowKey();
    if (hasFlagsField(out)) {
      out.writeBoolean(useShadowKey);
    }

    out.writeInt(regionsSize);
    {
      if (regionsSize > 0) {
        for (int i = 0; i < this.regions.size(); i++) {
          RegionCommit rc = (RegionCommit) this.regions.get(i);
          rc.toData(out, useShadowKey);
        }
      }
    }

    DataSerializer.writeObject(bridgeContext, out);

    DataSerializer.writeHashSet(this.farSiders, out);
  }

  private boolean hasFlagsField(final DataOutput out) {
    return hasFlagsField(InternalDataSerializer.getVersionForDataStream(out));
  }

  private boolean hasFlagsField(final DataInput in) {
    return hasFlagsField(InternalDataSerializer.getVersionForDataStream(in));
  }

  private boolean hasFlagsField(final Version version) {
    return version.compareTo(Version.GEODE_180) >= 0;
  }

  private boolean useShadowKey() {
    return null == clientVersion;
  }

  @Override
  public String toString() {
    StringBuilder result = new StringBuilder(256);
    result.append("TXCommitMessage@").append(System.identityHashCode(this)).append("#")
        .append(this.sequenceNum).append(" processorId=").append(this.processorId).append(" txId=")
        .append(this.txIdent);

    if (this.farSiders != null) {
      Iterator fs = this.farSiders.iterator();
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
    if (this.regions != null) {
      Iterator it = this.regions.iterator();
      while (it.hasNext()) {
        result.append(' ').append(it.next());
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
    Iterator it = other.regions.iterator();
    while (it.hasNext()) {
      RegionCommit rc = (RegionCommit) it.next();
      if (!this.regions.contains(rc)) {
        if (logger.isDebugEnabled()) {
          logger.debug("TX: adding region commit: {} to: {}", rc, this);
        }
        rc.msg = this;
        this.regions.add(rc);
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
      return this.needsAck;
    }

    @Override // GemStoneAddition
    public boolean add(RegionCommit o) {
      RegionCommit rc = (RegionCommit) o;
      rc.incRefCount();
      if (!this.needsAck && rc.needsAck()) {
        this.needsAck = true;
      }
      return super.add(o);
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
      if (this.trimRC == rc) {
        return this.trimChild;
      }
      RegionCommitList result = new RegionCommitList(this);
      this.trimRC = rc;
      this.trimChild = result;
      result.remove(result.size() - 1);
      {
        Iterator it = result.iterator();
        while (it.hasNext()) {
          RegionCommit itrc = (RegionCommit) it.next();
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
      StringBuilder result = new StringBuilder(256);
      result.append('@').append(System.identityHashCode(this)).append(' ').append(super.toString());
      return result.toString();
    }
  }

  public static class RegionCommit {
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
    private transient ArrayList opKeys;
    /**
     * A list of TXEntryState; each one is the entry info for a distributed operation done by this
     * transaction. The list must be kept in sync with opKeys. Valid only on nearside.
     */
    private transient ArrayList opEntries;

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
      this.internalRegion = r;
      this.maxSize = maxSize;
    }

    /**
     * Used on farside who inits r later and never sets maxSize
     */
    RegionCommit(TXCommitMessage msg) {
      this.msg = msg;
    }

    public void incRefCount() {
      this.refCount++;
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
        logger.debug("begin processing TXCommitMessage {} for region {}", txIdent, this.regionPath);
      }
      try {
        if (!hookupRegion(dm)) {
          return false;
        }
        if (msg.isAckRequired()
            && (this.internalRegion == null || !this.internalRegion.getScope().isDistributed())) {
          if (logger.isDebugEnabled()) {
            logger.debug("Received unneeded commit data for region {}", this.regionPath);
          }
          this.msg.addProcessingException(new RegionDestroyedException(
              LocalizedStrings.TXCommitMessage_REGION_NOT_FOUND.toLocalizedString(),
              this.regionPath));
          this.internalRegion = null;
          return false;
        }
        this.needsUnlock = this.internalRegion.lockGII();
        this.internalRegion.txLRUStart();
        this.needsLRUEnd = true;
        if (this.internalRegion.isInitialized()) {
          // We don't want the txEvent to know anything about our regions
          // that are still doing gii.
          this.txEvent = txEvent;
        }
      } catch (RegionDestroyedException e) {
        this.msg.addProcessingException(e);
        // Region destroyed: Update cancelled
        if (logger.isDebugEnabled()) {
          logger.debug(
              "Received unneeded commit data for region {} because the region was destroyed.",
              this.regionPath, e);
        }
        this.internalRegion = null;
      }
      return this.internalRegion != null;
    }

    private boolean hookupRegion(DistributionManager dm) {
      this.internalRegion = getRegionByPath(dm, regionPath);
      if (this.internalRegion == null && this.parentRegionPath != null) {
        this.internalRegion = getRegionByPath(dm, this.parentRegionPath);
        this.regionPath = this.parentRegionPath;
      }
      if (this.internalRegion == null && dm.getSystem().isLoner()) {
        // If there are additional regions that the server enlisted in the tx,
        // which the client does not have, the client can just ignore the region
        // see bug 51922
        return false;
      }
      return true;
    }

    LocalRegion getRegionByPath(DistributionManager dm, String regionPath) {
      InternalCache cache = dm.getCache();
      return cache == null ? null : (LocalRegion) cache.getRegionByPath(regionPath);
    }

    /**
     * Called when processing is complete; only needs to be called if beginProcess returned true.
     */
    void endProcess() {
      if (this.internalRegion != null) {
        try {
          if (this.needsLRUEnd) {
            this.needsLRUEnd = false;
            this.internalRegion.txLRUEnd();
          }
        } finally {
          if (this.needsUnlock) {
            this.needsUnlock = false;
            this.internalRegion.unlockGII();
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
      return this.msg.getEventId(entryOp.eventOffset);
    }


    /**
     * Apply a single tx entry op on the far side
     */
    @SuppressWarnings("synthetic-access")
    protected void txApplyEntryOp(FarSideEntryOp entryOp, List<EntryEventImpl> pendingCallbacks) {
      if (this.internalRegion == null) {
        return;
      }
      EventID eventID = getEventId(entryOp);
      boolean isDuplicate = this.internalRegion.hasSeenEvent(eventID);
      boolean callbacksOnly =
          (this.internalRegion.getDataPolicy() == DataPolicy.PARTITION) || isDuplicate;
      if (this.internalRegion instanceof PartitionedRegion) {
        /*
         * This happens when we don't have the bucket and are getting adjunct notification
         */
        // No need to release because it is added to pendingCallbacks and they will be released
        // later
        EntryEventImpl eei =
            AbstractRegionMap.createCallbackEvent(this.internalRegion, entryOp.op, entryOp.key,
                entryOp.value, this.msg.txIdent, txEvent, getEventId(entryOp), entryOp.callbackArg,
                entryOp.filterRoutingInfo, this.msg.bridgeContext, null, entryOp.versionTag,
                entryOp.tailKey);
        if (entryOp.filterRoutingInfo != null) {
          eei.setLocalFilterInfo(
              entryOp.filterRoutingInfo.getFilterInfo(this.internalRegion.getCache().getMyId()));
        }
        if (isDuplicate) {
          eei.setPossibleDuplicate(true);
        }
        if (logger.isDebugEnabled()) {
          logger.debug("invoking transactional callbacks for {} key={} needsUnlock={} event={}",
              entryOp.op, entryOp.key, this.needsUnlock, eei);
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
            entryOp.op, entryOp.key, this.needsUnlock, getEventId(entryOp),
            entryOp.filterRoutingInfo);
      }
      if (entryOp.versionTag != null) {
        entryOp.versionTag.replaceNullIDs(this.msg.getSender());
      }
      if (entryOp.op.isDestroy()) {
        this.internalRegion.txApplyDestroy(entryOp.key, this.msg.txIdent, this.txEvent,
            this.needsUnlock,
            entryOp.op, getEventId(entryOp), entryOp.callbackArg, pendingCallbacks,
            entryOp.filterRoutingInfo, this.msg.bridgeContext, false /* origin remote */,
            null/* txEntryState */, entryOp.versionTag, entryOp.tailKey);
      } else if (entryOp.op.isInvalidate()) {
        this.internalRegion.txApplyInvalidate(entryOp.key, Token.INVALID, entryOp.didDestroy,
            this.msg.txIdent,
            this.txEvent, false /* localOp */, getEventId(entryOp), entryOp.callbackArg,
            pendingCallbacks, entryOp.filterRoutingInfo, this.msg.bridgeContext,
            null/* txEntryState */, entryOp.versionTag, entryOp.tailKey);
      } else {
        this.internalRegion.txApplyPut(entryOp.op, entryOp.key, entryOp.value, entryOp.didDestroy,
            this.msg.txIdent, this.txEvent, getEventId(entryOp), entryOp.callbackArg,
            pendingCallbacks, entryOp.filterRoutingInfo, this.msg.bridgeContext,
            null/* txEntryState */, entryOp.versionTag, entryOp.tailKey);
      }
    }

    /**
     * Apply a single tx entry op on the far side
     */
    @SuppressWarnings("synthetic-access")
    protected void txApplyEntryOpAdjunctOnly(FarSideEntryOp entryOp) {
      if (this.internalRegion == null) {
        return;
      }
      EventID eventID = getEventId(entryOp);
      boolean isDuplicate = this.internalRegion.hasSeenEvent(eventID);
      boolean callbacksOnly =
          (this.internalRegion.getDataPolicy() == DataPolicy.PARTITION) || isDuplicate;
      if (this.internalRegion instanceof PartitionedRegion) {

        PartitionedRegion pr = (PartitionedRegion) internalRegion;
        BucketRegion br = pr.getBucketRegion(entryOp.key);
        Set bucketOwners = br.getBucketOwners();
        InternalDistributedMember thisMember = this.internalRegion.getDistributionManager().getId();
        if (bucketOwners.contains(thisMember)) {
          return;
        }

        /*
         * This happens when we don't have the bucket and are getting adjunct notification
         */
        @Released
        EntryEventImpl eei =
            AbstractRegionMap.createCallbackEvent(this.internalRegion, entryOp.op, entryOp.key,
                entryOp.value, this.msg.txIdent, txEvent, getEventId(entryOp), entryOp.callbackArg,
                entryOp.filterRoutingInfo, this.msg.bridgeContext, null, entryOp.versionTag,
                entryOp.tailKey);
        try {
          if (entryOp.filterRoutingInfo != null) {
            eei.setLocalFilterInfo(
                entryOp.filterRoutingInfo.getFilterInfo(this.internalRegion.getCache().getMyId()));
          }
          if (isDuplicate) {
            eei.setPossibleDuplicate(true);
          }
          if (logger.isDebugEnabled()) {
            logger.debug("invoking transactional callbacks for {} key={} needsUnlock={} event={}",
                entryOp.op, entryOp.key, this.needsUnlock, eei);
          }
          // we reach this spot because the event is either delivered to this member
          // as an "adjunct" message or because the bucket was being created when
          // the message was sent and already reflects the change caused by this event.
          // In the latter case we need to invoke listeners
          final boolean skipListeners = !isDuplicate;
          eei.invokeCallbacks(this.internalRegion, skipListeners, true);
        } finally {
          eei.release();
        }
        return;
      }
    }

    boolean isEmpty() {
      return this.opKeys == null;
    }

    boolean needsAck() {
      return this.internalRegion.getScope().isDistributedAck();
    }

    void addOp(Object key, TXEntryState entry) {
      if (this.opKeys == null) {
        this.opKeys = new ArrayList(this.maxSize);
        this.opEntries = new ArrayList(this.maxSize);
      }
      this.opKeys.add(key);
      this.opEntries.add(entry);
    }


    public boolean isForceFireEvent(DistributionManager dm) {
      LocalRegion r = getRegionByPath(dm, regionPath);
      if (r instanceof PartitionedRegion || (r != null && r.isUsedForPartitionedRegionBucket())) {
        return false;
      }
      return true;
    }

    public void fromData(DataInput in, boolean hasShadowKey)
        throws IOException, ClassNotFoundException {
      this.regionPath = DataSerializer.readString(in);
      this.parentRegionPath = DataSerializer.readString(in);

      int size = in.readInt();
      if (size > 0) {
        this.opKeys = new ArrayList(size);
        this.opEntries = new ArrayList(size);
        final boolean largeModCount = in.readBoolean();
        this.memberId = DataSerializer.readObject(in);
        for (int i = 0; i < size; i++) {
          FarSideEntryOp entryOp = new FarSideEntryOp();
          // shadowkey is not being sent to clients
          entryOp.fromData(in, largeModCount, hasShadowKey);
          if (entryOp.versionTag != null && this.memberId != null) {
            entryOp.versionTag.setMemberID(this.memberId);
          }
          this.msg.addFarSideEntryOp(entryOp);
          this.opKeys.add(entryOp.key);
          this.opEntries.add(entryOp);
        }
      }
    }

    @Override
    public String toString() {
      StringBuilder result = new StringBuilder(64);
      if (this.regionPath != null) {
        result.append(this.regionPath);
      } else {
        result.append(this.internalRegion.getFullPath());
      }
      if (this.refCount > 0) {
        result.append(" refCount=").append(this.refCount);
      }
      return result.toString();
    }

    private void basicToData(DataOutput out, boolean useShadowKey) throws IOException {
      if (this.internalRegion != null) {
        DataSerializer.writeString(this.internalRegion.getFullPath(), out);
        if (this.internalRegion instanceof BucketRegion) {
          DataSerializer.writeString(
              ((Bucket) this.internalRegion).getPartitionedRegion().getFullPath(), out);
        } else {
          DataSerializer.writeString(null, out);
        }
      } else {
        DataSerializer.writeString(this.regionPath, out);
        DataSerializer.writeString(this.parentRegionPath, out);
      }

      if (isEmpty() || this.opKeys.size() == 0) {
        out.writeInt(0);
      } else {
        int size = this.opKeys.size();
        out.writeInt(size);

        final boolean largeModCount;
        if (this.msg.txState != null) {
          largeModCount = this.msg.txState.needsLargeModCount();
        } else {
          largeModCount = this.msg.needsLargeModCount;
        }
        out.writeBoolean(largeModCount);

        final boolean sendVersionTags =
            this.msg.clientVersion == null || Version.GFE_70.compareTo(this.msg.clientVersion) <= 0;
        if (sendVersionTags) {
          VersionSource member = this.memberId;
          if (member == null) {
            if (this.internalRegion == null) {
              Assert.assertTrue(this.msg.txState == null);
            } else {
              member = this.internalRegion.getVersionMember();
            }
          }
          DataSerializer.writeObject(member, out);
        }
        for (int i = 0; i < size; i++) {
          DataSerializer.writeObject(this.opKeys.get(i), out);
          if (this.msg.txState != null) {
            /* we are still on tx node and have the entry state */
            ((TXEntryState) this.opEntries.get(i)).toFarSideData(out, largeModCount,
                sendVersionTags, useShadowKey);
          } else {
            ((FarSideEntryOp) this.opEntries.get(i)).toData(out, largeModCount, sendVersionTags,
                useShadowKey);
          }
        }
      }
    }


    public void toData(DataOutput out, boolean useShadowKey) throws IOException {
      if (this.preserializedBuffer != null) {
        this.preserializedBuffer.rewind();
        this.preserializedBuffer.sendTo(out);
      } else if (this.refCount > 1) {
        Version v = InternalDataSerializer.getVersionForDataStream(out);
        HeapDataOutputStream hdos = new HeapDataOutputStream(1024, v);
        basicToData(hdos, useShadowKey);
        this.preserializedBuffer = hdos;
        this.preserializedBuffer.sendTo(out);
      } else {
        basicToData(out, useShadowKey);
      }
    }

    /**
     * Holds data that describes a tx entry op on the far side.
     *
     * @since GemFire 5.0
     */
    public class FarSideEntryOp implements Comparable {
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
        this.key = DataSerializer.readObject(in);
        this.op = Operation.fromOrdinal(in.readByte());
        if (largeModCount) {
          this.modSerialNum = in.readInt();
        } else {
          this.modSerialNum = in.readByte();
        }
        this.callbackArg = DataSerializer.readObject(in);
        this.filterRoutingInfo = DataSerializer.readObject(in);
        this.versionTag = DataSerializer.readObject(in);
        if (readShadowKey) {
          this.tailKey = in.readLong();
        }
        this.eventOffset = in.readInt();
        if (!this.op.isDestroy()) {
          this.didDestroy = in.readBoolean();
          if (!this.op.isInvalidate()) {
            boolean isTokenOrByteArray = in.readBoolean();
            if (isTokenOrByteArray) {
              // token or byte[]
              this.value = DataSerializer.readObject(in);
            } else {
              // CachedDeserializable, Object, or PDX
              this.value = CachedDeserializableFactory.create(DataSerializer.readByteArray(in),
                  GemFireCacheImpl.getInstance());
            }
          }
        }
      }

      public void toData(DataOutput out, boolean largeModCount, boolean sendVersionTag,
          boolean sendShadowKey) throws IOException {
        // DataSerializer.writeObject(this.key,out);
        /* Don't serialize key because caller did that already */

        out.writeByte(this.op.ordinal);
        if (largeModCount) {
          out.writeInt(this.modSerialNum);
        } else {
          out.writeByte(this.modSerialNum);
        }
        DataSerializer.writeObject(this.callbackArg, out);
        DataSerializer.writeObject(this.filterRoutingInfo, out);
        if (sendVersionTag) {
          DataSerializer.writeObject(this.versionTag, out);
        }
        if (sendShadowKey) {
          out.writeLong(this.tailKey);
        }
        out.writeInt(this.eventOffset);
        if (!this.op.isDestroy()) {
          out.writeBoolean(this.didDestroy);
          if (!this.op.isInvalidate()) {
            boolean sendObject = Token.isInvalidOrRemoved(this.value);
            sendObject = sendObject || this.value instanceof byte[];
            out.writeBoolean(sendObject);
            if (sendObject) {
              DataSerializer.writeObject(this.value, out);
            } else {
              DataSerializer.writeObjectAsByteArray(this.value, out);
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
        return this.modSerialNum;
      }

      public int compareTo(Object o) {
        FarSideEntryOp other = (FarSideEntryOp) o;
        return getSortValue() - other.getSortValue();
      }

      @Override
      public boolean equals(Object o) {
        if (o == null || !(o instanceof FarSideEntryOp)) {
          return false;
        }
        return compareTo(o) == 0;
      }

      @Override
      public int hashCode() {
        return getSortValue();
      }
    }
  }

  Object getTrackerKey() {
    if (this.lockId != null) {
      return this.lockId;
    } else {
      return this.txIdent;
    }
  }

  /**
   * Used to prevent processing of the message if we have reported to other FarSiders that we did
   * not received the CommitProcessMessage
   */
  boolean dontProcess() {
    return this.dontProcess;
  }

  /**
   * Indicate that this message should not be processed if we receive CommitProcessMessage (late)
   */
  void setDontProcess() {
    this.dontProcess = true;
  }

  boolean isProcessing() {
    return this.isProcessing;
  }

  private void setIsProcessing(boolean isProcessing) {
    this.isProcessing = isProcessing;
  }

  boolean wasProcessed() {
    return this.wasProcessed;
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
      final TXCommitMessage mess = waitForMessage(this.lockId, dm);
      Assert.assertTrue(mess != null, "Commit data for TXLockId: " + this.lockId + " not found");
      basicProcess(mess, dm);
    }

    public int getDSFID() {
      return COMMIT_PROCESS_FOR_LOCKID_MESSAGE;
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      InternalDataSerializer.invokeToData(this.lockId, out);
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      this.lockId = TXLockIdImpl.createFromData(in);
      Assert.assertTrue(this.lockId != null,
          "CommitProcessForLockIdMessage must have a non-null lockid!");
    }

    @Override
    public String toString() {
      StringBuilder result = new StringBuilder(128);
      result.append("CommitProcessForLockIdMessage@").append(System.identityHashCode(this))
          .append(" lockId=").append(this.lockId);
      return result.toString();
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
      final TXCommitMessage mess = waitForMessage(this.txId, dm);
      Assert.assertTrue(mess != null, "Commit data for TXId: " + this.txId + " not found");
      basicProcess(mess, dm);
    }

    public int getDSFID() {
      return COMMIT_PROCESS_FOR_TXID_MESSAGE;
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      InternalDataSerializer.invokeToData(this.txId, out);
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      this.txId = TXId.createFromData(in);
      Assert.assertTrue(this.txId != null,
          "CommitProcessMessageForTXId must have a non-null txid!");
    }

    @Override
    public String toString() {
      StringBuilder result = new StringBuilder(128);
      result.append("CommitProcessForTXIdMessage@").append(System.identityHashCode(this))
          .append(" txId=").append(this.txId);
      return result.toString();
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
      final boolean processMsgReceived = txTracker.commitProcessReceived(this.trackerKey, dm);
      if (!processMsgReceived) {
        if (logger.isDebugEnabled()) {
          logger.debug("CommitProcessQuery did not find {} in the history", this.trackerKey);
        }
      }

      // Reply to the fellow FarSider as to whether the
      // CommitProcess message was received
      CommitProcessQueryReplyMessage resp = new CommitProcessQueryReplyMessage(processMsgReceived);
      resp.setProcessorId(this.processorId);
      resp.setRecipient(this.getSender());
      dm.putOutgoing(resp);
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      DataSerializer.writeObject(this.trackerKey, out);
      out.writeInt(this.processorId);
    }

    public int getDSFID() {
      return COMMIT_PROCESS_QUERY_MESSAGE;
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      this.trackerKey = DataSerializer.readObject(in);
      this.processorId = in.readInt();
    }

    @Override
    public String toString() {
      StringBuilder result = new StringBuilder(128);
      result.append("CommitProcessQueryMessage@").append(System.identityHashCode(this))
          .append(" trackerKeyClass=").append(this.trackerKey.getClass().getName())
          .append(" trackerKey=").append(this.trackerKey).append(" processorId=")
          .append(this.processorId);
      return result.toString();
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
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      super.fromData(in);
      this.wasReceived = in.readBoolean();
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      super.toData(out);
      out.writeBoolean(this.wasReceived);
    }

    @Override
    public String toString() {
      StringBuilder result = new StringBuilder(128);
      result.append("CommitProcessQueryReplyMessage@").append(System.identityHashCode(this))
          .append(" wasReceived=").append(this.wasReceived).append(" processorId=")
          .append(this.processorId).append(" from ").append(this.getSender());
      return result.toString();
    }
  }

  /********************* Commit Process Query Response Processor *********************************/
  public static class CommitProcessQueryReplyProcessor extends ReplyProcessor21 {
    public boolean receivedOnePositive;

    CommitProcessQueryReplyProcessor(DistributionManager dm, Set members) {
      super(dm, members);
      this.receivedOnePositive = false;
    }

    @Override
    public void process(DistributionMessage msg) {
      CommitProcessQueryReplyMessage ccMess = (CommitProcessQueryReplyMessage) msg;
      if (ccMess.wasReceived()) {
        this.receivedOnePositive = true;
      }
      super.process(msg);
    }

    @Override
    protected boolean canStopWaiting() {
      return this.receivedOnePositive;
    }

    public boolean receivedACommitProcessMessage() {
      return this.receivedOnePositive;
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

  @Override
  public void memberDeparted(DistributionManager distributionManager,
      final InternalDistributedMember id, boolean crashed) {

    if (!getSender().equals(id)) {
      return;
    }
    this.dm.removeMembershipListener(this);

    synchronized (this) {
      if (isProcessing() || this.departureNoticed) {
        if (logger.isDebugEnabled()) {
          logger.debug("Member departed: Commit data is already being processed for lockid: {}",
              lockId);
        }
        return;
      }
      this.departureNoticed = true;
    }

    ThreadGroup group = LoggingThreadGroup.createThreadGroup("TXCommitMessage Threads", logger);

    // Send message to fellow FarSiders (aka recipients), if any, to
    // determine if any one of them have received a CommitProcessMessage
    if (this.farSiders != null && !this.farSiders.isEmpty()) {
      if (logger.isDebugEnabled()) {
        logger.debug(
            "Member departed: {} sending query for CommitProcess message to other recipients.", id);
      }

      // Create a new thread, send the CommitProcessQuery, wait for a response and potentially
      // process
      Thread fellowFarSidersQuery = new Thread(group, "CommitProcessQuery Thread") {
        // Should I use a thread pool?, Darrel suggests look in DM somewhere or introduce a zero
        // sized thread pool
        @Override
        public void run() {
          final TXCommitMessage mess = TXCommitMessage.this;
          Object trackerKey = mess.getTrackerKey();
          DistributedMember member = getMemberFromTrackerKey(trackerKey);
          if (!mess.getSender().equals(member)) {
            /*
             * Do not send a CommitProcessQueryMessage when the sender of CommitMessage is not the
             * member in the tracker key. (If this happens we are the redundant node for PR, and the
             * primary just crashed).
             */
            txTracker.removeMessage(mess);
            return;
          }
          CommitProcessQueryReplyProcessor replProc =
              new CommitProcessQueryReplyProcessor(mess.dm, mess.farSiders);
          CommitProcessQueryMessage query =
              new CommitProcessQueryMessage(mess.getTrackerKey(), replProc.getProcessorId());
          query.setRecipients(mess.farSiders);
          mess.dm.putOutgoing(query);
          // Wait for any one positive response or all negative responses.
          // (while() loop removed for bug 36983 - you can't loop on waitForReplies()
          TXCommitMessage.this.dm.getCancelCriterion().checkCancelInProgress(null);
          try {
            replProc.waitForRepliesUninterruptibly();
          } catch (ReplyException e) {
            e.handleCause();
          }
          if (replProc.receivedACommitProcessMessage()) {
            if (logger.isDebugEnabled()) {
              logger.debug(
                  "Transaction associated with lockID: {} from orign {} is processing due to a received \"commit process\" message",
                  mess.lockId, id);
            }

            try {
              // Set processor to zero to avoid the ack to the now departed origin
              mess.processorId = 0;
              mess.basicProcess();
            } finally {
              txTracker.processed(mess);
            }
          } else {
            if (logger.isDebugEnabled()) {
              logger.debug(
                  "Transaction associated with lockID: {} from origin {} ignored.  No other recipients received \"commit process\" message",
                  mess.lockId, id);
            }
            txTracker.removeMessage(mess);
          }

        }

        private DistributedMember getMemberFromTrackerKey(Object trackerKey) {
          if (trackerKey instanceof TXId) {
            TXId id1 = (TXId) trackerKey;
            return id1.getMemberId();
          } else if (trackerKey instanceof TXLockId) {
            TXLockId id2 = (TXLockId) trackerKey;
            return id2.getMemberId();
          }
          return null;
        }
      };
      fellowFarSidersQuery.setDaemon(true);
      fellowFarSidersQuery.start();
    } else {
      if (logger.isDebugEnabled()) {
        logger.debug("Member departed: {}. Processing commit data.", getSender());
      }

      // Optimimal case where we are the only FarSider, assume we
      // will never get the CommitProcess message, but it
      // doesn't matter since we can commit anyway.
      // Start a new thread to process the commit
      Thread originDepartedCommit = new Thread(group, "Origin Departed Commit") {
        @Override
        public void run() {
          final TXCommitMessage mess = TXCommitMessage.this;
          try {
            // Set processor to zero to avoid the ack to the now departed origin
            mess.processorId = 0;
            mess.basicProcess();
          } finally {
            txTracker.processed(mess);
          }
        }
      };
      originDepartedCommit.setDaemon(true);
      originDepartedCommit.start();
    }
  }

  void setUpdateLockMembers() {
    this.lockNeedsUpdate = true;
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
    if (this.lockNeedsUpdate && this.lockId != null) {
      TXLockService.createDTLS(this.dm.getSystem()).updateParticipants(this.lockId,
          this.msgMap.keySet());
    }
  }

  /**
   * Reply processor which collects all CommitReplyExceptions and emits a detailed failure exception
   * if problems occur
   *
   * @since GemFire 5.7
   */
  private class CommitReplyProcessor extends ReliableReplyProcessor21 {
    private HashMap msgMap;

    public CommitReplyProcessor(DistributionManager dm, Set initMembers, HashMap msgMap) {
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
    protected void processException(DistributionMessage msg, ReplyException ex) {
      if (msg instanceof ReplyMessage) {
        synchronized (this) {
          if (this.exception == null) {
            // Exception Container
            this.exception = new CommitExceptionCollectingException(txIdent);
          }
          CommitExceptionCollectingException cce =
              (CommitExceptionCollectingException) this.exception;
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

    public Set getCacheClosedMembers() {
      if (this.exception != null) {
        CommitExceptionCollectingException cce =
            (CommitExceptionCollectingException) this.exception;
        return cce.getCacheClosedMembers();
      } else {
        return Collections.emptySet();
      }
    }

    public Set getRegionDestroyedMembers(String regionFullPath) {
      if (this.exception != null) {
        CommitExceptionCollectingException cce =
            (CommitExceptionCollectingException) this.exception;
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
    private final Set<InternalDistributedMember> cacheExceptions;
    /**
     * key=region path, value=Set of members
     */
    private final Map<String, Set<InternalDistributedMember>> regionExceptions;
    /**
     * List of exceptions that were unexpected and caused the tx to fail
     */
    private final Map fatalExceptions;

    private final TXId id;

    public CommitExceptionCollectingException(TXId txIdent) {
      this.cacheExceptions = new HashSet();
      this.regionExceptions = new HashMap();
      this.fatalExceptions = new HashMap();
      this.id = txIdent;
    }

    /**
     * Determine if the commit processing was incomplete, if so throw a detailed exception
     * indicating the source of the problem
     */
    public void handlePotentialCommitFailure(
        HashMap<InternalDistributedMember, RegionCommitList> msgMap) {
      if (fatalExceptions.size() > 0) {
        StringBuilder errorMessage = new StringBuilder("Incomplete commit of transaction ")
            .append(id).append(".  Caused by the following exceptions: ");
        for (Iterator i = fatalExceptions.entrySet().iterator(); i.hasNext();) {
          Map.Entry me = (Map.Entry) i.next();
          DistributedMember mem = (DistributedMember) me.getKey();
          errorMessage.append(" From member: ").append(mem).append(" ");
          List exceptions = (List) me.getValue();
          for (Iterator ei = exceptions.iterator(); ei.hasNext();) {
            Exception e = (Exception) ei.next();
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
    private void handleRegionDestroyed(
        HashMap<InternalDistributedMember, RegionCommitList> msgMap) {
      if (regionExceptions == null || regionExceptions.isEmpty()) {
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
    private void handleClosedMembers(HashMap<InternalDistributedMember, RegionCommitList> msgMap) {
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
      return this.cacheExceptions;
    }

    public Set getRegionDestroyedMembers(String regionFullPath) {
      Set members = (Set) this.regionExceptions.get(regionFullPath);
      if (members == null) {
        members = Collections.emptySet();
      }
      return members;
    }

    /**
     * Protected by (this)
     */
    public void addExceptionsFromMember(InternalDistributedMember member, Set exceptions) {
      for (Iterator iter = exceptions.iterator(); iter.hasNext();) {
        Exception ex = (Exception) iter.next();
        if (ex instanceof CancelException) {
          cacheExceptions.add(member);
        } else if (ex instanceof RegionDestroyedException) {
          String r = ((RegionDestroyedException) ex).getRegionFullPath();
          Set<InternalDistributedMember> members = regionExceptions.get(r);
          if (members == null) {
            members = new HashSet();
            regionExceptions.put(r, members);
          }
          members.add(member);
        } else {
          List el = (List) this.fatalExceptions.get(member);
          if (el == null) {
            el = new ArrayList(2);
            this.fatalExceptions.put(member, el);
          }
          el.add(ex);
        }
      }
    }
  }

  public void hookupRegions(DistributionManager dm) {
    if (regions != null) {
      Iterator it = regions.iterator();
      while (it.hasNext()) {
        RegionCommit rc = (RegionCommit) it.next();
        rc.hookupRegion(dm);
      }
    }

  }


  /**
   * Disable firing of TX Listeners. Currently on used on clients.
   *
   * @param b disable the listeners
   */
  public void setDisableListeners(boolean b) {
    disableListeners = true;
  }

  public Version getClientVersion() {
    return clientVersion;
  }

  public void setClientVersion(Version clientVersion) {
    this.clientVersion = clientVersion;
  }

}

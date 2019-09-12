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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.MembershipListener;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.cache.locks.TXLockId;
import org.apache.geode.internal.logging.LogService;

/**
 * TXFarSideCMTracker tracks received and processed TXCommitMessages, for transactions that contain
 * changes for DACK regions. Its main purpose is to allow recovery in the event that the VM which
 * orinated the TXCommitMessage exits the DistributedSystem. Tracking is done by using TXLockIds or
 * TXIds. It is designed for these failure cases:
 *
 * <ol>
 *
 * <li>The TX Originator has died during sending the second message which one or more of the
 * recipients (aka Far Siders) missed. To help in this case, each of the Far Siders will broadcast a
 * message to determine if the second commit message was received.</li>
 *
 * <li>The TX Grantor (the reservation system) has noticed that the TX Originator has died and
 * queries each of the Far Siders to determine if the reservation (aka <code>TXLockId</code>) given
 * to the TX Originator is no longer needed (the transaction has been processed)</li>
 *
 * <li>The TX Grantor has died and a new one is considering granting new reservations, but before
 * doing so must query all of the members to know if all the previous granted reservations (aka
 * <code>TXLockId</code>s are no longer needed (the transactions have been processed)</li>
 *
 * </ol>
 *
 * @since GemFire 4.0
 *
 */
public class TXFarSideCMTracker {
  private static final Logger logger = LogService.getLogger();

  private final Map txInProgress;
  private final Object txHistory[];
  private int lastHistoryItem;

  /**
   * Constructor for TXFarSideCMTracker
   *
   * @param historySize The number of processed transactions to remember in the event that fellow
   *        Far Siders did not receive the second message.
   */
  public TXFarSideCMTracker(int historySize) {
    this.txInProgress = new HashMap();
    this.txHistory = new Object[historySize];
    this.lastHistoryItem = 0;
  }

  public int getHistorySize() {
    return this.txHistory.length;
  }

  /**
   * Answers fellow "Far Siders" question about an DACK transaction when the transaction originator
   * died before it sent the CommitProcess message.
   */
  public boolean commitProcessReceived(Object key) {
    final TXCommitMessage message;
    synchronized (txInProgress) {
      message = (TXCommitMessage) getTxInProgress().get(key);
      if (foundTxInProgress(message)) {
        return true;
      }

      if (foundFromHistory(key)) {
        return true;
      }
    }

    if (message != null) {
      synchronized (message) {
        if (!message.isProcessing()) {
          // Prevent any potential future processing
          // of this message
          message.setDontProcess();
          return false;
        } else {
          return true;
        }
      }
    }

    return false;
  }

  Map getTxInProgress() {
    return txInProgress;
  }

  boolean foundTxInProgress(TXCommitMessage message) {
    return null != message && message.isProcessing();
  }

  boolean foundFromHistory(Object key) {
    for (int i = this.txHistory.length - 1; i >= 0; --i) {
      if (key.equals(this.txHistory[i])) {
        return true;
      }
    }
    return false;
  }

  /**
   * Answers new Grantor query regarding whether it can start handing out new locks. Waits until
   * txInProgress is empty.
   */
  public void waitForAllToProcess() throws InterruptedException {
    if (Thread.interrupted())
      throw new InterruptedException(); // wisest to do this before the synchronize below
    // Assume that a thread interrupt is only set in the
    // case of a shutdown, in that case we don't need to wait
    // around any longer, propagating the interrupt is reasonable behavior
    boolean messageWritten = false;
    synchronized (this.txInProgress) {
      while (!this.txInProgress.isEmpty()) {
        logger.info("Lock grantor recovery is waiting for transactions to complete: {}",
            txInProgress);
        messageWritten = true;
        this.txInProgress.wait();
      }
    }
    if (messageWritten) {
      logger.info("Wait for transactions completed");
    }
  }

  /**
   * Answers existing Grantor's question about the status of a reservation/lock given to a
   * departed/ing Originator (this will most likely be called nearly the same time as
   * commitProcessReceived
   */
  public void waitToProcess(TXLockId lockId, DistributionManager dm) {
    waitForMemberToDepart(lockId.getMemberId(), dm);
    final TXCommitMessage commitMessage;
    synchronized (this.txInProgress) {
      commitMessage = (TXCommitMessage) this.txInProgress.get(lockId);
    }
    if (commitMessage != null) {
      synchronized (commitMessage) {
        // tx in progress, we must wait until its done
        while (!(commitMessage.wasProcessed() || commitMessage.isDepartureNoticed())) {
          try {
            commitMessage.wait(100);
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            logger.error(
                String.format("Waiting to complete on message %s caught an interrupted exception",
                    commitMessage),
                ie);
            break;
          }
        }
      }
    } else {
      // tx may have completed
      for (int i = this.txHistory.length - 1; i >= 0; --i) {
        if (lockId.equals(this.txHistory[i])) {
          return;
        }
      }
    }
  }

  /**
   * Register a <code>MemberhipListener</code>, wait until the member is gone.
   */
  private void waitForMemberToDepart(final InternalDistributedMember memberId,
      DistributionManager dm) {
    if (!dm.getDistributionManagerIds().contains(memberId)) {
      return;
    }

    final Object lock = new Object();
    final MembershipListener memEar = new MembershipListener() {
      // MembershipListener implementation
      @Override
      public void memberJoined(DistributionManager distributionManager,
          InternalDistributedMember id) {}

      @Override
      public void memberSuspect(DistributionManager distributionManager,
          InternalDistributedMember id, InternalDistributedMember whoSuspected, String reason) {}

      @Override
      public void memberDeparted(DistributionManager distributionManager,
          InternalDistributedMember id, boolean crashed) {
        if (memberId.equals(id)) {
          synchronized (lock) {
            lock.notifyAll();
          }
        }
      }

      @Override
      public void quorumLost(DistributionManager distributionManager,
          Set<InternalDistributedMember> failures, List<InternalDistributedMember> remaining) {}
    };
    try {
      List memberSet = dm.addMembershipListenerAndGetDistributionManagerIds(memEar);

      // Still need to wait
      synchronized (lock) {
        while (memberSet.contains(memberId)) {
          try {
            lock.wait();
            memberSet = dm.getDistributionManagerIds();
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            // TODO Log an error here
            return;
          }
        }
      } // synchronized memberId
    } finally {
      // Its gone, we can proceed
      dm.removeMembershipListener(memEar);
    }
  }

  /**
   * Indicate that the transaction message has been processed and to place it in the transaction
   * history
   */
  public TXCommitMessage processed(TXCommitMessage processedMess) {
    final TXCommitMessage mess;
    final Object key = processedMess.getTrackerKey();
    synchronized (this.txInProgress) {
      mess = (TXCommitMessage) this.txInProgress.remove(key);
      if (mess != null) {
        this.txHistory[this.lastHistoryItem++] = key;
        if (lastHistoryItem >= txHistory.length) {
          lastHistoryItem = 0;
        }
        // For any waitForAllToComplete
        if (txInProgress.isEmpty()) {
          this.txInProgress.notifyAll();
        }
      }
    }
    if (mess != null) {
      synchronized (mess) {
        mess.setProcessed(true);
        // For any waitToComplete
        mess.notifyAll();
      }
    }
    return mess;
  }

  /**
   * Indicate that this message is never going to be processed, typically used in the case where
   * none of the FarSiders received the CommitProcessMessage
   **/
  public void removeMessage(TXCommitMessage deadMess) {
    synchronized (this.txInProgress) {
      this.txInProgress.remove(deadMess.getTrackerKey());
      // For any waitForAllToComplete
      if (txInProgress.isEmpty()) {
        this.txInProgress.notifyAll();
      }
    }
  }

  /**
   * Retrieve the commit message associated with the lock
   */
  public TXCommitMessage get(Object key) {
    final TXCommitMessage mess;
    synchronized (this.txInProgress) {
      mess = (TXCommitMessage) this.txInProgress.get(key);
    }
    return mess;
  }

  public TXCommitMessage waitForMessage(Object key, DistributionManager dm) {
    TXCommitMessage msg = null;
    synchronized (this.txInProgress) {
      msg = (TXCommitMessage) this.txInProgress.get(key);
      while (msg == null) {
        try {
          dm.getSystem().getCancelCriterion().checkCancelInProgress(null);
          this.txInProgress.wait();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        msg = (TXCommitMessage) this.txInProgress.get(key);
      }
    }
    return msg;
  }

  /**
   * The transcation commit message has been received
   */
  public void add(TXCommitMessage msg) {
    synchronized (this.txInProgress) {
      final Object key = msg.getTrackerKey();
      if (key == null) {
        Assert.assertTrue(false, "TXFarSideCMTracker must have a non-null key for message " + msg);
      }
      this.txInProgress.put(key, msg);
      this.txInProgress.notifyAll();
    }
  }

  // TODO we really need to keep around only one msg for each thread on a client
  private Map<TXId, TXCommitMessage> failoverMap =
      Collections.synchronizedMap(new LinkedHashMap<TXId, TXCommitMessage>() {
        @Override
        protected boolean removeEldestEntry(Entry eldest) {
          return size() > TXManagerImpl.FAILOVER_TX_MAP_SIZE;
        };
      });

  public void saveTXForClientFailover(TXId txId, TXCommitMessage msg) {
    this.failoverMap.put(txId, msg);
  }

  public TXCommitMessage getTXCommitMessage(TXId txId) {
    return this.failoverMap.get(txId);
  }

  /**
   * a static TXFarSideCMTracker is held by TXCommitMessage and is cleared when the cache has
   * finished closing
   */
  public void clearForCacheClose() {
    this.failoverMap.clear();
    this.lastHistoryItem = 0;
    Arrays.fill(this.txHistory, null);
  }

  @VisibleForTesting
  public int getFailoverMapSize() {
    return failoverMap.size();
  }
}

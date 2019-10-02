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

package org.apache.geode.internal.cache.locks;

import java.util.Set;
import java.util.concurrent.RejectedExecutionException;

import org.apache.logging.log4j.Logger;

import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.locks.DLockBatch;
import org.apache.geode.distributed.internal.locks.DLockGrantor;
import org.apache.geode.distributed.internal.locks.DLockLessorDepartureHandler;
import org.apache.geode.distributed.internal.locks.DLockService;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.logging.LogService;

/**
 * Handles departure of lessor (lease holder) by sending a message asking each participant if it's
 * ok to release the leases. Upon receipt of all replies the lease will be automatically released.
 *
 */
public class TXLessorDepartureHandler implements DLockLessorDepartureHandler {
  private static final Logger logger = LogService.getLogger();

  private final Object stateLock = new Object();
  private boolean processingDepartures;

  @Override
  public void waitForInProcessDepartures() throws InterruptedException {
    synchronized (stateLock) {
      while (processingDepartures) {
        stateLock.wait();
      }
    }
  }

  @Override
  public void handleDepartureOf(InternalDistributedMember owner, DLockGrantor grantor) {
    // get DTLS
    TXLockService dtls = TXLockService.getDTLS();
    if (dtls == null)
      return;
    try {
      if (!dtls.isLockGrantor()) {
        logger.debug("This member is not lock grantor; exiting TXLessorDepartureHandler");
        return;
      }

      DLockService dlock = ((TXLockServiceImpl) dtls).getInternalDistributedLockService();

      // DLockGrantor grantor = DLockGrantor.waitForGrantor(dlock, true);
      if (grantor == null || grantor.isDestroyed()) {
        logger.debug(
            "Lock grantor does not exist or has been destroyed; exiting TXLessorDepartureHandler");
        return;
      }

      // verify owner has active txLock
      DLockBatch[] batches = grantor.getLockBatches(owner);
      if (batches.length == 0) {
        logger.debug("{} has no active lock batches; exiting TXLessorDepartureHandler", owner);
        return;
      }
      sendRecoveryMsgs(dlock.getDistributionManager(), batches, owner, grantor);
    } catch (IllegalStateException e) {
      // ignore... service was destroyed
    } // outer try-catch
  }

  private void sendRecoveryMsgs(final DistributionManager dm, final DLockBatch[] batches,
      final InternalDistributedMember owner, final DLockGrantor grantor) {

    synchronized (stateLock) {
      processingDepartures = true;
    }
    Runnable recoverTx = () -> {
      try {
        for (int i = 0; i < batches.length; i++) {
          TXLockBatch batch = (TXLockBatch) batches[i];
          // send TXOriginatorDepartureMessage
          Set participants = batch.getParticipants();
          TXOriginatorRecoveryProcessor.sendMessage(participants, owner, batch.getTXLockId(),
              grantor, dm);
        }
      } finally {
        clearProcessingDepartures();
      }
    };

    try {
      dm.getExecutors().getWaitingThreadPool().execute(recoverTx);
    } catch (RejectedExecutionException e) {
      // this shouldn't happen unless we're shutting down or someone has set a size constraint
      // on the waiting-pool using a system property
      if (!dm.getCancelCriterion().isCancelInProgress()) {
        logger.warn("Unable to schedule background cleanup of transactions for departed member {}."
            + "  Performing in-line cleanup of the transactions.");
        recoverTx.run();
      }
    }
  }

  private void clearProcessingDepartures() {
    synchronized (stateLock) {
      processingDepartures = false;
      stateLock.notifyAll();
    }
  }

}

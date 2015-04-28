/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.internal.cache.locks;

import java.util.Set;
import java.util.concurrent.RejectedExecutionException;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.locks.DLockBatch;
import com.gemstone.gemfire.distributed.internal.locks.DLockGrantor;
import com.gemstone.gemfire.distributed.internal.locks.DLockLessorDepartureHandler;
import com.gemstone.gemfire.distributed.internal.locks.DLockService;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.logging.LogService;

/**
 * Handles departure of lessor (lease holder) by sending a message asking
 * each participant if it's ok to release the leases. Upon receipt of all
 * replies the lease will be automatically released.
 *
 * @author Kirk Lund
 */
public class TXLessorDepartureHandler
implements DLockLessorDepartureHandler {
  private static final Logger logger = LogService.getLogger();
  
  public void handleDepartureOf(InternalDistributedMember owner, DLockGrantor grantor) {
    // get DTLS
    TXLockService dtls = TXLockService.getDTLS();
    if (dtls == null) return;
    try {
      if (!dtls.isLockGrantor()) {
        logger.debug("This member is not lock grantor; exiting TXLessorDepartureHandler");
        return;
      }
      
      DLockService dlock = 
          ((TXLockServiceImpl)dtls).getInternalDistributedLockService();
          
      //DLockGrantor grantor = DLockGrantor.waitForGrantor(dlock, true);
      if (grantor == null || grantor.isDestroyed()) {
        logger.debug("Lock grantor does not exist or has been destroyed; exiting TXLessorDepartureHandler");
        return;
      }
      
      // verify owner has active txLock
      DLockBatch[] batches = grantor.getLockBatches(owner);
      if (batches.length == 0) {
        logger.debug("{} has no active lock batches; exiting TXLessorDepartureHandler", owner);
        return;
      }

      sendRecoveryMsgs(dlock.getDistributionManager(), batches, owner, grantor);
    }
    catch (IllegalStateException e) {
      // ignore... service was destroyed
    } // outer try-catch
  }
  
  private void sendRecoveryMsgs(final DM dm,
                                final DLockBatch[] batches,
                                final InternalDistributedMember owner,
                                final DLockGrantor grantor) {
    try {
      dm.getWaitingThreadPool().execute(new Runnable() {
          public void run() {
            for (int i = 0; i < batches.length; i++) {
              TXLockBatch batch = (TXLockBatch) batches[i];
              // send TXOriginatorDepartureMessage
              Set participants = batch.getParticipants(); 
              TXOriginatorRecoveryProcessor.sendMessage(
                 participants, owner, batch.getTXLockId(), grantor, dm);
            }
          }
        });
    }
    catch (RejectedExecutionException e) {
      if (logger.isDebugEnabled()) {
        logger.debug("Rejected sending recovery messages for departure of tx originator {}", owner, e);
      }
    }
  }

}


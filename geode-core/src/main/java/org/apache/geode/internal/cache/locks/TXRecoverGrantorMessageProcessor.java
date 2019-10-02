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

import java.util.concurrent.RejectedExecutionException;

import org.apache.logging.log4j.Logger;

import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.locks.DLockRecoverGrantorProcessor;
import org.apache.geode.distributed.internal.locks.DLockRemoteToken;
import org.apache.geode.distributed.internal.locks.DLockService;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.cache.TXCommitMessage;
import org.apache.geode.internal.logging.LogService;

/**
 * Provides processing of DLockRecoverGrantorProcessor. Reply will not be sent until all locks are
 * released.
 */
public class TXRecoverGrantorMessageProcessor
    implements DLockRecoverGrantorProcessor.MessageProcessor {

  private static final Logger logger = LogService.getLogger();

  @Override
  public void process(final DistributionManager dm,
      final DLockRecoverGrantorProcessor.DLockRecoverGrantorMessage msg) {

    try {
      dm.getExecutors().getWaitingThreadPool().execute(new Runnable() {
        @Override
        public void run() {
          processDLockRecoverGrantorMessage(dm, msg);
        }
      });
    } catch (RejectedExecutionException e) {
      logger.debug("Rejected processing of {}", msg, e);
    }
  }

  protected void processDLockRecoverGrantorMessage(final DistributionManager dm,
      final DLockRecoverGrantorProcessor.DLockRecoverGrantorMessage msg) {

    ReplyException replyException = null;
    int replyCode = DLockRecoverGrantorProcessor.DLockRecoverGrantorReplyMessage.OK;
    DLockRemoteToken[] heldLocks = new DLockRemoteToken[0];

    if (logger.isDebugEnabled()) {
      logger.debug("[TXRecoverGrantorMessageProcessor.process]");
    }
    boolean gotRecoveryLock = false;
    TXLockServiceImpl dtls = null;
    try {
      Assert.assertTrue(msg.getServiceName().startsWith(DLockService.DTLS),
          "TXRecoverGrantorMessageProcessor cannot handle service " + msg.getServiceName());

      // get the service from the name
      DLockService svc = DLockService.getInternalServiceNamed(msg.getServiceName());

      if (svc != null) {
        dtls = (TXLockServiceImpl) TXLockService.getDTLS();
        if (dtls != null) {
          // use TXLockServiceImpl recoveryLock to delay reply...
          dtls.acquireRecoveryWriteLock();
          gotRecoveryLock = true;

          // Wait for all the received transactions to finish processing
          TXCommitMessage.getTracker().waitForAllToProcess();
        }
      }
    } catch (InterruptedException t) {
      Thread.currentThread().interrupt();
      logger.warn("[TXRecoverGrantorMessageProcessor.process] throwable:",
          t);
      replyException = new ReplyException(t);
    } catch (RuntimeException t) {
      logger.warn("[TXRecoverGrantorMessageProcessor.process] throwable:",
          t);
      if (replyException == null) {
        replyException = new ReplyException(t);
      } else {
        logger.warn(String.format("More than one exception thrown in %s",
            this),
            t);
      }
    }
    // catch (VirtualMachineError err) {
    // SystemFailure.initiateFailure(err);
    // // If this ever returns, rethrow the error. We're poisoned
    // // now, so don't let this thread continue.
    // throw err;
    // }
    // catch (Throwable t) {
    // // Whenever you catch Error or Throwable, you must also
    // // catch VirtualMachineError (see above). However, there is
    // // _still_ a possibility that you are dealing with a cascading
    // // error condition, so you also need to check to see if the JVM
    // // is still usable:
    // SystemFailure.checkFailure();
    // if (replyException == null) {
    // replyException = new ReplyException(t);
    // }
    // }
    finally {
      if (gotRecoveryLock && dtls != null) {
        dtls.releaseRecoveryWriteLock();
      }

      DLockRecoverGrantorProcessor.DLockRecoverGrantorReplyMessage replyMsg =
          new DLockRecoverGrantorProcessor.DLockRecoverGrantorReplyMessage();
      replyMsg.setReplyCode(replyCode);
      replyMsg.setHeldLocks(heldLocks);
      replyMsg.setProcessorId(msg.getProcessorId());
      replyMsg.setRecipient(msg.getSender());
      replyMsg.setException(replyException);
      if (msg.getSender().equals(dm.getId())) {
        // process in-line in this VM
        if (logger.isDebugEnabled()) {
          logger.debug("[TXRecoverGrantorMessageProcessor.process] locally process reply");
        }
        replyMsg.setSender(dm.getId());
        replyMsg.dmProcess((ClusterDistributionManager) dm);
      } else {
        if (logger.isDebugEnabled()) {
          logger.debug("[TXRecoverGrantorMessageProcessor.process] send reply");
        }
        dm.putOutgoing(replyMsg);
      }
    }
  }

}

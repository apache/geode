/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/**
 * 
 */
package com.gemstone.gemfire.internal.cache.tier.sockets.command;

import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.cache.tier.CachedRegionHelper;
import com.gemstone.gemfire.internal.cache.tier.Command;
import com.gemstone.gemfire.internal.cache.tier.MessageType;
import com.gemstone.gemfire.internal.cache.tier.sockets.*;
import com.gemstone.gemfire.distributed.internal.DistributionStats;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;

import java.io.IOException;

public class Ping extends BaseCommand {

  private final static Ping singleton = new Ping();

  public static Command getCommand() {
    return singleton;
  }

  private Ping() {
  }

  @Override
  public void cmdExecute(Message msg, ServerConnection servConn, long start)
      throws IOException {
    final boolean isDebugEnabled = logger.isDebugEnabled();
    if (isDebugEnabled) {
      logger.debug("{}: rcv tx: {} from {} rcvTime: {}", servConn.getName(), msg.getTransactionId(), servConn.getSocketString(), (DistributionStats.getStatTime() - start));
    }
    ClientHealthMonitor chm = ClientHealthMonitor.getInstance();
    if (chm != null)
      chm.receivedPing(servConn.getProxyID());
    CachedRegionHelper crHelper = servConn.getCachedRegionHelper();
    if (crHelper.emulateSlowServer() > 0) {
      // this.logger.fine("SlowServer", new Exception());
      boolean interrupted = Thread.interrupted();
      try {
        Thread.sleep(crHelper.emulateSlowServer());
      }
      catch (InterruptedException ugh) {
        interrupted = true;
        servConn.getCachedRegionHelper().getCache().getCancelCriterion()
            .checkCancelInProgress(ugh);
      }
      finally {
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
      }
    }
    writeReply(msg, servConn);
    servConn.setAsTrue(RESPONDED);
    if (isDebugEnabled) {
      logger.debug("{}: Sent ping reply to {}", servConn.getName(), servConn.getSocketString());
    }
  }

  @Override
  protected void writeReply(Message origMsg, ServerConnection servConn)
      throws IOException {
    Message replyMsg = servConn.getReplyMessage();
    servConn.getCache().getCancelCriterion().checkCancelInProgress(null);
    replyMsg.setMessageType(MessageType.REPLY);
    if (Version.GFE_81.compareTo(servConn.getClientVersion()) > 0) {
      replyMsg.setNumberOfParts(1);
      replyMsg.setTransactionId(origMsg.getTransactionId());
      replyMsg.addBytesPart(OK_BYTES);
    } else {
      replyMsg.setNumberOfParts(2);
      replyMsg.setTransactionId(origMsg.getTransactionId());
      replyMsg.addBytesPart(OK_BYTES);
      long cacheTime = InternalDistributedSystem.getConnectedInstance()
          .getDistributionManager().cacheTimeMillis();
      replyMsg.addLongPart(cacheTime);
    }
    replyMsg.send(servConn);
    if (logger.isTraceEnabled()) {
      logger.trace("{}: rpl tx: {}", servConn.getName(), origMsg.getTransactionId());
    }
  }
}

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

import com.gemstone.gemfire.cache.client.internal.DestroyOp;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.tier.Command;
import com.gemstone.gemfire.internal.cache.tier.MessageType;
import com.gemstone.gemfire.internal.cache.tier.sockets.Message;
import com.gemstone.gemfire.internal.cache.tier.sockets.ServerConnection;
import com.gemstone.gemfire.internal.cache.versions.VersionTag;

import java.io.IOException;

/**
 * @author bruces
 *
 */
public class Destroy70 extends Destroy65 {
  private final static Destroy70 singleton = new Destroy70();

  public static Command getCommand() {
    return singleton;
  }

  private Destroy70() {
  }

  @Override
  protected void writeReplyWithRefreshMetadata(Message origMsg,
      ServerConnection servConn, PartitionedRegion pr,
      boolean entryNotFoundForRemove, byte nwHop, VersionTag versionTag) throws IOException {
    Message replyMsg = servConn.getReplyMessage();
    servConn.getCache().getCancelCriterion().checkCancelInProgress(null);
    replyMsg.setMessageType(MessageType.REPLY);
    int flags = 0;
    int numParts = 3;
    if (versionTag != null) {
      flags |= DestroyOp.HAS_VERSION_TAG;
      numParts++;
    }
    flags |= DestroyOp.HAS_ENTRY_NOT_FOUND_PART;
//    if (logger.isDebugEnabled()) {
//      logger.fine("writing response with metadata and " + numParts + " parts");
//    }
    replyMsg.setNumberOfParts(numParts);
    replyMsg.setTransactionId(origMsg.getTransactionId());
    replyMsg.addIntPart(flags);
    if (versionTag != null) {
      replyMsg.addObjPart(versionTag);
    }
    replyMsg.addBytesPart(new byte[]{pr.getMetadataVersion().byteValue(), nwHop});
    replyMsg.addIntPart(entryNotFoundForRemove? 1 : 0);
    pr.getPrStats().incPRMetaDataSentCount();
    replyMsg.send(servConn);
    if (logger.isTraceEnabled()) {
      logger.trace("{}: rpl with REFRESH_METADAT tx: {}", servConn.getName(), origMsg.getTransactionId());
    }
  }

  @Override
  protected void writeReply(Message origMsg, ServerConnection servConn,
      boolean entryNotFound, VersionTag versionTag)
  throws IOException {
    if (logger.isDebugEnabled()) {
      logger.debug("Destroy70.writeReply(entryNotFound={}, tag={})", entryNotFound, versionTag);
    }
    Message replyMsg = servConn.getReplyMessage();
    servConn.getCache().getCancelCriterion().checkCancelInProgress(null);
    replyMsg.setMessageType(MessageType.REPLY);
    int flags = 0;
    int numParts = 3;
    if (versionTag != null) {
      flags |= DestroyOp.HAS_VERSION_TAG;
      numParts++;
    }
    flags |= DestroyOp.HAS_ENTRY_NOT_FOUND_PART;
//    if (logger.isDebugEnabled()) {
//      logger.fine("writing response with 1-byte metadata and " + numParts + " parts");
//    }
    replyMsg.setNumberOfParts(numParts);
    replyMsg.setTransactionId(origMsg.getTransactionId());
    replyMsg.addIntPart(flags);
    if (versionTag != null) {
//      if (logger.isDebugEnabled()) {
//        logger.fine("wrote version tag in response: " + versionTag);
//      }
      replyMsg.addObjPart(versionTag);
//    } else {
//      if (logger.isDebugEnabled()) {
//        logger.fine("response has no version tag");
//    }
    }
    replyMsg.addBytesPart(OK_BYTES); // make old single-hop code happy by puting byte[]{0} here
    replyMsg.addIntPart(entryNotFound? 1 : 0);
    replyMsg.send(servConn);
    if (logger.isTraceEnabled()) {
      logger.trace("{}: rpl tx: {} parts={}", servConn.getName(), origMsg.getTransactionId(), replyMsg.getNumberOfParts());
    }
  }
}

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
import com.gemstone.gemfire.cache.client.internal.InvalidateOp;
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
public class Invalidate70 extends Invalidate {

  private final static Invalidate70 singleton = new Invalidate70();

  public static Command getCommand() {
    return singleton;
  }

  private Invalidate70() {
  }

  @Override
  protected void writeReplyWithRefreshMetadata(Message origMsg,
      ServerConnection servConn, PartitionedRegion pr,
      byte nwHop, VersionTag versionTag) throws IOException {
    Message replyMsg = servConn.getReplyMessage();
    servConn.getCache().getCancelCriterion().checkCancelInProgress(null);
    replyMsg.setMessageType(MessageType.REPLY);
    int flags = 0;
    int numParts = 2;
    if (versionTag != null) {
      flags |= InvalidateOp.HAS_VERSION_TAG;
      numParts++;
    }
    replyMsg.setNumberOfParts(numParts);
    replyMsg.setTransactionId(origMsg.getTransactionId());
    replyMsg.addIntPart(flags);
    if (versionTag != null) {
      replyMsg.addObjPart(versionTag);
    }
    replyMsg.addBytesPart(new byte[]{pr.getMetadataVersion().byteValue(), nwHop});
    pr.getPrStats().incPRMetaDataSentCount();
    replyMsg.send(servConn);
    if (logger.isTraceEnabled()) {
      logger.trace("{}: rpl with REFRESH_METADAT tx: {}", servConn.getName(), origMsg.getTransactionId());
    }
  }

  @Override
  protected void writeReply(Message origMsg, ServerConnection servConn,
      VersionTag versionTag)
  throws IOException {
    Message replyMsg = servConn.getReplyMessage();
    servConn.getCache().getCancelCriterion().checkCancelInProgress(null);
    replyMsg.setMessageType(MessageType.REPLY);
    int flags = 0;
    int numParts = 2;
    if (versionTag != null) {
      flags |= DestroyOp.HAS_VERSION_TAG;
      numParts++;
    }
    replyMsg.setNumberOfParts(numParts);
    replyMsg.setTransactionId(origMsg.getTransactionId());
    replyMsg.addIntPart(flags);
    if (versionTag != null) {
      if (logger.isDebugEnabled()) {
        logger.debug("wrote version tag in response: {}", versionTag);
      }
      replyMsg.addObjPart(versionTag);
    } else {
      if (logger.isDebugEnabled()) {
        logger.debug("response has no version tag");
      }
    }
    replyMsg.addBytesPart(OK_BYTES); // make old single-hop code happy by putting byte[]{0} here
    replyMsg.send(servConn);
    if (logger.isTraceEnabled()) {
      logger.trace("{}: rpl tx: {} parts={}", servConn.getName(), origMsg.getTransactionId(), replyMsg.getNumberOfParts());
    }
  }
}

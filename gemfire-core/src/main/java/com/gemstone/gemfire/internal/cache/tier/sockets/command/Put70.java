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

import com.gemstone.gemfire.cache.client.internal.PutOp;
import com.gemstone.gemfire.internal.Version;
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
public class Put70 extends Put65 {

  private final static Put70 singleton = new Put70();

  public static Command getCommand() {
    return singleton;
  }

  private Put70() {
  }

  @Override
  protected void writeReply(Message origMsg, ServerConnection servConn,
      boolean sendOldValue, boolean oldValueIsObject, Object oldValue,
      VersionTag versionTag)
  throws IOException {
    Message replyMsg = servConn.getReplyMessage();
    servConn.getCache().getCancelCriterion().checkCancelInProgress(null);
    replyMsg.setMessageType(MessageType.REPLY);
    int flags = 0;
    int parts = 2;
    if (sendOldValue) {
      flags |= PutOp.HAS_OLD_VALUE_FLAG;
      if (oldValueIsObject) {
        flags |= PutOp.OLD_VALUE_IS_OBJECT_FLAG;
      }
      parts++;
    }
    if (versionTag != null) {
      flags |= PutOp.HAS_VERSION_TAG;
      parts++;
    }
    replyMsg.setNumberOfParts(parts);
    replyMsg.setTransactionId(origMsg.getTransactionId());
    replyMsg.addBytesPart(OK_BYTES);
    replyMsg.addIntPart(flags);
    if (sendOldValue) {
      replyMsg.addObjPart(oldValue);
    }
    if (versionTag != null) {
      replyMsg.addObjPart(versionTag);
    }
    replyMsg.send(servConn);
    if (logger.isTraceEnabled()) {
      logger.trace("{}: rpl tx: {} parts={}", servConn.getName(), origMsg.getTransactionId(), replyMsg.getNumberOfParts());
    }
  }

  @Override
  protected void writeReplyWithRefreshMetadata(Message origMsg,
      ServerConnection servConn, PartitionedRegion pr,
      boolean sendOldValue, boolean oldValueIsObject, Object oldValue, byte nwHopType, VersionTag versionTag)
  throws IOException {
    Message replyMsg = servConn.getReplyMessage();
    servConn.getCache().getCancelCriterion().checkCancelInProgress(null);
    replyMsg.setMessageType(MessageType.REPLY);
    int flags = 0;
    int parts = 1;
    parts++; // flags
    if (sendOldValue) {
      flags |= PutOp.HAS_OLD_VALUE_FLAG;
      if (oldValueIsObject) {
        flags |= PutOp.OLD_VALUE_IS_OBJECT_FLAG;
      }
      parts++;
    }
    if (versionTag != null) {
      flags |= PutOp.HAS_VERSION_TAG;
      parts++;
    }
    replyMsg.setNumberOfParts(parts);
    replyMsg.setTransactionId(origMsg.getTransactionId());
    replyMsg.addBytesPart(new byte[]{pr.getMetadataVersion().byteValue(), nwHopType});
    replyMsg.addIntPart(flags);
    if (sendOldValue) {
//      if (logger.fineEnabled()) {
//        logger.fine("sending old value in Put response");
//      }
      replyMsg.addObjPart(oldValue);
    }
    if (versionTag != null) {
      replyMsg.addObjPart(versionTag);
    }
    replyMsg.send(servConn);
    pr.getPrStats().incPRMetaDataSentCount();
    if (logger.isTraceEnabled()) {
      logger.trace("{}: rpl with REFRESH_METADAT tx: {} parts={}", servConn.getName(), origMsg.getTransactionId(), replyMsg.getNumberOfParts());
    }
  }
}

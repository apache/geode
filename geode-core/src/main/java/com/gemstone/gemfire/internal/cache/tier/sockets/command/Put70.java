/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
    replyMsg.addBytesPart(new byte[]{pr.getMetadataVersion(), nwHopType});
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

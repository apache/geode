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
/**
 * 
 */
package org.apache.geode.internal.cache.tier.sockets.command;

import org.apache.geode.i18n.LogWriterI18n;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.tier.CachedRegionHelper;
import org.apache.geode.internal.cache.tier.Command;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.*;
import org.apache.geode.distributed.internal.DistributionStats;
import org.apache.geode.distributed.internal.InternalDistributedSystem;

import java.io.IOException;

public class Ping extends BaseCommand {

  private final static Ping singleton = new Ping();

  public static Command getCommand() {
    return singleton;
  }

  private Ping() {}

  @Override
  public void cmdExecute(Message msg, ServerConnection servConn, long start) throws IOException {
    final boolean isDebugEnabled = logger.isDebugEnabled();
    if (isDebugEnabled) {
      logger.debug("{}: rcv tx: {} from {} rcvTime: {}", servConn.getName(), msg.getTransactionId(),
          servConn.getSocketString(), (DistributionStats.getStatTime() - start));
    }
    ClientHealthMonitor chm = ClientHealthMonitor.getInstance();
    if (chm != null)
      chm.receivedPing(servConn.getProxyID());
    CachedRegionHelper crHelper = servConn.getCachedRegionHelper();

    writeReply(msg, servConn);
    servConn.setAsTrue(RESPONDED);
    if (isDebugEnabled) {
      logger.debug("{}: Sent ping reply to {}", servConn.getName(), servConn.getSocketString());
    }
  }

  @Override
  protected void writeReply(Message origMsg, ServerConnection servConn) throws IOException {
    Message replyMsg = servConn.getReplyMessage();
    servConn.getCache().getCancelCriterion().checkCancelInProgress(null);
    replyMsg.setMessageType(MessageType.REPLY);
    replyMsg.setNumberOfParts(1);
    replyMsg.setTransactionId(origMsg.getTransactionId());
    replyMsg.addBytesPart(OK_BYTES);
    replyMsg.send(servConn);
    if (logger.isTraceEnabled()) {
      logger.trace("{}: rpl tx: {}", servConn.getName(), origMsg.getTransactionId());
    }
  }
}

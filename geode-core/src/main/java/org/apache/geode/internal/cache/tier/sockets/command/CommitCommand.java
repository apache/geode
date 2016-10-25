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
package org.apache.geode.internal.cache.tier.sockets.command;

import org.apache.geode.cache.CommitConflictException;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.i18n.LogWriterI18n;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.cache.TXCommitMessage;
import org.apache.geode.internal.cache.TXId;
import org.apache.geode.internal.cache.TXManagerImpl;
import org.apache.geode.internal.cache.TXStateProxy;
import org.apache.geode.internal.cache.tier.Command;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.BaseCommand;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.internal.i18n.LocalizedStrings;

import java.io.IOException;

/**
 * This is the base command which read the parts for the
 * MessageType.COMMIT.<br>
 * 
 * @since GemFire 6.6
 */
public class CommitCommand extends BaseCommand {

  private final static CommitCommand singleton = new CommitCommand();

  public static Command getCommand() {
    return singleton;
  }

  private CommitCommand() {
  }

  @Override
  public void cmdExecute(Message msg, ServerConnection servConn, long start)
      throws IOException {
    servConn.setAsTrue(REQUIRES_RESPONSE);
    TXManagerImpl txMgr = (TXManagerImpl)servConn.getCache().getCacheTransactionManager();
    InternalDistributedMember client = (InternalDistributedMember) servConn.getProxyID().getDistributedMember();
    int uniqId = msg.getTransactionId();
    TXId txId = new TXId(client, uniqId);
    TXCommitMessage commitMsg = null;
    if (txMgr.isHostedTxRecentlyCompleted(txId)) {
      commitMsg = txMgr.getRecentlyCompletedMessage(txId);
      if (logger.isDebugEnabled()) {
        logger.debug("TX: returning a recently committed txMessage for tx: {}", txId);
      }
      if (!txMgr.isExceptionToken(commitMsg)) {
        writeCommitResponse(commitMsg, msg, servConn);
        commitMsg.setClientVersion(null);  // fixes bug 46529
        servConn.setAsTrue(RESPONDED);
      } else {
        sendException(msg, servConn, txMgr.getExceptionForToken(commitMsg, txId));
      }
      txMgr.removeHostedTXState(txId);
      return;
    }
    boolean wasInProgress = txMgr.setInProgress(true); // fixes bug 43350
    final TXStateProxy txProxy = txMgr.getTXState();
    Assert.assertTrue(txProxy != null);
    if (logger.isDebugEnabled()) {
      logger.debug("TX: committing client tx: {}", txId);
    }
    try {

      txId = txProxy.getTxId();

      txProxy.setCommitOnBehalfOfRemoteStub(true);
      txMgr.commit();

      commitMsg = txProxy.getCommitMessage();
      writeCommitResponse(commitMsg, msg, servConn);
      servConn.setAsTrue(RESPONDED);
    }
    catch (Exception e) {
      sendException(msg, servConn,e);
    } finally {
      if(txId!=null) {
        txMgr.removeHostedTXState(txId);
      }
      if (!wasInProgress) {
        txMgr.setInProgress(false);
      }
      if (commitMsg != null) {
        commitMsg.setClientVersion(null);  // fixes bug 46529
      }
    }
  }

  
  
  protected static void writeCommitResponse(TXCommitMessage response,
      Message origMsg, ServerConnection servConn)
      throws IOException {
    Message responseMsg = servConn.getResponseMessage();
    responseMsg.setMessageType(MessageType.RESPONSE);
    responseMsg.setTransactionId(origMsg.getTransactionId());
    responseMsg.setNumberOfParts(1);
    if( response != null ) {
    	response.setClientVersion(servConn.getClientVersion());
    }
    responseMsg.addObjPart(response, zipValues);
    servConn.getCache().getCancelCriterion().checkCancelInProgress(null);
    if (logger.isDebugEnabled()) {
      logger.debug("TX: sending a nonNull response for transaction: {}", new TXId((InternalDistributedMember) servConn.getProxyID().getDistributedMember(), origMsg.getTransactionId()));
    }
    responseMsg.send(servConn);
    origMsg.clearParts();
  }
  
  private void sendException(Message msg,
      ServerConnection servConn, Throwable e) throws IOException {
      writeException(msg, MessageType.EXCEPTION,
          e, false,servConn);
      servConn.setAsTrue(RESPONDED);
  }
  
  
}

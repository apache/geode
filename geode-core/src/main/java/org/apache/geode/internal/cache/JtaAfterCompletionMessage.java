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
package org.apache.geode.internal.cache;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.Cache;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.TXRemoteCommitMessage.RemoteCommitResponse;
import org.apache.geode.internal.cache.TXRemoteCommitMessage.TXRemoteCommitReplyMessage;
import org.apache.geode.internal.logging.LogService;

/**
 *
 */
public class JtaAfterCompletionMessage extends TXMessage {

  private static final Logger logger = LogService.getLogger();

  private int status;

  private int processorType;

  public JtaAfterCompletionMessage() {}

  @Override
  public int getProcessorType() {
    return this.processorType;
  }

  public JtaAfterCompletionMessage(int status, int txUniqId,
      InternalDistributedMember onBehalfOfClientMember, ReplyProcessor21 processor) {
    super(txUniqId, onBehalfOfClientMember, processor);
    this.status = status;
  }

  public static RemoteCommitResponse send(Cache cache, int txId,
      InternalDistributedMember onBehalfOfClientMember, int status, DistributedMember recipient) {
    final InternalDistributedSystem system =
        (InternalDistributedSystem) cache.getDistributedSystem();
    final Set recipients = Collections.singleton(recipient);
    RemoteCommitResponse response = new RemoteCommitResponse(system, recipients);
    JtaAfterCompletionMessage msg =
        new JtaAfterCompletionMessage(status, txId, onBehalfOfClientMember, response);
    msg.setRecipients(recipients);
    // bug #43087 - hang sending JTA synchronizations from delegate server
    if (system.threadOwnsResources()) {
      msg.processorType = DistributionManager.SERIAL_EXECUTOR;
    } else {
      msg.processorType = DistributionManager.HIGH_PRIORITY_EXECUTOR;
    }
    system.getDistributionManager().putOutgoing(msg);
    return response;
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.geode.internal.cache.TXMessage#operateOnTx(org.apache.geode.internal.cache.TXId)
   */
  @Override
  protected boolean operateOnTx(TXId txId, DistributionManager dm) throws RemoteOperationException {
    TXManagerImpl txMgr = GemFireCacheImpl.getInstance().getTXMgr();
    if (logger.isDebugEnabled()) {
      logger.debug("JTA: Calling afterCompletion for :{}", txId);
    }
    TXStateProxy txState = txMgr.getTXState();
    txState.setCommitOnBehalfOfRemoteStub(true);
    txState.afterCompletion(status);
    TXCommitMessage cmsg = txState.getCommitMessage();
    TXRemoteCommitReplyMessage.send(getSender(), getProcessorId(), cmsg, getReplySender(dm));
    txMgr.removeHostedTXState(txId);
    return false;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.geode.internal.DataSerializableFixedID#getDSFID()
   */
  public int getDSFID() {
    return JTA_AFTER_COMPLETION_MESSAGE;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.geode.internal.cache.TXMessage#toData(java.io.DataOutput)
   */
  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    out.writeInt(this.status);
    out.writeInt(this.processorType);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.geode.internal.cache.TXMessage#fromData(java.io.DataInput)
   */
  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.status = in.readInt();
    this.processorType = in.readInt();
  }

}

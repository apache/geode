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
package org.apache.geode.internal.cache.tier.sockets;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Set;

import org.apache.geode.CancelException;
import org.apache.geode.distributed.internal.DM;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.HighPriorityDistributionMessage;
import org.apache.geode.distributed.internal.MessageWithReply;
import org.apache.geode.distributed.internal.ReplyMessage;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.internal.InternalDataSerializer;

/**
 * Send interest registration to another server. Since interest registration performs a state-flush
 * operation this message must not transmitted on an ordered socket.
 * <p>
 * Extracted from CacheClientNotifier
 */
public class ServerInterestRegistrationMessage extends HighPriorityDistributionMessage
    implements MessageWithReply {

  private ClientProxyMembershipID clientId;
  private ClientInterestMessageImpl clientMessage;
  private int processorId;

  ServerInterestRegistrationMessage(ClientProxyMembershipID clientId,
      ClientInterestMessageImpl clientInterestMessage) {
    this.clientId = clientId;
    this.clientMessage = clientInterestMessage;
  }

  public ServerInterestRegistrationMessage() {
    // deserializing in fromData
  }

  static void sendInterestChange(DM dm, ClientProxyMembershipID clientId,
      ClientInterestMessageImpl clientInterestMessage) {
    ServerInterestRegistrationMessage registrationMessage =
        new ServerInterestRegistrationMessage(clientId, clientInterestMessage);

    Set recipients = dm.getOtherDistributionManagerIds();
    registrationMessage.setRecipients(recipients);

    ReplyProcessor21 replyProcessor = new ReplyProcessor21(dm, recipients);
    registrationMessage.processorId = replyProcessor.getProcessorId();

    dm.putOutgoing(registrationMessage);

    try {
      replyProcessor.waitForReplies();
    } catch (InterruptedException ignore) {
      Thread.currentThread().interrupt();
    }
  }

  @Override
  protected void process(DistributionManager dm) {
    // Get the proxy for the proxy id
    try {
      CacheClientNotifier clientNotifier = CacheClientNotifier.getInstance();
      if (clientNotifier != null) {
        CacheClientProxy proxy = clientNotifier.getClientProxy(this.clientId);
        // If this VM contains a proxy for the requested proxy id, forward the
        // message on to the proxy for processing
        if (proxy != null) {
          proxy.processInterestMessage(this.clientMessage);
        }
      }
    } finally {
      ReplyMessage reply = new ReplyMessage();
      reply.setProcessorId(this.processorId);
      reply.setRecipient(getSender());
      try {
        dm.putOutgoing(reply);
      } catch (CancelException ignore) {
        // can't send a reply, so ignore the exception
      }
    }
  }

  @Override
  public int getDSFID() {
    return SERVER_INTEREST_REGISTRATION_MESSAGE;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    out.writeInt(this.processorId);
    InternalDataSerializer.invokeToData(this.clientId, out);
    InternalDataSerializer.invokeToData(this.clientMessage, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.processorId = in.readInt();
    this.clientId = new ClientProxyMembershipID();
    InternalDataSerializer.invokeFromData(this.clientId, in);
    this.clientMessage = new ClientInterestMessageImpl();
    InternalDataSerializer.invokeFromData(this.clientMessage, in);
  }
}

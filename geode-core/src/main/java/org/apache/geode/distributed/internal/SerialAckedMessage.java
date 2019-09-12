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
package org.apache.geode.distributed.internal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.DataSerializer;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;

/**
 * A message that is sent to a given collection of managers and then awaits replies. It is used by
 * some tests to flush the serial communication channels after no-ack tests.
 *
 */
public class SerialAckedMessage extends SerialDistributionMessage implements MessageWithReply {
  private static final Logger logger = LogService.getLogger();

  /** The is of the distribution manager that sent the message */
  private InternalDistributedMember id;
  private int processorId;

  transient ClusterDistributionManager originDm;
  private transient ReplyProcessor21 rp;

  public SerialAckedMessage() {
    super();
    InternalDistributedSystem ds = InternalDistributedSystem.getAnyInstance();
    if (ds != null) { // this constructor is used in serialization as well as when sending to others
      this.originDm = (ClusterDistributionManager) ds.getDistributionManager();
      this.id = this.originDm.getDistributionManagerId();
    }
  }

  /**
   * send the message and wait for replies
   *
   * @param recipients the destination manager ids
   * @param multicast whether to use multicast or unicast
   * @throws InterruptedException if the operation is interrupted (as by shutdown)
   * @throws ReplyException if an exception was sent back by another manager
   */
  public void send(Collection recipients, boolean multicast)
      throws InterruptedException, ReplyException {
    final boolean isDebugEnabled = logger.isDebugEnabled();

    if (Thread.interrupted())
      throw new InterruptedException();
    recipients = new HashSet(recipients);
    DistributedMember me = originDm.getDistributionManagerId();
    if (recipients.contains(me)) {
      recipients.remove(me);
    }
    // this message is only used by battery tests so we can log info level debug
    // messages
    if (isDebugEnabled) {
      logger.debug("Recipients for SerialAckedMessage are {}", recipients);
    }
    rp = new ReplyProcessor21(originDm, recipients);
    processorId = rp.getProcessorId();
    setRecipients(recipients);
    setMulticast(multicast);
    Set failures = originDm.putOutgoing(this);
    if (failures != null && failures.size() > 0) {
      for (Iterator i = failures.iterator(); i.hasNext();) {
        InternalDistributedMember mbr = (InternalDistributedMember) i.next();
        if (isDebugEnabled) {
          logger.debug("Unable to send serial acked message to {}", mbr);
        }
        // rp.removeMember(mbr, true);
      }
    }

    rp.waitForReplies();
  }


  /**
   * Sets the id of the distribution manager that is shutting down
   */
  void setDistributionManagerId(InternalDistributedMember id) {
    this.id = id;
  }

  /** set the reply processor id that's used to wait for acknowledgements */
  public void setProcessorId(int pid) {
    processorId = pid;
  }

  /** return the reply processor id that's used to wait for acknowledgements */
  @Override
  public int getProcessorId() {
    return processorId;
  }

  /**
   * Adds the distribution manager that is started up to the current DM's list of members.
   *
   * This method is invoked on the receiver side
   */
  @Override
  protected void process(ClusterDistributionManager dm) {
    Assert.assertTrue(this.id != null);
    ReplyMessage reply = new ReplyMessage();
    reply.setProcessorId(processorId);
    reply.setRecipient(getSender());
    dm.putOutgoing(reply);
  }

  @Override
  public int getDSFID() {
    return SERIAL_ACKED_MESSAGE;
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    super.toData(out, context);
    out.writeInt(processorId);
    DataSerializer.writeObject(this.id, out);
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {

    super.fromData(in, context);
    processorId = in.readInt();
    this.id = (InternalDistributedMember) DataSerializer.readObject(in);
  }

  @Override
  public String toString() {
    return "SerialAckedMessage from=" + this.id + ";processorId=" + this.processorId;
  }

}

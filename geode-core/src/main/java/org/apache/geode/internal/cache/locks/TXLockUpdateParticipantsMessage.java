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

package org.apache.geode.internal.cache.locks;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Set;

import org.apache.geode.DataSerializer;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.MessageWithReply;
import org.apache.geode.distributed.internal.PooledDistributionMessage;
import org.apache.geode.distributed.internal.ReplyMessage;
import org.apache.geode.distributed.internal.locks.DLockGrantor;
import org.apache.geode.distributed.internal.locks.DLockService;
import org.apache.geode.distributed.internal.locks.LockGrantorDestroyedException;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;

/**
 * A message to update the Grantor with the latest TXLock participants This class was added as part
 * of the solution to bug 32999. It is used to update the Grantor that holds the lock batch for a
 * given TXLockId. This update is needed in the event that a recovery is needed when the TXLock
 * Lessor (the origin VM of the transaction) crashes/departs before or while sending the
 * TXCommitMessage but after making the reservation for the transaction.
 *
 * @see org.apache.geode.distributed.internal.locks.DLockGrantor#getLockBatch(Object)
 * @see org.apache.geode.distributed.internal.locks.DLockGrantor#updateLockBatch(Object,
 *      org.apache.geode.distributed.internal.locks.DLockBatch)
 * @see TXLockBatch#getBatchId
 * @see TXLessorDepartureHandler
 * @see TXOriginatorRecoveryProcessor
 * @see org.apache.geode.internal.cache.TXFarSideCMTracker
 *
 * @since GemFire 4.1.1
 */
public class TXLockUpdateParticipantsMessage extends PooledDistributionMessage
    implements MessageWithReply {

  private transient TXLockId txLockId;
  private transient String serviceName;
  private transient Set updatedParticipants;
  private transient int processorId;

  public TXLockUpdateParticipantsMessage(TXLockId txLockId, String serviceName,
      Set updatedParticipants, int processorId) {
    this.txLockId = txLockId;
    this.serviceName = serviceName;
    this.updatedParticipants = updatedParticipants;
    this.processorId = processorId;
  }

  public TXLockUpdateParticipantsMessage() {
    this.txLockId = null;
    this.serviceName = null;
    this.updatedParticipants = null;
  }

  @Override
  public void process(ClusterDistributionManager dm) {
    // dm.getLogger().info("DEBUG Processing " + this);
    DLockService svc = DLockService.getInternalServiceNamed(this.serviceName);
    if (svc != null) {
      updateParticipants(svc, this.txLockId, this.updatedParticipants);
    }
    TXLockUpdateParticipantsReplyMessage reply = new TXLockUpdateParticipantsReplyMessage();
    reply.setProcessorId(this.processorId);
    reply.setRecipient(getSender());
    dm.putOutgoing(reply);
  }

  /**
   * Update the Grantor with a new set of Participants. This method is meant to be used in a local
   * context (does <b>NOT</b> involve any messaging)
   */
  public static void updateParticipants(DLockService svc, TXLockId txLockId,
      Set updatedParticipants) {
    DLockGrantor grantor = null;
    try {
      grantor = DLockGrantor.waitForGrantor(svc);
      if (grantor != null) {
        try {
          TXLockBatch txb = (TXLockBatch) grantor.getLockBatch(txLockId);
          if (txb == null) {
            // we became grantor after the original grantor left
            // fixes bug 42656
            return;
          }
          txb.setParticipants(updatedParticipants);
          grantor.updateLockBatch(txLockId, txb);
        } catch (LockGrantorDestroyedException ignoreit) {
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public int getProcessorId() {
    return this.processorId;
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    super.toData(out, context);
    out.writeInt(this.processorId);
    InternalDataSerializer.invokeToData(this.txLockId, out);
    DataSerializer.writeString(this.serviceName, out);
    InternalDataSerializer.writeSet(this.updatedParticipants, out);
  }

  @Override
  public int getDSFID() {
    return TX_LOCK_UPDATE_PARTICIPANTS_MESSAGE;
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    this.processorId = in.readInt();
    this.txLockId = TXLockIdImpl.createFromData(in);
    this.serviceName = DataSerializer.readString(in);
    this.updatedParticipants = InternalDataSerializer.readSet(in);
  }

  @Override
  public String toString() {
    return "TXLockUpdateParticipantsMessage for " + "service=" + this.serviceName
        + "; updatedParticipants=" + this.updatedParticipants + "; txLockId=" + this.txLockId;
  }

  /**
   * The simple reply message that the sender waits for
   */
  public static class TXLockUpdateParticipantsReplyMessage extends ReplyMessage {

    @Override
    public int getDSFID() {
      return TX_LOCK_UPDATE_PARTICIPANTS_REPLY_MESSAGE;
    }

    @Override
    public void fromData(DataInput in,
        DeserializationContext context) throws IOException, ClassNotFoundException {
      super.fromData(in, context);
    }

    @Override
    public void toData(DataOutput out,
        SerializationContext context) throws IOException {
      super.toData(out, context);
    }

    @Override
    public String toString() {
      return "TXLockUpdateParticipantsReplyMessage processorId=" + super.processorId + "; sender="
          + this.getSender();
    }
  }

}

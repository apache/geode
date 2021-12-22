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

package org.apache.geode.distributed.internal.locks;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.logging.log4j.Logger;

import org.apache.geode.DataSerializer;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.MessageWithReply;
import org.apache.geode.distributed.internal.PooledDistributionMessage;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.ReplyMessage;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * A processor for telling the old grantor that it is deposed by a new grantor. Processor waits for
 * ack before completing.
 *
 * @since GemFire 4.0 (renamed from ExpectTransferProcessor)
 */
public class DeposeGrantorProcessor extends ReplyProcessor21 {

  private static final Logger logger = LogService.getLogger();

  ////////// Public static entry point /////////


  /**
   * Send a message to oldGrantor telling it that it is deposed by newGrantor. Send does not
   * complete until an ack is received or the oldGrantor leaves the system.
   */
  static void send(String serviceName, InternalDistributedMember oldGrantor,
      InternalDistributedMember newGrantor, long newGrantorVersion, int newGrantorSerialNumber,
      DistributionManager dm) {
    final InternalDistributedMember elder = dm.getId();
    if (elder.equals(oldGrantor)) {
      doOldGrantorWork(serviceName, elder, newGrantor, newGrantorVersion, newGrantorSerialNumber,
          dm, null);
    } else {
      DeposeGrantorProcessor processor = new DeposeGrantorProcessor(dm, oldGrantor);
      DeposeGrantorMessage.send(serviceName, oldGrantor, newGrantor, newGrantorVersion,
          newGrantorSerialNumber, dm, processor);
      try {
        processor.waitForRepliesUninterruptibly();
      } catch (ReplyException e) {
        e.handleCause();
      }
    }
  }

  protected static void doOldGrantorWork(final String serviceName,
      final InternalDistributedMember elder, final InternalDistributedMember youngTurk,
      final long newGrantorVersion, final int newGrantorSerialNumber, final DistributionManager dm,
      final DeposeGrantorMessage msg) {
    try {
      DLockService svc = DLockService.getInternalServiceNamed(serviceName);
      if (svc != null) {
        LockGrantorId newLockGrantorId =
            new LockGrantorId(dm, youngTurk, newGrantorVersion, newGrantorSerialNumber);
        svc.deposeOlderLockGrantorId(newLockGrantorId);
      }
    } finally {
      if (msg != null) {
        msg.reply(dm);
      }
    }
  }

  //////////// Instance methods //////////////

  /**
   * Creates a new instance of DeposeGrantorProcessor
   */
  private DeposeGrantorProcessor(DistributionManager dm, InternalDistributedMember oldGrantor) {
    super(dm, oldGrantor);
  }


  /////////////// Inner message classes //////////////////

  public static class DeposeGrantorMessage extends PooledDistributionMessage
      implements MessageWithReply {
    private int processorId;
    private String serviceName;
    private InternalDistributedMember newGrantor;
    private long newGrantorVersion;
    private int newGrantorSerialNumber;

    protected static void send(String serviceName, InternalDistributedMember oldGrantor,
        InternalDistributedMember newGrantor, long newGrantorVersion, int newGrantorSerialNumber,
        DistributionManager dm, ReplyProcessor21 proc) {
      DeposeGrantorMessage msg = new DeposeGrantorMessage();
      msg.serviceName = serviceName;
      msg.newGrantor = newGrantor;
      msg.newGrantorVersion = newGrantorVersion;
      msg.newGrantorSerialNumber = newGrantorSerialNumber;
      msg.processorId = proc.getProcessorId();
      msg.setRecipient(oldGrantor);
      if (logger.isTraceEnabled(LogMarker.DLS_VERBOSE)) {
        logger.trace(LogMarker.DLS_VERBOSE, "DeposeGrantorMessage sending {} to {}", msg,
            oldGrantor);
      }
      dm.putOutgoing(msg);
    }

    @Override
    public int getProcessorId() {
      return processorId;
    }

    void reply(DistributionManager dm) {
      ReplyMessage.send(getSender(), getProcessorId(), null, dm);
    }

    @Override
    protected void process(final ClusterDistributionManager dm) {
      // if we are currently the grantor then
      // mark it as being destroyed until we hear from this.newGrantor
      // or it goes away or the grantor that sent us this message goes away.

      InternalDistributedMember elder = getSender();
      InternalDistributedMember youngTurk = newGrantor;

      doOldGrantorWork(serviceName, elder, youngTurk, newGrantorVersion,
          newGrantorSerialNumber, dm, this);
    }

    @Override
    public int getDSFID() {
      return DEPOSE_GRANTOR_MESSAGE;
    }

    @Override
    public void fromData(DataInput in,
        DeserializationContext context) throws IOException, ClassNotFoundException {
      super.fromData(in, context);
      processorId = in.readInt();
      serviceName = DataSerializer.readString(in);
      newGrantor = DataSerializer.readObject(in);
      newGrantorVersion = in.readLong();
      newGrantorSerialNumber = in.readInt();
    }

    @Override
    public void toData(DataOutput out,
        SerializationContext context) throws IOException {
      super.toData(out, context);
      out.writeInt(processorId);
      DataSerializer.writeString(serviceName, out);
      DataSerializer.writeObject(newGrantor, out);
      out.writeLong(newGrantorVersion);
      out.writeInt(newGrantorSerialNumber);
    }

    @Override
    public String toString() {
      return "DeposeGrantorMessage (serviceName='" + serviceName
          + "' processorId=" + processorId + " newGrantor="
          + newGrantor + " newGrantorVersion=" + newGrantorVersion
          + " newGrantorSerialNumber=" + newGrantorSerialNumber + ")";
    }
  }
}

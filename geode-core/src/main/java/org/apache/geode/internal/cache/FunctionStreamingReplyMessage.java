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
package org.apache.geode.internal.cache;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.NotSerializableException;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.DataSerializer;
import org.apache.geode.distributed.DistributedSystemDisconnectedException;
import org.apache.geode.distributed.internal.DM;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.ReplyMessage;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.logging.LogService;

public class FunctionStreamingReplyMessage extends ReplyMessage {
  private static final Logger logger = LogService.getLogger();

  /** the number of this message */
  protected int msgNum;

  /** whether this message is the last one in this series */
  protected boolean lastMsg;

  protected Object result;

  /**
   * @param msgNum message number in this series (0-based)
   * @param lastMsg if this is the last message in this series
   */
  public static void send(InternalDistributedMember recipient, int processorId,
      ReplyException exception, DM dm, Object result, int msgNum, boolean lastMsg) {
    FunctionStreamingReplyMessage m = new FunctionStreamingReplyMessage();
    m.processorId = processorId;
    if (exception != null) {
      m.setException(exception);
      if (logger.isDebugEnabled()) {
        logger.debug("Replying with exception: {}", m, exception);
      }
    }
    m.setRecipient(recipient);
    m.msgNum = msgNum;
    m.lastMsg = lastMsg;
    m.result = result;
    dm.putOutgoing(m);
  }

  public int getMessageNumber() {
    return this.msgNum;
  }

  public boolean isLastMessage() {
    return this.lastMsg;
  }

  public Object getResult() {
    return this.result;
  }

  @Override
  public int getDSFID() {
    return FUNCTION_STREAMING_REPLY_MESSAGE;
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.msgNum = in.readInt();
    this.lastMsg = in.readBoolean();
    this.processorId = in.readInt();
    try {
      this.result = DataSerializer.readObject(in);
    } catch (Exception e) { // bug fix 40670
      // Seems odd to throw a NonSerializableEx when it has already been
      // serialized and we are failing because we can't deserialize.
      NotSerializableException ioEx = new NotSerializableException();
      ioEx.initCause(e);
      throw ioEx;
    }
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    out.writeInt(this.msgNum);
    out.writeBoolean(this.lastMsg);
    out.writeInt(this.processorId);

    // soubhik. fix for ticket 40670
    try {
      DataSerializer.writeObject(this.result, out);
    } catch (Exception ex) {
      if (ex instanceof CancelException) {
        throw new DistributedSystemDisconnectedException(ex);
      }
      NotSerializableException ioEx =
          new NotSerializableException(this.result.getClass().getName());
      ioEx.initCause(ex);
      throw ioEx;
    }
  }

  @Override
  public String toString() {
    StringBuffer buff = new StringBuffer();
    buff.append(getClass().getName());
    buff.append("(processorId=");
    buff.append(this.processorId);
    buff.append(" from ");
    buff.append(this.getSender());
    ReplyException ex = this.getException();
    if (ex != null) {
      buff.append(" with exception ");
      buff.append(ex);
    }
    buff.append(";msgNum ");
    buff.append(this.msgNum);
    buff.append(";lastMsg=");
    buff.append(this.lastMsg);
    buff.append(")");
    return buff.toString();
  }
}

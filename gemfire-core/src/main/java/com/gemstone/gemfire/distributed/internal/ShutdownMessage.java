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
package com.gemstone.gemfire.distributed.internal;

import com.gemstone.gemfire.*;
import com.gemstone.gemfire.internal.*;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

import java.io.*;
import com.gemstone.gemfire.distributed.internal.membership.*;

/**
 * A message that is sent to all other distribution manager when
 * a distribution manager shuts down.
 * 
 * N.B. -- this is a SerialDistributionMessage due to bug32980
 */
public final class ShutdownMessage extends HighPriorityDistributionMessage implements AdminMessageType, MessageWithReply {
  /** The is of the distribution manager that is shutting down */
  protected InternalDistributedMember id;
  private int processorId;

  /**
   * Sets the id of the distribution manager that is shutting down
   */
  void setDistributionManagerId(InternalDistributedMember id) {
    this.id = id;
  }
  
  @Override
  public boolean sendViaUDP() {
    return true;
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

  @Override
  public boolean getInlineProcess() {
    return false;
  }
  
  /**
   * Removes the distribution manager that is started up from the current
   * DM's list of members.
   *
   * This method is invoked on the receiver side
   */
  @Override
  protected void process(final DistributionManager dm) {
    Assert.assertTrue(this.id != null);
    // The peer goes deaf after sending us this message, so do not
    // attempt a reply.
    
//    final ReplyMessage reply = new ReplyMessage();
//    reply.setProcessorId(processorId);
//    reply.setRecipient(getSender());
    // can't send a response in a UDP receiver thread or we might miss
    // the other side going away due to blocking receipt of views
//    if (DistributionMessage.isPreciousThread()) {
//      dm.getWaitingThreadPool().execute(new Runnable() {
//        public void run() {
//          dm.putOutgoing(reply);
//          dm.handleManagerDeparture(ShutdownMessage.this.id, false, "shutdown message received");
//        }
//      });
//    }
//    else {
//      dm.putOutgoing(reply);
      dm.shutdownMessageReceived(id, LocalizedStrings.ShutdownMessage_SHUTDOWN_MESSAGE_RECEIVED.toLocalizedString());
//    }
  }

  public int getDSFID() {
    return SHUTDOWN_MESSAGE;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    out.writeInt(processorId);
    DataSerializer.writeObject(this.id, out);
  }

  @Override
  public void fromData(DataInput in)
    throws IOException, ClassNotFoundException {

    super.fromData(in);
    processorId = in.readInt();
    this.id = (InternalDistributedMember) DataSerializer.readObject(in);
  }

  @Override
  public String toString() {
    return LocalizedStrings.ShutdownMessage_SHUTDOWNMESSAGE_DM_0_HAS_SHUTDOWN.toLocalizedString(this.id);
  }

}

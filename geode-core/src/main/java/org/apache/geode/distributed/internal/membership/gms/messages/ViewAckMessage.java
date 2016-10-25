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
package org.apache.geode.distributed.internal.membership.gms.messages;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.geode.DataSerializer;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.HighPriorityDistributionMessage;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.distributed.internal.membership.NetView;

public class ViewAckMessage extends HighPriorityDistributionMessage {

  int viewId;
  boolean preparing;
  NetView alternateView;
  
  public ViewAckMessage(InternalDistributedMember recipient, int viewId, boolean preparing) {
    super();
    setRecipient(recipient);
    this.viewId = viewId;
    this.preparing = preparing;
  }
  
  public ViewAckMessage(InternalDistributedMember recipient, NetView alternateView) {
    super();
    setRecipient(recipient);
    this.alternateView = alternateView;
    this.preparing = true;
  }
  
  public ViewAckMessage() {
    // no-arg constructor for serialization
  }
  
  public int getViewId() {
    return viewId;
  }
  
  public NetView getAlternateView() {
    return this.alternateView;
  }
  
  public boolean isPrepareAck() {
    return preparing;
  }
  
  @Override
  public int getDSFID() {
    return VIEW_ACK_MESSAGE;
  }

  @Override
  public int getProcessorType() {
    return 0;
  }

  @Override
  public void process(DistributionManager dm) {
    throw new IllegalStateException("this message is not intended to execute in a thread pool");
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    out.writeInt(this.viewId);
    out.writeBoolean(this.preparing);
    DataSerializer.writeObject(this.alternateView, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.viewId = in.readInt();
    this.preparing = in.readBoolean();
    this.alternateView = DataSerializer.readObject(in);
  }
  
  @Override
  public String toString() {
    String s = getSender() == null? getRecipientsDescription() : ""+getSender();
    return "ViewAckMessage("+s+"; "+this.viewId+"; preparing="+preparing+"; altview="+this.alternateView+")";
  }

}

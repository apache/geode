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
package com.gemstone.gemfire.distributed.internal.membership.gms.messages;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.HighPriorityDistributionMessage;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.distributed.internal.membership.NetView;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ViewRejectMessage extends HighPriorityDistributionMessage {

  private int viewId;
  private NetView rejectedView;
  private String reason;

  public ViewRejectMessage(InternalDistributedMember recipient, int viewId, NetView rejectedView, String reason) {
    super();
    setRecipient(recipient);
    this.viewId = viewId;
    this.rejectedView = rejectedView;
    this.reason = reason;
  }

  public ViewRejectMessage() {
    // no-arg constructor for serialization
  }
  
  public int getViewId() {
    return viewId;
  }
  
  public NetView getRejectedView() {
    return this.rejectedView;
  }
  

  @Override
  public int getDSFID() {
    // TODO Auto-generated method stub
    return VIEW_REJECT_MESSAGE;
  }

  public String getReason() {
    return reason;
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
    DataSerializer.writeObject(this.rejectedView, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.viewId = in.readInt();
    this.rejectedView = DataSerializer.readObject(in);
  }
  
  @Override
  public String toString() {
    String s = getSender() == null? getRecipientsDescription() : ""+getSender();
    return "ViewRejectMessage("+s+"; "+this.viewId+";  rejectedView="+this.rejectedView +")";
  }

}

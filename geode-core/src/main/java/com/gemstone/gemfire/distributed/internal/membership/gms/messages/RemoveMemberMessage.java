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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.HighPriorityDistributionMessage;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.Version;

public class RemoveMemberMessage extends HighPriorityDistributionMessage
  implements HasMemberID {
  private InternalDistributedMember memberID;
  private String reason;

  
  public RemoveMemberMessage(InternalDistributedMember recipient, InternalDistributedMember id, String reason) {
    super();
    setRecipient(recipient);
    this.memberID = id;
    this.reason = reason;
  }
  
  public RemoveMemberMessage(List<InternalDistributedMember> recipients, InternalDistributedMember id, String reason) {
    super();
    setRecipients(recipients);
    this.memberID = id;
    this.reason = reason;
  }
  
  public RemoveMemberMessage() {
    // no-arg constructor for serialization
  }

  @Override
  public int getDSFID() {
    return REMOVE_MEMBER_REQUEST;
  }
  
  @Override
  public void process(DistributionManager dm) {
    throw new IllegalStateException("this message is not intended to execute in a thread pool"); 
  }

  public InternalDistributedMember getMemberID() {
    return memberID;
  }

  public String getReason() {
    return reason;
  }
  
  @Override
  public String toString() {
    return getShortClassName() + "(" + memberID
        + "; reason=" + reason + ")";
  }

  @Override
  public Version[] getSerializationVersions() {
    return null;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeObject(memberID, out);
    DataSerializer.writeString(reason, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    memberID = DataSerializer.readObject(in);
    reason = DataSerializer.readString(in);
  }

}

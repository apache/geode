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
package org.apache.geode.distributed.internal.membership.gms.messages;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.geode.DataSerializer;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.HighPriorityDistributionMessage;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Version;

public class SuspectMembersMessage extends HighPriorityDistributionMessage {
  final List<SuspectRequest> suspectRequests;

  public SuspectMembersMessage(List<InternalDistributedMember> recipients, List<SuspectRequest> s) {
    super();
    setRecipients(recipients);
    this.suspectRequests = s;
  }

  public SuspectMembersMessage() {
    // no-arg constructor for serialization
    suspectRequests = new ArrayList<SuspectRequest>();
  }

  @Override
  public int getDSFID() {
    return SUSPECT_MEMBERS_MESSAGE;
  }

  @Override
  public void process(DistributionManager dm) {
    throw new IllegalStateException("this message is not intended to execute in a thread pool");
  }

  public List<SuspectRequest> getMembers() {
    return suspectRequests;
  }

  @Override
  public String toString() {
    return "SuspectMembersMessage [suspectRequests=" + suspectRequests + "]";
  }

  @Override
  public Version[] getSerializationVersions() {
    return null;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    if (suspectRequests != null) {
      out.writeInt(suspectRequests.size());
      for (SuspectRequest sr : suspectRequests) {
        DataSerializer.writeObject(sr.getSuspectMember(), out);
        DataSerializer.writeString(sr.getReason(), out);
      }
    } else {
      out.writeInt(0);
    }
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    int size = in.readInt();
    for (int i = 0; i < size; i++) {
      SuspectRequest sr = new SuspectRequest(
          (InternalDistributedMember) DataSerializer.readObject(in), DataSerializer.readString(in));
      suspectRequests.add(sr);
    }
  }

}

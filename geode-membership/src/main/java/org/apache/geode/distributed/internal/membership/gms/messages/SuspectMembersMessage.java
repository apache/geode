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

import org.apache.geode.distributed.internal.membership.api.MemberIdentifier;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.SerializationContext;

/**
 * The HealthMonitor will send SuspectMembersMessages to inform other members of the cluster
 * that a member may have crashed. No response is required.
 */
public class SuspectMembersMessage<ID extends MemberIdentifier> extends AbstractGMSMessage<ID> {
  final List<SuspectRequest<ID>> suspectRequests;

  public SuspectMembersMessage(List<ID> recipients, List<SuspectRequest<ID>> s) {
    super();
    setRecipients(recipients);
    this.suspectRequests = s;
  }

  public SuspectMembersMessage() {
    // no-arg constructor for serialization
    suspectRequests = new ArrayList<>();
  }

  @Override
  public int getDSFID() {
    return SUSPECT_MEMBERS_MESSAGE;
  }

  public List<SuspectRequest<ID>> getMembers() {
    return suspectRequests;
  }

  @Override
  public String toString() {
    return "SuspectMembersMessage [suspectRequests=" + suspectRequests + "]";
  }

  @Override
  public KnownVersion[] getSerializationVersions() {
    return null;
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    if (suspectRequests != null) {
      out.writeInt(suspectRequests.size());
      for (SuspectRequest<ID> sr : suspectRequests) {
        context.getSerializer().writeObject(sr.getSuspectMember(), out);
        context.getSerializer().writeString(sr.getReason(), out);
      }
    } else {
      out.writeInt(0);
    }
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    int size = in.readInt();
    for (int i = 0; i < size; i++) {
      SuspectRequest<ID> sr = new SuspectRequest<>(
          context.getDeserializer().readObject(in), context.getDeserializer().readString(in));
      suspectRequests.add(sr);
    }
  }

}

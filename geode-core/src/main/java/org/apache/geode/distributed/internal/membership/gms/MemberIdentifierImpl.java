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
package org.apache.geode.distributed.internal.membership.gms;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.List;

import org.apache.geode.distributed.internal.membership.gms.api.MemberData;
import org.apache.geode.distributed.internal.membership.gms.api.MemberIdentifier;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.internal.serialization.Version;

/**
 * Unless overridden with a MemberIdentifierFactory GMS will use this as the member identifier
 * implementation. Geode uses InternalDistributedMember.
 */
public class MemberIdentifierImpl implements MemberIdentifier, Comparable<MemberIdentifierImpl> {
  private MemberData memberData;

  public MemberIdentifierImpl(MemberData memberInfo) {
    memberData = memberInfo;
  }

  @Override
  public MemberData getMemberData() {
    return memberData;
  }

  @Override
  public String getHostName() {
    return memberData.getHostName();
  }

  @Override
  public InetAddress getInetAddress() {
    return memberData.getInetAddress();
  }

  @Override
  public int getPort() {
    return memberData.getPort();
  }

  @Override
  public short getVersionOrdinal() {
    return memberData.getVersionOrdinal();
  }

  @Override
  public int getVmViewId() {
    return memberData.getVmViewId();
  }

  @Override
  public boolean preferredForCoordinator() {
    return memberData.preferredForCoordinator();
  }

  @Override
  public int getVmKind() {
    return memberData.getVmKind();
  }

  @Override
  public int getMemberWeight() {
    return memberData.getMemberWeight();
  }

  @Override
  public List<String> getGroups() {
    return Arrays.asList(memberData.getGroups());
  }

  @Override
  public int getDSFID() {
    return MEMBER_IDENTIFIER;
  }

  @Override
  public void toData(DataOutput out, SerializationContext context) throws IOException {
    memberData.writeEssentialData(out, context);
  }

  @Override
  public void fromData(DataInput in, DeserializationContext context)
      throws IOException, ClassNotFoundException {
    memberData = new GMSMemberData();
    memberData.readEssentialData(in, context);
  }

  @Override
  public Version[] getSerializationVersions() {
    return null;
  }

  @Override
  public int compareTo(MemberIdentifierImpl o) {
    return memberData.compareTo(o.memberData, true);
  }
}

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
package org.apache.geode.distributed.internal.membership.adapter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetAddress;

import org.apache.geode.distributed.DurableClientAttributes;
import org.apache.geode.distributed.internal.membership.NetMember;
import org.apache.geode.distributed.internal.membership.gms.GMSMember;
import org.apache.geode.internal.serialization.Version;

/**
 * GMSMemberAdapter implements the NetMember interface required by InternalDistributedMember
 * to supply it with membership details. GMSMemberAdapter delegates to a GMSMember, which
 * is the identifier class for group membership services.
 */
public class GMSMemberAdapter implements NetMember {

  private GMSMember gmsMember;
  private DurableClientAttributes durableClientAttributes;

  public GMSMemberAdapter(GMSMember gmsMember) {
    this.gmsMember = gmsMember;
    String durableId = gmsMember.getDurableId();
    if (durableId != null) {
      this.durableClientAttributes =
          new DurableClientAttributes(durableId, gmsMember.getDurableTimeout());
    }
  }

  public GMSMember getGmsMember() {
    return gmsMember;
  }

  @Override
  public InetAddress getInetAddress() {
    return gmsMember.getInetAddress();
  }

  @Override
  public int getPort() {
    return gmsMember.getPort();
  }

  @Override
  public void setPort(int p) {
    gmsMember.setPort(p);
  }

  @Override
  public short getVersionOrdinal() {
    return gmsMember.getVersionOrdinal();
  }

  @Override
  public boolean isNetworkPartitionDetectionEnabled() {
    return gmsMember.isNetworkPartitionDetectionEnabled();
  }

  @Override
  public void setNetworkPartitionDetectionEnabled(boolean enabled) {
    gmsMember.setNetworkPartitionDetectionEnabled(enabled);
  }

  @Override
  public boolean preferredForCoordinator() {
    return gmsMember.preferredForCoordinator();
  }

  @Override
  public void setPreferredForCoordinator(boolean preferred) {
    gmsMember.setPreferredForCoordinator(preferred);
  }

  @Override
  public byte getMemberWeight() {
    return gmsMember.getMemberWeight();
  }

  @Override
  public void setVersion(Version v) {
    gmsMember.setVersion(v);
  }

  @Override
  public int getProcessId() {
    return gmsMember.getProcessId();
  }

  @Override
  public void setProcessId(int id) {
    gmsMember.setProcessId(id);
  }

  @Override
  public byte getVmKind() {
    return gmsMember.getVmKind();
  }

  @Override
  public void setVmKind(int kind) {
    gmsMember.setVmKind(kind);
  }

  @Override
  public int getVmViewId() {
    return gmsMember.getVmViewId();
  }

  @Override
  public void setVmViewId(int id) {
    gmsMember.setVmViewId(id);
  }

  @Override
  public int getDirectPort() {
    return gmsMember.getDirectPort();
  }

  @Override
  public void setDirectPort(int port) {
    gmsMember.setDirectPort(port);
  }

  @Override
  public String getName() {
    return gmsMember.getName();
  }

  @Override
  public void setName(String name) {
    gmsMember.setName(name);
  }

  @Override
  public DurableClientAttributes getDurableClientAttributes() {
    return this.durableClientAttributes;
  }

  @Override
  public void setDurableTimeout(int newValue) {
    if (durableClientAttributes != null) {
      durableClientAttributes.updateTimeout(newValue);
      gmsMember.setDurableTimeout(newValue);
    }
  }

  @Override
  public void setHostName(String hostName) {
    gmsMember.setHostName(hostName);
  }

  @Override
  public String getHostName() {
    return gmsMember.getHostName();
  }

  @Override
  public boolean isPartial() {
    return gmsMember.isPartial();
  }

  @Override
  public void setDurableClientAttributes(DurableClientAttributes attributes) {
    durableClientAttributes = attributes;
    if (attributes != null) {
      gmsMember.setDurableId(attributes.getId());
      gmsMember.setDurableTimeout(attributes.getTimeout());
    }
  }

  @Override
  public String[] getGroups() {
    return gmsMember.getGroups();
  }

  @Override
  public void setGroups(String[] groups) {
    gmsMember.setGroups(groups);
  }

  @Override
  public boolean hasAdditionalData() {
    return gmsMember.hasAdditionalData();
  }

  @Override
  public void writeAdditionalData(DataOutput out) throws IOException {
    gmsMember.writeAdditionalData(out);
  }

  @Override
  public void readAdditionalData(DataInput in) throws ClassNotFoundException, IOException {
    gmsMember.readAdditionalData(in);
  }

  @Override
  public int compareAdditionalData(NetMember other) {
    return gmsMember.compareAdditionalData(((GMSMemberAdapter) other).getGmsMember());
  }

  @Override
  public int compareTo(NetMember o) {
    return gmsMember.compareTo(((GMSMemberAdapter) o).getGmsMember());
  }

  @Override
  public String toString() {
    return "adapter(" + gmsMember + ")";
  }

  public void setGmsMember(GMSMember canonicalID) {
    gmsMember = canonicalID;
  }
}

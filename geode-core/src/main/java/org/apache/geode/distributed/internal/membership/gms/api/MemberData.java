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
package org.apache.geode.distributed.internal.membership.gms.api;

import java.io.DataOutput;
import java.io.IOException;
import java.net.InetAddress;

import org.jgroups.util.UUID;

import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;

/**
 * MemberIdentifiers are created with a MemberData component. Use MemberDataBuilder to create
 * one.
 */
public interface MemberData {
  boolean isPartial();

  int getPort();

  boolean preferredForCoordinator();

  void setPreferredForCoordinator(boolean preferred);

  String getDurableId();

  int getDurableTimeout();

  InetAddress getInetAddress();

  short getVersionOrdinal();

  void setVersionOrdinal(short versionOrdinal);

  void setUUID(UUID u);

  UUID getUUID();

  long getUuidMSBs();

  long getUuidLSBs();

  boolean isNetworkPartitionDetectionEnabled();

  byte getMemberWeight();

  InetAddress getInetAddr();

  int getProcessId();

  byte getVmKind();

  int getVmViewId();

  void setVmViewId(int id);

  int getDirectChannelPort();

  String getName();

  String[] getRoles();

  void setUdpPort(int udpPort);

  void setNetworkPartitionDetectionEnabled(boolean networkPartitionDetectionEnabled);

  void setMemberWeight(byte memberWeight);

  void setInetAddr(InetAddress inetAddr);

  void setProcessId(int processId);

  void setVmKind(int vmKind);

  void setVersion(org.apache.geode.internal.serialization.Version v);

  void setDirectChannelPort(int directPort);

  void setName(String name);

  String[] getGroups();

  void setGroups(String[] groups);

  void setPort(int p);

  /**
   * checks to see if this address has UUID information needed to send messages via JGroups
   */
  boolean hasUUID();

  String getHostName();

  void setHostName(String hostName);

  void setDurableTimeout(int newValue);

  void setDurableId(String id);


  void writeEssentialData(DataOutput out,
      SerializationContext context) throws IOException;

  void readEssentialData(java.io.DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException;


  boolean hasAdditionalData();

  void writeAdditionalData(DataOutput out) throws IOException;

  void readAdditionalData(java.io.DataInput in) throws ClassNotFoundException, IOException;


  int compareTo(MemberData o, boolean compareUUIDs);

  int compareTo(MemberData o, boolean compareUUIDs, boolean compareViewIds);

  int compareAdditionalData(MemberData his);

}

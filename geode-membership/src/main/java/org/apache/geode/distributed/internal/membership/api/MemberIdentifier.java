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
package org.apache.geode.distributed.internal.membership.api;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.net.InetAddress;
import java.util.List;
import java.util.function.Function;

import org.jetbrains.annotations.NotNull;
import org.jgroups.util.UUID;

import org.apache.geode.internal.serialization.DataSerializableFixedID;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.internal.serialization.Version;

/**
 * MemberIdentifier should be implemented by a user of GMS if the default member identifier
 * is insufficient. Geode implements InternalDistributedMember.
 *
 * @see MemberIdentifierFactory - a factory to create identifiers you can inject into GMS
 * @see MembershipBuilder - where you inject the factory
 * @see MemberDataBuilder - used to build the MemberData objects held by identifiers
 */
public interface MemberIdentifier extends DataSerializableFixedID {
  /**
   * The type for regular members
   */
  int NORMAL_DM_TYPE = 10;
  /**
   * The DM type for locator members
   */
  int LOCATOR_DM_TYPE = 11;
  /**
   * The DM type for deprecated admin-only members
   */
  int ADMIN_ONLY_DM_TYPE = 12;
  /**
   * The DM type for stand-alone members (usually clients)
   */
  int LONER_DM_TYPE = 13;

  /**
   * Return the GMSMemberData associated with this identifier
   */
  MemberData getMemberData();

  /**
   * Return the hostname, if any, associated with this identifier (may be null)
   */
  String getHostName();

  /**
   * Return the InetAddress associated with this identifier
   */
  InetAddress getInetAddress();

  /**
   * Return the membership port associated with this identifier
   */
  int getMembershipPort();

  /**
   * Return the serialization version ordinal associated with this identifier
   */
  short getVersionOrdinal();

  /**
   * Return the view identifier in which this identifier was used to join the cluster
   */
  int getVmViewId();

  /**
   * Return whether this identifier is preferred as a membership coordinator over nodes that are not
   * preferred
   */
  boolean preferredForCoordinator();

  /**
   * Return the type of identifier (normal, locator, admin, loner)
   */
  int getVmKind();

  /**
   * Returns the additional member weight assigned to this identifier
   */
  int getMemberWeight();

  /**
   * Returns the server group names associated with this identifier
   */
  List<String> getGroups();

  /**
   * Set the view number in which this node joined the cluster
   */
  void setVmViewId(int viewNumber);

  /**
   * Set whether this member is preferred to be the membership coordinator
   * over nodes that have this set to false
   */
  void setPreferredForCoordinator(boolean preferred);

  /**
   * Set the tcp/ip communications port on which this node listens
   */
  void setDirectChannelPort(int dcPort);

  /**
   * Set the type of node
   */
  void setVmKind(int dmType);

  /**
   * Get the Geode version of this member
   */
  Version getVersion();

  /**
   * Replace the current member data with the given member data. This can be used to fill out a
   * MemberIdentifier that was created from a partial data created by readEssentialData.
   *
   * @param memberData the replacement member data
   */
  void setMemberData(MemberData memberData);

  void setIsPartial(boolean b);

  /**
   * An InternalDistributedMember created for a test or via readEssentialData will be a Partial ID,
   * possibly not having ancillary info like "name".
   *
   * @return true if this is a partial ID
   */
  boolean isPartial();

  void setDurableId(String id);

  void setDurableTimeout(int newValue);

  int getDirectChannelPort();

  String getName();

  void addFixedToString(StringBuilder sb, boolean useIpAddress);

  void writeExternal(ObjectOutput out) throws IOException;

  void readExternal(ObjectInput in) throws IOException, ClassNotFoundException;

  void toDataPre_GEODE_1_15_0_0(DataOutput out, SerializationContext context) throws IOException;

  void toDataPre_GFE_9_0_0_0(DataOutput out, SerializationContext context) throws IOException;

  void fromDataPre_GEODE_1_15_0_0(DataInput in, DeserializationContext context)
      throws IOException, ClassNotFoundException;

  void fromDataPre_GFE_9_0_0_0(DataInput in, DeserializationContext context)
      throws IOException, ClassNotFoundException;

  void _readEssentialData(DataInput in, Function<InetAddress, String> hostnameResolver)
      throws IOException, ClassNotFoundException;

  void writeEssentialData(DataOutput out) throws IOException;

  void setPort(int p);

  String getHost();

  int getProcessId();

  String getId();

  String getUniqueId();

  void setVersionForTest(Version v);

  void setUniqueTag(String tag);

  int compareTo(@NotNull MemberIdentifier memberIdentifier, boolean compareMemberData,
      boolean compareViewIds);

  String getUniqueTag();

  void setName(String name);

  String getDurableId();

  int getDurableTimeout();

  void setHostName(String hostName);

  void setProcessId(int id);

  boolean hasUUID();

  long getUuidLeastSignificantBits();

  long getUuidMostSignificantBits();

  boolean isNetworkPartitionDetectionEnabled();

  void setUUID(UUID randomUUID);

  void setMemberWeight(byte b);

  void setUdpPort(int i);

  UUID getUUID();
}

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

import java.net.InetAddress;
import java.util.List;

import org.apache.geode.internal.serialization.DataSerializableFixedID;
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
  Version getVersionObject();

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
}

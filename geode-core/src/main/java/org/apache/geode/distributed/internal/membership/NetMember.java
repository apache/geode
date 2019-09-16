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
package org.apache.geode.distributed.internal.membership;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetAddress;

import org.apache.geode.distributed.DurableClientAttributes;
import org.apache.geode.internal.serialization.Version;

/**
 * This is the SPI for the basic element of membership provided in Geode.
 *
 *
 */
public interface NetMember extends Comparable<NetMember> {

  InetAddress getInetAddress();

  int getPort();

  void setPort(int p);

  short getVersionOrdinal();

  /**
   * return a flag stating whether the member has network partition detection enabled
   *
   * @since GemFire 5.6
   */
  boolean isNetworkPartitionDetectionEnabled();

  void setNetworkPartitionDetectionEnabled(boolean enabled);

  /**
   * return a flag stating whether the member can be the membership coordinator
   *
   * @since GemFire 5.6
   */
  boolean preferredForCoordinator();

  /**
   * Set whether this member ID is preferred for coordinator. This is mostly useful for unit tests
   * because it does not distribute this status to other members in the distributed system.
   *
   */
  void setPreferredForCoordinator(boolean preferred);

  byte getMemberWeight();

  void setVersion(Version v);

  int getProcessId();

  void setProcessId(int id);

  byte getVmKind();

  void setVmKind(int kind);

  int getVmViewId();

  void setVmViewId(int id);

  int getDirectPort();

  void setDirectPort(int port);

  String getName();

  void setName(String name);

  DurableClientAttributes getDurableClientAttributes();

  void setDurableClientAttributes(DurableClientAttributes attributes);

  String[] getGroups();

  void setGroups(String[] groups);

  /** whether this NetMember has additional data to be serialized as part of a DistributedMember */
  boolean hasAdditionalData();

  /** write identity information not known by DistributedMember instances */
  void writeAdditionalData(DataOutput out) throws IOException;

  /** read identity information not known by DistributedMember instances */
  void readAdditionalData(DataInput in) throws ClassNotFoundException, IOException;

  /** compare data that is not known to DistributedMember instances */
  int compareAdditionalData(NetMember other);

  void setDurableTimeout(int newValue);

  void setHostName(String hostName);

  String getHostName();

  /** is this a partial ID created without full identifier information? */
  boolean isPartial();
}

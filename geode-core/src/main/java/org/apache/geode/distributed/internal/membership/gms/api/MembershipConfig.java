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

import java.net.InetAddress;

import org.apache.geode.distributed.internal.DistributionConfig;

public interface MembershipConfig {
  /** stall time to wait for concurrent join/leave/remove requests to be received */
  long MEMBER_REQUEST_COLLECTION_INTERVAL =
      Long.getLong(DistributionConfig.GEMFIRE_PREFIX + "member-request-collection-interval", 300);
  /** in a small cluster we might want to involve all members in operations */
  int SMALL_CLUSTER_SIZE = 9;

  boolean isReconnecting();

  int getLocatorWaitTime();

  long getJoinTimeout();

  int[] getMembershipPortRange();

  long getMemberTimeout();

  int getLossThreshold();

  int getMemberWeight();

  boolean isMulticastEnabled();

  boolean isNetworkPartitionDetectionEnabled();

  boolean isUDPSecurityEnabled();

  boolean areLocatorsPreferredAsCoordinators();

  String getSecurityUDPDHAlgo();

  int getMcastPort();

  String getLocators();

  String getStartLocator();

  boolean getEnableNetworkPartitionDetection();

  String getBindAddress();

  String getSecurityPeerAuthInit();

  boolean getDisableTcp();

  String getName();

  String getRoles();

  String getGroups();

  String getDurableClientId();

  int getDurableClientTimeout();

  InetAddress getMcastAddress();

  int getMcastTtl();

  int getMcastSendBufferSize();

  int getMcastRecvBufferSize();

  int getUdpFragmentSize();

  int getUdpRecvBufferSize();

  int getUdpSendBufferSize();

  int getMcastByteAllowance();

  float getMcastRechargeThreshold();

  int getMcastRechargeBlockMs();

  long getAckWaitThreshold();

  boolean getDisableAutoReconnect();

  int getSecurityPeerMembershipTimeout();

  long getAckSevereAlertThreshold();

  int getVmKind();

  boolean isMcastEnabled();

  boolean isTcpDisabled();

  Object getOldDSMembershipInfo();

  boolean getIsReconnectingDS();
}

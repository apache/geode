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


import org.apache.geode.annotations.internal.MakeImmutable;
import org.apache.geode.util.internal.GeodeGlossary;

/**
 * MembershipConfig is used to set parameters for a new Membership instance created with
 * a MembershipBuilder. Most of the settings are configured in Geode with DistributedSystem
 * properties (that is, gemfire.properties).
 */
public interface MembershipConfig {
  /** stall time to wait for concurrent join/leave/remove requests to be received */
  long MEMBER_REQUEST_COLLECTION_INTERVAL =
      Long.getLong(GeodeGlossary.GEMFIRE_PREFIX + "member-request-collection-interval", 300);
  /** in a small cluster we might want to involve all members in operations */
  int SMALL_CLUSTER_SIZE = 9;

  @MakeImmutable
  int[] DEFAULT_MEMBERSHIP_PORT_RANGE = {41000, 61000};

  int DEFAULT_LOCATOR_WAIT_TIME = 0;
  String DEFAULT_SECURITY_UDP_DHALGO = "";
  int DEFAULT_UDP_FRAGMENT_SIZE = 60000;
  String DEFAULT_START_LOCATOR = "";
  int DEFAULT_MEMBER_TIMEOUT = 5000;
  int DEFAULT_LOSS_THRESHOLD = 51;
  int DEFAULT_MEMBER_WEIGHT = 0;
  boolean DEFAULT_ENABLE_NETWORK_PARTITION_DETECTION = true;
  int DEFAULT_MCAST_PORT = 0;
  String DEFAULT_LOCATORS = "";
  String DEFAULT_BIND_ADDRESS = "";
  String DEFAULT_SECURITY_PEER_AUTH_INIT = "";
  boolean DEFAULT_DISABLE_TCP = false;
  String DEFAULT_NAME = "";
  String DEFAULT_ROLES = "";
  String DEFAULT_GROUPS = "";
  String DEFAULT_DURABLE_CLIENT_ID = "";
  int DEFAULT_DURABLE_CLIENT_TIMEOUT = 300;
  String DEFAULT_MCAST_ADDRESS = "239.192.81.1";
  int DEFAULT_MCAST_TTL = 32;
  int DEFAULT_MCAST_SEND_BUFFER_SIZE = 65535;
  int DEFAULT_MCAST_RECV_BUFFER_SIZE = 1048576;
  int DEFAULT_UDP_RECV_BUFFER_SIZE = 1048576;
  int DEFAULT_UDP_SEND_BUFFER_SIZE = 65535;
  int DEFAULT_UDP_RECV_BUFFER_SIZE_REDUCED = 65535;
  int DEFAULT_MCAST_BYTE_ALLOWANCE = 1048576;
  float DEFAULT_MCAST_RECHARGE_THRESHOLD = (float) 0.25;
  int DEFAULT_MCAST_RECHARGE_BLOCKING_MS = 5000;
  int DEFAULT_ACK_WAIT_THRESHOLD = 15;
  boolean DEFAULT_DISABLE_AUTO_RECONNECT = false;
  int DEFAULT_SECURITY_PEER_VERIFYMEMBER_TIMEOUT = 1000;
  int DEFAULT_ACK_SEVERE_ALERT_THRESHOLD = 0;
  Object DEFAULT_OLD_MEMBERSHIP_INFO = null;
  boolean DEFAULT_IS_RECONNECTING_DS = false;
  int DEFAULT_JOIN_TIMEOUT = 24000;

  String LOCATORS = "locators";
  String START_LOCATOR = "start-locator";

  default boolean isReconnecting() {
    return false;
  }

  default int getLocatorWaitTime() {
    return DEFAULT_LOCATOR_WAIT_TIME;
  }

  default long getJoinTimeout() {
    return DEFAULT_JOIN_TIMEOUT;
  }

  default int[] getMembershipPortRange() {
    return DEFAULT_MEMBERSHIP_PORT_RANGE;
  }

  default long getMemberTimeout() {
    return DEFAULT_MEMBER_TIMEOUT;
  }

  default int getLossThreshold() {
    return DEFAULT_LOSS_THRESHOLD;
  }

  default int getMemberWeight() {
    return DEFAULT_MEMBER_WEIGHT;
  }

  default boolean isMulticastEnabled() {
    return getMcastPort() > 0;
  }

  default boolean isNetworkPartitionDetectionEnabled() {
    return DEFAULT_ENABLE_NETWORK_PARTITION_DETECTION;
  }

  default boolean isUDPSecurityEnabled() {
    return !getSecurityUDPDHAlgo().isEmpty();
  }

  default boolean areLocatorsPreferredAsCoordinators() {
    return isNetworkPartitionDetectionEnabled();
  }

  default String getSecurityUDPDHAlgo() {
    return DEFAULT_SECURITY_UDP_DHALGO;
  }

  default int getMcastPort() {
    return DEFAULT_MCAST_PORT;
  }

  default String getLocators() {
    return DEFAULT_LOCATORS;
  }

  default String getStartLocator() {
    return DEFAULT_START_LOCATOR;
  }

  default String getBindAddress() {
    return DEFAULT_BIND_ADDRESS;
  };

  default String getSecurityPeerAuthInit() {
    return DEFAULT_SECURITY_PEER_AUTH_INIT;
  }

  default boolean getDisableTcp() {
    return DEFAULT_DISABLE_TCP;
  }

  default String getName() {
    return DEFAULT_NAME;
  }

  default String getRoles() {
    return DEFAULT_ROLES;
  }

  default String getGroups() {
    return DEFAULT_GROUPS;
  }

  default String getDurableClientId() {
    return DEFAULT_DURABLE_CLIENT_ID;
  }

  default int getDurableClientTimeout() {
    return DEFAULT_DURABLE_CLIENT_TIMEOUT;
  }

  default String getMcastAddress() {
    return DEFAULT_MCAST_ADDRESS;
  }

  default int getMcastTtl() {
    return DEFAULT_MCAST_TTL;
  }

  default int getMcastSendBufferSize() {
    return DEFAULT_MCAST_SEND_BUFFER_SIZE;
  }

  default int getMcastRecvBufferSize() {
    return DEFAULT_MCAST_RECV_BUFFER_SIZE;
  }

  default int getUdpFragmentSize() {
    return DEFAULT_UDP_FRAGMENT_SIZE;
  }

  default int getUdpRecvBufferSize() {
    return DEFAULT_UDP_RECV_BUFFER_SIZE;
  }

  default int getUdpSendBufferSize() {
    return DEFAULT_UDP_SEND_BUFFER_SIZE;
  }

  default int getMcastByteAllowance() {
    return DEFAULT_MCAST_BYTE_ALLOWANCE;
  }

  default float getMcastRechargeThreshold() {
    return DEFAULT_MCAST_RECHARGE_THRESHOLD;
  }

  default int getMcastRechargeBlockMs() {
    return DEFAULT_MCAST_RECHARGE_BLOCKING_MS;
  }

  default long getAckWaitThreshold() {
    return DEFAULT_ACK_WAIT_THRESHOLD;
  }

  default boolean getDisableAutoReconnect() {
    return DEFAULT_DISABLE_AUTO_RECONNECT;
  }

  default int getSecurityPeerMembershipTimeout() {
    return DEFAULT_SECURITY_PEER_VERIFYMEMBER_TIMEOUT;
  }

  default long getAckSevereAlertThreshold() {
    return DEFAULT_ACK_SEVERE_ALERT_THRESHOLD;
  }

  default int getVmKind() {
    return MemberIdentifier.NORMAL_DM_TYPE;
  }

  default Object getOldMembershipInfo() {
    return DEFAULT_OLD_MEMBERSHIP_INFO;
  }

  default boolean getIsReconnectingDS() {
    return DEFAULT_IS_RECONNECTING_DS;
  }

  default boolean getHasLocator() {
    return false;
  }
}

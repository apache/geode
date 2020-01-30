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


import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.distributed.internal.membership.api.MembershipConfig;
import org.apache.geode.internal.admin.remote.RemoteTransportConfig;
import org.apache.geode.internal.net.SocketCreator;
import org.apache.geode.util.internal.GeodeGlossary;

/**
 * This is a MembershipConfig built from the Geode core RemoteTransportConfig and
 * DistributionConfig objects.
 */
public class ServiceConfig implements MembershipConfig {

  /** various settings from Geode configuration */
  private final long joinTimeout;
  private final int[] membershipPortRange;
  private final long memberTimeout;

  private final boolean isReconnecting;
  private final Integer lossThreshold;
  private final Integer memberWeight;
  private final boolean networkPartitionDetectionEnabled;
  private final int locatorWaitTime;

  /** the configuration for the distributed system */
  private final DistributionConfig dconfig;

  /** the transport config from the distribution manager */
  private final RemoteTransportConfig transport;
  private final boolean locatorsPreferredAsCoordinators;

  public ServiceConfig(RemoteTransportConfig transport, DistributionConfig theConfig) {
    this.dconfig = theConfig;
    this.transport = transport;
    this.isReconnecting = (transport.getOldDSMembershipInfo() != null);

    long defaultJoinTimeout = 24000;
    if (isReconnecting || theConfig.getLocators().length() > 0 && !Locator.hasLocator()) {
      defaultJoinTimeout = 60000;
    }

    // we need to have enough time to figure out that the coordinator has crashed &
    // find a new one
    long minimumJoinTimeout = dconfig.getMemberTimeout() * 2L + MEMBER_REQUEST_COLLECTION_INTERVAL;
    if (defaultJoinTimeout < minimumJoinTimeout) {
      defaultJoinTimeout = minimumJoinTimeout;
    }

    joinTimeout = Long.getLong("p2p.joinTimeout", defaultJoinTimeout).longValue();

    // if network partition detection is enabled, we must connect to the locators
    // more frequently in order to make sure we're not isolated from them
    SocketCreator.resolve_dns = true;
    if (theConfig.getEnableNetworkPartitionDetection()) {
      if (!SocketCreator.FORCE_DNS_USE) {
        SocketCreator.resolve_dns = false;
      }
    }

    membershipPortRange = theConfig.getMembershipPortRange();

    memberTimeout = theConfig.getMemberTimeout();

    int configuredLossThreshold =
        Integer.getInteger(GeodeGlossary.GEMFIRE_PREFIX + "network-partition-threshold",
            DEFAULT_LOSS_THRESHOLD);
    if (configuredLossThreshold < 51) {
      lossThreshold = 51;
    } else if (configuredLossThreshold > 100) {
      lossThreshold = 100;
    } else {
      lossThreshold = configuredLossThreshold;
    }

    memberWeight = Integer.getInteger(GeodeGlossary.GEMFIRE_PREFIX + "member-weight",
        DEFAULT_MEMBER_WEIGHT);
    locatorWaitTime = theConfig.getLocatorWaitTime();

    networkPartitionDetectionEnabled = theConfig.getEnableNetworkPartitionDetection();
    locatorsPreferredAsCoordinators =
        determineLocatorsPreferredAsCoordinators(networkPartitionDetectionEnabled, dconfig);
  }

  private static boolean determineLocatorsPreferredAsCoordinators(
      final boolean networkPartitionDetectionEnabled, final DistributionConfig dconfig) {
    boolean locatorsAreCoordinators;

    if (networkPartitionDetectionEnabled) {
      locatorsAreCoordinators = true;
    } else {
      // check if security is enabled
      String prop = dconfig.getSecurityPeerAuthInit();
      locatorsAreCoordinators = (prop != null && prop.length() > 0);
      if (!locatorsAreCoordinators) {
        locatorsAreCoordinators =
            Boolean.getBoolean(InternalLocator.LOCATORS_PREFERRED_AS_COORDINATORS);
      }
    }
    return locatorsAreCoordinators;
  }


  @Override
  public boolean isReconnecting() {
    return isReconnecting;
  }

  @Override
  public int getLocatorWaitTime() {
    return locatorWaitTime;
  }


  @Override
  public long getJoinTimeout() {
    return joinTimeout;
  }


  @Override
  public int[] getMembershipPortRange() {
    return membershipPortRange;
  }


  @Override
  public long getMemberTimeout() {
    return memberTimeout;
  }


  @Override
  public int getLossThreshold() {
    return lossThreshold;
  }


  @Override
  public int getMemberWeight() {
    return memberWeight;
  }

  @Override
  public boolean isMulticastEnabled() {
    return transport.isMcastEnabled();
  }

  @Override
  public boolean isNetworkPartitionDetectionEnabled() {
    return networkPartitionDetectionEnabled;
  }

  @Override
  public boolean isUDPSecurityEnabled() {
    return !dconfig.getSecurityUDPDHAlgo().isEmpty();
  }

  @Override
  public boolean areLocatorsPreferredAsCoordinators() {
    return locatorsPreferredAsCoordinators;
  }

  @Override
  public String getSecurityUDPDHAlgo() {
    return dconfig.getSecurityUDPDHAlgo();
  }

  @Override
  public int getMcastPort() {
    return dconfig.getMcastPort();
  }

  @Override
  public String getLocators() {
    return dconfig.getLocators();
  }

  @Override
  public String getStartLocator() {
    return dconfig.getStartLocator();
  }

  public boolean getEnableNetworkPartitionDetection() {
    return dconfig.getEnableNetworkPartitionDetection();
  }

  @Override
  public String getBindAddress() {
    return dconfig.getBindAddress();
  }

  @Override
  public String getSecurityPeerAuthInit() {
    return dconfig.getSecurityPeerAuthInit();
  }

  @Override
  public boolean getDisableTcp() {
    return dconfig.getDisableTcp();
  }

  @Override
  public String getName() {
    return dconfig.getName();
  }

  @Override
  public String getRoles() {
    return dconfig.getRoles();
  }

  @Override
  public String getGroups() {
    return dconfig.getGroups();
  }

  @Override
  public String getDurableClientId() {
    return dconfig.getDurableClientId();
  }

  @Override
  public int getDurableClientTimeout() {
    return dconfig.getDurableClientTimeout();
  }

  @Override
  public String getMcastAddress() {
    return dconfig.getMcastAddress().getHostAddress();
  }

  @Override
  public int getMcastTtl() {
    return dconfig.getMcastTtl();
  }

  @Override
  public int getMcastSendBufferSize() {
    return dconfig.getMcastSendBufferSize();
  }

  @Override
  public int getMcastRecvBufferSize() {
    return dconfig.getMcastRecvBufferSize();
  }

  @Override
  public int getUdpFragmentSize() {
    return dconfig.getUdpFragmentSize();
  }

  @Override
  public int getUdpRecvBufferSize() {
    return dconfig.getUdpRecvBufferSize();
  }

  @Override
  public int getUdpSendBufferSize() {
    return dconfig.getUdpSendBufferSize();
  }

  @Override
  public int getMcastByteAllowance() {
    return dconfig.getMcastFlowControl().getByteAllowance();
  }

  @Override
  public float getMcastRechargeThreshold() {
    return dconfig.getMcastFlowControl().getRechargeThreshold();
  }

  @Override
  public int getMcastRechargeBlockMs() {
    return dconfig.getMcastFlowControl().getRechargeBlockMs();
  }

  @Override
  public long getAckWaitThreshold() {
    return dconfig.getAckWaitThreshold();
  }

  @Override
  public boolean getDisableAutoReconnect() {
    return dconfig.getDisableAutoReconnect();
  }

  @Override
  public int getSecurityPeerMembershipTimeout() {
    return dconfig.getSecurityPeerMembershipTimeout();
  }

  @Override
  public long getAckSevereAlertThreshold() {
    return dconfig.getAckSevereAlertThreshold();
  }

  @Override
  public int getVmKind() {
    return transport.getVmKind();
  }

  @Override
  public Object getOldMembershipInfo() {
    return transport.getOldDSMembershipInfo();
  }

  @Override
  public boolean getIsReconnectingDS() {
    return transport.getIsReconnectingDS();
  }

  @Override
  public boolean getHasLocator() {
    return Locator.hasLocator();
  }
}

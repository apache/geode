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
package org.apache.geode.internal.admin.remote;

import static java.util.Collections.emptySet;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.geode.distributed.ConfigurationProperties.BIND_ADDRESS;
import static org.apache.geode.distributed.ConfigurationProperties.DISABLE_AUTO_RECONNECT;
import static org.apache.geode.distributed.ConfigurationProperties.DISABLE_TCP;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_ADDRESS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.MEMBERSHIP_PORT_RANGE;
import static org.apache.geode.distributed.ConfigurationProperties.TCP_PORT;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.commons.lang3.StringUtils;

import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.membership.api.MembershipInformation;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.admin.TransportConfig;
import org.apache.geode.internal.net.SSLConfig;

/**
 * Tranport config for RemoteGfManagerAgent.
 */
public class RemoteTransportConfig implements TransportConfig {

  private final boolean mcastEnabled;
  private final boolean tcpDisabled;
  private final boolean disableAutoReconnect;
  private final DistributionLocatorId mcastId;
  private final Set<DistributionLocatorId> ids;
  private final String bindAddress;
  private final SSLConfig sslConfig;
  private final String membershipPortRange;
  private final int tcpPort;
  private boolean isReconnectingDS;
  private MembershipInformation oldDSMembershipInfo;
  private final int vmKind;

  // -------------------------------------------------------------------------
  // Constructor(s)
  // -------------------------------------------------------------------------

  /**
   * Creates a new <code>RemoteTransportConfig</code> from the configuration information in a
   * <code>DistributionConfig</code>. We assume that <code>config</code> already been checked for
   * errors.
   *
   * @since GemFire 3.0
   */
  public RemoteTransportConfig(DistributionConfig config, int vmKind) {
    if (config.getBindAddress() == null) {
      bindAddress = DistributionConfig.DEFAULT_BIND_ADDRESS;
    } else {
      bindAddress = config.getBindAddress();
    }
    this.vmKind = vmKind;
    tcpPort = config.getTcpPort();
    membershipPortRange = getMembershipPortRangeString(config.getMembershipPortRange());
    sslConfig = new SSLConfig.Builder().build();

    String initialHosts = config.getLocators();
    if (initialHosts == null) {
      initialHosts = "";
    }
    initialHosts = initialHosts.trim();

    if (config.getMcastPort() > 0) {
      mcastId = new DistributionLocatorId(config.getMcastAddress(), config.getMcastPort(),
          config.getBindAddress(), sslConfig);
      mcastEnabled = true;
    } else {
      mcastEnabled = false;
      mcastId = null;
    }

    tcpDisabled = config.getDisableTcp();
    disableAutoReconnect = config.getDisableAutoReconnect();

    // See what type of discovery is being used
    if (initialHosts.length() == 0) {
      // loner system
      ids = emptySet();
    } else {
      Set<DistributionLocatorId> locators = new HashSet<>();
      StringTokenizer stringTokenizer = new StringTokenizer(initialHosts, ",");
      while (stringTokenizer.hasMoreTokens()) {
        String locator = stringTokenizer.nextToken();
        if (StringUtils.isNotEmpty(locator)) {
          locators.add(DistributionLocatorId.unmarshal(locator));
        }
      }

      if (mcastEnabled) {
        locators.add(mcastId);
      }
      ids = Collections.unmodifiableSet(locators);
    }
  }

  /**
   * Constructs a transport config given a collection of {@link DistributionLocatorId} instances.
   */
  public RemoteTransportConfig(boolean isMcastEnabled, boolean isTcpDisabled,
      boolean isAutoReconnectDisabled, String bindAddress, SSLConfig sslConfig,
      Collection<DistributionLocatorId> ids,
      String membershipPortRange, int tcpPort, int vmKind) {

    if (bindAddress == null) {
      this.bindAddress = DistributionConfig.DEFAULT_BIND_ADDRESS;
    } else {
      this.bindAddress = bindAddress;
    }

    this.sslConfig = sslConfig;

    mcastEnabled = isMcastEnabled;
    tcpDisabled = isTcpDisabled;
    disableAutoReconnect = isAutoReconnectDisabled;

    DistributionLocatorId mid = null;
    if (isMcastEnabled) {
      if (ids.isEmpty()) {
        throw new IllegalArgumentException("expected at least one host/port id");
      }
      for (final DistributionLocatorId id : ids) {
        if (id.isMcastId()) {
          mid = id;
          break;
        }
      }
    }
    this.ids = Collections.unmodifiableSet(new HashSet<>(ids));
    mcastId = mid;
    if (mcastEnabled) {
      Assert.assertTrue(mcastId != null);
    }
    this.membershipPortRange = membershipPortRange;
    this.tcpPort = tcpPort;
    this.vmKind = vmKind;
  }


  private static String getMembershipPortRangeString(int[] membershipPortRange) {
    String membershipPortRangeString = "";
    if (membershipPortRange != null && membershipPortRange.length == 2) {
      membershipPortRangeString = membershipPortRange[0] + "-" + membershipPortRange[1];
    }

    return membershipPortRangeString;
  }

  // -------------------------------------------------------------------------
  // Attribute(s)
  // -------------------------------------------------------------------------

  /**
   * Returns the set of DistributionLocatorId instances that define this transport. The set is
   * unmodifiable.
   */
  public Set<DistributionLocatorId> getIds() {
    return ids;
  }

  /**
   * Returns true iff multicast is enabled in this transport. Multicast must be enabled in order to
   * use multicast discovery.
   */
  public boolean isMcastEnabled() {
    return mcastEnabled;
  }

  public DistributionLocatorId getMcastId() {
    return mcastId;
  }

  public int getVmKind() {
    return vmKind;
  }

  public boolean isTcpDisabled() {
    return tcpDisabled;
  }

  public String getBindAddress() {
    return bindAddress;
  }

  public SSLConfig getSSLConfig() {
    return sslConfig;
  }

  public String getMembershipPortRange() {
    return membershipPortRange;
  }

  public int getTcpPort() {
    return tcpPort;
  }

  public boolean getIsReconnectingDS() {
    return isReconnectingDS;
  }

  public void setIsReconnectingDS(boolean isReconnectingDS) {
    this.isReconnectingDS = isReconnectingDS;
  }

  public MembershipInformation getOldDSMembershipInfo() {
    return oldDSMembershipInfo;
  }

  public void setOldDSMembershipInfo(MembershipInformation oldDSMembershipInfo) {
    this.oldDSMembershipInfo = oldDSMembershipInfo;
  }

  /**
   * Returns a <code>Properties</code> based on this config that is appropriate to use with
   * {@link org.apache.geode.distributed.DistributedSystem#connect}.
   *
   * @since GemFire 4.0
   */
  Properties toDSProperties() {
    Properties props = new Properties();
    props.setProperty(BIND_ADDRESS, bindAddress);
    // System.out.println("entering ds port range property of " + this.membershipPortRange);
    if (membershipPortRange != null) {
      props.setProperty(MEMBERSHIP_PORT_RANGE, membershipPortRange);
    }
    if (tcpPort != 0) {
      props.setProperty(TCP_PORT, String.valueOf(tcpPort));
    }
    if (mcastEnabled) {
      // Fix bug 32849
      props.setProperty(MCAST_ADDRESS, mcastId.getHostName());
      props.setProperty(MCAST_PORT, String.valueOf(mcastId.getPort()));

    } else {
      props.setProperty(MCAST_PORT, String.valueOf(0));
    }
    // Create locator string
    StringBuilder locators = new StringBuilder();
    for (Iterator<DistributionLocatorId> iter = ids.iterator(); iter.hasNext();) {
      DistributionLocatorId locator = iter.next();
      if (!locator.isMcastId()) {
        String baddr = locator.getBindAddress();
        if (!isBlank(baddr)) {
          locators.append(baddr);
        } else {
          locators.append(locator.getHostName());
        }
        locators.append("[");
        locators.append(locator.getPort());
        locators.append("]");

        if (iter.hasNext()) {
          locators.append(",");
        }
      }
    }
    String tempLocatorString = locators.toString();
    if (tempLocatorString.endsWith(",")) {
      tempLocatorString = tempLocatorString.substring(0, tempLocatorString.length() - 1);
    }

    props.setProperty(LOCATORS, tempLocatorString);

    sslConfig.toDSProperties(props);

    props.setProperty(DISABLE_TCP, tcpDisabled ? "true" : "false");

    props.setProperty(DISABLE_AUTO_RECONNECT, disableAutoReconnect ? "true" : "false");

    return props;
  }

  private String toString(boolean noMcast) {
    StringBuilder result = new StringBuilder();
    boolean first = true;
    for (final DistributionLocatorId id : ids) {
      if (noMcast && id.isMcastId()) {
        continue;
      }
      if (!first) {
        result.append(',');
      } else {
        first = false;
      }
      result.append(id.toString());
    }
    return result.toString();
  }

  // -------------------------------------------------------------------------
  // Methods overridden from java.lang.Object
  // -------------------------------------------------------------------------

  @Override
  public String toString() {
    return toString(false);
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof RemoteTransportConfig) {
      RemoteTransportConfig other = (RemoteTransportConfig) o;
      return (mcastEnabled == other.mcastEnabled) && ids.equals(other.ids);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return ids.hashCode();
  }

}

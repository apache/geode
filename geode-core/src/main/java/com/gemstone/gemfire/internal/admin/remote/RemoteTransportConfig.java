/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.internal.admin.remote;

import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.admin.SSLConfig;
import com.gemstone.gemfire.internal.admin.TransportConfig;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

import java.util.*;

import static com.gemstone.gemfire.distributed.DistributedSystemConfigProperties.*;

/**
 * Tranport config for RemoteGfManagerAgent.
 * 
 *
 */
public class RemoteTransportConfig implements TransportConfig {
  
  private final boolean mcastEnabled;
  private final boolean tcpDisabled;
  private final boolean disableAutoReconnect;
  private final DistributionLocatorId mcastId;
  private final Set ids;
  private final String bindAddress;
  private final SSLConfig sslConfig;
  private final String membershipPortRange;
  private int tcpPort;
  private boolean isReconnectingDS;
  private Object oldDSMembershipInfo;
  private int vmKind = -1;

  // -------------------------------------------------------------------------
  //   Constructor(s)
  // -------------------------------------------------------------------------

  /**
   * Creates a new <code>RemoteTransportConfig</code> from the
   * configuration information in a <code>DistributionConfig</code>.
   * We assume that <code>config</code> already been checked for
   * errors.
   *
   * @since GemFire 3.0
   */
  public RemoteTransportConfig(DistributionConfig config, int vmKind) {
    if (config.getBindAddress() == null) {
      this.bindAddress = DistributionConfig.DEFAULT_BIND_ADDRESS;
    } else {
      this.bindAddress = config.getBindAddress();
    }
    this.vmKind = vmKind;
    this.tcpPort = config.getTcpPort();
    this.membershipPortRange = 
            getMembershipPortRangeString(config.getMembershipPortRange());
    this.sslConfig = new SSLConfig();
    
    String initialHosts = config.getLocators();
    if (initialHosts == null)
      initialHosts = "";
    initialHosts = initialHosts.trim();
    
    if (config.getMcastPort() > 0) {
      this.mcastId = new DistributionLocatorId(config.getMcastAddress(), 
                                               config.getMcastPort(), 
                                               config.getBindAddress(),
                                               this.sslConfig);
      this.mcastEnabled = true;
    }
    else {
      this.mcastEnabled = false;
      this.mcastId = null;
    }
    
    this.tcpDisabled = config.getDisableTcp();
    this.disableAutoReconnect = config.getDisableAutoReconnect();

    // See what type of discovery is being used
    if (initialHosts.length() == 0) {
      // loner system
      this.ids = Collections.EMPTY_SET;
      return;
    }
    else {
      HashSet locators = new HashSet();
      int startIdx = 0;
      int endIdx = -1;
      do {
        String locator;
        endIdx = initialHosts.indexOf(',', startIdx);
        if (endIdx == -1) {
          locator = initialHosts.substring(startIdx);
        } else {
          locator = initialHosts.substring(startIdx, endIdx);
          startIdx = endIdx+1;
        }
        locators.add(new DistributionLocatorId(locator));

      } while (endIdx != -1);
    
      if (this.mcastEnabled) {
        locators.add(this.mcastId);
      }
      this.ids = Collections.unmodifiableSet(locators);
      if (this.mcastEnabled) {
        Assert.assertTrue(this.mcastId != null);
      }
    }
  }

  /**
   * Constructs a transport config given a collection of {@link
   * DistributionLocatorId} instances.
   */
  public RemoteTransportConfig(
    boolean isMcastEnabled,
    boolean isTcpDisabled,
    boolean isAutoReconnectDisabled,
    String bindAddress, 
    SSLConfig sslConfig,
    Collection ids, String membershipPortRange,
    int tcpPort, int vmKind)
  {
    DistributionLocatorId mid = null;
    
    if (bindAddress == null) {
      this.bindAddress = DistributionConfig.DEFAULT_BIND_ADDRESS;
    } else {
      this.bindAddress = bindAddress;
    }
    
    this.sslConfig = sslConfig;
    
    this.mcastEnabled = isMcastEnabled;
    this.tcpDisabled = isTcpDisabled;
    this.disableAutoReconnect = isAutoReconnectDisabled;
    if (isMcastEnabled) {
      if (ids.size() < 1) {
        throw new IllegalArgumentException(LocalizedStrings.RemoteTransportConfig_EXPECTED_AT_LEAST_ONE_HOSTPORT_ID.toLocalizedString());
      }
      Iterator it = ids.iterator();
      while (it.hasNext() && mid == null) {
        DistributionLocatorId id = (DistributionLocatorId)it.next();
        if (id.isMcastId()) {
          mid = id;
          //System.out.println("mcast id: " + id);
        }
        else {
          //System.out.println("non-mcast id: " + id);
        }
      }
    }
    this.ids = Collections.unmodifiableSet(new HashSet(ids));
    this.mcastId = mid;
    if (this.mcastEnabled) {
      Assert.assertTrue(this.mcastId != null);
    }
    this.membershipPortRange = membershipPortRange;
    this.tcpPort = tcpPort;
    this.vmKind = vmKind;
 }
  
  
  private static String getMembershipPortRangeString(int[] membershipPortRange) {
    String membershipPortRangeString = "";
    if (membershipPortRange != null && 
        membershipPortRange.length == 2) {
      membershipPortRangeString = membershipPortRange[0] + "-" + 
                                  membershipPortRange[1];
    }
    
    return membershipPortRangeString;
  }

  // -------------------------------------------------------------------------
  //   Attribute(s)
  // -------------------------------------------------------------------------
  
  /**
   * Returns the set of DistributionLocatorId instances that define this
   * transport. The set is unmodifiable.
   */
  public Set getIds() {
    return this.ids;
  }
  
  /**
   * Returns true iff multicast is enabled in this transport.
   * Multicast must be enabled in order to use multicast discovery.
   */
  public boolean isMcastEnabled() {
    return this.mcastEnabled;
  }
  
  public DistributionLocatorId getMcastId() {
    return this.mcastId;
  }
  
  public int getVmKind() {
    return this.vmKind;
  }
  
  public boolean isTcpDisabled() {
    return this.tcpDisabled;
  }

  public String getBindAddress() {
	  return this.bindAddress;
  }
  
  public SSLConfig getSSLConfig() {
	  return this.sslConfig;
  }
  
  public String getMembershipPortRange() {
    return this.membershipPortRange;
  }
  
  public int getTcpPort() {
    return this.tcpPort;
  }
  
  public boolean getIsReconnectingDS() {
    return isReconnectingDS;
  }

  public void setIsReconnectingDS(boolean isReconnectingDS) {
    this.isReconnectingDS = isReconnectingDS;
  }

  public Object getOldDSMembershipInfo() {
    return oldDSMembershipInfo;
  }

  public void setOldDSMembershipInfo(Object oldDSMembershipInfo) {
    this.oldDSMembershipInfo = oldDSMembershipInfo;
  }

  /**
   * Returns a <code>Properties</code> based on this config that is
   * appropriate to use with {@link
   * com.gemstone.gemfire.distributed.DistributedSystem#connect}.
   *
   * @since GemFire 4.0
   */
  Properties toDSProperties() {
    Properties props = new Properties();
    props.setProperty(BIND_ADDRESS,
                      bindAddress);
//    System.out.println("entering ds port range property of " + this.membershipPortRange);
    if (this.membershipPortRange != null) {
      props.setProperty(MEMBERSHIP_PORT_RANGE, this.membershipPortRange);
    }
    if (this.tcpPort != 0) {
      props.setProperty(TCP_PORT, String.valueOf(this.tcpPort));
    }
    if (this.mcastEnabled) {
       // Fix bug 32849
      props.setProperty(MCAST_ADDRESS,
                         String.valueOf(this.mcastId.getHost().getHostAddress()));
      props.setProperty(MCAST_PORT,
                        String.valueOf(this.mcastId.getPort()));

    }
    else {
      props.setProperty(MCAST_PORT,
                        String.valueOf(0));
    }
    // Create locator string
    StringBuffer locators = new StringBuffer();
    for (Iterator iter = this.ids.iterator(); iter.hasNext(); ) {
      DistributionLocatorId locator =
          (DistributionLocatorId) iter.next();
      if (!locator.isMcastId()) {
        String baddr = locator.getBindAddress();
        if (baddr != null && baddr.trim().length() > 0) {
          locators.append(baddr);
        }
        else {
          locators.append(locator.getHost().getCanonicalHostName());
        }
        locators.append("[");
        locators.append(locator.getPort());
        locators.append("]");

        if (iter.hasNext()) {
          locators.append(",");
        }
      }
    }

    props.setProperty(LOCATORS,
        locators.toString());

    this.sslConfig.toDSProperties(props);
    
    props.setProperty(DISABLE_TCP,
      this.tcpDisabled? "true" : "false");
    
    props.setProperty(DISABLE_AUTO_RECONNECT, this.disableAutoReconnect? "true" : "false");

    return props;
  }

  private String toString(boolean noMcast) {
    StringBuffer result = new StringBuffer();
    boolean first = true;
    Iterator it = ids.iterator();
    while (it.hasNext()) {
      DistributionLocatorId dli = (DistributionLocatorId)it.next();
      if (noMcast && dli.isMcastId()) {
        continue;
      }
      if (!first) {
        result.append(',');
      } else {
        first = false;
      }
      result.append(dli.toString());
    }
    return result.toString();
  }
  
  /** returns a locators string suitable for use in locators= in gemfire.properties */
  public String locatorsString() {
    return this.toString(true);
  }
  

 // -------------------------------------------------------------------------
  //   Methods overridden from java.lang.Object
  // -------------------------------------------------------------------------
  
  @Override
  public String toString() {
    return toString(false);
  }

  @Override
  public boolean equals(Object o) {
    if (o != null && o instanceof RemoteTransportConfig) {
      RemoteTransportConfig other = (RemoteTransportConfig)o;
      return (this.mcastEnabled == other.mcastEnabled)
        && this.ids.equals(other.ids);
    }
    return false;
  }
  
  @Override
  public int hashCode() {
    return this.ids.hashCode();
  }
  
}

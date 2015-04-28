/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.admin.remote;

import com.gemstone.gemfire.distributed.internal.*;
import com.gemstone.gemfire.internal.*;
import com.gemstone.gemfire.internal.admin.*;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

import java.util.*;

/**
 * Tranport config for RemoteGfManagerAgent.
 * 
 * @author    Darrel Schneider
 * @author    Pete Matern
 * @author    David Whitlock
 * @author    Kirk Lund
 *
 */
public class RemoteTransportConfig implements TransportConfig {
  
  private final boolean mcastEnabled;
  private final boolean mcastDiscovery;
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

  // -------------------------------------------------------------------------
  //   Constructor(s)
  // -------------------------------------------------------------------------
  
  public RemoteTransportConfig(int port) {
    this(port, null);
  }
  
  /**
   * Constructs a simple transport config that specifies just a port.
   * The port must be the one a DistributionLocator is listening
   * to on the local host. 
   */
  public RemoteTransportConfig(int port, String bindAddress) {
    if (bindAddress == null) {
      this.bindAddress = DistributionConfig.DEFAULT_BIND_ADDRESS;
    } else {
      this.bindAddress = bindAddress;
    }
    this.sslConfig = new SSLConfig();
    this.mcastEnabled = false;
    this.mcastDiscovery = false;
    this.tcpDisabled = false;
    this.disableAutoReconnect = false;
    this.mcastId = null;
    this.ids = Collections.singleton(new DistributionLocatorId(port, bindAddress));
    this.membershipPortRange = 
       getMembershipPortRangeString(DistributionConfig.DEFAULT_MEMBERSHIP_PORT_RANGE);
  }

  /**
   * Creates a new <code>RemoteTransportConfig</code> from the
   * configuration information in a <code>DistributionConfig</code>.
   * We assume that <code>config</code> already been checked for
   * errors.
   *
   * @since 3.0
   */
  public RemoteTransportConfig(DistributionConfig config) {
    if (config.getBindAddress() == null) {
      this.bindAddress = DistributionConfig.DEFAULT_BIND_ADDRESS;
    } else {
      this.bindAddress = config.getBindAddress();
    }
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
      if (!this.mcastEnabled) {
        // loner system
        this.mcastDiscovery = false;
        this.ids = Collections.EMPTY_SET;
      }
      else {
        // multicast discovery
        this.mcastDiscovery = true;
        this.ids = Collections.singleton(this.mcastId);
      }
      return;
    }
    else {
      // locator-based discovery
      this.mcastDiscovery = false;

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
    boolean isMcastDiscovery, 
    boolean isTcpDisabled,
    boolean isAutoReconnectDisabled,
    String bindAddress, 
    SSLConfig sslConfig,
    Collection ids, String membershipPortRange,
    int tcpPort)
  {
    DistributionLocatorId mid = null;
    
    if (bindAddress == null) {
      this.bindAddress = DistributionConfig.DEFAULT_BIND_ADDRESS;
    } else {
      this.bindAddress = bindAddress;
    }
    
    this.sslConfig = sslConfig;
    
    this.mcastEnabled = isMcastEnabled;
    this.mcastDiscovery = isMcastDiscovery;
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
   * Returns true if config picked multicast.
   * Returns false if config picked locators.
   */
  public boolean isMcastDiscovery() {
    return this.mcastDiscovery;
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
   * @since 4.0
   */
  Properties toDSProperties() {
    Properties props = new Properties();
    props.setProperty(DistributionConfig.BIND_ADDRESS_NAME,
                      bindAddress);
//    System.out.println("entering ds port range property of " + this.membershipPortRange);
    if (this.membershipPortRange != null) {
      props.setProperty(DistributionConfig.MEMBERSHIP_PORT_RANGE_NAME, this.membershipPortRange);
    }
    if (this.tcpPort != 0) {
      props.setProperty(DistributionConfig.TCP_PORT_NAME, String.valueOf(this.tcpPort));
    }
//System.out.println("RemoteTransportConfig.mcastEnabled=" + this.mcastEnabled);
//System.out.println("RemoteTransportConfig.mcastDiscovery=" + this.mcastDiscovery);
//Thread.currentThread().dumpStack();
    if (this.mcastEnabled) {
       // Fix bug 32849
       props.setProperty(DistributionConfig.MCAST_ADDRESS_NAME,
                         String.valueOf(this.mcastId.getHost().getHostAddress()));
       props.setProperty(DistributionConfig.MCAST_PORT_NAME,
                        String.valueOf(this.mcastId.getPort()));

    }
    else {
      props.setProperty(DistributionConfig.MCAST_PORT_NAME, 
                        String.valueOf(0));
    }
    if (!this.mcastDiscovery) {
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

      props.setProperty(DistributionConfig.LOCATORS_NAME,
                        locators.toString());
    }
    this.sslConfig.toDSProperties(props);
    
    props.setProperty(DistributionConfig.DISABLE_TCP_NAME,
      this.tcpDisabled? "true" : "false");
    
    props.setProperty(DistributionConfig.DISABLE_AUTO_RECONNECT_NAME, this.disableAutoReconnect? "true" : "false");

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
        && (this.mcastDiscovery == other.mcastDiscovery)
        && this.ids.equals(other.ids);
    }
    return false;
  }
  
  @Override
  public int hashCode() {
    return this.ids.hashCode() + (isMcastDiscovery() ? 1 : 0);
  }
  
}

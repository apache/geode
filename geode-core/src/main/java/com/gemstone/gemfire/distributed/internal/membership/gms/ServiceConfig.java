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
package com.gemstone.gemfire.distributed.internal.membership.gms;

import com.gemstone.gemfire.distributed.Locator;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalLocator;
import com.gemstone.gemfire.internal.net.SocketCreator;
import com.gemstone.gemfire.internal.admin.remote.RemoteTransportConfig;

import java.net.InetAddress;

public class ServiceConfig {

  /** stall time to wait for concurrent join/leave/remove requests to be received */
  public static final long MEMBER_REQUEST_COLLECTION_INTERVAL = Long.getLong(DistributionConfig.GEMFIRE_PREFIX + "member-request-collection-interval", 300);

  /** various settings from Geode configuration */
  private long joinTimeout;
  private int[] membershipPortRange;
  private int udpRecvBufferSize;
  private int udpSendBufferSize;
  private long memberTimeout;
  private Integer lossThreshold;
  private Integer memberWeight;
  private boolean networkPartitionDetectionEnabled;
  private int locatorWaitTime;

  /** the configuration for the distributed system */
  private DistributionConfig dconfig;
  
  /** the transport config from the distribution manager */
  private RemoteTransportConfig transport;


  public int getLocatorWaitTime() {
    return locatorWaitTime;
  }


  public long getJoinTimeout() {
    return joinTimeout;
  }


  public int[] getMembershipPortRange() {
    return membershipPortRange;
  }


  public int getUdpRecvBufferSize() {
    return udpRecvBufferSize;
  }


  public int getUdpSendBufferSize() {
    return udpSendBufferSize;
  }


  public long getMemberTimeout() {
    return memberTimeout;
  }


  public int getLossThreshold() {
    return lossThreshold;
  }


  public int getMemberWeight() {
    return memberWeight;
  }


  public boolean isNetworkPartitionDetectionEnabled() {
    return networkPartitionDetectionEnabled;
  }
  
  public void setNetworkPartitionDetectionEnabled(boolean enabled) {
    this.networkPartitionDetectionEnabled = enabled;
  }
  
  /**
   * returns the address that will be used by the DirectChannel to
   * identify this member
   */
  public InetAddress getInetAddress() {
    String bindAddress = this.dconfig.getBindAddress();

    try {
      /* note: had to change the following to make sure the prop wasn't empty 
         in addition to not null for admin.DistributedSystemFactory */
      if (bindAddress != null && bindAddress.length() > 0) {
        return InetAddress.getByName(bindAddress);

      }
      else {
       return SocketCreator.getLocalHost();
      }
    }
    catch (java.net.UnknownHostException unhe) {
      throw new RuntimeException(unhe);

    }
  }

  public boolean areLocatorsPreferredAsCoordinators() {
    boolean locatorsAreCoordinators = false;

    if (networkPartitionDetectionEnabled) {
      locatorsAreCoordinators = true;
    }
    else {
      // check if security is enabled
      String prop = dconfig.getSecurityPeerAuthInit();
      locatorsAreCoordinators =  (prop != null && prop.length() > 0);
      if (!locatorsAreCoordinators) {
        locatorsAreCoordinators = Boolean.getBoolean(InternalLocator.LOCATORS_PREFERRED_AS_COORDINATORS);
      }
    }
    return locatorsAreCoordinators;
  }
  
  public DistributionConfig getDistributionConfig() {
    return this.dconfig;
  }


  public RemoteTransportConfig getTransport() {
    return this.transport;
  }
  

  public ServiceConfig(RemoteTransportConfig transport, DistributionConfig theConfig) {
    this.dconfig = theConfig;
    this.transport = transport;
    
    long defaultJoinTimeout = 24000;
    if (theConfig.getLocators().length() > 0 && !Locator.hasLocators()) {
      defaultJoinTimeout = 60000;
    }
    
    // we need to have enough time to figure out that the coordinator has crashed &
    // find a new one
    long minimumJoinTimeout = dconfig.getMemberTimeout() * 2 + MEMBER_REQUEST_COLLECTION_INTERVAL;
    if (defaultJoinTimeout < minimumJoinTimeout) {
      defaultJoinTimeout = minimumJoinTimeout;
    };
    
    joinTimeout = Long.getLong("p2p.joinTimeout", defaultJoinTimeout).longValue();
    
    // if network partition detection is enabled, we must connect to the locators
    // more frequently in order to make sure we're not isolated from them
    if (theConfig.getEnableNetworkPartitionDetection()) {
      if (!SocketCreator.FORCE_DNS_USE) {
        SocketCreator.resolve_dns = false;
      }
    }


    membershipPortRange = theConfig.getMembershipPortRange();
    
    udpRecvBufferSize = DistributionConfig.DEFAULT_UDP_RECV_BUFFER_SIZE_REDUCED;
    udpSendBufferSize = theConfig.getUdpSendBufferSize();

    memberTimeout = theConfig.getMemberTimeout();

    // The default view-ack timeout in 7.0 is 12347 ms but is adjusted based on the member-timeout.
    // We don't want a longer timeout than 12437 because new members will likely time out trying to 
    // connect because their join timeouts are set to expect a shorter period
    int ackCollectionTimeout = theConfig.getMemberTimeout() * 2 * 12437 / 10000;
    if (ackCollectionTimeout < 1500) {
      ackCollectionTimeout = 1500;
    } else if (ackCollectionTimeout > 12437) {
      ackCollectionTimeout = 12437;
    }
    ackCollectionTimeout = Integer.getInteger(DistributionConfig.GEMFIRE_PREFIX + "VIEW_ACK_TIMEOUT", ackCollectionTimeout).intValue();

    lossThreshold = Integer.getInteger(DistributionConfig.GEMFIRE_PREFIX + "network-partition-threshold", 51);
    if (lossThreshold < 51) lossThreshold = 51;
    if (lossThreshold > 100) lossThreshold = 100;

    memberWeight = Integer.getInteger(DistributionConfig.GEMFIRE_PREFIX + "member-weight", 0);
    locatorWaitTime = theConfig.getLocatorWaitTime();
    
    networkPartitionDetectionEnabled = theConfig.getEnableNetworkPartitionDetection();
  }

}

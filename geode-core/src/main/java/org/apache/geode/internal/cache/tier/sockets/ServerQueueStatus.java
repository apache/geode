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
package org.apache.geode.internal.cache.tier.sockets;

import org.apache.geode.distributed.DistributedMember;

/**
 * Status of HARegionQueue on server when the client is sonnecting/reconnecting. This holds the
 * information abt HARegionQueue and this gets populated as a response of Handshake to server. This
 * wrapper object is used to hold the various info retrurned from Handshake. such as endpoint type
 * and queue size.
 *
 * @since GemFire 5.5
 *
 */
public class ServerQueueStatus {
  /**
   * interval that server sends pings if connection is idle. This is only known for
   * CacheClientUpdater subscription feed status objects
   */
  private final int pingInterval;
  /** queueSize of HARegionQueue for this client */
  private int qSize = 0;
  /** Endpoint type for this endpoint */
  private byte endpointType = (byte) 0;
  private DistributedMember memberId = null;
  /** size of the PDX registry on the server. Currently only set for gateways */
  private int pdxSize = 0;

  /**
   * Constructor Called when connectionsPerServer is nto equal to 0
   *
   */
  public ServerQueueStatus(byte endpointType, int queueSize, DistributedMember memberId,
      int pingInterval) {
    qSize = queueSize;
    this.endpointType = endpointType;
    this.memberId = memberId;
    this.pingInterval = pingInterval;
  }

  /**
   * Constructor Called when connectionsPerServer is nto equal to 0
   *
   */
  public ServerQueueStatus(byte endpointType, int queueSize, DistributedMember memberId) {
    qSize = queueSize;
    this.endpointType = endpointType;
    this.memberId = memberId;
    pingInterval = -1;
  }

  /**
   * returns true if the endpoint is primary
   */
  public boolean isPrimary() {
    return endpointType == (byte) 2;
  }

  /**
   * returns true if the endpoint is redundant
   */
  public boolean isRedundant() {
    return endpointType == (byte) 1;
  }

  /**
   * returns true if the endpoint is Non redundant
   */
  public boolean isNonRedundant() {
    return endpointType == (byte) 0;
  }

  /**
   * returns size of the HARegionQueue for this client
   */
  public int getServerQueueSize() {
    return qSize;
  }

  /** returns the time between server-to-client ping messages on idle subscription connections */
  public int getPingInterval() {
    if (pingInterval < 0) {
      throw new IllegalStateException(
          "ping interval is only known for a subscription feed connection");
    }
    return pingInterval;
  }

  public String toString() {
    StringBuilder buffer = new StringBuilder();
    buffer.append("ServerQueueStatus [").append("queueSize=").append(qSize)
        .append("; endpointType=").append(getTypeAsString()).append("; pingInterval=")
        .append(pingInterval).append("ms]");
    return buffer.toString();
  }

  protected String getTypeAsString() {
    String type = null;
    if (isNonRedundant()) {
      type = "NON_REDUNDANT";
    } else if (isRedundant()) {
      type = "REDUNDANT";
    } else {
      type = "PRIMARY";
    }
    return type;
  }



  public int getPdxSize() {
    return pdxSize;
  }

  public void setPdxSize(int pdxSize) {
    this.pdxSize = pdxSize;
  }

  /**
   * The member id of the server we connected to.
   *
   * @return the memberid
   */
  public DistributedMember getMemberId() {
    return memberId;
  }
}

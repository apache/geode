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

package org.apache.geode.management.configuration;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;

import org.apache.geode.management.runtime.GatewayReceiverInfo;


/**
 * Use this to configure the gateway receiver in the cluster
 *
 * setting the bindAddress and hostname for clients are not supported by this api.
 */

public class GatewayReceiver extends GroupableConfiguration<GatewayReceiverInfo> {
  /**
   * Returns {@link #getGroup()} or {@link AbstractConfiguration#CLUSTER} if group is null.
   */
  @Override
  @JsonIgnore
  public String getId() {
    String id = getGroup();
    if (isCluster(id)) {
      id = CLUSTER;
    }
    return id;
  }

  /**
   * the url end points to retrieve the gateway receivers
   */
  public static final String GATEWAY_RECEIVERS_ENDPOINTS = "/gateways/receivers";

  @Override
  public String getEndpoint() {
    return GATEWAY_RECEIVERS_ENDPOINTS;
  }

  private List<ClassName> gatewayTransportFilters;
  private Integer startPort;
  private Integer endPort;
  private Integer maximumTimeBetweenPings;
  private Integer socketBufferSize;
  private Boolean manualStart;

  /**
   * get the list of transport filters
   * if you modify the returned list, you will be modifying the list owned by this config object.
   */
  public List<ClassName> getGatewayTransportFilters() {
    return gatewayTransportFilters;
  }

  /**
   * set the gateway transport filters
   */
  public void setGatewayTransportFilters(
      List<ClassName> gatewayTransportFilters) {
    this.gatewayTransportFilters = gatewayTransportFilters;
  }

  /**
   * get the starting port
   */
  public Integer getStartPort() {
    return startPort;
  }

  /**
   * set the starting port
   */
  public void setStartPort(Integer startPort) {
    this.startPort = startPort;
  }

  /**
   * get the end port
   */
  public Integer getEndPort() {
    return endPort;
  }

  /**
   * set the end port
   */
  public void setEndPort(Integer endPort) {
    this.endPort = endPort;
  }

  /**
   * get the maximum time between pings in milliseconds, the default is
   * CacheServer.DEFAULT_MAXIMUM_TIME_BETWEEN_PINGS = 60000
   */
  public Integer getMaximumTimeBetweenPings() {
    return maximumTimeBetweenPings;
  }

  /**
   * set the maximum time between pings in milliseconds
   */
  public void setMaximumTimeBetweenPings(Integer maximumTimeBetweenPings) {
    this.maximumTimeBetweenPings = maximumTimeBetweenPings;
  }

  /**
   * get the socket buffer size for socket buffers from the receiver to the sender.
   * CacheServer.DEFAULT_SOCKET_BUFFER_SIZE = 32768;
   */
  public Integer getSocketBufferSize() {
    return socketBufferSize;
  }

  /**
   * set the socket buffer size for socket buffers from the receiver to the sender.
   */
  public void setSocketBufferSize(Integer socketBufferSize) {
    this.socketBufferSize = socketBufferSize;
  }

  /**
   * is this gateway receiver manually started
   */
  public Boolean isManualStart() {
    return manualStart;
  }

  /**
   * set the manualStart
   */
  public void setManualStart(Boolean manualStart) {
    this.manualStart = manualStart;
  }
}

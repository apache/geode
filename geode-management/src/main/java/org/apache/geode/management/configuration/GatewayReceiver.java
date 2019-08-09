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

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;

import org.apache.geode.cache.configuration.CacheElement;
import org.apache.geode.cache.configuration.DeclarableType;
import org.apache.geode.management.api.CorrespondWith;
import org.apache.geode.management.api.RestfulEndpoint;
import org.apache.geode.management.runtime.GatewayReceiverInfo;


/**
 * As a configuration object, user should not be able to set bind address and
 * hostname for clients on it.
 */

public class GatewayReceiver extends CacheElement implements RestfulEndpoint,
    CorrespondWith<GatewayReceiverInfo> {
  @Override
  @JsonIgnore
  public String getId() {
    return getConfigGroup();
  }

  public static final String GATEWAY_RECEIVERS_ENDPOINTS = "/gateways/receivers";

  @Override
  public String getEndpoint() {
    return GATEWAY_RECEIVERS_ENDPOINTS;
  }

  protected List<DeclarableType> gatewayTransportFilters = new ArrayList<>();
  protected String startPort;
  protected String endPort;
  protected String maximumTimeBetweenPings;
  protected String socketBufferSize;
  protected boolean manualStart = false;

  public List<DeclarableType> getGatewayTransportFilters() {
    return gatewayTransportFilters;
  }

  public void addGatewayTransportFilter(DeclarableType gatewayTransportFilter) {
    this.gatewayTransportFilters.add(gatewayTransportFilter);
  }

  public String getStartPort() {
    return startPort;
  }

  public void setStartPort(String startPort) {
    this.startPort = startPort;
  }

  public String getEndPort() {
    return endPort;
  }

  public void setEndPort(String endPort) {
    this.endPort = endPort;
  }

  public String getMaximumTimeBetweenPings() {
    return maximumTimeBetweenPings;
  }

  public void setMaximumTimeBetweenPings(String maximumTimeBetweenPings) {
    this.maximumTimeBetweenPings = maximumTimeBetweenPings;
  }

  public String getSocketBufferSize() {
    return socketBufferSize;
  }

  public void setSocketBufferSize(String socketBufferSize) {
    this.socketBufferSize = socketBufferSize;
  }

  public boolean isManualStart() {
    return manualStart;
  }

  public void setManualStart(boolean manualStart) {
    this.manualStart = manualStart;
  }
}

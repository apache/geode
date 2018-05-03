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
package org.apache.geode.management.internal.cli.functions;

import java.io.Serializable;

import org.apache.geode.cache.configuration.CacheConfig.GatewayReceiver;
import org.apache.geode.cache.configuration.ClassWithParametersType;

/**
 * This class stores the arguments provided in the create gateway-receiver command.
 */
public class GatewayReceiverFunctionArgs implements Serializable {
  private static final long serialVersionUID = -5158224572470173267L;

  private final Boolean manualStart;

  private final Integer startPort;

  private final Integer endPort;

  private final String bindAddress;

  private final Integer socketBufferSize;

  private final Integer maximumTimeBetweenPings;

  private final String[] gatewayTransportFilters;

  private final String hostnameForSenders;

  private final Boolean ifNotExists;

  public GatewayReceiverFunctionArgs(GatewayReceiver configuration, Boolean ifNotExists) {
    this.manualStart = configuration.isManualStart();
    this.startPort =
        configuration.getStartPort() != null ? Integer.valueOf(configuration.getStartPort()) : null;
    this.endPort =
        configuration.getEndPort() != null ? Integer.valueOf(configuration.getEndPort()) : null;
    this.bindAddress = configuration.getBindAddress();
    this.socketBufferSize = configuration.getSocketBufferSize() != null
        ? Integer.valueOf(configuration.getSocketBufferSize()) : null;
    this.maximumTimeBetweenPings = configuration.getMaximumTimeBetweenPings() != null
        ? Integer.valueOf(configuration.getMaximumTimeBetweenPings()) : null;
    this.gatewayTransportFilters = configuration.getGatewayTransportFilter() != null
        ? configuration.getGatewayTransportFilter().stream()
            .map(ClassWithParametersType::getClassName).toArray(String[]::new)
        : null;
    this.hostnameForSenders = configuration.getHostnameForSenders();
    this.ifNotExists = ifNotExists;
  }

  public Boolean isManualStart() {
    return this.manualStart;
  }

  public Integer getStartPort() {
    return this.startPort;
  }

  public Integer getEndPort() {
    return this.endPort;
  }

  public String getBindAddress() {
    return this.bindAddress;
  }

  public Integer getSocketBufferSize() {
    return this.socketBufferSize;
  }

  public Integer getMaximumTimeBetweenPings() {
    return this.maximumTimeBetweenPings;
  }

  public String[] getGatewayTransportFilters() {
    return this.gatewayTransportFilters;
  }

  public String getHostnameForSenders() {
    return hostnameForSenders;
  }

  public Boolean getIfNotExists() {
    return ifNotExists;
  }
}

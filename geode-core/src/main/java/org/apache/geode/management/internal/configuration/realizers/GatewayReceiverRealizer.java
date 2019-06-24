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

package org.apache.geode.management.internal.configuration.realizers;

import static org.apache.geode.management.internal.configuration.domain.DeclarableTypeInstantiator.newInstance;

import java.util.List;

import org.apache.commons.lang3.NotImplementedException;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.configuration.DeclarableType;
import org.apache.geode.cache.configuration.GatewayReceiverConfig;
import org.apache.geode.cache.wan.GatewayReceiverFactory;
import org.apache.geode.management.api.RealizationResult;

public class GatewayReceiverRealizer implements ConfigurationRealizer<GatewayReceiverConfig> {

  @Override
  public RealizationResult create(GatewayReceiverConfig config, Cache cache) {
    GatewayReceiverFactory gatewayReceiverFactory = cache.createGatewayReceiverFactory();

    String startPort = config.getStartPort();
    if (startPort != null) {
      gatewayReceiverFactory.setStartPort(Integer.valueOf(startPort));
    }

    String endPort = config.getEndPort();
    if (endPort != null) {
      gatewayReceiverFactory.setEndPort(Integer.valueOf(endPort));
    }

    String bindAddress = config.getBindAddress();
    if (bindAddress != null) {
      gatewayReceiverFactory.setBindAddress(bindAddress);
    }

    String hostnameForSenders = config.getHostnameForSenders();
    if (hostnameForSenders != null) {
      gatewayReceiverFactory.setHostnameForSenders(hostnameForSenders);
    }

    String maxTimeBetweenPings = config.getMaximumTimeBetweenPings();
    if (maxTimeBetweenPings != null) {
      gatewayReceiverFactory.setMaximumTimeBetweenPings(Integer.valueOf(maxTimeBetweenPings));
    }

    String socketBufferSize = config.getSocketBufferSize();
    if (socketBufferSize != null) {
      gatewayReceiverFactory.setSocketBufferSize(Integer.valueOf(socketBufferSize));
    }

    Boolean manualStart = config.isManualStart();
    if (manualStart != null) {
      gatewayReceiverFactory.setManualStart(manualStart);
    }

    List<DeclarableType> gatewayTransportFilters = config.getGatewayTransportFilters();
    if (gatewayTransportFilters != null) {
      for (DeclarableType gatewayTransportFilter : gatewayTransportFilters) {
        gatewayReceiverFactory
            .addGatewayTransportFilter(newInstance(gatewayTransportFilter, cache));
      }
    }

    gatewayReceiverFactory.create();
    RealizationResult result = new RealizationResult().setSuccess(true);
    return result;
  }

  @Override
  public boolean exists(GatewayReceiverConfig config, Cache cache) {
    return cache.getGatewayReceivers() != null && cache.getGatewayReceivers().size() > 0;
  }

  @Override
  public RealizationResult update(GatewayReceiverConfig config, Cache cache) {
    throw new NotImplementedException("Not implemented");
  }

  @Override
  public RealizationResult delete(GatewayReceiverConfig config, Cache cache) {
    throw new NotImplementedException("Not implemented");
  }
}

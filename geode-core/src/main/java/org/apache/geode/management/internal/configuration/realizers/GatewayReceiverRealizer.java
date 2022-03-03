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

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.configuration.DeclarableType;
import org.apache.geode.cache.configuration.GatewayReceiverConfig;
import org.apache.geode.cache.wan.GatewayReceiverFactory;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.api.RealizationResult;
import org.apache.geode.management.configuration.GatewayReceiver;
import org.apache.geode.management.internal.beans.GatewayReceiverMBeanBridge;
import org.apache.geode.management.internal.configuration.converters.GatewayReceiverConverter;
import org.apache.geode.management.runtime.GatewayReceiverInfo;

public class GatewayReceiverRealizer
    implements ConfigurationRealizer<GatewayReceiver, GatewayReceiverInfo> {

  private final GatewayReceiverConverter converter = new GatewayReceiverConverter();

  @Override
  public RealizationResult create(GatewayReceiver config, InternalCache cache) {
    return create(converter.fromConfigObject(config), cache);
  }

  /**
   * Need to keep this method since we are using the realizer in the GatewayReceiverFunction
   */
  public RealizationResult create(GatewayReceiverConfig config, InternalCache cache) {
    GatewayReceiverFactory gatewayReceiverFactory = cache.createGatewayReceiverFactory();

    String startPort = config.getStartPort();
    if (startPort != null) {
      gatewayReceiverFactory.setStartPort(Integer.parseInt(startPort));
    }

    String endPort = config.getEndPort();
    if (endPort != null) {
      gatewayReceiverFactory.setEndPort(Integer.parseInt(endPort));
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
      gatewayReceiverFactory.setMaximumTimeBetweenPings(Integer.parseInt(maxTimeBetweenPings));
    }

    String socketBufferSize = config.getSocketBufferSize();
    if (socketBufferSize != null) {
      gatewayReceiverFactory.setSocketBufferSize(Integer.parseInt(socketBufferSize));
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
    return new RealizationResult().setSuccess(true);
  }

  @Override
  public boolean exists(GatewayReceiver config, InternalCache cache) {
    return cache.getGatewayReceivers() != null && cache.getGatewayReceivers().size() > 0;
  }

  @Override
  public GatewayReceiverInfo get(GatewayReceiver config, InternalCache cache) {
    if (cache.getGatewayReceivers() == null || cache.getGatewayReceivers().size() == 0) {
      return null;
    }

    org.apache.geode.cache.wan.GatewayReceiver receiver =
        cache.getGatewayReceivers().iterator().next();
    return generateGatewayReceiverInfo(receiver);
  }

  @VisibleForTesting
  GatewayReceiverInfo generateGatewayReceiverInfo(
      org.apache.geode.cache.wan.GatewayReceiver receiver) {
    GatewayReceiverMBeanBridge bridge = new GatewayReceiverMBeanBridge(receiver);
    GatewayReceiverInfo info = new GatewayReceiverInfo();
    info.setBindAddress(receiver.getBindAddress());
    info.setHostnameForSenders(receiver.getHostnameForSenders());
    info.setPort(receiver.getPort());
    info.setRunning(receiver.isRunning());
    info.setSenderCount(bridge.getClientConnectionCount());
    info.setConnectedSenders(bridge.getConnectedGatewaySenders());
    return info;
  }

  @Override
  public RealizationResult update(GatewayReceiver config, InternalCache cache) {
    throw new NotImplementedException("Not implemented");
  }

  @Override
  public RealizationResult delete(GatewayReceiver config, InternalCache cache) {
    throw new NotImplementedException("Not implemented");
  }
}

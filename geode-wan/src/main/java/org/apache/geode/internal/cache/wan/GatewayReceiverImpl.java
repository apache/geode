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
package org.apache.geode.internal.cache.wan;

import static org.apache.geode.internal.AvailablePort.SOCKET;
import static org.apache.geode.internal.AvailablePort.getAddress;
import static org.apache.geode.internal.AvailablePort.getRandomAvailablePortInRange;
import static org.apache.geode.internal.AvailablePort.isPortAvailable;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache.wan.GatewayReceiver;
import org.apache.geode.cache.wan.GatewayTransportFilter;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.ResourceEvent;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalCacheServer;
import org.apache.geode.internal.inet.LocalHostUtil;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * @since GemFire 7.0
 */
public class GatewayReceiverImpl implements GatewayReceiver {

  private static final Logger logger = LogService.getLogger();

  private final InternalCache cache;
  private final String hostnameForSenders;
  private final int startPort;
  private final int endPort;
  private final int maximumTimeBetweenPings;
  private final int socketBufferSize;
  private final boolean manualStart;
  private final List<GatewayTransportFilter> gatewayTransportFilters;
  private final String bindAddress;
  private final Function<Integer, Boolean> isPortAvailableFunction;
  private final Function<PortRange, Integer> getRandomAvailablePortInRangeFunction;

  private volatile int port;
  private volatile InternalCacheServer receiverServer;

  GatewayReceiverImpl(final InternalCache cache, final int startPort, final int endPort,
      final int maximumTimeBetweenPings, final int socketBufferSize, final String bindAddress,
      final List<GatewayTransportFilter> gatewayTransportFilters, final String hostnameForSenders,
      final boolean manualStart) {
    this(cache, startPort, endPort, maximumTimeBetweenPings, socketBufferSize, bindAddress,
        gatewayTransportFilters, hostnameForSenders, manualStart,
        port -> isPortAvailable(port, SOCKET, getAddress(SOCKET)),
        portRange -> getRandomAvailablePortInRange(portRange.startPort, portRange.endPort, SOCKET));
  }

  @VisibleForTesting
  GatewayReceiverImpl(final InternalCache cache, final int startPort, final int endPort,
      final int maximumTimeBetweenPings, final int socketBufferSize, final String bindAddress,
      final List<GatewayTransportFilter> gatewayTransportFilters, final String hostnameForSenders,
      final boolean manualStart, final boolean isPortAvailableResult,
      final int getRandomAvailablePortInRangeResult) {
    this(cache, startPort, endPort, maximumTimeBetweenPings, socketBufferSize, bindAddress,
        gatewayTransportFilters, hostnameForSenders, manualStart, port -> isPortAvailableResult,
        portRange -> getRandomAvailablePortInRangeResult);
  }

  private GatewayReceiverImpl(final InternalCache cache, final int startPort, final int endPort,
      final int maximumTimeBetweenPings, final int socketBufferSize, final String bindAddress,
      final List<GatewayTransportFilter> gatewayTransportFilters, final String hostnameForSenders,
      final boolean manualStart, final Function<Integer, Boolean> isPortAvailableFunction,
      final Function<PortRange, Integer> getRandomAvailablePortInRangeFunction) {
    this.cache = cache;
    this.hostnameForSenders = hostnameForSenders;
    this.startPort = startPort;
    this.endPort = endPort;
    this.maximumTimeBetweenPings = maximumTimeBetweenPings;
    this.socketBufferSize = socketBufferSize;
    this.bindAddress = bindAddress;
    this.gatewayTransportFilters = gatewayTransportFilters;
    this.manualStart = manualStart;
    this.isPortAvailableFunction = isPortAvailableFunction;
    this.getRandomAvailablePortInRangeFunction = getRandomAvailablePortInRangeFunction;
  }

  @Override
  public String getHostnameForSenders() {
    return hostnameForSenders;
  }

  @Override
  public String getHost() {
    if (receiverServer != null) {
      return receiverServer.getExternalAddress();
    }

    if (hostnameForSenders != null && !hostnameForSenders.isEmpty()) {
      return hostnameForSenders;
    }

    if (bindAddress != null && !bindAddress.isEmpty()) {
      return bindAddress;
    }

    try {
      return LocalHostUtil.getLocalHostName();
    } catch (UnknownHostException e) {
      throw new IllegalStateException("Could not get host name", e);
    }
  }

  @Override
  public List<GatewayTransportFilter> getGatewayTransportFilters() {
    return gatewayTransportFilters;
  }

  @Override
  public int getMaximumTimeBetweenPings() {
    return maximumTimeBetweenPings;
  }

  @Override
  public int getPort() {
    return port;
  }

  @Override
  public int getStartPort() {
    return startPort;
  }

  @Override
  public int getEndPort() {
    return endPort;
  }

  @Override
  public int getSocketBufferSize() {
    return socketBufferSize;
  }

  @Override
  public boolean isManualStart() {
    return manualStart;
  }

  @Override
  public CacheServer getServer() {
    return receiverServer;
  }

  private boolean tryToStart(int port) {
    if (!isPortAvailableFunction.apply(port)) {
      return false;
    }

    CacheServer cacheServer = receiverServer;

    cacheServer.setPort(port);
    cacheServer.setSocketBufferSize(socketBufferSize);
    cacheServer.setMaximumTimeBetweenPings(maximumTimeBetweenPings);
    if (hostnameForSenders != null && !hostnameForSenders.isEmpty()) {
      cacheServer.setHostnameForClients(hostnameForSenders);
    }
    cacheServer.setBindAddress(bindAddress);
    cacheServer.setGroups(new String[] {GatewayReceiver.RECEIVER_GROUP});

    try {
      cacheServer.start();
      this.port = port;
      return true;
    } catch (IOException e) {
      logger.info("Failed to create server socket on {}[{}]", bindAddress, port);
      return false;
    }
  }

  @Override
  public void start() {
    if (receiverServer == null) {
      receiverServer = cache.addGatewayReceiverServer(this);
    }

    if (receiverServer.isRunning()) {
      logger.warn("Gateway Receiver is already running");
      return;
    }

    int loopStartPort = getPortToStart();
    int port = loopStartPort;

    while (!tryToStart(port)) {
      // get next port to try
      if (port == endPort && startPort != endPort) {
        port = startPort;
      } else {
        port++;
      }
      if (port == loopStartPort || port > endPort) {
        throw new GatewayReceiverException("No available free port found in the given range (" +
            startPort + "-" + endPort + ")");
      }
    }

    logger.info("The GatewayReceiver started on port : {}", this.port);

    InternalDistributedSystem system = cache.getInternalDistributedSystem();
    system.handleResourceEvent(ResourceEvent.GATEWAYRECEIVER_START, this);
  }

  private int getPortToStart() {
    // choose a random port from the given port range
    int randomPort;
    if (startPort == endPort) {
      randomPort = startPort;
    } else {
      randomPort = getRandomAvailablePortInRangeFunction.apply(new PortRange(startPort, endPort));
    }
    return randomPort;
  }

  @Override
  public void stop() {
    if (!isRunning()) {
      throw new GatewayReceiverException("Gateway Receiver is not running");
    }

    receiverServer.stop();
  }

  @Override
  public void destroy() {
    logger.info("Destroying Gateway Receiver: {}", this);

    if (receiverServer == null) {
      // receiver was not started
      cache.removeGatewayReceiver(this);
    } else {
      if (receiverServer.isRunning()) {
        throw new GatewayReceiverException(
            "Gateway Receiver is running and needs to be stopped first");
      }
      cache.removeGatewayReceiver(this);
      cache.removeGatewayReceiverServer(receiverServer);
    }

    InternalDistributedSystem system = cache.getInternalDistributedSystem();
    system.handleResourceEvent(ResourceEvent.GATEWAYRECEIVER_DESTROY, this);
  }

  @Override
  public String getBindAddress() {
    return bindAddress;
  }

  @Override
  public boolean isRunning() {
    if (receiverServer != null) {
      return receiverServer.isRunning();
    }
    return false;
  }

  @Override
  public String toString() {
    return new StringBuilder().append("Gateway Receiver").append("@")
        .append(Integer.toHexString(hashCode())).append("'; port=").append(getPort())
        .append("; bindAddress=").append(getBindAddress()).append("'; hostnameForSenders=")
        .append(getHostnameForSenders()).append("; maximumTimeBetweenPings=")
        .append(getMaximumTimeBetweenPings()).append("; socketBufferSize=")
        .append(getSocketBufferSize()).append("; isManualStart=").append(isManualStart())
        .append("; group=").append(Arrays.toString(new String[] {GatewayReceiver.RECEIVER_GROUP}))
        .append("]").toString();
  }

  private static class PortRange {

    private final int startPort;
    private final int endPort;

    private PortRange(int startPort, int endPort) {
      this.startPort = startPort;
      this.endPort = endPort;
    }
  }
}

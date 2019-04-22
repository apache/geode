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

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache.wan.GatewayReceiver;
import org.apache.geode.cache.wan.GatewayTransportFilter;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.ResourceEvent;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.net.SocketCreator;

/**
 * @since GemFire 7.0
 */
@SuppressWarnings("deprecation")
public class GatewayReceiverImpl implements GatewayReceiver {

  private static final Logger logger = LogService.getLogger();

  private final InternalCache cache;

  private final String hostnameForSenders;
  private final int startPort;
  private final int endPort;
  private final int timeBetPings;
  private final int socketBufferSize;
  private final boolean manualStart;
  private final List<GatewayTransportFilter> filters;
  private final String bindAdd;

  private GatewayReceiverServer receiver;
  private int port;

  GatewayReceiverImpl(InternalCache cache, int startPort, int endPort,
      int timeBetPings, int buffSize, String bindAdd, List<GatewayTransportFilter> filters,
      String hostnameForSenders, boolean manualStart) {
    this.cache = cache;
    this.hostnameForSenders = hostnameForSenders;
    this.startPort = startPort;
    this.endPort = endPort;
    this.timeBetPings = timeBetPings;
    socketBufferSize = buffSize;
    this.bindAdd = bindAdd;
    this.filters = filters;
    this.manualStart = manualStart;
  }

  @Override
  public String getHostnameForSenders() {
    return hostnameForSenders;
  }

  @Override
  public String getHost() {
    if (receiver != null) {
      return receiver.getExternalAddress();
    }

    if (hostnameForSenders != null && !hostnameForSenders.isEmpty()) {
      return hostnameForSenders;
    }

    if (bindAdd != null && !bindAdd.isEmpty()) {
      return bindAdd;
    }

    try {
      return SocketCreator.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      throw new IllegalStateException("Could not get host name", e);
    }
  }

  @Override
  public List<GatewayTransportFilter> getGatewayTransportFilters() {
    return filters;
  }

  @Override
  public int getMaximumTimeBetweenPings() {
    return timeBetPings;
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
    return receiver;
  }

  private boolean tryToStart(int port) {
    if (!AvailablePort.isPortAvailable(port, SOCKET, getAddress(SOCKET))) {
      return false;
    }

    receiver.setPort(port);
    receiver.setSocketBufferSize(socketBufferSize);
    receiver.setMaximumTimeBetweenPings(timeBetPings);
    if (hostnameForSenders != null && !hostnameForSenders.isEmpty()) {
      receiver.setHostnameForClients(hostnameForSenders);
    }
    receiver.setBindAddress(bindAdd);
    receiver.setGroups(new String[] {GatewayReceiver.RECEIVER_GROUP});
    try {
      receiver.start();
      this.port = port;
      return true;
    } catch (IOException e) {
      logger.info("Failed to create server socket on {}[{}]", bindAdd, port);
      return false;
    }
  }

  @Override
  public void start() {
    if (receiver == null) {
      receiver = cache.addGatewayReceiverServer(this);
    }
    if (receiver.isRunning()) {
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
      randomPort = getRandomAvailablePortInRange(startPort, endPort, SOCKET);
    }
    return randomPort;
  }

  @Override
  public void stop() {
    if (!isRunning()) {
      throw new GatewayReceiverException("Gateway Receiver is not running");
    }
    receiver.stop();
  }

  @Override
  public void destroy() {
    logger.info("Destroying Gateway Receiver: {}", this);
    if (receiver == null) {
      // receiver was not started
      cache.removeGatewayReceiver(this);
    } else {
      if (receiver.isRunning()) {
        throw new GatewayReceiverException(
            "Gateway Receiver is running and needs to be stopped first");
      }
      cache.removeGatewayReceiver(this);
      cache.removeCacheServer(receiver);
    }
    InternalDistributedSystem system = cache.getInternalDistributedSystem();
    system.handleResourceEvent(ResourceEvent.GATEWAYRECEIVER_DESTROY, this);
  }

  @Override
  public String getBindAddress() {
    return bindAdd;
  }

  @Override
  public boolean isRunning() {
    if (receiver != null) {
      return receiver.isRunning();
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
}

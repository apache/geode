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

import java.io.IOException;
import java.net.BindException;
import java.net.SocketException;
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
import org.apache.geode.internal.net.SocketCreator;
import org.apache.geode.internal.cache.CacheServerImpl;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LocalizedMessage;

/**
 * @since GemFire 7.0
 */
@SuppressWarnings("deprecation")
public class GatewayReceiverImpl implements GatewayReceiver {

  private static final Logger logger = LogService.getLogger();

  private String host;

  private int startPort;

  private int endPort;

  private int port;

  private int timeBetPings;

  private int socketBufferSize;

  private boolean manualStart;

  private final List<GatewayTransportFilter> filters;

  private String bindAdd;

  private CacheServer receiver;

  private final InternalCache cache;

  public GatewayReceiverImpl(InternalCache cache, int startPort, int endPort, int timeBetPings,
      int buffSize, String bindAdd, List<GatewayTransportFilter> filters, String hostnameForSenders,
      boolean manualStart) {
    this.cache = cache;

    /*
     * If user has set hostNameForSenders then it should take precedence over bindAddress. If user
     * hasn't set either hostNameForSenders or bindAddress then getLocalHost().getHostName() should
     * be used.
     */
    if (hostnameForSenders == null || hostnameForSenders.isEmpty()) {
      if (bindAdd == null || bindAdd.isEmpty()) {
        try {
          logger
              .warn(LocalizedMessage.create(LocalizedStrings.GatewayReceiverImpl_USING_LOCAL_HOST));
          this.host = SocketCreator.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
          throw new IllegalStateException(
              LocalizedStrings.GatewayReceiverImpl_COULD_NOT_GET_HOST_NAME.toLocalizedString(), e);
        }
      } else {
        this.host = bindAdd;
      }
    } else {
      this.host = hostnameForSenders;
    }

    this.startPort = startPort;
    this.endPort = endPort;
    this.timeBetPings = timeBetPings;
    this.socketBufferSize = buffSize;
    this.bindAdd = bindAdd;
    this.filters = filters;
    this.manualStart = manualStart;
  }

  public List<GatewayTransportFilter> getGatewayTransportFilters() {
    return this.filters;
  }

  public int getMaximumTimeBetweenPings() {
    return this.timeBetPings;
  }

  public int getPort() {
    return this.port;
  }

  public int getStartPort() {
    return this.startPort;
  }

  public int getEndPort() {
    return this.endPort;
  }

  public int getSocketBufferSize() {
    return this.socketBufferSize;
  }

  public boolean isManualStart() {
    return this.manualStart;
  }

  public CacheServer getServer() {
    return receiver;
  }

  public void start() throws IOException {
    if (receiver == null) {
      receiver = this.cache.addCacheServer(true);
    }
    if (receiver.isRunning()) {
      logger.warn(LocalizedMessage.create(LocalizedStrings.GatewayReceiver_IS_ALREADY_RUNNING));
      return;
    }
    boolean started = false;
    this.port = getPortToStart();
    while (!started && this.port != -1) {
      receiver.setPort(this.port);
      receiver.setSocketBufferSize(socketBufferSize);
      receiver.setMaximumTimeBetweenPings(timeBetPings);
      receiver.setHostnameForClients(host);
      receiver.setBindAddress(bindAdd);
      receiver.setGroups(new String[] {GatewayReceiver.RECEIVER_GROUP});
      ((CacheServerImpl) receiver).setGatewayTransportFilter(this.filters);
      try {
        receiver.start();
        started = true;
      } catch (BindException be) {
        if (be.getCause() != null
            && be.getCause().getMessage().contains("assign requested address")) {
          throw new GatewayReceiverException(
              LocalizedStrings.SocketCreator_FAILED_TO_CREATE_SERVER_SOCKET_ON_0_1
                  .toLocalizedString(new Object[] {bindAdd, this.port}));
        }
        // ignore as this port might have been used by other threads.
        logger.warn(LocalizedMessage.create(LocalizedStrings.GatewayReceiver_Address_Already_In_Use,
            this.port));
        this.port = getPortToStart();
      } catch (SocketException se) {
        if (se.getMessage().contains("Address already in use")) {
          logger.warn(LocalizedMessage
              .create(LocalizedStrings.GatewayReceiver_Address_Already_In_Use, this.port));
          this.port = getPortToStart();

        } else {
          throw se;
        }
      }

    }
    if (!started) {
      throw new IllegalStateException("No available free port found in the given range.");
    }
    logger
        .info(LocalizedMessage.create(LocalizedStrings.GatewayReceiver_STARTED_ON_PORT, this.port));

    InternalDistributedSystem system = this.cache.getInternalDistributedSystem();
    system.handleResourceEvent(ResourceEvent.GATEWAYRECEIVER_START, this);

  }

  private int getPortToStart() {
    // choose a random port from the given port range
    int rPort;
    if (this.startPort == this.endPort) {
      rPort = this.startPort;
    } else {
      rPort = AvailablePort.getRandomAvailablePortInRange(this.startPort, this.endPort,
          AvailablePort.SOCKET);
    }
    return rPort;
  }

  public void stop() {
    if (!isRunning()) {
      throw new GatewayReceiverException(
          LocalizedStrings.GatewayReceiver_IS_NOT_RUNNING.toLocalizedString());
    }
    receiver.stop();
  }

  public String getHost() {
    return this.host;
  }

  public String getBindAddress() {
    return this.bindAdd;
  }

  public boolean isRunning() {
    if (this.receiver != null) {
      return this.receiver.isRunning();
    }
    return false;
  }

  public String toString() {
    return new StringBuffer().append("Gateway Receiver").append("@")
        .append(Integer.toHexString(hashCode())).append(" [").append("host='").append(getHost())
        .append("'; port=").append(getPort()).append("; bindAddress=").append(getBindAddress())
        .append("; maximumTimeBetweenPings=").append(getMaximumTimeBetweenPings())
        .append("; socketBufferSize=").append(getSocketBufferSize()).append("; isManualStart=")
        .append(isManualStart()).append("; group=")
        .append(Arrays.toString(new String[] {GatewayReceiver.RECEIVER_GROUP})).append("]")
        .toString();
  }

}

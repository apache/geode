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
package org.apache.geode.internal.cache.xmlcache;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache.wan.GatewayReceiver;
import org.apache.geode.cache.wan.GatewayTransportFilter;
import org.apache.geode.logging.internal.log4j.api.LogService;

public class GatewayReceiverCreation implements GatewayReceiver {
  private static final Logger logger = LogService.getLogger();

  private final Cache cache;

  private String host;

  private int startPort;

  private int endPort;

  private String portRange;

  private List<GatewayTransportFilter> transFilter = new ArrayList<>();

  private int maxTimeBetweenPings;

  private int socketBufferSize;

  private String bindAddress;

  private final String hostnameForSenders;

  private final boolean manualStart;

  private CacheServer receiver;

  public GatewayReceiverCreation(Cache cache, GatewayReceiver gatewayReceiver) {
    this(cache, gatewayReceiver.getStartPort(), gatewayReceiver.getEndPort(),
        gatewayReceiver.getMaximumTimeBetweenPings(), gatewayReceiver.getSocketBufferSize(),
        gatewayReceiver.getBindAddress(), gatewayReceiver.getGatewayTransportFilters(),
        gatewayReceiver.getHostnameForSenders(), gatewayReceiver.isManualStart());
  }

  public GatewayReceiverCreation(Cache cache, int startPort, int endPort, int timeBetPings,
      int buffSize, String bindAdd, List<GatewayTransportFilter> filters, String hostnameForSenders,
      boolean manualStart) {
    this.cache = cache;

    this.startPort = startPort;
    this.endPort = endPort;
    maxTimeBetweenPings = timeBetPings;
    socketBufferSize = buffSize;
    bindAddress = bindAdd;
    this.hostnameForSenders = hostnameForSenders;
    transFilter = filters;
    this.manualStart = manualStart;
  }

  @Override
  public List<GatewayTransportFilter> getGatewayTransportFilters() {
    return transFilter;
  }

  @Override
  public int getMaximumTimeBetweenPings() {
    return maxTimeBetweenPings;
  }

  @Override
  public int getPort() {
    return startPort;
  }

  public String getPortRange() {
    return portRange;
  }

  @Override
  public int getSocketBufferSize() {
    return socketBufferSize;
  }

  public void setMaximumTimeBetweenPings(int time) {
    maxTimeBetweenPings = time;
  }

  public void setStartPort(int port) {
    startPort = port;
  }

  public void setEndPort(int port) {
    endPort = port;
  }

  public void setSocketBufferSize(int socketBufferSize) {
    this.socketBufferSize = socketBufferSize;
  }

  @Override
  public String getHostnameForSenders() {
    return hostnameForSenders;
  }

  @Override
  public String getHost() {
    throw new IllegalStateException("getHost should not be invoked on GatewayReceiverCreation");
  }

  @Override
  public String getBindAddress() {
    return bindAddress;
  }

  public void setBindAddress(String address) {
    bindAddress = address;
  }

  @Override
  public void start() throws IOException {
    if (receiver == null) {
      // add a cache server and set its port to random port
      receiver = cache.addCacheServer();
      receiver.setPort(endPort + 1);
    }
  }

  @Override
  public void stop() {

  }

  @Override
  public void destroy() {

  }

  @Override
  public boolean isRunning() {
    return false;
  }

  public void addGatewayTransportFilter(GatewayTransportFilter filter) {
    transFilter.add(filter);
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.cache.wan.GatewayReceiver#getStartPort()
   */
  @Override
  public int getStartPort() {
    return startPort;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.cache.wan.GatewayReceiver#getEndPort()
   */
  @Override
  public int getEndPort() {
    return endPort;
  }

  @Override
  public boolean isManualStart() {
    return manualStart;
  }

  @Override
  public CacheServer getServer() {
    return null;
  }

}

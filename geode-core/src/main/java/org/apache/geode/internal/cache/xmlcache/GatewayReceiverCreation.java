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
import org.apache.geode.logging.internal.LogService;

public class GatewayReceiverCreation implements GatewayReceiver {
  private static final Logger logger = LogService.getLogger();

  private Cache cache;

  private String host;

  private int startPort;

  private int endPort;

  private String portRange;

  private List<GatewayTransportFilter> transFilter = new ArrayList<GatewayTransportFilter>();

  private int maxTimeBetweenPings;

  private int socketBufferSize;

  private String bindAddress;

  private String hostnameForSenders;

  private boolean manualStart;

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
    this.maxTimeBetweenPings = timeBetPings;
    this.socketBufferSize = buffSize;
    this.bindAddress = bindAdd;
    this.hostnameForSenders = hostnameForSenders;
    this.transFilter = filters;
    this.manualStart = manualStart;
  }

  @Override
  public List<GatewayTransportFilter> getGatewayTransportFilters() {
    return this.transFilter;
  }

  @Override
  public int getMaximumTimeBetweenPings() {
    return this.maxTimeBetweenPings;
  }

  @Override
  public int getPort() {
    return this.startPort;
  }

  public String getPortRange() {
    return this.portRange;
  }

  @Override
  public int getSocketBufferSize() {
    return this.socketBufferSize;
  }

  public void setMaximumTimeBetweenPings(int time) {
    this.maxTimeBetweenPings = time;
  }

  public void setStartPort(int port) {
    this.startPort = port;
  }

  public void setEndPort(int port) {
    this.endPort = port;
  }

  public void setSocketBufferSize(int socketBufferSize) {
    this.socketBufferSize = socketBufferSize;
  }

  @Override
  public String getHostnameForSenders() {
    return this.hostnameForSenders;
  }

  @Override
  public String getHost() {
    throw new IllegalStateException("getHost should not be invoked on GatewayReceiverCreation");
  }

  @Override
  public String getBindAddress() {
    return this.bindAddress;
  }

  public void setBindAddress(String address) {
    this.bindAddress = address;
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
    this.transFilter.add(filter);
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.cache.wan.GatewayReceiver#getStartPort()
   */
  @Override
  public int getStartPort() {
    return this.startPort;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.cache.wan.GatewayReceiver#getEndPort()
   */
  @Override
  public int getEndPort() {
    return this.endPort;
  }

  @Override
  public boolean isManualStart() {
    return this.manualStart;
  }

  @Override
  public CacheServer getServer() {
    return null;
  }

}

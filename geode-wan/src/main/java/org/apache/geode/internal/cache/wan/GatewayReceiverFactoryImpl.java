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

import static org.apache.geode.management.internal.cli.functions.GatewayReceiverCreateFunction.A_GATEWAY_RECEIVER_ALREADY_EXISTS_ON_THIS_MEMBER;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.geode.cache.wan.GatewayReceiver;
import org.apache.geode.cache.wan.GatewayReceiverFactory;
import org.apache.geode.cache.wan.GatewayTransportFilter;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.ResourceEvent;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.xmlcache.CacheCreation;
import org.apache.geode.internal.cache.xmlcache.GatewayReceiverCreation;

/**
 * @since GemFire 7.0
 */
public class GatewayReceiverFactoryImpl implements GatewayReceiverFactory {

  private int startPort = GatewayReceiver.DEFAULT_START_PORT;

  private int endPort = GatewayReceiver.DEFAULT_END_PORT;

  private int timeBetPings = GatewayReceiver.DEFAULT_MAXIMUM_TIME_BETWEEN_PINGS;

  private int socketBuffSize = GatewayReceiver.DEFAULT_SOCKET_BUFFER_SIZE;

  private String bindAdd = GatewayReceiver.DEFAULT_BIND_ADDRESS;

  private String hostnameForSenders = GatewayReceiver.DEFAULT_HOSTNAME_FOR_SENDERS;

  private boolean manualStart = GatewayReceiver.DEFAULT_MANUAL_START;

  private List<GatewayTransportFilter> filters = new ArrayList<GatewayTransportFilter>();

  private InternalCache cache;

  public GatewayReceiverFactoryImpl() {
    // nothing
  }

  public GatewayReceiverFactoryImpl(InternalCache cache) {
    this.cache = cache;
  }

  @Override
  public GatewayReceiverFactory addGatewayTransportFilter(GatewayTransportFilter filter) {
    this.filters.add(filter);
    return this;
  }

  @Override
  public GatewayReceiverFactory removeGatewayTransportFilter(GatewayTransportFilter filter) {
    this.filters.remove(filter);
    return this;
  }

  @Override
  public GatewayReceiverFactory setMaximumTimeBetweenPings(int time) {
    this.timeBetPings = time;
    return this;
  }

  @Override
  public GatewayReceiverFactory setStartPort(int port) {
    this.startPort = port;
    return this;
  }

  @Override
  public GatewayReceiverFactory setEndPort(int port) {
    this.endPort = port;
    return this;
  }

  @Override
  public GatewayReceiverFactory setSocketBufferSize(int size) {
    this.socketBuffSize = size;
    return this;
  }

  @Override
  public GatewayReceiverFactory setBindAddress(String address) {
    this.bindAdd = address;
    return this;
  }

  @Override
  public GatewayReceiverFactory setHostnameForSenders(String address) {
    this.hostnameForSenders = address;
    return this;
  }

  @Override
  public GatewayReceiverFactory setManualStart(boolean start) {
    this.manualStart = start;
    return this;
  }

  @Override
  public GatewayReceiver create() {
    if (this.startPort > this.endPort) {
      throw new IllegalStateException(
          "Please specify either start port a value which is less than end port.");
    }

    if ((this.cache.getGatewayReceivers() != null)
        && (!this.cache.getGatewayReceivers().isEmpty())) {
      throw new IllegalStateException(A_GATEWAY_RECEIVER_ALREADY_EXISTS_ON_THIS_MEMBER);
    }

    GatewayReceiver recv = null;
    if (this.cache instanceof GemFireCacheImpl) {
      recv = new GatewayReceiverImpl(this.cache, this.startPort, this.endPort, this.timeBetPings,
          this.socketBuffSize, this.bindAdd, this.filters, this.hostnameForSenders,
          this.manualStart);
      this.cache.addGatewayReceiver(recv);
      InternalDistributedSystem system =
          (InternalDistributedSystem) this.cache.getDistributedSystem();
      system.handleResourceEvent(ResourceEvent.GATEWAYRECEIVER_CREATE, recv);
      if (!this.manualStart) {
        try {
          recv.start();
        } catch (IOException ioe) {
          throw new GatewayReceiverException(
              "Exception occurred while starting gateway receiver",
              ioe);
        }
      }
    } else if (this.cache instanceof CacheCreation) {
      recv = new GatewayReceiverCreation(this.cache, this.startPort, this.endPort,
          this.timeBetPings, this.socketBuffSize, this.bindAdd, this.filters,
          this.hostnameForSenders, this.manualStart);
      this.cache.addGatewayReceiver(recv);
    }
    return recv;
  }

}

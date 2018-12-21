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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import org.apache.geode.cache.client.internal.locator.wan.LocatorMembershipListener;
import org.apache.geode.cache.wan.GatewayReceiver;
import org.apache.geode.cache.wan.GatewayReceiverFactory;
import org.apache.geode.cache.wan.GatewaySenderFactory;
import org.apache.geode.cache.wan.GatewayTransportFilter;
import org.apache.geode.distributed.internal.WanLocatorDiscoverer;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.wan.spi.WANFactory;

public class MyWANFactoryImpl implements WANFactory {

  public GatewayReceiverFactory myReceiverFactory;

  @Override
  public GatewaySenderFactory createGatewaySenderFactory(InternalCache cache) {
    return null;
  }

  @Override
  public GatewayReceiverFactory createGatewayReceiverFactory(InternalCache cache) {
    myReceiverFactory = spy(new MyGatewayReceiverFactoryImpl(cache));
    return myReceiverFactory;
  }

  @Override
  public WanLocatorDiscoverer createLocatorDiscoverer() {
    return null;
  }

  @Override
  public LocatorMembershipListener createLocatorMembershipListener() {
    return null;
  }

  @Override
  public void initialize() {

  }

  static class MyGatewayReceiverFactoryImpl implements GatewayReceiverFactory {
    InternalCache cache;
    int startPort;
    int endPort;
    int socketBuffSize;
    int timeBetPings;
    boolean manualStart;
    String bindAdd;
    String hostnameForSenders;

    public MyGatewayReceiverFactoryImpl(InternalCache cache) {
      this.cache = cache;
    }

    @Override
    public GatewayReceiverFactory setStartPort(int startPort) {
      this.startPort = startPort;
      return this;
    }

    @Override
    public GatewayReceiverFactory setEndPort(int endPort) {
      this.endPort = endPort;
      return this;
    }

    @Override
    public GatewayReceiverFactory setSocketBufferSize(int socketBufferSize) {
      this.socketBuffSize = socketBufferSize;
      return this;
    }

    @Override
    public GatewayReceiverFactory setBindAddress(String address) {
      this.bindAdd = address;
      return this;
    }

    @Override
    public GatewayReceiverFactory addGatewayTransportFilter(GatewayTransportFilter filter) {
      return null;
    }

    @Override
    public GatewayReceiverFactory removeGatewayTransportFilter(GatewayTransportFilter filter) {
      return null;
    }

    @Override
    public GatewayReceiverFactory setMaximumTimeBetweenPings(int time) {
      this.timeBetPings = time;
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
      GatewayReceiver receiver = mock(GatewayReceiver.class);
      when(receiver.isManualStart()).thenReturn(this.manualStart);
      when(receiver.getBindAddress()).thenReturn(this.bindAdd);
      when(receiver.getEndPort()).thenReturn(this.endPort);
      when(receiver.getStartPort()).thenReturn(this.startPort);
      when(receiver.getSocketBufferSize()).thenReturn(this.socketBuffSize);
      when(receiver.getHostnameForSenders()).thenReturn(this.hostnameForSenders);
      when(receiver.getMaximumTimeBetweenPings()).thenReturn(this.timeBetPings);
      this.cache.addGatewayReceiver(receiver);
      return receiver;
    }

    public boolean isManualStart() {
      return this.manualStart;
    }
  }
}

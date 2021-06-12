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
package org.apache.geode.cache.wan.internal.client.locator;

import org.apache.geode.cache.client.internal.locator.wan.LocatorJoinMessage;
import org.apache.geode.cache.client.internal.locator.wan.LocatorMembershipListener;
import org.apache.geode.cache.client.internal.locator.wan.RemoteLocatorJoinRequest;
import org.apache.geode.cache.client.internal.locator.wan.RemoteLocatorJoinResponse;
import org.apache.geode.cache.client.internal.locator.wan.RemoteLocatorPingRequest;
import org.apache.geode.cache.client.internal.locator.wan.RemoteLocatorPingResponse;
import org.apache.geode.cache.client.internal.locator.wan.RemoteLocatorRequest;
import org.apache.geode.cache.client.internal.locator.wan.RemoteLocatorResponse;
import org.apache.geode.cache.wan.GatewayReceiverFactory;
import org.apache.geode.cache.wan.GatewaySenderFactory;
import org.apache.geode.cache.wan.internal.GatewayReceiverFactoryImpl;
import org.apache.geode.cache.wan.internal.GatewaySenderFactoryImpl;
import org.apache.geode.distributed.internal.WanLocatorDiscoverer;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.wan.spi.WANFactory;
import org.apache.geode.internal.serialization.DataSerializableFixedID;

public class WANFactoryImpl implements WANFactory {

  @Override
  public void initialize() {
    InternalDataSerializer.getDSFIDSerializer().registerDSFID(
        DataSerializableFixedID.REMOTE_LOCATOR_JOIN_REQUEST,
        RemoteLocatorJoinRequest.class);
    InternalDataSerializer.getDSFIDSerializer().registerDSFID(
        DataSerializableFixedID.REMOTE_LOCATOR_JOIN_RESPONSE,
        RemoteLocatorJoinResponse.class);
    InternalDataSerializer.getDSFIDSerializer().registerDSFID(
        DataSerializableFixedID.REMOTE_LOCATOR_REQUEST,
        RemoteLocatorRequest.class);
    InternalDataSerializer.getDSFIDSerializer().registerDSFID(
        DataSerializableFixedID.LOCATOR_JOIN_MESSAGE,
        LocatorJoinMessage.class);
    InternalDataSerializer.getDSFIDSerializer().registerDSFID(
        DataSerializableFixedID.REMOTE_LOCATOR_PING_REQUEST,
        RemoteLocatorPingRequest.class);
    InternalDataSerializer.getDSFIDSerializer().registerDSFID(
        DataSerializableFixedID.REMOTE_LOCATOR_PING_RESPONSE,
        RemoteLocatorPingResponse.class);
    InternalDataSerializer.getDSFIDSerializer().registerDSFID(
        DataSerializableFixedID.REMOTE_LOCATOR_RESPONSE,
        RemoteLocatorResponse.class);
  }

  @Override
  public GatewaySenderFactory createGatewaySenderFactory(InternalCache cache) {
    return new GatewaySenderFactoryImpl(cache, cache.getStatisticsClock());
  }

  @Override
  public GatewayReceiverFactory createGatewayReceiverFactory(InternalCache cache) {
    return new GatewayReceiverFactoryImpl(cache);
  }

  @Override
  public WanLocatorDiscoverer createLocatorDiscoverer() {
    return new WanLocatorDiscovererImpl();
  }

  @Override
  public LocatorMembershipListener createLocatorMembershipListener() {
    return new LocatorMembershipListenerImpl();
  }
}

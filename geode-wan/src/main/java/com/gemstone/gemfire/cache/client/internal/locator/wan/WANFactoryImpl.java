/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.cache.client.internal.locator.wan;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.wan.GatewayReceiverFactory;
import com.gemstone.gemfire.cache.wan.GatewaySenderFactory;
import com.gemstone.gemfire.distributed.internal.WanLocatorDiscoverer;
import com.gemstone.gemfire.internal.DSFIDFactory;
import com.gemstone.gemfire.internal.DataSerializableFixedID;
import com.gemstone.gemfire.internal.cache.wan.GatewayReceiverFactoryImpl;
import com.gemstone.gemfire.internal.cache.wan.GatewaySenderFactoryImpl;
import com.gemstone.gemfire.internal.cache.wan.spi.WANFactory;

public class WANFactoryImpl implements WANFactory {
  
  @Override
  public void initialize() {
    DSFIDFactory.registerDSFID(
        DataSerializableFixedID.REMOTE_LOCATOR_JOIN_REQUEST,
        RemoteLocatorJoinRequest.class);
    DSFIDFactory.registerDSFID(
        DataSerializableFixedID.REMOTE_LOCATOR_JOIN_RESPONSE,
        RemoteLocatorJoinResponse.class);
    DSFIDFactory.registerDSFID(DataSerializableFixedID.REMOTE_LOCATOR_REQUEST,
        RemoteLocatorRequest.class);
    DSFIDFactory.registerDSFID(DataSerializableFixedID.LOCATOR_JOIN_MESSAGE,
        LocatorJoinMessage.class);
    DSFIDFactory.registerDSFID(
        DataSerializableFixedID.REMOTE_LOCATOR_PING_REQUEST,
        RemoteLocatorPingRequest.class);
    DSFIDFactory.registerDSFID(
        DataSerializableFixedID.REMOTE_LOCATOR_PING_RESPONSE,
        RemoteLocatorPingResponse.class);
    DSFIDFactory.registerDSFID(DataSerializableFixedID.REMOTE_LOCATOR_RESPONSE,
        RemoteLocatorResponse.class);
  }

  @Override
  public GatewaySenderFactory createGatewaySenderFactory(Cache cache) {
    return new GatewaySenderFactoryImpl(cache);
  }

  @Override
  public GatewayReceiverFactory createGatewayReceiverFactory(Cache cache) {
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

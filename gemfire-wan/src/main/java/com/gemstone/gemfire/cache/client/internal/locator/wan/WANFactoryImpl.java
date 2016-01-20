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

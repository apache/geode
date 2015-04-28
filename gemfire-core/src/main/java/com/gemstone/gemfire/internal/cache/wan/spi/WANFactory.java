package com.gemstone.gemfire.internal.cache.wan.spi;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.client.internal.locator.wan.LocatorMembershipListener;
import com.gemstone.gemfire.cache.wan.GatewayReceiverFactory;
import com.gemstone.gemfire.cache.wan.GatewaySenderFactory;
import com.gemstone.gemfire.distributed.internal.WanLocatorDiscoverer;

public interface WANFactory {

  GatewaySenderFactory createGatewaySenderFactory(Cache cache);

  GatewayReceiverFactory createGatewayReceiverFactory(Cache cache);

  WanLocatorDiscoverer createLocatorDiscoverer();

  LocatorMembershipListener createLocatorMembershipListener();

  void initialize();

}

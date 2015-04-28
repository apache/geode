package com.gemstone.gemfire.internal.cache.wan;

import java.util.Iterator;
import java.util.ServiceLoader;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.client.internal.locator.wan.LocatorMembershipListener;
import com.gemstone.gemfire.cache.wan.GatewayReceiverFactory;
import com.gemstone.gemfire.cache.wan.GatewaySenderFactory;
import com.gemstone.gemfire.distributed.internal.WanLocatorDiscoverer;
import com.gemstone.gemfire.internal.cache.wan.spi.WANFactory;

public class WANServiceProvider {
  private static final WANFactory factory;
  
  static {
    ServiceLoader<WANFactory> loader = ServiceLoader.load(WANFactory.class);
    Iterator<WANFactory> itr = loader.iterator();
    if(!itr.hasNext()) {
      factory = null;
    } else {
      factory = itr.next();
      factory.initialize();
    }
  }
  
  public static GatewaySenderFactory createGatewaySenderFactory(Cache cache) {
    if(factory == null) {
      throw new IllegalStateException("WAN service is not available.");
    }
    return factory.createGatewaySenderFactory(cache);
    
  }

  public static GatewayReceiverFactory createGatewayReceiverFactory(
      Cache cache) {
    if(factory == null) {
      throw new IllegalStateException("WAN service is not available.");
    }
    return factory.createGatewayReceiverFactory(cache);
  }

  public static WanLocatorDiscoverer createLocatorDiscoverer() {
    if(factory == null) {
      return null;
    }
    return factory.createLocatorDiscoverer();
  }

  public static LocatorMembershipListener createLocatorMembershipListener() {
    if(factory == null) {
      return null;
    }
    return factory.createLocatorMembershipListener();
  }
  
  private WANServiceProvider() {
    
  }
}

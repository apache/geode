package com.gemstone.gemfire.modules.gatewaydelta;

import com.gemstone.gemfire.cache.Cache;

public interface GatewayDeltaEvent {
  
  public void apply(Cache cache);
}

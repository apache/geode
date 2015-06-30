package com.gemstone.gemfire.modules.gatewaydelta;

public interface GatewayDelta {

  public static final String GATEWAY_DELTA_REGION_NAME = "__gatewayDelta";
  
  public GatewayDeltaEvent getCurrentGatewayDeltaEvent();
 
  public void setCurrentGatewayDeltaEvent(GatewayDeltaEvent currentGatewayDeltaEvent);
}

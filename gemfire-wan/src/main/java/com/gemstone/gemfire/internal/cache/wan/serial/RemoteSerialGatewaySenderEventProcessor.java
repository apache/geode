package com.gemstone.gemfire.internal.cache.wan.serial;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.internal.cache.wan.GatewaySenderEventRemoteDispatcher;
import com.gemstone.gemfire.internal.cache.wan.GatewaySenderEventCallbackDispatcher;
import com.gemstone.gemfire.cache.wan.GatewaySender;
import com.gemstone.gemfire.internal.cache.wan.AbstractGatewaySender;
import com.gemstone.gemfire.internal.logging.LogService;

public class RemoteSerialGatewaySenderEventProcessor extends
    SerialGatewaySenderEventProcessor {

  private static final Logger logger = LogService.getLogger();
  public RemoteSerialGatewaySenderEventProcessor(AbstractGatewaySender sender,
      String id) {
    super(sender, id);
  }

  public void initializeEventDispatcher() {
    if (logger.isDebugEnabled()) {
      logger.debug(" Creating the GatewayEventRemoteDispatcher");
    }
    // In case of serial there is a way to create gatewaysender and attach
    // asynceventlistener. Not sure of the use-case but there are dunit tests
    // To make them passuncommenting the below condition
    if (this.sender.getRemoteDSId() != GatewaySender.DEFAULT_DISTRIBUTED_SYSTEM_ID) {
      this.dispatcher = new GatewaySenderEventRemoteDispatcher(this);
    }else{
      this.dispatcher = new GatewaySenderEventCallbackDispatcher(this);
    }
  }
  
}

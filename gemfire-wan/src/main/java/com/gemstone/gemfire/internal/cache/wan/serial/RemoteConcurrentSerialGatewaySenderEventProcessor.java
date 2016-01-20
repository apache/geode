package com.gemstone.gemfire.internal.cache.wan.serial;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.internal.cache.wan.AbstractGatewaySender;
import com.gemstone.gemfire.internal.logging.LogService;

public class RemoteConcurrentSerialGatewaySenderEventProcessor extends
    ConcurrentSerialGatewaySenderEventProcessor {

  private static final Logger logger = LogService.getLogger();
  
  public RemoteConcurrentSerialGatewaySenderEventProcessor(
      AbstractGatewaySender sender) {
    super(sender);
  }

  @Override
  protected void initializeMessageQueue(String id) {
    for (int i = 0; i < sender.getDispatcherThreads(); i++) {
      processors.add(new RemoteSerialGatewaySenderEventProcessor(this.sender, id
          + "." + i));
      if (logger.isDebugEnabled()) {
        logger.debug("Created the RemoteSerialGatewayEventProcessor_{}->{}", i, processors.get(i));
      }
    }
  }
  
}

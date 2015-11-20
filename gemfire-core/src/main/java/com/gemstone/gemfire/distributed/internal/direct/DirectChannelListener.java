package com.gemstone.gemfire.distributed.internal.direct;

import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.DistributionMessage;

public interface DirectChannelListener {

  /**
   * Event indicating a message has been delivered that we need to process.
   * 
   * @param o the message that should be processed.
   */
  public void messageReceived(DistributionMessage o);
  

  /**
   * Return the distribution manager for this receiver
   * @return the distribution manager
   */
  public DistributionManager getDM();
  
}

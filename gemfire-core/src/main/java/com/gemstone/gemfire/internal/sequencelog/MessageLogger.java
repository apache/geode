/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.sequencelog;

import com.gemstone.gemfire.distributed.internal.DistributionMessage;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;

/**
 * @author dsmith
 *
 */
public class MessageLogger {
  
  public static final SequenceLogger LOGGER = SequenceLoggerImpl.getInstance();
  
  public static boolean isEnabled() {
    return LOGGER.isEnabled(GraphType.MESSAGE);
  }
  
  public static void logMessage(DistributionMessage message, InternalDistributedMember source, InternalDistributedMember dest) {
    if(isEnabled()) {
      LOGGER.logTransition(GraphType.MESSAGE, message.getClass().getSimpleName(), "", "received", source, dest);
    }
  }

}

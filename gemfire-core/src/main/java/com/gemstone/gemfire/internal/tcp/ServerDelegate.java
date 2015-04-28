/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.tcp;

import com.gemstone.gemfire.distributed.internal.DistributionMessage;
import com.gemstone.gemfire.distributed.internal.membership.*;
import com.gemstone.gemfire.i18n.LogWriterI18n;


/** <p>ServerDelegate is a conduit plugin that receives
    {@link com.gemstone.gemfire.distributed.internal.DistributionMessage}
    objects received from other conduits.</p>

    @see com.gemstone.gemfire.distributed.internal.direct.DirectChannel

    @author Bruce Schuchardt
    @since 2.0
   
  */
public interface ServerDelegate {

  public void receive( DistributionMessage message, int bytesRead,
                       Stub connId );

  public LogWriterI18n getLogger();

  /**
   * Called when a possibly new member is detected by receiving a direct channel
   * message from him.
   */
  public void newMemberConnected(InternalDistributedMember member, Stub id);
}

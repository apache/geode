/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.distributed.internal;

import com.gemstone.gemfire.distributed.internal.membership.*;

/**
 * A marker interface that denotes {@link DistributionMessage}s that
 * require a reply.  Messages that do not implement this interface can
 * be sent asynchronously through the transport layer.
 *
 * @author David Whitlock
 *
 * @since 3.0
 */
public interface MessageWithReply {

  /**
   * Returns the id of the {@link 
   * com.gemstone.gemfire.distributed.internal.ReplyProcessor21} that is used to
   * handle the reply to this message.
   */
  public int getProcessorId();

  /**
   * Returns the id the sender who is waiting for a reply.
   */
  public InternalDistributedMember getSender();
}

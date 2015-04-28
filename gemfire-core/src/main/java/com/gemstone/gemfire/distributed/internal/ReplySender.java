/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.distributed.internal;

import java.util.Set;

import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.cache.DirectReplyMessage;

/**
 * This interface is used by direct ack messages to send a reply
 * to the original sender of the message. Any message which implements
 * {@link DirectReplyMessage} must reply by calling putOutgoing on the
 * ReplySender returned by {@link DistributionMessage#getReplySender(DM)}
 * 
 * The reply sender may be the distribution manager itself, or it may send
 * the reply directly back on the same socket the message as received on.
 * @author dsmith
 *
 */
public interface ReplySender {
  
  public Set putOutgoing(DistributionMessage msg);

}

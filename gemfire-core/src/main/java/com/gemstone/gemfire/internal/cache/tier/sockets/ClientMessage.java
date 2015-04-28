/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.cache.tier.sockets;

import com.gemstone.gemfire.internal.DataSerializableFixedID;
import com.gemstone.gemfire.internal.cache.Conflatable;

import java.io.IOException;

/**
 * Interface <code>ClientMessage</code> is a message representing a cache
 * operation that is sent from a server to an interested client. 
 *
 * @author Barry Oglesby
 *
 * @since 5.5
 */
public interface ClientMessage extends Conflatable, DataSerializableFixedID {

  /**
   * Returns a <code>Message</code> generated from the fields of this
   * <code>ClientMessage</code>.
   *
   * @param proxy the proxy that is dispatching this message
   * @return a <code>Message</code> generated from the fields of this
   *         <code>ClientUpdateMessage</code>
   * @throws IOException
   * @see com.gemstone.gemfire.internal.cache.tier.sockets.Message
   */
  public Message getMessage(CacheClientProxy proxy, boolean notify) throws IOException;
}

/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.cache.tier.sockets;

/**
 * Statistics supported by cache/server Message.
 * @since 5.0.2
 */
public interface MessageStats {
  public void incReceivedBytes(long v);
  public void incSentBytes(long v);
  public void incMessagesBeingReceived(int bytes);
  public void decMessagesBeingReceived(int bytes);
}

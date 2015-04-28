/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/**
 * 
 */
package com.gemstone.gemfire.internal.cache.tier;

import com.gemstone.gemfire.internal.cache.tier.sockets.*;

/**
 * @author ashahid
 * 
 */
public interface Command {
  public void execute(Message msg, ServerConnection servConn);

  public final int RESPONDED = 1;

  public final int REQUIRES_RESPONSE = 2;

  public final int REQUIRES_CHUNKED_RESPONSE = 3;
}

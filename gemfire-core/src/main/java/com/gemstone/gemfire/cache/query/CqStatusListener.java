/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.cache.query;


/**
 * Extension of CqListener. Adds two new methods to CqListener, one that
 *  is called when the cq is connected and one that is called when
 *  the cq is disconnected
 * 
 *
 * @author jhuynh 
 * @since 7.0
 */

public interface CqStatusListener extends CqListener {

  /**
   * Called when the cq loses connection with all servers
   */  
  public void onCqDisconnected();
 
  /**
   * Called when the cq establishes a connection with a server
   */
  public void onCqConnected();
}

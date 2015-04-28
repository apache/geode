/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.cache.query;

/**
 * This interface gives information on the state of a CqQuery. 
 * It is provided by the getState method of the CqQuery instance. 
 * 
 * @author anil
 * @since 5.5
 */

public interface CqState {
          
  /**
   * Returns the state in string form.
   */
  public String toString();
  
  /**
   * Returns true if the CQ is in Running state.
   */
  public boolean isRunning();

  /**
   * Returns true if the CQ is in Stopped state.
   */
  public boolean isStopped();
  
  /**
   * Returns true if the CQ is in Closed state.
   */
  public boolean isClosed();
  
  /**
   * Returns true if the CQ is in Closing state.
   */
  public boolean isClosing();
  
}

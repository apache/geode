/*=========================================================================
 * Copyright Copyright (c) 2000-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 * $Id: DefaultQueryService.java,v 1.2 2005/02/01 17:19:20 vaibhav Exp $
 *=========================================================================
 */

package com.gemstone.gemfire.cache.query.internal;

import com.gemstone.gemfire.cache.query.CqState;

/**
 * Offers methods to get CQ state.
 *
 * @author anil
 * @since 5.5
 */

public class CqStateImpl implements CqState {

  public static final int STOPPED = 0;

  public static final int RUNNING = 1;

  public static final int CLOSED = 2;
  
  public static final int CLOSING = 3;
 
  public static final int INIT = 4;
 
  private volatile int state = INIT;
  
  
  /**
   * Returns true if the CQ is in Running state.
   */
  public boolean isRunning(){
    return (this.state == RUNNING);
  }

  /**
   * Returns true if the CQ is in Stopped or Initializing state.
   */
  public boolean isStopped() {
    return (this.state == STOPPED || this.state == INIT);
  }
  
  /**
   * Returns true if the CQ is in Closed state.
   */
  public boolean isClosed() {
    return (this.state == CLOSED);    
  }
  
  /**
   * Returns true if the CQ is in the Closing state.
   */
  public boolean isClosing() {
    return (this.state == CLOSING);
  }
  
  /**
   * Sets the state of CQ.
   * @param state
   */
  public void setState(int state){
    this.state = state;
  }

  /**
   * Returns the integer state of CQ.
   */
  public int getState() {
    return this.state;
  }
  
  /**
   * Returns the state in string form.
   */
  @Override
  public String toString() {
    switch (this.state){    
      case STOPPED:
        return "STOPPED";
      case RUNNING:
        return "RUNNING";
      case CLOSED:
        return "CLOSED";
      default:
        return "UNKNOWN";
    }
  }

}

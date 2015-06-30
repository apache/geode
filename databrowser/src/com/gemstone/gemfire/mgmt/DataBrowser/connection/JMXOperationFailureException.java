/*=========================================================================
 * (c) Copyright 2002-2009, GemStone Systems, Inc. All Rights Reserved.
 * 1260 NW Waterhouse Ave., Suite 200,  Beaverton, OR 97006
 * All Rights Reserved.
 *=========================================================================*/
package com.gemstone.gemfire.mgmt.DataBrowser.connection;

/**
 * This is a base exception class used by the JMXCallExecutor.
 * @see com.gemstone.gemfire.tools.jmx.JMXCallExecutor
 * 
 * @author Hrishi
 **/
public class JMXOperationFailureException extends Exception {

 
  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  /**
   * The constructor.
   * @param message - the detail message.
   * @param cause - the cause. 
   */
  public JMXOperationFailureException(String arg0, Throwable arg1) {
    super(arg0, arg1);
  }

  /**
   * The constructor.
   * @param message - the detail message.
   */
  public JMXOperationFailureException(String message) {
    super(message);
  }

  /**
   * The constructor.
   * @param cause - the cause.
   */
  public JMXOperationFailureException(Throwable arg0) {
    super(arg0);
  }
  
}

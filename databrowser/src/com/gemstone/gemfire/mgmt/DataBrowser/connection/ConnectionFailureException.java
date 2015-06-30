/*=========================================================================
 * (c)Copyright 2002-2011, GemStone Systems, Inc. All Rights Reserved.
 * 1260 NW Waterhouse Ave., Suite 200, Beaverton, OR 97006
 * All Rights Reserved.
 * =======================================================================*/
package com.gemstone.gemfire.mgmt.DataBrowser.connection;

public class ConnectionFailureException extends Exception {
  private static final long serialVersionUID = -7476581939729817468L;

  protected boolean retryAllowed;

  public ConnectionFailureException() {
    super();
  }

  public ConnectionFailureException(String arg0, Throwable arg1, boolean retryAllowed) {
    super(arg0, arg1);
    this.retryAllowed = retryAllowed;
  }

  public ConnectionFailureException(String arg0, boolean retryAllowed) {
    super(arg0);
    this.retryAllowed = retryAllowed;
  }

  public ConnectionFailureException(Throwable arg0, boolean retryAllowed) {
    super(arg0);
    this.retryAllowed = retryAllowed;
  }
  
  public ConnectionFailureException(String arg0, Throwable arg1) {
    this(arg0, arg1, false);
  }

  public ConnectionFailureException(String arg0) {
    this(arg0, false);
  }

  public ConnectionFailureException(Throwable arg0) {
    this(arg0, false);
  }

  /**
   * @return the retryAllowed
   */
  public boolean isRetryAllowed() {
    return retryAllowed;
  }
  
  @Override
  public String toString() {
    return super.toString()+", isRetryAllowed : "+retryAllowed;
  }
  
}

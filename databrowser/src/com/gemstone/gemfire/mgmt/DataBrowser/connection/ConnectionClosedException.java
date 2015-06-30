/*========================================================================= 
 * (c)Copyright 2002-2009, GemStone Systems, Inc. All Rights Reserved.
 * 1260 NW Waterhouse Ave., Suite 200, Beaverton, OR 97006 
 * All Rights Reserved.
 * =======================================================================*/
package com.gemstone.gemfire.mgmt.DataBrowser.connection;

public class ConnectionClosedException extends Exception {

  public ConnectionClosedException() {
    super();
  }

  public ConnectionClosedException(String message, Throwable cause) {
    super(message, cause);
  }

  public ConnectionClosedException(String message) {
    super(message);
  }

  public ConnectionClosedException(Throwable cause) {
    super(cause);
  }
}

/*========================================================================= 
 * (c)Copyright 2002-2009, GemStone Systems, Inc. All Rights Reserved.
 * 1260 NW Waterhouse Ave., Suite 200, Beaverton, OR 97006 
 * All Rights Reserved.
 * =======================================================================*/
package com.gemstone.gemfire.mgmt.DataBrowser.connection;

/**
 * @author hgadre
 *
 */
public class VersionMismatchException extends ConnectionFailureException {

  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  public VersionMismatchException() {
    super();
  }

  public VersionMismatchException(String arg0, Throwable arg1) {
    super(arg0, arg1);
  }

  public VersionMismatchException(String arg0) {
    super(arg0);
  }

  public VersionMismatchException(Throwable arg0) {
    super(arg0);
  }

}

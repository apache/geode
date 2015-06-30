/*========================================================================= 
 * (c)Copyright 2002-2009, GemStone Systems, Inc. All Rights Reserved.
 * 1260 NW Waterhouse Ave., Suite 200, Beaverton, OR 97006 
 * All Rights Reserved.
 * =======================================================================*/
package com.gemstone.gemfire.mgmt.DataBrowser.query.cq;

import com.gemstone.gemfire.mgmt.DataBrowser.query.QueryExecutionException;

public class CQException extends QueryExecutionException {

  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  public CQException(String arg0, Throwable arg1) {
    super(arg0, arg1);
  }

  public CQException(String arg0) {
    super(arg0);
  }

  public CQException(Throwable arg0) {
    super(arg0);
  }
}

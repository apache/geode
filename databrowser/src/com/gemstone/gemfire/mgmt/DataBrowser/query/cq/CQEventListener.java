/*========================================================================= 
 * (c)Copyright 2002-2009, GemStone Systems, Inc. All Rights Reserved.
 * 1260 NW Waterhouse Ave., Suite 200, Beaverton, OR 97006 
 * All Rights Reserved.
 * =======================================================================*/
package com.gemstone.gemfire.mgmt.DataBrowser.query.cq;

import com.gemstone.gemfire.mgmt.DataBrowser.query.cq.event.ICQEvent;

public interface CQEventListener {

  /**
   * This method is invoked when an event is occurred on the region
   * that satisfied the query condition of this CQ or an exception scenario.
   */
  public void onEvent(ICQEvent aCqEvent);
  
  /**
   * This method is invoked when a given CQ is stopped.
   */
  public void close();
  

}

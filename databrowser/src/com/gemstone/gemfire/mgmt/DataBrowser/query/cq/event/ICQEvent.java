/*========================================================================= 
 * (c)Copyright 2002-2009, GemStone Systems, Inc. All Rights Reserved.
 * 1260 NW Waterhouse Ave., Suite 200, Beaverton, OR 97006 
 * All Rights Reserved.
 * =======================================================================*/
package com.gemstone.gemfire.mgmt.DataBrowser.query.cq.event;

import com.gemstone.gemfire.mgmt.DataBrowser.query.cq.EventData;


public interface ICQEvent {
  
  public EventData getEventData(); 
  
  public Throwable getThrowable();

}

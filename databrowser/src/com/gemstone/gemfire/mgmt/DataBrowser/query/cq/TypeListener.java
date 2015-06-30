/*========================================================================= 
 * (c)Copyright 2002-2009, GemStone Systems, Inc. All Rights Reserved.
 * 1260 NW Waterhouse Ave., Suite 200, Beaverton, OR 97006 
 * All Rights Reserved.
 * =======================================================================*/
package com.gemstone.gemfire.mgmt.DataBrowser.query.cq;

import com.gemstone.gemfire.mgmt.DataBrowser.query.IntrospectionResult;

public interface TypeListener {
  
  /**
   * This method is invoked when a new type is introduced as part of 
   * the execution of the CQ. This method is primarily useful for the 
   * user-interface components, preparing them to lay out data of this
   * type.
   */
  public void onNewTypeAdded(IntrospectionResult result);

}

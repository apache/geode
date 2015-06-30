/*=========================================================================
 * (c) Copyright 2002-2009, GemStone Systems, Inc. All Rights Reserved.
 * 1260 NW Waterhouse Ave., Suite 200,  Beaverton, OR 97006
 * All Rights Reserved.
 *=========================================================================*/
package com.gemstone.gemfire.mgmt.DataBrowser.connection;

import com.gemstone.gemfire.mgmt.DataBrowser.model.IMemberEvent;

/**
 * This interface provides call back functionality for listening GemFire member 
 * status changes.
 * 
 * @author Hrishi
 **/
public interface GemFireMemberListener {

  /**
   * This method is called when a member joins a given GemFire system.
   * 
   * @param memEvent instance of {@link IMemberEvent}.
   */
  public void memberEventReceived(IMemberEvent memEvent);

}

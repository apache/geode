/*========================================================================= 
 * (c)Copyright 2002-2009, GemStone Systems, Inc. All Rights Reserved.
 * 1260 NW Waterhouse Ave., Suite 200, Beaverton, OR 97006 
 * All Rights Reserved.
 * =======================================================================*/
package com.gemstone.gemfire.mgmt.DataBrowser.connection;


/**
 * This interface abstracts the GemFire connection related functionality including,
 * 1. Ability to retrieve the information about GemFire members.
 * 2. Ability to execute query on a specific GemFire member.
 * 3. Ability to attach/detach call-back listeners for GemFire member/region related updates.
 * 
 * @author Hrishi
 **/
public interface GemFireConnection extends GFMemberDiscovery, GFQueryExecution {
  
  /**
   * This method closes the under-laying GemFire connection.
   */
  public void close();

}

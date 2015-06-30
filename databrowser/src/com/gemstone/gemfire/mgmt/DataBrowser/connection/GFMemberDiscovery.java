/*========================================================================= 
 * (c)Copyright 2002-2009, GemStone Systems, Inc. All Rights Reserved.
 * 1260 NW Waterhouse Ave., Suite 200, Beaverton, OR 97006 
 * All Rights Reserved.
 * =======================================================================*/
package com.gemstone.gemfire.mgmt.DataBrowser.connection;

import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.mgmt.DataBrowser.model.member.GemFireMember;

/**
 * @author Hrishi
 *
 */
public interface GFMemberDiscovery {
  
  /**
   * This method returns the list of members connected to a given GemFire system.
   * @return List of GemFire members.
   */
  public GemFireMember[] getMembers();
  
  /**
   * This method returns the information about a specified member.
   *  
   * @param id The unique identifier of the member for which we need information.
   * @return Member information.
   */
  public GemFireMember getMember(String id);
  
  /**
   * This method returns a QueryService instance corrosponding to a given GemFire member.
   * 
   * @param member The GemFire member for which we want QueryService instance.
   * @return Instance of QueryService.
   */
  public QueryService getQueryService(GemFireMember member);  
  
  /**
   * This method adds a call-back listener for the GemFire member updates.
   * @param listener The listener to be attached.
   */
  public void addGemFireMemberListener(GemFireMemberListener listener);
  
  /**
   * This method removes a call-back listener for the GemFire member updates.
   * @param listener The listener to be removed.
   */
  public void removeGemFireMemberListener(GemFireMemberListener listener);
  
  public void addConnectionNotificationListener(GemFireConnectionListener listener);
  
  public void removeConnectionNotificationListener(GemFireConnectionListener listener);
  
  /**
   * This method closes the under-laying GemFire connection.
   */
  public void close();
  
  public String getGemFireSystemVersion();
  
  public void setRefreshInterval(long time);
  
  public long getRefreshInterval();
}

package com.gemstone.gemfire.mgmt.DataBrowser.controller;

import com.gemstone.gemfire.mgmt.DataBrowser.connection.GemFireConnection;
import com.gemstone.gemfire.mgmt.DataBrowser.model.member.GemFireMember;
import com.gemstone.gemfire.mgmt.DataBrowser.model.region.GemFireRegion;

/**
 * Respesents the snap shot of DS at any time
 * 
 * @author mjha
 * 
 */
public class DSSnapShot {

  private GemFireConnection connection;

  public DSSnapShot(GemFireConnection cnxn) {
    this.connection = cnxn;
  }

  /**
   * This method returns the list of members connected to a given GemFire
   * system.
   * 
   * @return List of GemFire members.
   */
  public GemFireMember[] getMembers() {
    return connection.getMembers();
  }

  /**
   * This method returns the information about a specified member.
   * 
   * @param id
   *          The unique identifier of the member for which we need information.
   * @return Member information.
   */
  public GemFireMember getMember(String id) {
    return connection.getMember(id);
  }

  /**
   * This method returns a list of all regions defined on this GemFire member.
   * 
   * @return list of all defined regions.
   */
  public GemFireRegion[] getAllRegions(GemFireMember member) {
    return member.getAllRegions();
  }
  
}

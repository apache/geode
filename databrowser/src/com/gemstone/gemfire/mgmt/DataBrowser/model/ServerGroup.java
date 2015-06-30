/**
 * 
 */
package com.gemstone.gemfire.mgmt.DataBrowser.model;

import java.util.Collection;

import com.gemstone.gemfire.mgmt.DataBrowser.model.member.GemFireMember;

/**
 * @author mghosh
 *
 */
public class ServerGroup {

  private String        name_ = null;
  
  private Collection< GemFireMember >     dsMembers_ = null; 
  
  public static class Attributes {
  }

  /**
   * 
   */
  public ServerGroup() {
    // TODO Auto-generated constructor stub
  }

  /**
   * @param nm_ the name_ to set
   */
  public final void setName(String nm_) {
    this.name_ = nm_;
  }

  /**
   * @return the name_
   */
  public final String getName() {
    return this.name_;
  }

  /**
   * @return the dsMembers_
   */
  public final Collection<GemFireMember> getDsMembers_() {
    return this.dsMembers_;
  }

  /**
   * @param dsMmbrs_ the dsMembers_ to set
   */
  public final void setDsMembers(Collection<GemFireMember> dsMmbrs_) {
    this.dsMembers_ = dsMmbrs_;
  }

  
}

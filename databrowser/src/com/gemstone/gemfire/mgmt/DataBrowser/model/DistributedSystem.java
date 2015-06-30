/*=========================================================================
 * (c)Copyright 2002-2009, GemStone Systems, Inc. All Rights Reserved.
 * 1260 NW Waterhouse Ave., Suite 200, Beaverton, OR 97006
 * All Rights Reserved.
 * =======================================================================*/
package com.gemstone.gemfire.mgmt.DataBrowser.model;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.gemstone.gemfire.mgmt.DataBrowser.model.member.GemFireMember;

/**
 * This class represents the DistributedSystem model component.
 *
 * @author mghosh
 **/
public class DistributedSystem {
  public static final String DATA_BROWSER_NAME = "Data-Browser";

  private String       name_        = null;
  private boolean      isConnected_ = false;
  private Map<String, GemFireMember> members;

  public DistributedSystem(String name) {
    this.name_ = name;
    this.members = Collections.synchronizedMap(new HashMap<String, GemFireMember>());
  }

  public void disconnect() {
//    System.out.println("Disconnecting from DS " + this.toString());
  }

  /**
   * @return the name_
   */
  public final String getName() {
    return this.name_;
  }

  /**
   * @return the members_
   */
  public final Collection<GemFireMember> getMembers() {
    synchronized (members) {
      return Collections.unmodifiableCollection(this.members.values());
    }
  }

  /**
   * @param mbrs
   *          the members_ to set
   */
  public final void setMembers(Collection<GemFireMember> mbrs) {
    //this.members_ = members;
  }

  /**
   * @return the fIsConnected_
   */
  public final boolean isConnected() {
    return this.isConnected_;
  }

  /**
   * @param isConnected_
   *          the fIsConnected_ to set
   */
  public final void setConnected(boolean isConnected) {
    this.isConnected_ = isConnected;
  }

  public GemFireMember getMember(String id) {
    return this.members.get(id);
  }

  public final GemFireMember removeMember(String id) {
    GemFireMember member = null;
    member = this.members.remove(id);
    return member;
  }

  public final boolean containsMember(String id) {
    return this.members.containsKey(id);
  }

  public final boolean addMember(GemFireMember member) {
   if(!DATA_BROWSER_NAME.equalsIgnoreCase(member.getName())) {
    this.members.put(member.getId(), member);
    return true;
   }
   return false;
  }
}

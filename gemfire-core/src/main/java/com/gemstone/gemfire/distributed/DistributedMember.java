/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.distributed;

import java.util.List;
import java.util.Set;

/**
 * This is the fundamental representation of a member in a GemFire distributed
 * system. A process becomes a member by calling {@link 
 * DistributedSystem#connect}.
 * 
 * @author Kirk Lund
 * @since 5.0
 */
public interface DistributedMember extends Comparable<DistributedMember> {

  /**
   * Returns this member's name. The member name is set using
   * the "name" gemfire property. Returns "" if the member
   * does not have a name.
   * @since 7.0
   */
  public String getName();
  
  /**
   * Returns the canonical name of the host machine for this member.
   */
  public String getHost();
  
  /**
   * Returns the Roles that this member performs in the system.
   * Note that the result will contain both groups and roles.
   */
  public Set<Role> getRoles();
  
  /**
   * Returns the groups this member belongs to.
   * A member defines the groups it is in using the "groups"
   * gemfire property.
   * Note that the deprecated "roles" gemfire property
   * are also treated as groups so this result will contain
   * both groups and roles.
   * @return a list of groups that this member belongs to.
   */
  public List<String> getGroups();
 
  /**
   * Returns the process id for this member. This may return zero if the
   * platform or configuration does not allow native access to process info. 
   */
  public int getProcessId();
  
  /**
   * Returns a unique identifier for this member.
   */
  public String getId();
  
  /**
   * Returns the durable attributes for this client.
   */
  public DurableClientAttributes getDurableClientAttributes();

}



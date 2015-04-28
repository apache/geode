/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.distributed;

/**
 * Members of the distributed system can fill one or more user defined roles.
 * A role is metadata that describes how the member relates to other members
 * or what purpose it fills. 
 *
 * <a href="DistributedSystem.html#roles">Roles are specified</a> when 
 * connecting to the {@link DistributedSystem}.
 *
 * @author Kirk Lund
 * @since 5.0
 */
public interface Role extends Comparable<Role> {
  
  /** 
   * Returns the name of this role. 
   *
   * @return user-defined string name of this role
   */
  public String getName();
  
  /** 
   * Returns true if this role is currently present in distributed system.
   * If true, then at least one member in the system is configured with this
   * role, regardless of whether or not that member has a cache.
   *
   * @return true if this role is currently present in distributed system
   */
  public boolean isPresent();
  
  /** 
   * Returns the count of members currently filling this role. These members
   * may or may not have a cache.
   *
   * @return number of members in system currently filling this role
   */
  public int getCount();
}


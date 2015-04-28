/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
package com.gemstone.gemfire.admin;

import com.gemstone.gemfire.distributed.DistributedMember;

import java.net.InetAddress;

/**
 * Administrative interface for monitoring a GemFire system member.
 *
 * @author    Kirk Lund
 * @since     3.5
 *
 * @deprecated as of 7.0 use the {@link com.gemstone.gemfire.management} package instead
 */
public interface SystemMember {
  
  /** Gets the {@link AdminDistributedSystem} this member belongs to. */
  public AdminDistributedSystem getDistributedSystem();
  
  /** 
   * Gets identifying name of this member.
   * For applications this is the string form of {@link #getDistributedMember}.
   * For cache servers it is a unique cache server string.
   */
  public String getId();
  
  /** 
   * Retrieves display friendly name for this member.  If this member defined 
   * an optional name for its connection to the distributed system, that name 
   * will be returned.  Otherwise the returned value will be {@link
   * com.gemstone.gemfire.admin.SystemMember#getId}.
   *
   * @see com.gemstone.gemfire.distributed.DistributedSystem#connect
   * @see com.gemstone.gemfire.distributed.DistributedSystem#getName
   */
  public String getName();
  
  /** Gets the type of {@link SystemMemberType} this member is. */
  public SystemMemberType getType();
  
  /** Gets host name of the machine this member resides on. */
  public String getHost();

  /** Gets the host of this member as an <code>java.net.InetAddress<code>. */
  public InetAddress getHostAddress();
  
  /** Retrieves the log for this member. */
  public String getLog();

  /**
   * Returns the GemFire license this member is using.
   *
   * @deprecated Removed licensing in 8.0.
   */
   @Deprecated
   public java.util.Properties getLicense();

  /** Returns this member's GemFire version information. */
  public String getVersion();
  
  /** 
   * Gets the configuration parameters for this member.
   */
  public ConfigurationParameter[] getConfiguration();
  
  /**
   * Sets the configuration of this member.  The argument is an array of any
   * and all configuration parameters that are to be updated in the member.
   * <p>
   * The entire array of configuration parameters is then returned.
   *
   * @param parms subset of the configuration parameters to be changed
   * @return all configuration parameters including those that were changed
   * @throws com.gemstone.gemfire.admin.AdminException
   *         if this fails to make the configuration changes
   */
  public ConfigurationParameter[] setConfiguration(ConfigurationParameter[] parms) throws com.gemstone.gemfire.admin.AdminException;
  
  /** Refreshes this member's configuration from the member or it's properties */
  public void refreshConfig() throws com.gemstone.gemfire.admin.AdminException;
  
  /** 
   * Retrieves this members statistic resources. If the member is not running 
   * then an empty array is returned. 
   *
   *@param statisticsTypeName String ame of the Statistics Type
   * @return array of runtime statistic resources owned by this member
   * @since 5.7
   */
  public StatisticResource[] getStat(String statisticsTypeName) throws com.gemstone.gemfire.admin.AdminException;
  
  /** 
   * Retrieves this members statistic resources. If the member is not running 
   * then an empty array is returned. All Stats are returned
   *
   * @return array of runtime statistic resources owned by this member
   */
  public StatisticResource[] getStats() throws com.gemstone.gemfire.admin.AdminException;
  
  /**
   * Returns whether or not this system member hosts a GemFire {@link
   * com.gemstone.gemfire.cache.Cache Cache}.
   *
   * @see #getCache
   */
  public boolean hasCache()
    throws com.gemstone.gemfire.admin.AdminException;

  /**
   * Returns an object that provides admin access to this member's cache.
   * If the member currently has no cache then <code>null</code> is returned.
   */
  public SystemMemberCache getCache() throws com.gemstone.gemfire.admin.AdminException;
  
  /**
   * Returns the names of the membership roles filled by this member.
   *
   * @return array of string membership role names
   * @since 5.0
   */
  public String[] getRoles();
  
  /**
   * Returns the {@link com.gemstone.gemfire.distributed.DistributedMember}
   * that represents this system member.
   *
   * @return DistributedMember instance representing this system member
   * @since 5.0
   */
  public DistributedMember getDistributedMember();
}


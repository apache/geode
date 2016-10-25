/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.admin;

import org.apache.geode.distributed.DistributedMember;

import java.net.InetAddress;

/**
 * Administrative interface for monitoring a GemFire system member.
 *
 * @since GemFire     3.5
 *
 * @deprecated as of 7.0 use the <code><a href="{@docRoot}/org/apache/geode/management/package-summary.html">management</a></code> package instead
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
   * org.apache.geode.admin.SystemMember#getId}.
   *
   * @see org.apache.geode.distributed.DistributedSystem#connect
   * @see org.apache.geode.distributed.DistributedSystem#getName
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
   * @throws org.apache.geode.admin.AdminException
   *         if this fails to make the configuration changes
   */
  public ConfigurationParameter[] setConfiguration(ConfigurationParameter[] parms) throws org.apache.geode.admin.AdminException;
  
  /** Refreshes this member's configuration from the member or it's properties */
  public void refreshConfig() throws org.apache.geode.admin.AdminException;
  
  /** 
   * Retrieves this members statistic resources. If the member is not running 
   * then an empty array is returned. 
   *
   *@param statisticsTypeName String ame of the Statistics Type
   * @return array of runtime statistic resources owned by this member
   * @since GemFire 5.7
   */
  public StatisticResource[] getStat(String statisticsTypeName) throws org.apache.geode.admin.AdminException;
  
  /** 
   * Retrieves this members statistic resources. If the member is not running 
   * then an empty array is returned. All Stats are returned
   *
   * @return array of runtime statistic resources owned by this member
   */
  public StatisticResource[] getStats() throws org.apache.geode.admin.AdminException;
  
  /**
   * Returns whether or not this system member hosts a GemFire {@link
   * org.apache.geode.cache.Cache Cache}.
   *
   * @see #getCache
   */
  public boolean hasCache()
    throws org.apache.geode.admin.AdminException;

  /**
   * Returns an object that provides admin access to this member's cache.
   * If the member currently has no cache then <code>null</code> is returned.
   */
  public SystemMemberCache getCache() throws org.apache.geode.admin.AdminException;
  
  /**
   * Returns the names of the membership roles filled by this member.
   *
   * @return array of string membership role names
   * @since GemFire 5.0
   */
  public String[] getRoles();
  
  /**
   * Returns the {@link org.apache.geode.distributed.DistributedMember}
   * that represents this system member.
   *
   * @return DistributedMember instance representing this system member
   * @since GemFire 5.0
   */
  public DistributedMember getDistributedMember();
}


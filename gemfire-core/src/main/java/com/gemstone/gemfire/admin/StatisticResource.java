/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
package com.gemstone.gemfire.admin;

/**
 * Adminitrative interface for monitoring a statistic resource in a GemFire
 * system member.  A resource is comprised of one or many 
 * <code>Statistics</code>.
 *
 * @author    Kirk Lund
 * @since     3.5
 *
 * @deprecated as of 7.0 use the {@link com.gemstone.gemfire.management} package instead
 */
public interface StatisticResource {
  
  /**
   * Gets the identifying name of this resource.
   *
   * @return the identifying name of this resource
   */
  public String getName();

  /**
   * Gets the full description of this resource.
   *
   * @return the full description of this resource
   */
  public String getDescription();
  
  /**
   * Gets the classification type of this resource.
   *
   * @return the classification type of this resource
   * @since 5.0
   */
  public String getType();
  
  /**
   * Returns a display string of the {@link SystemMember} owning this 
   * resource.
   *
   * @return a display string of the owning {@link SystemMember}
   */
  public String getOwner();
  
  /**
   * Returns an ID that uniquely identifies the resource within the
   * {@link SystemMember} it belongs to.
   *
   * @return unique id within the owning {@link SystemMember}
   */
  public long getUniqueId();
  
  /**
   * Returns a read-only array of every {@link Statistic} in this resource.
   *
   * @return read-only array of every {@link Statistic} in this resource
   */
  public Statistic[] getStatistics();
  
  /**
   * Refreshes the values of every {@link Statistic} in this resource by
   * retrieving them from the member's VM.
   *
   * @throws com.gemstone.gemfire.admin.AdminException 
   *         if unable to refresh statistic values
   */
  public void refresh() throws com.gemstone.gemfire.admin.AdminException;
  
}


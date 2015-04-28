/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
package com.gemstone.gemfire.cache;

/**
 * Indicates that a {@link Region} reliability failure has occurred.
 * Reliability for a <code>Region</code> is defined by its 
 * {@link MembershipAttributes}.
 *
 * @author Kirk Lund
 * @since 5.0
 */
public abstract class RegionRoleException extends RoleException {
  
  /** The full path of the region affected by the reliability failure */
  private String regionFullPath;
  
  /** 
   * Constructs a <code>RegionRoleException</code> with a message.
   * @param s the String message
   * @param regionFullPath full path of region for which access was attempted
   */
  public RegionRoleException(String s, String regionFullPath) {
    super(s);
    this.regionFullPath = regionFullPath;
  }
  
  /** 
   * Constructs a <code>RegionRoleException</code> with a message and
   * a cause.
   * @param s the String message
   * @param regionFullPath full path of region for which access was attempted
   * @param ex the Throwable cause
   */
  public RegionRoleException(String s,  String regionFullPath, Throwable ex) {
    super(s, ex);
    this.regionFullPath = regionFullPath;
  }
  
  /** 
   * Returns the full path of the region for which access was attempted.
   * @return the full path of the region for which access was attempted
   */
  public String getRegionFullPath() {
    return this.regionFullPath;
  }
  
}


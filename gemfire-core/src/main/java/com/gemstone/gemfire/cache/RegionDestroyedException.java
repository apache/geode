/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
package com.gemstone.gemfire.cache;

/**
 * Indicates that the region has been destroyed. Further operations
 * on the region object are not allowed.
 *
 * @author Eric Zoerner
 *
 * @since 2.0
 */
public class RegionDestroyedException extends CacheRuntimeException {
private static final long serialVersionUID = 319804842308010754L;
  private String regionFullPath;
  
  /** Constructs a <code>RegionDestroyedException</code> with a message.
   * @param msg the String message
   */
  public RegionDestroyedException(String msg, String regionFullPath) {
    super(msg);
    this.regionFullPath = regionFullPath;
  }
  
  /** Constructs a <code>RegionDestroyedException</code> with a message and
   * a cause.
   * @param s the String message
   * @param ex the Throwable cause
   */
  public RegionDestroyedException(String s, String regionFullPath, Throwable ex) {
    super(s, ex);
    this.regionFullPath = regionFullPath;
  }
  
  public String getRegionFullPath() {
    return this.regionFullPath;
  }
}

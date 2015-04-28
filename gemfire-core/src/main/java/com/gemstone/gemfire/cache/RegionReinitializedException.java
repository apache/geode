/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
package com.gemstone.gemfire.cache;

/**
 * Indicates that the region has been reinitialized. Further operations
 * on the region object are not allowed using this region reference.
 * A new reference must be acquired from the Cache the region's parent
 * region.
 *
 * @author Eric Zoerner
 *
 * @since 4.0
 */
public class RegionReinitializedException extends RegionDestroyedException {
private static final long serialVersionUID = 8532904304288670752L;
//  private String regionFullPath;
  
  /** Constructs a <code>RegionReinitializedException</code> with a message.
   * @param msg the String message
   */
  public RegionReinitializedException(String msg, String regionFullPath) {
    super(msg, regionFullPath);
  }
  
  /** Constructs a <code>RegionDestroyedException</code> with a message and
   * a cause.
   * @param s the String message
   * @param ex the Throwable cause
   */
  public RegionReinitializedException(String s, String regionFullPath, Throwable ex) {
    super(s, regionFullPath, ex);
  }
}

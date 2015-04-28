/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.admin;

//import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheRuntimeException;

/**
 * Thrown by the administration API when the region administered by a
 * {@link SystemMemberRegion} has been closed or destroyed in system
 * member. 
 * <P>Also thrown by {@link com.gemstone.gemfire.DataSerializer#readRegion(java.io.DataInput)}
 * if the named region no longer exists.
 *
 * @since 3.5
 * @deprecated as of 7.0 use the {@link com.gemstone.gemfire.management} package instead
 */
public class RegionNotFoundException extends CacheRuntimeException {
private static final long serialVersionUID = 1758668137691463909L;

  public RegionNotFoundException(String message) {
    super(message);
  }
  
}

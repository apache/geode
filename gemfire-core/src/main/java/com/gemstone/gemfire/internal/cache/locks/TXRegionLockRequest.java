/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.internal.cache.locks;

import com.gemstone.gemfire.DataSerializable;
//import com.gemstone.gemfire.cache.Region;
import java.util.Set;

/** Specifies a set of keys to try-lock within the scope of a region */
public interface TXRegionLockRequest extends DataSerializable {
  
  /** The full path of the region containing the entries to try-lock */
  public String getRegionFullPath();
  
  /** The entries to try-lock. Returns a set of <code>Object</code> names */
  public Set getKeys();
  
  /** add the key to be locked*/
  public void addEntryKey(Object key);
  
  /** add the set of keys to be locked */
  public void addEntryKeys(Set<Object> s);
}


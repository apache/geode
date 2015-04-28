/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.versions;

/**
 * Read only interface for an object that holds an entry version.
 * @author dsmith
 *
 * @param <T>
 */
public interface VersionHolder<T extends VersionSource> {
  /**
   * @return the current version number for the corresponding entry
   */
  int getEntryVersion();
  
  /**
   * @return the region version number for the last modification
   */
  long getRegionVersion();
  
  /**
   * @return the time stamp of the operation
   */
  long getVersionTimeStamp();
  
  /**
   * @return the ID of the member that last changed the corresponding entry
   */
  T getMemberID();
  
  /**
   * @return the Distributed System Id of the system that last changed the corresponding entry
   */
  int getDistributedSystemId();
  
  /** get rvv internal high byte.  Used by region entries for transferring to storage */
  public short getRegionVersionHighBytes();
  
  /** get rvv internal low bytes.  Used by region entries for transferring to storage */
  public int getRegionVersionLowBytes();

}

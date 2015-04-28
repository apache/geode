/*
 *  =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *  ========================================================================
 */
package com.gemstone.gemfire.management;

import java.beans.ConstructorProperties;

import com.gemstone.gemfire.cache.Region;

/**
 * Composite data type used to distribute the results of a disk backup
 * operation.
 * 
 * @author rishim
 * @since 7.0
 */
public class DiskBackupResult {

  /**
   * Returns the name of the directory
   */
  private String diskDirectory;

  /**
   * whether the bacup operation was successful or not
   */
  private boolean offilne;

  @ConstructorProperties( { "diskDirectory", "offilne"
    
  })
  public DiskBackupResult(String diskDirectory, boolean offline) {
    this.diskDirectory = diskDirectory;
    this.offilne = offline;
  }

  /**
   * Returns the name of the directory where the files for this backup
   * were written.
   */
  public String getDiskDirectory() {
    return diskDirectory;
  }

  /**
   * Returns whether the backup was successful.
   * 
   * @return True if the backup was successful, false otherwise.
   */
  public boolean isOffilne() {
    return offilne;
  }
}

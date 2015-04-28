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

/**
 * Composite data type used to distribute attributes for the missing disk
 * store of a persistent member.
 *
 * @author rishim
 * @since 7.0
 *
 */
public class PersistentMemberDetails {

  private final String host;
  private final String directory;
  private final String diskStoreId;

  @ConstructorProperties( { "host", "directory", "diskStoreId" })
  public PersistentMemberDetails(final String host, final String directory, final String diskStoreId) {
    this.host = host;
    this.directory = directory;
    this.diskStoreId = diskStoreId;
  }

  /**
   * Returns the name or IP address of the host on which the member is
   * running.
   */
  public String getHost() {
    return this.host;
  }

  /**
   * Returns the directory in which the <code>DiskStore</code> is saved.
   */
  public String getDirectory() {
    return this.directory;
  }

  /**
   * Returns the ID of the <code>DiskStore</code>.
   */
  public String getDiskStoreId() {
    return this.diskStoreId;
  }
}

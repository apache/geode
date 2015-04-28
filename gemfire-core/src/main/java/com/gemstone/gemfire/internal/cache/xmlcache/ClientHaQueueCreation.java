/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.  
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.xmlcache;

import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

/**
 * This class represents the data given for binding an overflow mechanism to a
 * client subscription. It encapsulates eviction policy, capacity and overflowDirectory.
 * This object will get created for every <b>client-subscription</b> tag
 * 
 * @author aingle
 * @since 5.7
 */
public class ClientHaQueueCreation {

  private int haQueueCapacity = 0;

  private String haEvictionPolicy = null;

  private String overflowDirectory = null;
  
  private String diskStoreName;
  
  private boolean hasOverflowDirectory = false;
  
  public int getCapacity(){
    return this.haQueueCapacity ;
  }
  
  public void setCapacity(int capacity){
    this.haQueueCapacity = capacity;
  }
 
  public String getEvictionPolicy(){
    return this.haEvictionPolicy ;
  }

  public void setEvictionPolicy(String policy){
    this.haEvictionPolicy = policy;
  }

  /**
   * @deprecated as of prPersistSprint2
   */
  public String getOverflowDirectory(){
    if (this.getDiskStoreName() != null) {
      throw new IllegalStateException(LocalizedStrings.DiskStore_Deprecated_API_0_Cannot_Mix_With_DiskStore_1
          .toLocalizedString(new Object[] {"getOverflowDirectory", this.getDiskStoreName()}));
    }
    return this.overflowDirectory;
  }

  /**
   * @deprecated as of prPersistSprint2
   */
  public void setOverflowDirectory(String overflowDirectory){
    if (this.getDiskStoreName() != null) {
      throw new IllegalStateException(LocalizedStrings.DiskStore_Deprecated_API_0_Cannot_Mix_With_DiskStore_1
          .toLocalizedString(new Object[] {"setOverflowDirectory", this.getDiskStoreName()}));
    }
    this.overflowDirectory = overflowDirectory;
    setHasOverflowDirectory(true);

  }
  
  public String getDiskStoreName() {
    return this.diskStoreName;
  }
  
  public void setDiskStoreName(String diskStoreName) {
    if (hasOverflowDirectory()) {
      throw new IllegalStateException(LocalizedStrings.DiskStore_Deprecated_API_0_Cannot_Mix_With_DiskStore_1
          .toLocalizedString(new Object[] {"setDiskStoreName", this.getDiskStoreName()}));
    }
    this.diskStoreName = diskStoreName;
  }
  public boolean hasOverflowDirectory()
  {
    return this.hasOverflowDirectory;
  }
  private void setHasOverflowDirectory(boolean hasOverflowDirectory)
  {
    this.hasOverflowDirectory = hasOverflowDirectory;
  }
}

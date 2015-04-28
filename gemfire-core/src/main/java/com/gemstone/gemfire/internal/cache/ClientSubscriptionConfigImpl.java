/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/**
 * 
 */
package com.gemstone.gemfire.internal.cache;

import com.gemstone.gemfire.cache.server.ClientSubscriptionConfig;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
/**
 * 
 * Configuration parameters for client subscription
 * @author aingle
 */
public class ClientSubscriptionConfigImpl implements ClientSubscriptionConfig {
  
  /**
   *  To get client subscription
   */
  public static final String CLIENT_SUBSCRIPTION = "client_subscription";

  private int haQueueCapacity = 1;

  private String haEvictionPolicy = null;
  
  /**
   * The name of the directory in which to store overflowed client queue entries
   */
  private String overflowDirectory;

  /**
   * disk store name for overflow
   */
  private String diskStoreName;
  
  private boolean hasOverflowDirectory = false;
  
  public ClientSubscriptionConfigImpl(){
    this.haQueueCapacity = DEFAULT_CAPACITY;
    this.haEvictionPolicy = DEFAULT_EVICTION_POLICY;
    this.overflowDirectory = DEFAULT_OVERFLOW_DIRECTORY;
  }
  /**
   * Returns the capacity of the client client queue.
   * will be in MB for eviction-policy mem else
   * number of entries
   * @see #DEFAULT_CAPACITY
   * @since 5.7
   */
  public int getCapacity(){
    return this.haQueueCapacity ;
  }
  /**
   * Sets the capacity of the client client queue.
   * will be in MB for eviction-policy mem else
   * number of entries
   * @see #DEFAULT_CAPACITY
   * @since 5.7
   */
  public void setCapacity(int capacity){
    this.haQueueCapacity = capacity;
  }
  /**
   * Returns the eviction policy that is executed when capacity of the client client queue is reached.
   * @see #DEFAULT_EVICTION_POLICY
   * @since 5.7
   */
  public String getEvictionPolicy(){
    return this.haEvictionPolicy ;
  }
  /**
   * Sets the eviction policy that is executed when capacity of the client client queue is reached.
   * @see #DEFAULT_EVICTION_POLICY
   * @since 5.7
   */
  public void setEvictionPolicy(String policy){
    this.haEvictionPolicy = policy;
  }

  /**
   * Sets the overflow directory for a client client queue 
   * @param overflowDirectory the overflow directory for a client queue's overflowed entries
   * @since 5.7
   * @deprecated as of prPersistSprint2 
   */
  @Deprecated
  public void setOverflowDirectory(String overflowDirectory) {
    if (this.getDiskStoreName() != null) {
      throw new IllegalStateException(LocalizedStrings.DiskStore_Deprecated_API_0_Cannot_Mix_With_DiskStore_1
          .toLocalizedString(new Object[] {"setOverflowDirectory", this.getDiskStoreName()}));
    }
    this.overflowDirectory = overflowDirectory;
    setHasOverflowDirectory(true);
  }

  /**
   * Answers the overflow directory for a client queue's
   * overflowed client queue entries.
   * @return the overflow directory for a client queue's
   * overflowed entries
   * @since 5.7
   * @deprecated as of prPersistSprint2 
   */
  @Deprecated
  public String getOverflowDirectory() {
    if (this.getDiskStoreName() != null) {
      throw new IllegalStateException(LocalizedStrings.DiskStore_Deprecated_API_0_Cannot_Mix_With_DiskStore_1
          .toLocalizedString(new Object[] {"getOverflowDirectory", this.getDiskStoreName()}));
    }
    return this.overflowDirectory;
  }
  
  @Override
  public String toString() {
    String str = " Eviction policy "
        + this.getEvictionPolicy() + " capacity "
        + this.getCapacity();
    if (diskStoreName == null) {
      str += " Overflow Directory " + this.getOverflowDirectory();
    } else {
      str += " DiskStore Name: " + this.diskStoreName;
    }
    return str;
  }
  /**
   * get the diskStoreName for overflow
   * @since prPersistSprint2
   */
  public String getDiskStoreName() {
    return diskStoreName;
  }
  /**
   * Sets the disk store name for overflow  
   * @param diskStoreName 
   * @since prPersistSprint2
   */
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
  
  /*public boolean equals(ClientSubscriptionConfig other) {
    if (other != null && other.getEvictionPolicy() != null
        && other.getOverflowDirectory() != null) {
      if ((this.getEvictionPolicy() == other.getEvictionPolicy())
          && (this.getOverflowDirectory() == other.getOverflowDirectory())
          && (this.getCapacity() == this.getCapacity()))
        return true;
    }
    return false;
  }*/
}

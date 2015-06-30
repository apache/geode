/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.cache.hdfs;

/**
 * {@link HDFSEventQueueAttributes} represents the attributes of the buffer where events are 
 * accumulated before they are persisted to HDFS  
 * 
 * @author Hemant Bhanawat
 */
public interface HDFSEventQueueAttributes {

  /**
   * The Disk store that is required for overflow and persistence
   * @return    String
   */
  public String getDiskStoreName();

  /**
   * The maximum memory after which the data needs to be overflowed to disk
   * @return    int
   */
  public int getMaximumQueueMemory();

  /**
   * Represents the size of a batch per bucket that gets delivered
   * from the HDFS Event Queue to HDFS. A higher value means that 
   * less number of bigger batches are persisted to HDFS and hence 
   * big files are created on HDFS. But, bigger batches consume memory.  
   *  
   * This value is an indication. Batches per bucket with size less than the specified
   * are sent to HDFS if interval specified by {@link #getBatchTimeInterval()}
   * has elapsed.
   * @return    int
   */
  public int getBatchSizeMB();
  
  /**
   * Represents the batch time interval for a HDFS queue. This is the maximum time interval
   * that can elapse before a batch of data from a bucket is sent to HDFS.
   *
   * @return  int
   */
  public int getBatchTimeInterval();
  
  /**
   * Represents whether the  HDFS Event Queue is configured to be persistent or non-persistent
   * @return    boolean
   */
  public boolean isPersistent();

  /**
   * Represents whether or not the writing to the disk is synchronous.
   * 
   * @return boolean 
   */
  public boolean isDiskSynchronous();
  
  /**
   * Number of threads in VM consuming the events.
   * default is one.
   * 
   * @return int
   */
  public int getDispatcherThreads();
}

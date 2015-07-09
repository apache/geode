/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.cache.hdfs;

import com.gemstone.gemfire.cache.hdfs.internal.HDFSEventQueueAttributesImpl;
import com.gemstone.gemfire.cache.wan.GatewaySender;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

/**
 * Factory to create {@link HDFSEventQueueAttributes} . 
 * {@link HDFSEventQueueAttributes} represents the attributes of the buffer where events are 
 * accumulated before they are persisted to HDFS  
 * 
 * @author Hemant Bhanawat
 */
public class HDFSEventQueueAttributesFactory {

  /**
   * The default batch size
   */
  public static final int DEFAULT_BATCH_SIZE_MB = 32;

  /**
   * The default batch time interval in milliseconds
   */
  public static final int DEFAULT_BATCH_TIME_INTERVAL_MILLIS = 60000;

  /**
   * By default, queue is created for a read write HDFS store 
   */
  public static final boolean DEFAULT_FOR_WRITEONLY_HDFSSTORE = false;
  
  public HDFSEventQueueAttributesFactory() {
  }

  /**
   * Copy constructor for {@link HDFSEventQueueAttributes}. The method creates
   * an instance with same attribute values as of {@code attr}
   * 
   * @param attr
   */
  public HDFSEventQueueAttributesFactory(HDFSEventQueueAttributes attr) {
    setDiskStoreName(attr.getDiskStoreName());
    setMaximumQueueMemory(attr.getMaximumQueueMemory());
    setBatchTimeInterval(attr.getBatchTimeInterval());
    setBatchSizeMB(attr.getBatchSizeMB());
    setPersistent(attr.isPersistent());
    setDiskSynchronous(attr.isDiskSynchronous());
    setDispatcherThreads(attr.getDispatcherThreads());
  }
  
  /**
   * Sets the disk store name for overflow or persistence.
   * 
   * @param name
   */
  public HDFSEventQueueAttributesFactory setDiskStoreName(String name) {
    this.diskStoreName = name;
    return this;
  }
  
  /**
   * Sets the maximum amount of memory (in MB) for an
   * HDFS Event Queue.
   * 
   * @param memory
   *          The maximum amount of memory (in MB) for an
   *          HDFS Event Queue
   */
  public HDFSEventQueueAttributesFactory setMaximumQueueMemory(int memory) {
    this.maximumQueueMemory = memory;
    return this;
  }
  
  /**
   * Sets the batch time interval for a HDFS queue. This is the maximum time interval
   * that can elapse before a batch of data from a bucket is sent to HDFS.
   *
   * @param intervalMillis
   *          int time interval in milliseconds. Default is 60000 ms.
   */
  public HDFSEventQueueAttributesFactory setBatchTimeInterval(int intervalMillis){
    this.batchIntervalMillis = intervalMillis;
    return this;
  }

 
  /**
   * Sets the size of a batch per bucket that gets delivered
   * from the HDFS Event Queue to HDFS. Setting this to a higher value
   * would mean that less number of bigger batches are persisted to
   * HDFS and hence big files are created on HDFS. But, bigger batches
   * would consume memory.  
   *  
   * This value is an indication. Batches per bucket with size less than the specified
   * are sent to HDFS if interval specified by {@link #setBatchTimeInterval(int)}
   * has elapsed.
   *  
   * @param size
   *          The size of batches sent to HDFS in MB. Default is 32 MB.
   */
  public HDFSEventQueueAttributesFactory setBatchSizeMB(int size){
    this.batchSize = size;
    return this;
  }
  
  /**
   * Sets whether the HDFS Event Queue is persistent or not.
   * 
   * @param isPersistent
   *          Whether to enable persistence for an HDFS Event Queue..
   */
  public HDFSEventQueueAttributesFactory setPersistent(boolean isPersistent) {
    this.isPersistenceEnabled = isPersistent;
    return this;
  }
  /**
   * Sets whether or not the writing to the disk is synchronous.
   *
   * @param isSynchronous
   *          boolean if true indicates synchronous writes
   */
  public HDFSEventQueueAttributesFactory setDiskSynchronous(boolean isSynchronous) {
    this.diskSynchronous = isSynchronous;
    return this;
  }
  
  /**
   * Number of threads in VM to consumer the events
   * default is one.
   * 
   * @param dispatcherThreads
   */
  public void setDispatcherThreads(int dispatcherThreads) {
  	this.dispatcherThreads = dispatcherThreads;
  }
  
  /**
   * Creates the <code>HDFSEventQueueAttributes</code>.    * 
   * 
   */
  public HDFSEventQueueAttributes create() {
    return new HDFSEventQueueAttributesImpl(this.diskStoreName, this.maximumQueueMemory, 
        this.batchSize, this.isPersistenceEnabled,  this.batchIntervalMillis, this.diskSynchronous, this.dispatcherThreads);
  }

  private int maximumQueueMemory = GatewaySender.DEFAULT_MAXIMUM_QUEUE_MEMORY;
  private int batchIntervalMillis = HDFSEventQueueAttributesFactory.DEFAULT_BATCH_TIME_INTERVAL_MILLIS;
  private int batchSize = HDFSEventQueueAttributesFactory.DEFAULT_BATCH_SIZE_MB;
  private boolean diskSynchronous = GatewaySender.DEFAULT_DISK_SYNCHRONOUS; 
  private boolean isPersistenceEnabled = GatewaySender.DEFAULT_PERSISTENCE_ENABLED;
  private int dispatcherThreads = GatewaySender.DEFAULT_HDFS_DISPATCHER_THREADS;
  private String diskStoreName = null;
}

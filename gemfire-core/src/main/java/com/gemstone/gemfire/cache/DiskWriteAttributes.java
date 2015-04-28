/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache;

/**
 * Immutable parameter object for describing how {@linkplain
 * Region.Entry region entries} should be written to disk.
 *
 * @see DiskWriteAttributesFactory
 * @see AttributesFactory#setDiskWriteAttributes
 * @see RegionAttributes#getDiskWriteAttributes
 * @see Region#writeToDisk
 *
 * @author David Whitlock
 * @author Mitul Bid
 *
 * @since 3.2
 * @deprecated as of 6.5 use {@link DiskStore} instead
 */
@Deprecated
public interface DiskWriteAttributes
  extends java.io.Serializable {



  //////////////////////  Instance Methods  //////////////////////

 

  /**
   * Returns true if this <code>DiskWriteAttributes</code> object
   * configures synchronous writes.
   * 
   * @return Returns true if writes to disk are synchronous and false otherwise
   * @deprecated as of 6.5 use {@link RegionAttributes#isDiskSynchronous} instead.
   */
  @Deprecated
  public boolean isSynchronous();
  
  
  /** 
   * Returns true if the oplogs is to be rolled to a more condensed format (on disk)
   * 
   * @return Returns true if the oplogs is to be rolled or false otherwise
   */
  public boolean isRollOplogs();

  /** 
   * Get the maximum size in megabytes a single oplog (operation log) file should be 
   * 
   * @return the maximum size the operations log file can be
   * @deprecated as of 6.5 use {@link DiskStore#getMaxOplogSize()} 
   * instead.
   */
  @Deprecated
  public int getMaxOplogSize();

  /**
   * Returns the number of milliseconds that can elapse before
   * unwritten data is written to disk.  If this
   * <code>DiskWriteAttributes</code> configures synchronous writing,
   * then <code>timeInterval</code> has no meaning.
   * 
   * @return Returns the time interval in milliseconds that can elapse between two writes to disk
   * @deprecated as of 6.5 use {@link DiskStore#getTimeInterval()} 
   * instead.
   */
  @Deprecated
  public long getTimeInterval();

  /**
   * Returns the number of unwritten bytes of data that can be
   * enqueued before being written to disk.  If this
   * <code>DiskWriteAttributes</code> configures synchronous writing,
   * then <code>bytesThreshold</code> has no meaning.
   * 
   * @return Returns the number of bytes that can be buffered before being written to disk
   * @deprecated as of 6.5 use {@link DiskStore#getQueueSize()} 
   * instead.
   */
  @Deprecated
  public long getBytesThreshold();

  /**
   * Two <code>DiskWriteAttributes</code> are equal if the both
   * specify the synchronous writes, or they both specify asynchronous
   * writes with the same time interval, bytes threshold, maxOplogSize and
   * compaction values
   * 
   * @return true if o is equal else false
   */
  public boolean equals(Object o);

}

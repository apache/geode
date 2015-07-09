/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.hdfs.internal.hoplog;

import java.io.IOException;

/**
 * Defines an observer of asynchronous operations on sorted oplog files associated with a bucket.
 */
public interface HoplogListener {
  /**
   * Notifies creation of new sorted oplog files. A new file will be created after compaction or
   * other bucket maintenance activities
   * 
   * @throws IOException
   */
  void hoplogCreated(String regionFolder, int bucketId, Hoplog... oplogs) throws IOException;

  /**
   * Notifies file deletion. A file becomes redundant after compaction or other bucket maintenance
   * activities
   * @throws IOException 
   */
  void hoplogDeleted(String regionFolder, int bucketId, Hoplog... oplogs) throws IOException;
  
  /**
   * Notifies completion of a hoplog compaction cycle. 
   * @param region Region on which compaction was performed
   * @param bucket bucket id
   * @param isMajor true if major compaction was executed
   */
  void compactionCompleted(String region, int bucket, boolean isMajor);
}

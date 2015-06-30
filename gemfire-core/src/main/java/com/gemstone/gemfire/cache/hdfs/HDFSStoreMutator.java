/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.cache.hdfs;

import com.gemstone.gemfire.cache.hdfs.HDFSStore.HDFSCompactionConfig;
import com.gemstone.gemfire.cache.hdfs.HDFSStoreFactory.HDFSCompactionConfigFactory;

public interface HDFSStoreMutator {
  /**
   * {@link HDFSStoreFactory#setMaxFileSize(int)}
   */
  public HDFSStoreMutator setMaxFileSize(int maxFileSize);

  /**
   * {@link HDFSStore#getMaxFileSize()}
   * 
   * @return value to be used when mutator is executed on hdfsStore. -1 if not
   *         set
   */
  public int getMaxFileSize();
  
  /**
   * {@link HDFSStoreFactory#setFileRolloverInterval(int)}
   */
  public HDFSStoreMutator setFileRolloverInterval(int rolloverIntervalInSecs);
  
  /**
   * {@link HDFSStore#getFileRolloverInterval()}
   * 
   * @return value to be used when mutator is executed on hdfsStore. -1 if not
   *         set
   */
  public int getFileRolloverInterval();
  
  /**
   * {@link HDFSStore#getMinorCompaction()}
   * 
   * @return value to be used when mutator is executed on hdfsStore. null if
   *         not set
   */
  public Boolean getMinorCompaction();

  /**
   * {@link HDFSStoreFactory#setMinorCompaction(boolean)}
   */
  public HDFSCompactionConfigMutator setMinorCompaction(boolean auto);
  
  /**
   * Reuturns mutator for compaction configuration of hdfs store
   * @return instance of {@link HDFSCompactionConfigMutator}
   */
  public HDFSCompactionConfigMutator getCompactionConfigMutator();

  /**
   * Reuturns mutator for hdfs event queue of hdfs store
   * @return instance of {@link HDFSEventQueueAttributesMutator}
   */
  public HDFSEventQueueAttributesMutator getHDFSEventQueueAttributesMutator();
  
  public static interface HDFSEventQueueAttributesMutator {
    /**
     * {@link HDFSEventQueueAttributesFactory#setBatchSizeMB(int)}
     */
    public HDFSEventQueueAttributesMutator setBatchSizeMB(int size);
    
    /**
     * {@link HDFSEventQueueAttributes#getBatchSizeMB()}
     * 
     * @return value to be used when mutator is executed on hdfsStore. -1 if not
     *         set
     */
    public int getBatchSizeMB();
    
    /**
     * {@link HDFSEventQueueAttributesFactory#setBatchTimeInterval(int)}
     */
    public HDFSEventQueueAttributesMutator setBatchTimeInterval(int interval);
    
    /**
     * {@link HDFSEventQueueAttributes#getBatchTimeInterval()}
     * 
     * @return value to be used when mutator is executed on hdfsStore. -1 if not
     *         set
     */
    public int getBatchTimeInterval();
  }
  
  public static interface HDFSCompactionConfigMutator {
    /**
     * {@link HDFSCompactionConfigFactory#setMaxInputFileSizeMB(int)}
     */
    public HDFSCompactionConfigMutator setMaxInputFileSizeMB(int size);
    
    /**
     * {@link HDFSCompactionConfig#getMaxInputFileSizeMB()}
     * 
     * @return value to be used when mutator is executed on hdfsStore. -1 if
     *         not set
     */
    public int getMaxInputFileSizeMB();

    /**
     * {@link HDFSCompactionConfigFactory#setMinInputFileCount(int)}
     */
    public HDFSCompactionConfigMutator setMinInputFileCount(int count);
    
    /**
     * {@link HDFSCompactionConfig#getMinInputFileCount()}
     * 
     * @return value to be used when mutator is executed on hdfsStore. -1 if
     *         not set
     */
    public int getMinInputFileCount();

    /**
     * {@link HDFSCompactionConfigFactory#setMaxInputFileCount(int)}
     */
    public HDFSCompactionConfigMutator setMaxInputFileCount(int count);
    
    /**
     * {@link HDFSCompactionConfig#getMaxInputFileCount()}
     * 
     * @return value to be used when mutator is executed on hdfsStore. -1 if
     *         not set
     */
    public int getMaxInputFileCount();

    /**
     * {@link HDFSCompactionConfigFactory#setMaxThreads(int)}
     */
    public HDFSCompactionConfigMutator setMaxThreads(int count);
    
    /**
     * {@link HDFSCompactionConfig#getMaxThreads()}
     * 
     * @return value to be used when mutator is executed on hdfsStore. -1 if
     *         not set
     */
    public int getMaxThreads();

    /**
     * {@link HDFSCompactionConfigFactory#setAutoMajorCompaction(boolean)}
     */
    public HDFSCompactionConfigMutator setAutoMajorCompaction(boolean auto);
    
    /**
     * {@link HDFSCompactionConfig#getAutoMajorCompaction()}
     * 
     * @return value to be used when mutator is executed on hdfsStore. null if
     *         not set
     */
    public Boolean getAutoMajorCompaction();

    /**
     * {@link HDFSCompactionConfigFactory#setMajorCompactionIntervalMins(int)}
     */
    public HDFSCompactionConfigMutator setMajorCompactionIntervalMins(int interval);
    
    /**
     * {@link HDFSCompactionConfig#getMajorCompactionIntervalMins()}
     * 
     * @return value to be used when mutator is executed on hdfsStore. -1 if
     *         not set
     */
    public int getMajorCompactionIntervalMins();

    /**
     * {@link HDFSCompactionConfigFactory#setMajorCompactionMaxThreads(int)}
     */
    public HDFSCompactionConfigMutator setMajorCompactionMaxThreads(int count);
    
    /**
     * {@link HDFSCompactionConfig#getMajorCompactionMaxThreads()}
     * 
     * @return value to be used when mutator is executed on hdfsStore. -1 if
     *         not set
     */
    public int getMajorCompactionMaxThreads();
    
    /**
     * {@link HDFSCompactionConfigFactory#setOldFilesCleanupIntervalMins(int)}
     */
    public HDFSCompactionConfigMutator setOldFilesCleanupIntervalMins(int interval);
    
    /**
     * {@link HDFSCompactionConfig#getOldFilesCleanupIntervalMins()}
     * 
     * @return value to be used when mutator is executed on hdfsStore. -1 if
     *         not set
     */
    public int getOldFilesCleanupIntervalMins();
  }
}

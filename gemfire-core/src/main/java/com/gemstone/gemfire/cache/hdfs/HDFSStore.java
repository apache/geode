/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.cache.hdfs;

/**
 * Provides HDFS storage for one or more regions. The regions in the same HDFS
 * store will share the same persistence attributes.
 * <p>
 * Instances of this interface are created using {@link HDFSStoreFactory#create}
 * So to create a <code>HDFSStore</code> named <code>myDiskStore</code> do
 * this:
 * 
 * <PRE>
 * new HDFSStoreFactory().create(&quot;myDiskStore&quot;);
 * </PRE>
 * <p>
 * 
 * @author Hemant Bhanawat
 * @author Ashvin Agrawal
 */

public interface HDFSStore {
  public static final String DEFAULT_HOME_DIR = "gemfire";
  public static final float DEFAULT_BLOCK_CACHE_SIZE = 10f;
  public static final int DEFAULT_MAX_WRITE_ONLY_FILE_SIZE = 256; 
  public static final int DEFAULT_WRITE_ONLY_FILE_ROLLOVER_INTERVAL = 3600; 

  /**
   * @return name of HDFSStore provided at while creating the instance
   */
  public String getName();

  /**
   * @return Namenode URL associated with this store
   */
  public String getNameNodeURL();

  /**
   * @return Home directory where regions using this store will be persisted
   */
  public String getHomeDir();

  /**
   * @return hdfs client configuration referred by this store
   */
  public String getHDFSClientConfigFile();

  /**
   * @return the percentage of the heap to use for the block cache in the range
   * 0 ... 100
   */
  public float getBlockCacheSize();

  /**
   * For write only tables, data is written to a single file until the file 
   * reaches a size specified by this API or the time 
   * for file rollover specified by {@link #getFileRolloverInterval()} has passed.
   * Default is 256 MB.  
   *   
   * @return max file size in MB. 
   */
  public int getMaxFileSize();
  
  /**
   * For write only tables, data is written to a single file until the file 
   * reaches a certain size specified by {@link #getMaxFileSize()} or the time 
   * for file rollover has passed. Default is 3600 seconds. 
   *   
   * @return time in seconds after which a file will be rolled over into a new file
   */
  public int getFileRolloverInterval();
  
  /**
   * @return true if auto compaction is enabled
   */
  public boolean getMinorCompaction();

  /**
   * Return the HDFSEventQueueAttributes associated with this HDFSStore
   */
  public HDFSEventQueueAttributes getHDFSEventQueueAttributes();
  
  /**
   * Destroys this hdfs store. Removes the disk store from the cache. All
   * regions on this store must be closed.
   * 
   */
  public void destroy();

  /**
   * @return Instance of compaction configuration associated with this store
   */
  public HDFSCompactionConfig getHDFSCompactionConfig();
  
  /**
   * @return instance of mutator object that can be used to alter properties of
   *         this store
   */
  public HDFSStoreMutator createHdfsStoreMutator();
  
  /**
   * Applies new attribute values provided using mutator to this instance
   * dynmically.
   * 
   * @param mutator
   *          contains the changes
   * @return hdfsStore reference representing the old store configuration
   */
  public HDFSStore alter(HDFSStoreMutator mutator);
      
  public static interface HDFSCompactionConfig {
    public static final String INVALID = "invalid";
    public static final String SIZE_ORIENTED = "size-oriented";
    public static final String DEFAULT_STRATEGY = SIZE_ORIENTED;
    
    public static final boolean DEFAULT_AUTO_COMPACTION = true;
    public static final boolean DEFAULT_AUTO_MAJOR_COMPACTION = true;
    public static final int DEFAULT_MAX_INPUT_FILE_SIZE_MB = 512;
    public static final int DEFAULT_MAX_INPUT_FILE_COUNT = 10;
    public static final int DEFAULT_MIN_INPUT_FILE_COUNT = 4;
    public static final int DEFAULT_MAX_THREADS = 10;
    
    public static final int DEFAULT_MAJOR_COMPACTION_MAX_THREADS = 2;
    public static final int DEFAULT_MAJOR_COMPACTION_INTERVAL_MINS = 720;
    public static final int DEFAULT_OLD_FILE_CLEANUP_INTERVAL_MINS = 30;
    
    /**
     * @return name of the compaction strategy configured for this store
     */
    public String getCompactionStrategy();

    /**
     * @return size threshold (in MB). A file larger than this size will not be
     *         considered for compaction
     */
    public int getMaxInputFileSizeMB();

    /**
     * @return minimum count threshold. Compaction cycle will commence if the
     *         number of files to be compacted is more than this number
     */
    public int getMinInputFileCount();

    /**
     * @return maximum count threshold.  Compaction cycle will not include more
     *          files than the maximum
     */
    public int getMaxInputFileCount();

    /**
     * @return maximum number of threads executing minor compaction
     */
    public int getMaxThreads();

    /**
     * @return true if auto major compaction is enabled
     */
    public boolean getAutoMajorCompaction();

    /**
     * @return interval configuration that guides major compaction frequency
     */
    public int getMajorCompactionIntervalMins();

    /**
     * @return maximum number of threads executing major compaction
     */
    public int getMajorCompactionMaxThreads();
    
    /**
     * @return interval configuration that guides deletion of old files
     */
    public int getOldFilesCleanupIntervalMins();
  }
}

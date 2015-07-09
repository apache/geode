/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.cache.hdfs;

import com.gemstone.gemfire.GemFireConfigException;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.hdfs.HDFSStore.HDFSCompactionConfig;

/**
 * Factory for creating instances of {@link HDFSStore}. To get an instance of
 * this factory call {@link Cache#createHDFSStoreFactory}.
 * <P>
 * To use this factory configure it with the <code>set</code> methods and then
 * call {@link #create} to produce a HDFS store instance.
 * 
 * @author Hemant Bhanawat
 * @author Ashvin Agrawal
 */
public interface HDFSStoreFactory {

  /**
   * @param name
   *          name of HDFSStore provided at while creating the instance
   */
  public HDFSStoreFactory setName(String name);

  /**
   * @param url
   *          Namenode URL associated with this store
   */
  public HDFSStoreFactory setNameNodeURL(String url);

  /**
   * @param dir
   *          Home directory where regions using this store will be persisted
   */
  public HDFSStoreFactory setHomeDir(String dir);

  /**
   * @param file
   *          hdfs client configuration referred by this store
   */
  public HDFSStoreFactory setHDFSClientConfigFile(String file);

  /**
   * @param config
   *          Instance of compaction configuration associated with this store
   */
  public HDFSStoreFactory setHDFSCompactionConfig(HDFSCompactionConfig config);
  
  /**
   * @param percentage
   *          Size of the block cache as a percentage of the heap in the range
   *          0 ... 100 
   */
  public HDFSStoreFactory setBlockCacheSize(float percentage);
  
  /**
   * Sets the HDFS event queue attributes
   * This causes the store to use the {@link HDFSEventQueueAttributes}.
   * @param hdfsEventQueueAttrs the attributes of the HDFS Event queue
   * @return a reference to this RegionFactory object
   * 
   */
  public HDFSStoreFactory setHDFSEventQueueAttributes(HDFSEventQueueAttributes hdfsEventQueueAttrs);
  
  /**
   * For write only tables, data is written to a single file until the file 
   * reaches a size specified by this API or the time 
   * for file rollover specified by {@link #setFileRolloverInterval(int)} has passed.  
   * Default is 256 MB. 
   * 
   * @param maxFileSize max file size in MB
   */
  public HDFSStoreFactory setMaxFileSize(int maxFileSize);
  
  /**
   * For write only tables, data is written to a single file until the file 
   * reaches a certain size specified by {@link #setMaxFileSize(int)} or the time 
   * for file rollover has passed. Default is 3600 seconds. 
   * 
   * @param rolloverIntervalInSecs time in seconds after which a file will be rolled over into a new file
   */
  public HDFSStoreFactory setFileRolloverInterval(int rolloverIntervalInSecs);
  
  /**
   * @param auto
   *          true if auto compaction is enabled
   */
  public HDFSStoreFactory setMinorCompaction(boolean auto);

  /**
   * @param strategy
   *          name of the compaction strategy or null for letting system choose
   *          and apply default compaction strategy
   * @return instance of {@link HDFSCompactionConfigFactory}
   */
  public HDFSCompactionConfigFactory createCompactionConfigFactory(String strategy);

  public static interface HDFSCompactionConfigFactory {

    /**
     * @param size
     *          size threshold (in MB). A file larger than this size will not be
     *          considered for compaction
     */
    public HDFSCompactionConfigFactory setMaxInputFileSizeMB(int size);

    /**
     * @param count
     *          minimum count threshold. Compaction cycle will commence if the
     *          number of files to be compacted is more than this number
     */
    public HDFSCompactionConfigFactory setMinInputFileCount(int count);

    /**
     * @param count
     *          maximum count threshold.  Compaction cycle will not include more
     *          files than the maximum
     */
    public HDFSCompactionConfigFactory setMaxInputFileCount(int count);

    /**
     * @param count
     *          maximum number of threads executing minor compaction. Count must
     *          be greater than 0
     */
    public HDFSCompactionConfigFactory setMaxThreads(int count);

    /**
     * @param auto
     *          true if auto major compaction is enabled
     */
    public HDFSCompactionConfigFactory setAutoMajorCompaction(boolean auto);

    /**
     * @param interval
     *          interval configuration that guides major compaction frequency
     */
    public HDFSCompactionConfigFactory setMajorCompactionIntervalMins(int interval);

    /**
     * @param count
     *          maximum number of threads executing major compaction. Count must
     *          be greater than 0
     */
    public HDFSCompactionConfigFactory setMajorCompactionMaxThreads(int count);
    
    /**
     * @param interval
     *          interval configuration that guides deletion of old files
     */
    public HDFSCompactionConfigFactory setOldFilesCleanupIntervalMins(int interval);
    
    /**
     * Create a {@link HDFSCompactionConfig}. The returned instance will have
     * the same configuration as that this factory.
     * 
     * @return the newly created {@link HDFSCompactionConfig}
     * @throws GemFireConfigException
     *           if the cache xml is invalid
     */
    public HDFSCompactionConfig create() throws GemFireConfigException;
    
    /**
     * @return A {@link HDFSCompactionConfig} view of this factory
     * @throws GemFireConfigException
     */
    public HDFSCompactionConfig getConfigView();
  }

  /**
   * Create a new HDFS store. The returned HDFS store's configuration will be
   * the same as this factory's configuration.
   * 
   * @param name
   *          the name of the HDFSStore
   * @return the newly created HDFSStore.
   * @throws GemFireConfigException
   *           if the cache xml is invalid
   * @throws StoreExistsException
   *           if another instance of {@link HDFSStore} with the same exists
   */
  public HDFSStore create(String name) throws GemFireConfigException,
      StoreExistsException;

  // TODO this is the only non-factory instance getter in this class
  HDFSEventQueueAttributes getHDFSEventQueueAttributes();
}

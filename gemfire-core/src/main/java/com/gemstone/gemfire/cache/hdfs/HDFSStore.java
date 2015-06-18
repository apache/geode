/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.cache.hdfs;

import com.gemstone.gemfire.cache.wan.GatewaySender;

/**
 * HDFS stores provide a means of persisting data on HDFS. There can be multiple
 * instance of HDFS stores in a cluster. The regions connected using a HDFS
 * store will share the same HDFS persistence attributes. A user will normally
 * perform the following steps to enable HDFS persistence for a region:
 * <ol>
 * <li>[Optional] Creates a Disk store for reliability
 * <li>HDFS buffers will use local persistence till it is persisted on HDFS
 * <li>Creates a HDFS Store
 * <li>Creates a Region connected to HDFS Store Uses region API to create and
 * query data
 * </ol>
 * <p>
 * Instances of this interface are created using {@link HDFSStoreFactory#create}
 * 
 * @author Hemant Bhanawat
 * @author Ashvin Agrawal
 */

public interface HDFSStore {
  public static final String DEFAULT_HOME_DIR = "gemfire";
  public static final float DEFAULT_BLOCK_CACHE_SIZE = 10f;
  public static final int DEFAULT_MAX_WRITE_ONLY_FILE_SIZE = 256; 
  public static final int DEFAULT_WRITE_ONLY_FILE_ROLLOVER_INTERVAL = 3600;
  
  public static final int DEFAULT_BATCH_SIZE_MB = 32;
  public static final int DEFAULT_BATCH_INTERVAL_MILLIS = 60000;
  public static final boolean DEFAULT_WRITEONLY_HDFSSTORE = false;
  public static final boolean DEFAULT_BUFFER_PERSISTANCE = GatewaySender.DEFAULT_PERSISTENCE_ENABLED;
  public static final boolean DEFAULT_DISK_SYNCHRONOUS = GatewaySender.DEFAULT_DISK_SYNCHRONOUS;
  public static final int DEFAULT_MAX_BUFFER_MEMORY = GatewaySender.DEFAULT_MAXIMUM_QUEUE_MEMORY;
  public static final int DEFAULT_DISPATCHER_THREADS = GatewaySender.DEFAULT_HDFS_DISPATCHER_THREADS;
  
  public static final boolean DEFAULT_MINOR_COMPACTION = true;
  public static final int DEFAULT_MINOR_COMPACTION_THREADS = 10;
  public static final boolean DEFAULT_MAJOR_COMPACTION = true;
  public static final int DEFAULT_MAJOR_COMPACTION_THREADS = 2;
  public static final int DEFAULT_MAX_INPUT_FILE_SIZE_MB = 512;
  public static final int DEFAULT_MAX_INPUT_FILE_COUNT = 10;
  public static final int DEFAULT_MIN_INPUT_FILE_COUNT = 4;
  
  public static final int DEFAULT_MAJOR_COMPACTION_INTERVAL_MINS = 720;
  public static final int DEFAULT_OLD_FILE_CLEANUP_INTERVAL_MINS = 30;

  /**
   * @return A unique identifier for the HDFSStore
   */
  public String getName();

  /**
   * HDFSStore persists data on a HDFS cluster identified by cluster's NameNode
   * URL or NameNode Service URL. NameNode URL can also be provided via
   * hdfs-site.xml (see HDFSClientConfigFile). If the NameNode url is missing
   * HDFSStore creation will fail. HDFS client can also load hdfs configuration
   * files in the classpath. NameNode URL provided in this way is also fine.
   * 
   * @return Namenode url explicitly configured by user
   */
  public String getNameNodeURL();

  /**
   * HomeDir is the HDFS directory path in which HDFSStore stores files. The
   * value must not contain the NameNode URL. The owner of this node's JVM
   * process must have read and write access to this directory. The path could
   * be absolute or relative. If a relative path for HomeDir is provided, then
   * the HomeDir is created relative to /user/JVM_owner_name or, if specified,
   * relative to directory specified by the hdfs-root-dir property. As a best
   * practice, HDFS store directories should be created relative to a single
   * HDFS root directory. As an alternative, an absolute path beginning with the
   * "/" character to override the default root location can be provided.
   * 
   * @return path
   */
  public String getHomeDir();

  /**
   * The full path to the HDFS client configuration file, for e.g. hdfs-site.xml
   * or core-site.xml. This file must be accessible to any node where an
   * instance of this HDFSStore will be created. If each node has a local copy
   * of this configuration file, it is important for all the copies to be
   * "identical". Alternatively, by default HDFS client can also load some HDFS
   * configuration files if added in the classpath.
   * 
   * @return path
   */
  public String getHDFSClientConfigFile();

  /**
   * The maximum amount of memory in megabytes to be used by HDFSStore.
   * HDFSStore buffers data in memory to optimize HDFS IO operations. Once the
   * configured memory is utilized, data may overflow to disk.
   * 
   * @return max memory in MB
   */
  public int getMaxMemory();

  /**
   * @return the percentage of the heap to use for the block cache in the range
   * 0 ... 100
   */
  public float getBlockCacheSize();

  /**
   * HDFSStore buffer data is persisted on HDFS in batches. The BatchSize
   * defines the maximum size (in megabytes) of each batch that is written to
   * HDFS. This parameter, along with BatchInterval determines the rate at which
   * data is persisted on HDFS. A higher value means that less number of bigger
   * batches are persisted to HDFS and hence big files are created on HDFS. But,
   * bigger batches consume memory.
   * 
   * @return batchsize in MB
   */
  public int getBatchSize();
  
  /**
   * HDFSStore buffer data is persisted on HDFS in batches, and the
   * BatchInterval defines the maximum time that can elapse between writing
   * batches to HDFS. This parameter, along with BatchSize determines the rate
   * at which data is persisted on HDFS.
   * 
   * @return interval in seconds
   */
  public int getBatchInterval();
  
  /**
   * The maximum number of threads (per region) used to write batches to HDFS.
   * If you have a large number of clients that add or update data in a region,
   * then you may need to increase the number of dispatcher threads to avoid
   * bottlenecks when writing data to HDFS.
   * 
   * @return The maximum number of threads
   */
  public int getDispatcherThreads();
  
  /**
   * Configure if HDFSStore in-memory buffer data, that has not been persisted
   * on HDFS yet, should be persisted to a local disk to buffer prevent data
   * loss. Persisting data may impact write performance. If performance is
   * critical and buffer data loss is acceptable, disable persistence.
   * 
   * @return true if buffer is persisted locally
   */
  public boolean getBufferPersistent();

  /**
   * The named DiskStore to use for any local disk persistence needs of
   * HDFSStore, for e.g. store's buffer persistence and buffer overflow. If you
   * specify a value, the named DiskStore must exist. If you specify a null
   * value or you omit this option, default DiskStore is used.
   * 
   * @return disk store name
   */
  public String getDiskStoreName();

  /**
   * Synchronous flag indicates if synchronous disk writes are enabled or not.
   * 
   * @return true if enabled
   */
  public boolean getSynchronousDiskWrite();
  
  /**
   * For HDFS write-only regions, this defines the maximum size (in megabytes)
   * that an HDFS log file can reach before HDFSStore closes the file and begins
   * writing to a new file. This clause is ignored for HDFS read/write regions.
   * Keep in mind that the files are not available for MapReduce processing
   * until the file is closed; you can also set WriteOnlyFileRolloverInterval to
   * specify the maximum amount of time an HDFS log file remains open.
   * 
   * @return max file size in MB.
   */
  public int getMaxWriteOnlyFileSize();
  
  /**
   * For HDFS write-only regions, this defines the maximum time that can elapse
   * before HDFSStore closes an HDFS file and begins writing to a new file. This
   * configuration is ignored for HDFS read/write regions.
   * 
   * @return interval in seconds 
   */
  public int getWriteOnlyFileRolloverInterval();
  
  /**
   * Minor compaction reorganizes data in files to optimize read performance and
   * reduce number of files created on HDFS. Minor compaction process can be
   * I/O-intensive, tune the performance of minor compaction using
   * MinorCompactionThreads.
   * 
   * @return true if auto minor compaction is enabled
   */
  public boolean getMinorCompaction();

  /**
   * The maximum number of threads that HDFSStore uses to perform minor
   * compaction. You can increase the number of threads used for compaction as
   * necessary in order to fully utilize the performance of your HDFS cluster.
   * 
   * @return maximum number of threads executing minor compaction
   */
  public int getMinorCompactionThreads();

  /**
   * Major compaction removes old values of a key and deleted records from the
   * HDFS files, which can save space in HDFS and improve performance when
   * reading from HDFS. As major compaction process can be long-running and
   * I/O-intensive, tune the performance of major compaction using
   * MajorCompactionInterval and MajorCompactionThreads.
   * 
   * @return true if auto major compaction is enabled
   */
  public boolean getMajorCompaction();

  /**
   * The amount of time after which HDFSStore performs the next major compaction
   * cycle.
   * 
   * @return interval in seconds
   */
  public int getMajorCompactionInterval();

  /**
   * The maximum number of threads that HDFSStore uses to perform major
   * compaction. You can increase the number of threads used for compaction as
   * necessary in order to fully utilize the performance of your HDFS cluster.
   * 
   * @return maximum number of threads executing major compaction
   */
  public int getMajorCompactionThreads();
  
  /**
   * HDFSStore creates new files as part of periodic maintenance activity.
   * Existing files are deleted asynchronously. PurgeInterval defines the amount
   * of time old files remain available and could be externally, e.g. read by MR
   * jobs. After this interval has passed, old files are deleted.
   * 
   * @return interval configuration that guides deletion of old files
   */
  public int getPurgeInterval();
  
  /**
   * Permanently deletes all HDFS files associated with this this
   * {@link HDFSStore}. This operation will fail ( {@link IllegalStateException}
   * ) if any region is still using this store for persistence.
   */
  public void destroy();
  
  /**
   * @return new instance of mutator object that can be used to alter properties
   *         of this store
   */
  public HDFSStoreMutator createHdfsStoreMutator();
  
  /**
   * Identifies attributes configured in {@link HDFSStoreMutator} and applies
   * the new attribute values to this instance of {@link HDFSStore} dynamically.
   * Any property which is not set in {@link HDFSStoreMutator} remains
   * unaltered. In most cases altering the attributes does not cause existing
   * operations to terminate. The altered attributes are used in the next cycle
   * of the operation they impact.
   * 
   * @return hdfsStore reference representing the old {@link HDFSStore}
   */
  public HDFSStore alter(HDFSStoreMutator mutator);

  /**
   * This advanced configuration affects minor compaction.
   * @return size threshold (in MB). A file larger than this size will not be
   *         considered for compaction
   */
  public int getMaxInputFileSizeMB();

  /**
   * This advanced configuration affects minor compaction.
   * @return minimum count threshold. Compaction cycle will commence if the
   *         number of files to be compacted is more than this number
   */
  public int getMinInputFileCount();

  /**
   * This advanced configuration affects minor compaction.
   * @return maximum count threshold.  Compaction cycle will not include more
   *          files than the maximum
   */
  public int getMaxInputFileCount();
}

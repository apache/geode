/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.gemstone.gemfire.cache.hdfs;

import com.gemstone.gemfire.cache.wan.GatewaySender;

/**
 * HDFS stores provide a means of persisting data on HDFS. There can be multiple
 * instance of HDFS stores in a cluster. The regions connected using a HDFS
 * store will share the same HDFS persistence attributes. A user will normally
 * perform the following steps to enable HDFS persistence for a region:
 * <ol>
 * <li>[Optional] Creates a DiskStore for HDFS buffer reliability (HDFS buffers
 * will be persisted locally till data lands on HDFS)
 * <li>Creates a HDFS Store (connects to DiskStore created earlier)
 * <li>Creates a Region connected to HDFS Store
 * <li>Uses region API to create and query data
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
  public static final int DEFAULT_WRITE_ONLY_FILE_SIZE_LIMIT = 256;
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
  public static final int DEFAULT_INPUT_FILE_SIZE_MAX_MB = 512;
  public static final int DEFAULT_INPUT_FILE_COUNT_MAX = 10;
  public static final int DEFAULT_INPUT_FILE_COUNT_MIN = 4;

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
   * files in the classpath. The following precedence order is applied
   * <ol>
   * <li>URL explicitly configured in the HdfsStore
   * <li>URL provided in client configuration file:
   * {@link #getHDFSClientConfigFile()}
   * <li>URL provided in default configuration files loaded by hdfs-client
   * </ol>
   * 
   * HDFSStore will use the selected URL only. It will fail if the selected URL
   * is not reachable.
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
   *         0 ... 100
   */
  public float getBlockCacheSize();

  /**
   * HDFSStore buffer data is persisted on HDFS in batches. The BatchSize
   * defines the maximum size (in megabytes) of each batch that is written to
   * HDFS. This parameter, along with BatchInterval determines the rate at which
   * data is persisted on HDFS. A higher value causes fewer and bigger batches
   * to be persisted to HDFS and hence big files are created on HDFS. But,
   * bigger batches consume more memory.
   * 
   * @return batch size in MB
   */
  public int getBatchSize();

  /**
   * HDFSStore buffer data is persisted on HDFS in batches, and the
   * BatchInterval defines the number of milliseconds that can elapse between
   * writing batches to HDFS. This parameter, along with BatchSize determines
   * the rate at which data is persisted on HDFS.
   * 
   * @return batch interval in milliseconds
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
   * on HDFS yet, should be persisted to a local disk to prevent buffer data
   * loss. Persisting buffer data may impact write performance. If performance
   * is critical and buffer data loss is acceptable, disable persistence.
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
   * HDFS buffers can be persisted on local disk. Each region update record is
   * written to the disk synchronously if synchronous disk write is enabled.
   * Enable this option if the data being persisted is critical and no record
   * should be lost in case of a crash. This high reliability mode may increase
   * write latency. If synchronous mode is disabled, data is persisted in
   * batches which usually results in better performance.
   * 
   * @return true if enabled
   */
  public boolean getSynchronousDiskWrite();

  /**
   * For HDFS write-only regions, this defines the maximum size (in megabytes)
   * that an HDFS log file can reach before HDFSStore closes the file and begins
   * writing to a new file. This option is ignored for HDFS read/write regions.
   * Keep in mind that the files are not available for MapReduce processing
   * until the file is closed; you can also set WriteOnlyFileRolloverInterval to
   * specify the maximum amount of time an HDFS log file remains open.
   * 
   * @return max file size in MB.
   */
  public int getWriteOnlyFileRolloverSize();

  /**
   * For HDFS write-only regions, this defines the number of seconds that can
   * elapse before HDFSStore closes an HDFS file and begins writing to a new
   * file. This configuration is ignored for HDFS read/write regions.
   * 
   * @return interval in seconds
   */
  public int getWriteOnlyFileRolloverInterval();

  /**
   * Minor compaction reorganizes data in files to optimize read performance and
   * reduce number of files created on HDFS. Minor compaction process can be
   * I/O-intensive, tune the performance of minor compaction using
   * MinorCompactionThreads. Minor compaction is not applicable to write-only
   * regions.
   * 
   * @return true if auto minor compaction is enabled
   */
  public boolean getMinorCompaction();

  /**
   * The maximum number of threads that HDFSStore uses to perform minor
   * compaction. You can increase the number of threads used for compaction as
   * necessary in order to fully utilize the performance of your HDFS cluster.
   * Minor compaction is not applicable to write-only regions.
   * 
   * @return maximum number of threads executing minor compaction
   */
  public int getMinorCompactionThreads();

  /**
   * Major compaction removes old values of a key and deleted records from the
   * HDFS files, which can save space in HDFS and improve performance when
   * reading from HDFS. As major compaction process can be long-running and
   * I/O-intensive, tune the performance of major compaction using
   * MajorCompactionInterval and MajorCompactionThreads. Major compaction is not
   * applicable to write-only regions.
   * 
   * @return true if auto major compaction is enabled
   */
  public boolean getMajorCompaction();

  /**
   * The number of minutes after which HDFSStore performs the next major
   * compaction cycle. Major compaction is not applicable to write-only regions.
   * 
   * @return interval in minutes
   */
  public int getMajorCompactionInterval();

  /**
   * The maximum number of threads that HDFSStore uses to perform major
   * compaction. You can increase the number of threads used for compaction as
   * necessary in order to fully utilize the performance of your HDFS cluster.
   * Major compaction is not applicable to write-only regions.
   * 
   * @return maximum number of threads executing major compaction
   */
  public int getMajorCompactionThreads();

  /**
   * HDFSStore may create new files as part of periodic maintenance activity. It
   * deletes old files asynchronously. PurgeInterval defines the number of
   * minutes for which old files will remain available to be consumed
   * externally, e.g. read by MR jobs. After this interval, old files are
   * deleted. This configuration is not applicable to write-only regions
   * 
   * @return old file purge interval in minutes
   */
  public int getPurgeInterval();

  /**
   * Permanently deletes all HDFS files associated with this {@link HDFSStore}.
   * This operation will fail if any region is still using this store for
   * persistence.
   * 
   * @exception IllegalStateException
   *              if any region using this hdfsStore still exists
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
   * A file larger than this size, in megabytes, will not be compacted by minor
   * compactor. Increasing this value will result in compaction of bigger files.
   * This will lower the number of files on HDFS at the cost of increased IO.
   * This option is for advanced users and will need tuning in special cases
   * only. This option is not applicable to write-only regions.
   * 
   * @return size threshold (in MB)
   */
  public int getInputFileSizeMax();

  /**
   * A minimum number of files must exist in a bucket directory on HDFS before
   * minor compaction will start compaction. Keeping a higher value for this
   * option will reduce the frequency of minor compaction, which in turn may
   * result in reduced IO overhead. However it may result in increased pressure
   * on HDFS NameNode. This option is for advanced users and will need tuning in
   * special cases only. This option is not applicable to write-only regions.
   * 
   * @return minimum number of files for minor compaction to get triggered
   */
  public int getInputFileCountMin();

  /**
   * The maximum number of files compacted by Minor compactor in a cycle.
   * Keeping a higher value for this option will reduce the frequency of minor
   * compaction, which in turn may result in reduced IO overhead. However it may
   * result in large number of concurrent IO operations which in-turn may
   * degrade the performance. This option is for advanced users and will need
   * tuning in special cases only. This option is not applicable to write-only
   * regions.
   * 
   * @return maximum number of files minor compacted in one cycle
   */
  public int getInputFileCountMax();
}

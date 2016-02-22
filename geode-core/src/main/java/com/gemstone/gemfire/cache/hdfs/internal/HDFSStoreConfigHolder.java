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

package com.gemstone.gemfire.cache.hdfs.internal;

import java.io.Serializable;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.GemFireConfigException;
import com.gemstone.gemfire.cache.hdfs.HDFSStore;
import com.gemstone.gemfire.cache.hdfs.HDFSStoreFactory;
import com.gemstone.gemfire.cache.hdfs.HDFSStoreMutator;
import com.gemstone.gemfire.cache.hdfs.StoreExistsException;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheXml;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;


/**
 * Class to hold all hdfs store related configuration. Instead of copying the
 * same members in two different classes, factory and store, this class will be
 * used. The idea is let HdfsStoreImpl and HdfsStoreCreation delegate get calls,
 * set calls and copy constructor calls this class. Moreover this config holder
 * can be entirely replaced to support alter config
 * 
 * @author ashvina
 */
public class HDFSStoreConfigHolder implements HDFSStore, HDFSStoreFactory ,Serializable {  
  private String name = null;
  private String namenodeURL = null;
  private String homeDir = DEFAULT_HOME_DIR;
  private String clientConfigFile = null;
  private float blockCacheSize = DEFAULT_BLOCK_CACHE_SIZE;
  private int maxFileSize = DEFAULT_WRITE_ONLY_FILE_SIZE_LIMIT;
  private int fileRolloverInterval = DEFAULT_WRITE_ONLY_FILE_ROLLOVER_INTERVAL;
  protected boolean isAutoCompact = DEFAULT_MINOR_COMPACTION;
  protected boolean autoMajorCompact = DEFAULT_MAJOR_COMPACTION;
  protected int maxConcurrency = DEFAULT_MINOR_COMPACTION_THREADS;
  protected int majorCompactionConcurrency = DEFAULT_MAJOR_COMPACTION_THREADS;
  protected int majorCompactionIntervalMins = DEFAULT_MAJOR_COMPACTION_INTERVAL_MINS;
  protected int maxInputFileSizeMB = DEFAULT_INPUT_FILE_SIZE_MAX_MB;
  protected int maxInputFileCount = DEFAULT_INPUT_FILE_COUNT_MAX;
  protected int minInputFileCount = DEFAULT_INPUT_FILE_COUNT_MIN;
  protected int oldFileCleanupIntervalMins = DEFAULT_OLD_FILE_CLEANUP_INTERVAL_MINS;
  
  protected int batchSize = DEFAULT_BATCH_SIZE_MB;
  protected int batchIntervalMillis = DEFAULT_BATCH_INTERVAL_MILLIS;
  protected int maximumQueueMemory = DEFAULT_MAX_BUFFER_MEMORY;
  protected boolean isPersistenceEnabled = DEFAULT_BUFFER_PERSISTANCE;
  protected String diskStoreName = null;
  protected boolean diskSynchronous = DEFAULT_DISK_SYNCHRONOUS; 
  protected int dispatcherThreads = DEFAULT_DISPATCHER_THREADS;
  
  private static final Logger logger = LogService.getLogger();
  protected final String logPrefix;

  public HDFSStoreConfigHolder() {
    this(null);
  }

  /**
   * @param config configuration source for creating this instance 
   */
  public HDFSStoreConfigHolder(HDFSStore config) {
    this.logPrefix = "<" + getName() + "> ";
    if (config == null) {
      return;
    }
    
    this.name = config.getName();
    this.namenodeURL = config.getNameNodeURL();
    this.homeDir = config.getHomeDir();
    this.clientConfigFile = config.getHDFSClientConfigFile();
    this.blockCacheSize = config.getBlockCacheSize();
    this.maxFileSize = config.getWriteOnlyFileRolloverSize();
    this.fileRolloverInterval = config.getWriteOnlyFileRolloverInterval();
    isAutoCompact = config.getMinorCompaction();
    maxConcurrency = config.getMinorCompactionThreads();
    autoMajorCompact = config.getMajorCompaction();
    majorCompactionConcurrency = config.getMajorCompactionThreads();
    majorCompactionIntervalMins = config.getMajorCompactionInterval();
    maxInputFileSizeMB = config.getInputFileSizeMax();
    maxInputFileCount = config.getInputFileCountMax();
    minInputFileCount = config.getInputFileCountMin();
    oldFileCleanupIntervalMins = config.getPurgeInterval();
    
    batchSize = config.getBatchSize();
    batchIntervalMillis = config.getBatchInterval();
    maximumQueueMemory = config.getMaxMemory();
    isPersistenceEnabled = config.getBufferPersistent();
    diskStoreName = config.getDiskStoreName();
    diskSynchronous = config.getSynchronousDiskWrite();
    dispatcherThreads = config.getDispatcherThreads();
  }
  
  public void resetDefaultValues() {
    name = null;
    namenodeURL = null;
    homeDir = null;
    clientConfigFile = null;
    blockCacheSize = -1f;
    maxFileSize = -1;
    fileRolloverInterval = -1;
    
    isAutoCompact = false;
    maxConcurrency = -1;
    maxInputFileSizeMB = -1;
    maxInputFileCount = -1;
    minInputFileCount = -1;
    oldFileCleanupIntervalMins = -1;

    autoMajorCompact = false;
    majorCompactionConcurrency = -1;
    majorCompactionIntervalMins = -1;
    
    batchSize = -1;
    batchIntervalMillis = -1;
    maximumQueueMemory = -1;
    isPersistenceEnabled = false;
    diskStoreName = null;
    diskSynchronous = false; 
    dispatcherThreads = -1;
  }
  
  public void copyFrom(HDFSStoreMutator mutator) {
    if (mutator.getWriteOnlyFileRolloverInterval() >= 0) {
      logAttrMutation("fileRolloverInterval", mutator.getWriteOnlyFileRolloverInterval());
      setWriteOnlyFileRolloverInterval(mutator.getWriteOnlyFileRolloverInterval());
    }
    if (mutator.getWriteOnlyFileRolloverSize() >= 0) {
      logAttrMutation("MaxFileSize", mutator.getWriteOnlyFileRolloverInterval());
      setWriteOnlyFileRolloverSize(mutator.getWriteOnlyFileRolloverSize());
    }
    
    if (mutator.getMinorCompaction() != null) {
      logAttrMutation("MinorCompaction", mutator.getMinorCompaction());
      setMinorCompaction(mutator.getMinorCompaction());
    }
    
    if (mutator.getMinorCompactionThreads() >= 0) {
      logAttrMutation("MaxThreads", mutator.getMinorCompactionThreads());
      setMinorCompactionThreads(mutator.getMinorCompactionThreads());
    }
    
    if (mutator.getMajorCompactionInterval() > -1) {
      logAttrMutation("MajorCompactionIntervalMins", mutator.getMajorCompactionInterval());
      setMajorCompactionInterval(mutator.getMajorCompactionInterval());
    }
    if (mutator.getMajorCompactionThreads() >= 0) {
      logAttrMutation("MajorCompactionMaxThreads", mutator.getMajorCompactionThreads());
      setMajorCompactionThreads(mutator.getMajorCompactionThreads());
    }
    if (mutator.getMajorCompaction() != null) {
      logAttrMutation("AutoMajorCompaction", mutator.getMajorCompaction());
      setMajorCompaction(mutator.getMajorCompaction());
    }
    if (mutator.getInputFileCountMax() >= 0) {
      logAttrMutation("maxInputFileCount", mutator.getInputFileCountMax());
      setInputFileCountMax(mutator.getInputFileCountMax());
    }
    if (mutator.getInputFileSizeMax() >= 0) {
      logAttrMutation("MaxInputFileSizeMB", mutator.getInputFileSizeMax());
      setInputFileSizeMax(mutator.getInputFileSizeMax());
    }
    if (mutator.getInputFileCountMin() >= 0) {
      logAttrMutation("MinInputFileCount", mutator.getInputFileCountMin());
      setInputFileCountMin(mutator.getInputFileCountMin());
    }    
    if (mutator.getPurgeInterval() >= 0) {
      logAttrMutation("OldFilesCleanupIntervalMins", mutator.getPurgeInterval());
      setPurgeInterval(mutator.getPurgeInterval());
    }
    
    if (mutator.getBatchSize() >= 0) {
      logAttrMutation("batchSizeMB", mutator.getWriteOnlyFileRolloverInterval());
      setBatchSize(mutator.getBatchSize());
    }
    if (mutator.getBatchInterval() >= 0) {
      logAttrMutation("batchTimeInterval", mutator.getWriteOnlyFileRolloverInterval());
      setBatchInterval(mutator.getBatchInterval());
    }
  }

  void logAttrMutation(String name, Object value) {
    if (logger.isDebugEnabled()) {
      logger.debug("{}Alter " + name + ":" + value, logPrefix);
    }
  }

  @Override
  public String getName() {
    return name;
  }
  @Override
  public HDFSStoreFactory setName(String name) {
    this.name = name;
    return this;
  }

  @Override
  public String getNameNodeURL() {
    return namenodeURL;
  }
  @Override
  public HDFSStoreFactory setNameNodeURL(String namenodeURL) {
    this.namenodeURL = namenodeURL;
    return this;
  }

  @Override
  public String getHomeDir() {
    return homeDir;
  }
  @Override
  public HDFSStoreFactory setHomeDir(String homeDir) {
    this.homeDir = homeDir;
    return this;
  }

  @Override
  public String getHDFSClientConfigFile() {
    return clientConfigFile;
  }
  @Override
  public HDFSStoreFactory setHDFSClientConfigFile(String clientConfigFile) {
    this.clientConfigFile = clientConfigFile;
    return this;
  }
  
  @Override
  public HDFSStoreFactory setBlockCacheSize(float percentage) {
    if(percentage < 0 || percentage > 100) {
      throw new IllegalArgumentException("Block cache size must be between 0 and 100, inclusive");
    }
    this.blockCacheSize  = percentage;
    return this;
  }
  
  @Override
  public float getBlockCacheSize() {
    return blockCacheSize;
  }
  
  @Override
  public HDFSStoreFactory setWriteOnlyFileRolloverSize(int maxFileSize) {
    assertIsPositive(CacheXml.HDFS_WRITE_ONLY_FILE_ROLLOVER_INTERVAL, maxFileSize);
    this.maxFileSize = maxFileSize;
    return this;
  }
  @Override
  public int getWriteOnlyFileRolloverSize() {
    return maxFileSize;
  }

  @Override
  public HDFSStoreFactory setWriteOnlyFileRolloverInterval(int count) {
    assertIsPositive(CacheXml.HDFS_TIME_FOR_FILE_ROLLOVER, count);
    this.fileRolloverInterval = count;
    return this;
  }
  @Override
  public int getWriteOnlyFileRolloverInterval() {
    return fileRolloverInterval;
  }
  
  @Override
  public boolean getMinorCompaction() {
    return isAutoCompact;
  }
  @Override
  public HDFSStoreFactory setMinorCompaction(boolean auto) {
    this.isAutoCompact = auto;
    return this;
  }

  @Override
  public HDFSStoreFactory setMinorCompactionThreads(int count) {
    assertIsPositive(CacheXml.HDFS_MINOR_COMPACTION_THREADS, count);
    this.maxConcurrency = count;
    return this;
  }
  @Override
  public int getMinorCompactionThreads() {
    return maxConcurrency;
  }

  @Override
  public HDFSStoreFactory setMajorCompaction(boolean auto) {
    this.autoMajorCompact = auto;
    return this;
  }
  @Override
  public boolean getMajorCompaction() {
    return autoMajorCompact;
  }

  @Override
  public HDFSStoreFactory setMajorCompactionInterval(int count) {
    HDFSStoreCreation.assertIsPositive(CacheXml.HDFS_MAJOR_COMPACTION_INTERVAL, count);
    this.majorCompactionIntervalMins = count;
    return this;
  }
  @Override
  public int getMajorCompactionInterval() {
    return majorCompactionIntervalMins;
  }

  @Override
  public HDFSStoreFactory setMajorCompactionThreads(int count) {
    HDFSStoreCreation.assertIsPositive(CacheXml.HDFS_MAJOR_COMPACTION_THREADS, count);
    this.majorCompactionConcurrency = count;
    return this;
  }
  @Override
  public int getMajorCompactionThreads() {
    return majorCompactionConcurrency;
  }
  
  @Override
  public HDFSStoreFactory setInputFileSizeMax(int size) {
    HDFSStoreCreation.assertIsPositive("HDFS_COMPACTION_MAX_INPUT_FILE_SIZE_MB", size);
    this.maxInputFileSizeMB = size;
    return this;
  }
  @Override
  public int getInputFileSizeMax() {
    return maxInputFileSizeMB;
  }

  @Override
  public HDFSStoreFactory setInputFileCountMin(int count) {
    HDFSStoreCreation.assertIsPositive("HDFS_COMPACTION_MIN_INPUT_FILE_COUNT", count);
    this.minInputFileCount = count;
    return this;
  }
  @Override
  public int getInputFileCountMin() {
    return minInputFileCount;
  }

  @Override
  public HDFSStoreFactory setInputFileCountMax(int count) {
    HDFSStoreCreation.assertIsPositive("HDFS_COMPACTION_MAX_INPUT_FILE_COUNT", count);
    this.maxInputFileCount = count;
    return this;
  }
  @Override
  public int getInputFileCountMax() {
    return maxInputFileCount;
  }

  @Override
  public int getPurgeInterval() {
    return oldFileCleanupIntervalMins ;
  }    
  @Override
  public HDFSStoreFactory setPurgeInterval(int interval) {
    assertIsPositive(CacheXml.HDFS_PURGE_INTERVAL, interval);
    this.oldFileCleanupIntervalMins = interval;
    return this;
  }
  
  protected void validate() {
    if (minInputFileCount > maxInputFileCount) {
      throw new IllegalArgumentException(
          LocalizedStrings.HOPLOG_MIN_IS_MORE_THAN_MAX
          .toLocalizedString(new Object[] {
              "HDFS_COMPACTION_MIN_INPUT_FILE_COUNT",
              minInputFileCount,
              "HDFS_COMPACTION_MAX_INPUT_FILE_COUNT",
              maxInputFileCount }));
    }
  }

  /**
   * This method should not be called on this class.
   * @see HDFSStoreFactory#create(String)
   */
  @Override
  public HDFSStore create(String name) throws GemFireConfigException,
      StoreExistsException {
    throw new UnsupportedOperationException();
  }

  /**
   * This method should not be called on this class.
   * @see HDFSStoreImpl#destroy()
   */
  @Override
  public void destroy() {
    throw new UnsupportedOperationException();
  }
  
  public static void assertIsPositive(String name, int count) {
    if (count < 1) {
      throw new IllegalArgumentException(
          LocalizedStrings.DiskWriteAttributesImpl_0_HAS_TO_BE_POSITIVE_NUMBER_AND_THE_VALUE_GIVEN_1_IS_NOT_ACCEPTABLE
              .toLocalizedString(new Object[] { name, count }));
    }
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("HDFSStoreConfigHolder@");
    builder.append(System.identityHashCode(this));
    builder.append(" [");
    appendStrProp(builder, name, "name");
    appendStrProp(builder, namenodeURL, "namenodeURL");
    appendStrProp(builder, homeDir, "homeDir");
    appendStrProp(builder, clientConfigFile, "clientConfigFile");
    if (blockCacheSize > -1) {
      builder.append("blockCacheSize=");
      builder.append(blockCacheSize);
      builder.append(", ");
    }
    appendIntProp(builder, maxFileSize, "maxFileSize");
    appendIntProp(builder, fileRolloverInterval, "fileRolloverInterval");
    appendBoolProp(builder, isAutoCompact, "isAutoCompact");
    appendBoolProp(builder, autoMajorCompact, "autoMajorCompact");
    appendIntProp(builder, maxConcurrency, "maxConcurrency");
    appendIntProp(builder, majorCompactionConcurrency, "majorCompactionConcurrency");
    appendIntProp(builder, majorCompactionIntervalMins, "majorCompactionIntervalMins");
    appendIntProp(builder, maxInputFileSizeMB, "maxInputFileSizeMB");
    appendIntProp(builder, maxInputFileCount, "maxInputFileCount");
    appendIntProp(builder, minInputFileCount, "minInputFileCount");
    appendIntProp(builder, oldFileCleanupIntervalMins, "oldFileCleanupIntervalMins");
    appendIntProp(builder, batchSize, "batchSize");
    appendIntProp(builder, batchIntervalMillis, "batchInterval");
    appendIntProp(builder, maximumQueueMemory, "maximumQueueMemory");
    appendIntProp(builder, dispatcherThreads, "dispatcherThreads");
    appendBoolProp(builder, isPersistenceEnabled, "isPersistenceEnabled");
    appendStrProp(builder, diskStoreName, "diskStoreName");
    appendBoolProp(builder, diskSynchronous, "diskSynchronous");

    builder.append("]");
    return builder.toString();
  }

  private void appendStrProp(StringBuilder builder, String value, String name) {
    if (value != null) {
      builder.append(name + "=");
      builder.append(value);
      builder.append(", ");
    }
  }

  private void appendIntProp(StringBuilder builder, int value, String name) {
    if (value > -1) {
      builder.append(name + "=");
      builder.append(value);
      builder.append(", ");
    }
  }
  
  private void appendBoolProp(StringBuilder builder, boolean value, String name) {
    builder.append(name + "=");
    builder.append(value);
    builder.append(", ");
  }

  @Override
  public HDFSStoreMutator createHdfsStoreMutator() {
    // as part of alter execution, hdfs store will replace the config holder
    // completely. Hence mutator at the config holder is not needed
    throw new UnsupportedOperationException();
  }

  @Override
  public HDFSStore alter(HDFSStoreMutator mutator) {
    // as part of alter execution, hdfs store will replace the config holder
    // completely. Hence mutator at the config holder is not needed
    throw new UnsupportedOperationException();
  }

  @Override
  public String getDiskStoreName() {
    return this.diskStoreName;
  }
  @Override
  public HDFSStoreFactory setDiskStoreName(String name) {
    this.diskStoreName = name;
    return this;
  }

  @Override
  public int getBatchInterval() {
    return this.batchIntervalMillis;
  }
  @Override
  public HDFSStoreFactory setBatchInterval(int intervalMillis){
    this.batchIntervalMillis = intervalMillis;
    return this;
  }
  
  @Override
  public boolean getBufferPersistent() {
    return isPersistenceEnabled;
  }
  @Override
  public HDFSStoreFactory setBufferPersistent(boolean isPersistent) {
    this.isPersistenceEnabled = isPersistent;
    return this;
  }

  @Override
  public int getDispatcherThreads() {
    return dispatcherThreads;
  }
  @Override
  public HDFSStoreFactory setDispatcherThreads(int dispatcherThreads) {
    this.dispatcherThreads = dispatcherThreads;
    return this;
  }
  
  @Override
  public int getMaxMemory() {
    return this.maximumQueueMemory;
  }
  @Override
  public HDFSStoreFactory setMaxMemory(int memory) {
    this.maximumQueueMemory = memory;
    return this;
  }
  
  @Override
  public int getBatchSize() {
    return this.batchSize;
  }
  @Override
  public HDFSStoreFactory setBatchSize(int size){
    this.batchSize = size;
    return this;
  }
  
  @Override
  public boolean getSynchronousDiskWrite() {
    return this.diskSynchronous;
  }
  @Override
  public HDFSStoreFactory setSynchronousDiskWrite(boolean isSynchronous) {
    this.diskSynchronous = isSynchronous;
    return this;
  }
}

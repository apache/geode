/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.cache.hdfs.internal;

import java.io.Serializable;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.GemFireConfigException;
import com.gemstone.gemfire.cache.hdfs.HDFSEventQueueAttributes;
import com.gemstone.gemfire.cache.hdfs.HDFSEventQueueAttributesFactory;
import com.gemstone.gemfire.cache.hdfs.HDFSStore;
import com.gemstone.gemfire.cache.hdfs.HDFSStoreFactory;
import com.gemstone.gemfire.cache.hdfs.HDFSStoreMutator;
import com.gemstone.gemfire.cache.hdfs.HDFSStoreMutator.HDFSCompactionConfigMutator;
import com.gemstone.gemfire.cache.hdfs.HDFSStoreMutator.HDFSEventQueueAttributesMutator;
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
  private int maxFileSize = DEFAULT_MAX_WRITE_ONLY_FILE_SIZE;
  private int fileRolloverInterval = DEFAULT_WRITE_ONLY_FILE_ROLLOVER_INTERVAL;
  protected boolean isAutoCompact = HDFSCompactionConfig.DEFAULT_AUTO_COMPACTION;

  private AbstractHDFSCompactionConfigHolder compactionConfig = null;

  private HDFSEventQueueAttributes hdfsEventQueueAttrs = new HDFSEventQueueAttributesFactory().create();
  
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
      // initialize default compaction strategy and leave the rest for getting
      // set later
      this.compactionConfig = AbstractHDFSCompactionConfigHolder.createInstance(null);
      return;
    }
    
    this.name = config.getName();
    this.namenodeURL = config.getNameNodeURL();
    this.homeDir = config.getHomeDir();
    this.clientConfigFile = config.getHDFSClientConfigFile();
    setHDFSCompactionConfig(config.getHDFSCompactionConfig());
    this.blockCacheSize = config.getBlockCacheSize();
    setHDFSEventQueueAttributes(config.getHDFSEventQueueAttributes());
    this.maxFileSize = config.getMaxFileSize();
    this.fileRolloverInterval = config.getFileRolloverInterval();
    setMinorCompaction(config.getMinorCompaction());
  }
  
  public void resetDefaultValues() {
    name = null;
    namenodeURL = null;
    homeDir = null;
    clientConfigFile = null;
    blockCacheSize = -1f;
    maxFileSize = -1;
    fileRolloverInterval = -1;
    
    compactionConfig.resetDefaultValues();
    isAutoCompact = false;
    
    // TODO reset hdfseventqueueattributes;
  }
  
  public void copyFrom(HDFSStoreMutator mutator) {
    if (mutator.getFileRolloverInterval() >= 0) {
      logAttrMutation("fileRolloverInterval", mutator.getFileRolloverInterval());
      setFileRolloverInterval(mutator.getFileRolloverInterval());
    }
    if (mutator.getMaxFileSize() >= 0) {
      logAttrMutation("MaxFileSize", mutator.getFileRolloverInterval());
      setMaxFileSize(mutator.getMaxFileSize());
    }
    
    compactionConfig.copyFrom(mutator.getCompactionConfigMutator());
    if (mutator.getMinorCompaction() != null) {
      logAttrMutation("MinorCompaction", mutator.getMinorCompaction());
      setMinorCompaction(mutator.getMinorCompaction());
    }
    
    HDFSEventQueueAttributesFactory newFactory = new HDFSEventQueueAttributesFactory(hdfsEventQueueAttrs);
    HDFSEventQueueAttributesMutator qMutator = mutator.getHDFSEventQueueAttributesMutator();

    if (qMutator.getBatchSizeMB() >= 0) {
      logAttrMutation("batchSizeMB", mutator.getFileRolloverInterval());
      newFactory.setBatchSizeMB(qMutator.getBatchSizeMB());
    }
    if (qMutator.getBatchTimeInterval() >= 0) {
      logAttrMutation("batchTimeInterval", mutator.getFileRolloverInterval());
      newFactory.setBatchTimeInterval(qMutator.getBatchTimeInterval());
    }
    hdfsEventQueueAttrs = newFactory.create();
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
  
  /**
   * Sets the HDFS event queue attributes
   * This causes the store to use the {@link HDFSEventQueueAttributes}.
   * @param hdfsEventQueueAttrs the attributes of the HDFS Event queue
   */
  public HDFSStoreFactory setHDFSEventQueueAttributes(HDFSEventQueueAttributes hdfsEventQueueAttrs) {
    this.hdfsEventQueueAttrs  = hdfsEventQueueAttrs;
    return this;
  }
  @Override
  public HDFSEventQueueAttributes getHDFSEventQueueAttributes() {
    return hdfsEventQueueAttrs;
  }

  @Override
  public AbstractHDFSCompactionConfigHolder getHDFSCompactionConfig() {
    return compactionConfig;
  }
  @Override
  public HDFSStoreConfigHolder setHDFSCompactionConfig(HDFSCompactionConfig config) {
    if (config == null) {
      return this;
    }
    
    String s = config.getCompactionStrategy();
    compactionConfig = AbstractHDFSCompactionConfigHolder.createInstance(s);
    compactionConfig.copyFrom(config);
    return this;
  }
  @Override
  public HDFSCompactionConfigFactory createCompactionConfigFactory(String name) {
    return AbstractHDFSCompactionConfigHolder.createInstance(name);
  }
  
  @Override
  public HDFSStoreFactory setMaxFileSize(int maxFileSize) {
    assertIsPositive(CacheXml.HDFS_WRITE_ONLY_FILE_ROLLOVER_INTERVAL, maxFileSize);
    this.maxFileSize = maxFileSize;
    return this;
  }
  @Override
  public int getMaxFileSize() {
    return maxFileSize;
  }

  @Override
  public HDFSStoreFactory setFileRolloverInterval(int count) {
    assertIsPositive(CacheXml.HDFS_TIME_FOR_FILE_ROLLOVER, count);
    this.fileRolloverInterval = count;
    return this;
  }
  @Override
  public int getFileRolloverInterval() {
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

  /**
   * Abstract config class for compaction configuration. A concrete class must
   * extend setters for all configurations it consumes. This class will throw an
   * exception for any unexpected configuration. Concrete class must also
   * validate the configuration
   * 
   * @author ashvina
   */
  public static abstract class AbstractHDFSCompactionConfigHolder implements
      HDFSCompactionConfig, HDFSCompactionConfigFactory , Serializable{
    protected int maxInputFileSizeMB = HDFSCompactionConfig.DEFAULT_MAX_INPUT_FILE_SIZE_MB;
    protected int maxInputFileCount = HDFSCompactionConfig.DEFAULT_MAX_INPUT_FILE_COUNT;
    protected int minInputFileCount = HDFSCompactionConfig.DEFAULT_MIN_INPUT_FILE_COUNT;

    protected int maxConcurrency = HDFSCompactionConfig.DEFAULT_MAX_THREADS;
    
    protected boolean autoMajorCompact = HDFSCompactionConfig.DEFAULT_AUTO_MAJOR_COMPACTION;
    protected int majorCompactionConcurrency = HDFSCompactionConfig.DEFAULT_MAJOR_COMPACTION_MAX_THREADS;
    protected int majorCompactionIntervalMins = HDFSCompactionConfig.DEFAULT_MAJOR_COMPACTION_INTERVAL_MINS;
    protected int oldFileCleanupIntervalMins = HDFSCompactionConfig.DEFAULT_OLD_FILE_CLEANUP_INTERVAL_MINS;
    
    
    public AbstractHDFSCompactionConfigHolder() {
      
    }
    
    void copyFrom(HDFSCompactionConfig config) {
      setMaxInputFileSizeMB(config.getMaxInputFileSizeMB());
      setMaxInputFileCount(config.getMaxInputFileCount());
      setMinInputFileCount(config.getMinInputFileCount());
      setMaxThreads(config.getMaxThreads());
      setAutoMajorCompaction(config.getAutoMajorCompaction());
      setMajorCompactionMaxThreads(config.getMajorCompactionMaxThreads());
      setMajorCompactionIntervalMins(config.getMajorCompactionIntervalMins());
      setOldFilesCleanupIntervalMins(config.getOldFilesCleanupIntervalMins());
    }
    
    void copyFrom(HDFSCompactionConfigMutator mutator) {
      if (mutator.getMaxInputFileCount() >= 0) {
        logAttrMutation("maxInputFileCount", mutator.getMaxInputFileCount());
        setMaxInputFileCount(mutator.getMaxInputFileCount());
      }
      if (mutator.getMaxInputFileSizeMB() >= 0) {
        logAttrMutation("MaxInputFileSizeMB", mutator.getMaxInputFileSizeMB());
        setMaxInputFileSizeMB(mutator.getMaxInputFileSizeMB());
      }
      if (mutator.getMaxThreads() >= 0) {
        logAttrMutation("MaxThreads", mutator.getMaxThreads());
        setMaxThreads(mutator.getMaxThreads());
      }
      if (mutator.getMinInputFileCount() >= 0) {
        logAttrMutation("MinInputFileCount", mutator.getMinInputFileCount());
        setMinInputFileCount(mutator.getMinInputFileCount());
      }
      
      if (mutator.getMajorCompactionIntervalMins() > -1) {
        logAttrMutation("MajorCompactionIntervalMins", mutator.getMajorCompactionIntervalMins());
        setMajorCompactionIntervalMins(mutator.getMajorCompactionIntervalMins());
      }
      if (mutator.getMajorCompactionMaxThreads() >= 0) {
        logAttrMutation("MajorCompactionMaxThreads", mutator.getMajorCompactionMaxThreads());
        setMajorCompactionMaxThreads(mutator.getMajorCompactionMaxThreads());
      }
      if (mutator.getAutoMajorCompaction() != null) {
        logAttrMutation("AutoMajorCompaction", mutator.getAutoMajorCompaction());
        setAutoMajorCompaction(mutator.getAutoMajorCompaction());
      }
      
      if (mutator.getOldFilesCleanupIntervalMins() >= 0) {
        logAttrMutation("OldFilesCleanupIntervalMins", mutator.getOldFilesCleanupIntervalMins());
        setOldFilesCleanupIntervalMins(mutator.getOldFilesCleanupIntervalMins());
      }
    }
    
    void logAttrMutation(String name, Object value) {
      if (logger.isDebugEnabled()) {
        logger.debug("Alter " + name + ":" + value);
      }
    }
    
    public void resetDefaultValues() {
      maxInputFileSizeMB = -1;
      maxInputFileCount = -1;
      minInputFileCount = -1;
      maxConcurrency = -1;

      autoMajorCompact = false;
      majorCompactionConcurrency = -1;
      majorCompactionIntervalMins = -1;
      oldFileCleanupIntervalMins = -1;
    }

    @Override
    public HDFSCompactionConfigFactory setMaxInputFileSizeMB(int size) {
      throw new GemFireConfigException("This configuration is not applicable to configured compaction strategy");
    }
    @Override
    public int getMaxInputFileSizeMB() {
      return maxInputFileSizeMB;
    }

    @Override
    public HDFSCompactionConfigFactory setMinInputFileCount(int count) {
      throw new GemFireConfigException("This configuration is not applicable to configured compaction strategy");
    }
    @Override
    public int getMinInputFileCount() {
      return minInputFileCount;
    }

    @Override
    public HDFSCompactionConfigFactory setMaxInputFileCount(int size) {
      throw new GemFireConfigException("This configuration is not applicable to configured compaction strategy");
    }
    @Override
    public int getMaxInputFileCount() {
      return maxInputFileCount;
    }

    @Override
    public HDFSCompactionConfigFactory setMaxThreads(int count) {
      assertIsPositive(CacheXml.HDFS_MINOR_COMPACTION_THREADS, count);
      this.maxConcurrency = count;
      return this;
    }
    @Override
    public int getMaxThreads() {
      return maxConcurrency;
    }

    @Override
    public HDFSCompactionConfigFactory setAutoMajorCompaction(boolean auto) {
      this.autoMajorCompact = auto;
      return this;
    }
    @Override
    public boolean getAutoMajorCompaction() {
      return autoMajorCompact;
    }

    @Override
    public HDFSCompactionConfigFactory setMajorCompactionIntervalMins(int count) {
      throw new GemFireConfigException("This configuration is not applicable to configured compaction strategy");
    }
    @Override
    public int getMajorCompactionIntervalMins() {
      return majorCompactionIntervalMins;
    }

    @Override
    public HDFSCompactionConfigFactory setMajorCompactionMaxThreads(int count) {
      throw new GemFireConfigException("This configuration is not applicable to configured compaction strategy");
    }
    @Override
    public int getMajorCompactionMaxThreads() {
      return majorCompactionConcurrency;
    }
    
    
    @Override
    public int getOldFilesCleanupIntervalMins() {
      return oldFileCleanupIntervalMins ;
    }    
    @Override
    public HDFSCompactionConfigFactory setOldFilesCleanupIntervalMins(int interval) {
      assertIsPositive(CacheXml.HDFS_PURGE_INTERVAL, interval);
      this.oldFileCleanupIntervalMins = interval;
      return this;
    }

    @Override
    public HDFSCompactionConfig getConfigView() {
      return (HDFSCompactionConfig) this;
    }
    
    @Override
    public HDFSCompactionConfig create() throws GemFireConfigException {
      AbstractHDFSCompactionConfigHolder config = createInstance(getCompactionStrategy());
      config.copyFrom(this);
      config.validate();
      return config;
    }
    
    protected void validate() {
    }

    public static AbstractHDFSCompactionConfigHolder createInstance(String name) {
      if (name == null) {
        name = DEFAULT_STRATEGY;
      }

      if (name.equalsIgnoreCase(SIZE_ORIENTED)) {
        return new SizeTieredHdfsCompactionConfigHolder();
      }

      return new InvalidCompactionConfigHolder();
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      builder.append("AbstractHDFSCompactionConfigHolder@");
      builder.append(System.identityHashCode(this));
      builder.append("[autoMajorCompact=");
      builder.append(autoMajorCompact);
      builder.append(", ");
      if (maxInputFileSizeMB > -1) {
        builder.append("maxInputFileSizeMB=");
        builder.append(maxInputFileSizeMB);
        builder.append(", ");
      }
      if (maxInputFileCount > -1) {
        builder.append("maxInputFileCount=");
        builder.append(maxInputFileCount);
        builder.append(", ");
      }
      if (minInputFileCount > -1) {
        builder.append("minInputFileCount=");
        builder.append(minInputFileCount);
        builder.append(", ");
      }
      if (maxConcurrency > -1) {
        builder.append("maxConcurrency=");
        builder.append(maxConcurrency);
        builder.append(", ");
      }
      if (majorCompactionConcurrency > -1) {
        builder.append("majorCompactionConcurrency=");
        builder.append(majorCompactionConcurrency);
        builder.append(", ");
      }
      if (majorCompactionIntervalMins > -1) {
        builder.append("majorCompactionIntervalMins=");
        builder.append(majorCompactionIntervalMins);
        builder.append(", ");
      }
      if (oldFileCleanupIntervalMins > -1) {
        builder.append("oldFileCleanupIntervalMins=");
        builder.append(oldFileCleanupIntervalMins);
      }
      builder.append("]");
      return builder.toString();
    }
  }
  
  public static class InvalidCompactionConfigHolder extends AbstractHDFSCompactionConfigHolder {
    @Override
    public String getCompactionStrategy() {
      return INVALID;
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
    builder.append(" [name=");
    builder.append(name);
    builder.append(", ");
    if (namenodeURL != null) {
      builder.append("namenodeURL=");
      builder.append(namenodeURL);
      builder.append(", ");
    }
    if (homeDir != null) {
      builder.append("homeDir=");
      builder.append(homeDir);
      builder.append(", ");
    }
    if (clientConfigFile != null) {
      builder.append("clientConfigFile=");
      builder.append(clientConfigFile);
      builder.append(", ");
    }
    if (blockCacheSize > -1) {
      builder.append("blockCacheSize=");
      builder.append(blockCacheSize);
      builder.append(", ");
    }
    if (maxFileSize > -1) {
      builder.append("maxFileSize=");
      builder.append(maxFileSize);
      builder.append(", ");
    }
    if (fileRolloverInterval > -1) {
      builder.append("fileRolloverInterval=");
      builder.append(fileRolloverInterval);
      builder.append(", ");
    }
    builder.append("minorCompaction=");
    builder.append(isAutoCompact);
    builder.append(", ");

    if (compactionConfig != null) {
      builder.append("compactionConfig=");
      builder.append(compactionConfig);
      builder.append(", ");
    }
    if (hdfsEventQueueAttrs != null) {
      builder.append("hdfsEventQueueAttrs=");
      builder.append(hdfsEventQueueAttrs);
    }
    builder.append("]");
    return builder.toString();
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
}
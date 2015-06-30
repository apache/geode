/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.cache.hdfs.internal;

import com.gemstone.gemfire.GemFireConfigException;
import com.gemstone.gemfire.cache.hdfs.HDFSEventQueueAttributes;
import com.gemstone.gemfire.cache.hdfs.HDFSStore;
import com.gemstone.gemfire.cache.hdfs.HDFSStore.HDFSCompactionConfig;
import com.gemstone.gemfire.cache.hdfs.HDFSStoreFactory;
import com.gemstone.gemfire.cache.hdfs.StoreExistsException;
import com.gemstone.gemfire.cache.hdfs.internal.HDFSStoreConfigHolder.AbstractHDFSCompactionConfigHolder;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

/**
 * HDFS store configuration.
 * 
 * <pre>
 * {@code
 * <hdfs-store name="" home-dir="" namenode-url="">
 * <hdfs-compaction strategy="" auto-compact="" max-input-file-size-mb="" 
 *                  min-input-file-count="" max-input-file-count="" 
 *                  max-concurrency="" auto-major-compaction="" 
 *                  major-compaction-interval-mins="" major-compaction-concurrency=""/>
 * </hdfs-store>
 * }
 * </pre>
 * 
 * @author ashvina
 */
public class HDFSStoreCreation implements HDFSStoreFactory {
  protected HDFSStoreConfigHolder configHolder;
  
  public HDFSStoreCreation() {
    this(null);
  }

  /**
   * Copy constructor for HDFSStoreCreation
   * @param config configuration source for creating this instance 
   */
  public HDFSStoreCreation(HDFSStoreCreation config) {
    this.configHolder = new HDFSStoreConfigHolder(config == null ? null
        : config.configHolder);
  }

  @Override
  public HDFSStoreFactory setName(String name) {
    configHolder.setName(name);
    return this;
  }

  @Override
  public HDFSStoreFactory setNameNodeURL(String namenodeURL) {
    configHolder.setNameNodeURL(namenodeURL);
    return this;
  }

  @Override
  public HDFSStoreFactory setHomeDir(String homeDir) {
    configHolder.setHomeDir(homeDir);
    return this;
  }

  @Override
  public HDFSStoreFactory setHDFSClientConfigFile(String clientConfigFile) {
    configHolder.setHDFSClientConfigFile(clientConfigFile);
    return this;
  }
  
  @Override
  public HDFSStoreFactory setBlockCacheSize(float percentage) {
    configHolder.setBlockCacheSize(percentage);
    return this;
  }
  
  /**
   * Sets the HDFS event queue attributes
   * This causes the store to use the {@link HDFSEventQueueAttributes}.
   * @param hdfsEventQueueAttrs the attributes of the HDFS Event queue
   * @return a reference to this RegionFactory object
   * 
   */
  public HDFSStoreFactory setHDFSEventQueueAttributes(HDFSEventQueueAttributes hdfsEventQueueAttrs) {
    configHolder.setHDFSEventQueueAttributes(hdfsEventQueueAttrs);
    return this;
  }
  
  @Override
  public HDFSEventQueueAttributes getHDFSEventQueueAttributes() {
    return configHolder.getHDFSEventQueueAttributes();
  }

  @Override
  public HDFSStoreFactory setHDFSCompactionConfig(HDFSCompactionConfig config) {
    configHolder.setHDFSCompactionConfig(config);
    return this;
  }

  @Override
  public HDFSCompactionConfigFactory createCompactionConfigFactory(String name) {
    return configHolder.createCompactionConfigFactory(name);
  }
  @Override
  
  public HDFSStoreFactory setMaxFileSize(int maxFileSize) {
    configHolder.setMaxFileSize(maxFileSize);
    return this;
  }

  @Override
  public HDFSStoreFactory setFileRolloverInterval(int count) {
    configHolder.setFileRolloverInterval(count);
    return this;
  }

  @Override
  public HDFSStoreFactory setMinorCompaction(boolean auto) {
    configHolder.setMinorCompaction(auto);
    return this;
  }
  
  /**
   * Config class for compaction configuration. A concrete class must
   * extend setters for all configurations it consumes. This class will throw an
   * exception for any unexpected configuration. Concrete class must also
   * validate the configuration
   * 
   * @author ashvina
   */
  public static class HDFSCompactionConfigFactoryImpl implements
      HDFSCompactionConfigFactory {
    private AbstractHDFSCompactionConfigHolder configHolder;

    @Override
    public HDFSCompactionConfigFactory setMaxInputFileSizeMB(int size) {
      configHolder.setMaxInputFileSizeMB(size);
      return this;
    }

    @Override
    public HDFSCompactionConfigFactory setMinInputFileCount(int count) {
      configHolder.setMinInputFileCount(count);
      return this;
    }

    @Override
    public HDFSCompactionConfigFactory setMaxInputFileCount(int count) {
      configHolder.setMaxInputFileCount(count);
      return this;
    }

    @Override
    public HDFSCompactionConfigFactory setMaxThreads(int count) {
      configHolder.setMaxThreads(count);
      return this;
    }

    @Override
    public HDFSCompactionConfigFactory setAutoMajorCompaction(boolean auto) {
      configHolder.setAutoMajorCompaction(auto);
      return this;
    }

    @Override
    public HDFSCompactionConfigFactory setMajorCompactionIntervalMins(int count) {
      configHolder.setMajorCompactionIntervalMins(count);
      return this;
    }

    @Override
    public HDFSCompactionConfigFactory setMajorCompactionMaxThreads(int count) {
      configHolder.setMajorCompactionMaxThreads(count);
      return this;
    }
        
    @Override
    public HDFSCompactionConfigFactory setOldFilesCleanupIntervalMins(int interval) {
      configHolder.setOldFilesCleanupIntervalMins(interval);
      return this;
    }

    @Override
    public HDFSCompactionConfig getConfigView() {
      return configHolder.getConfigView();
    }
    
    @Override
    public HDFSCompactionConfig create() throws GemFireConfigException {
      HDFSCompactionConfigFactoryImpl config = createInstance(configHolder.getCompactionStrategy());
      config.configHolder.copyFrom(this.configHolder);
      config.configHolder.validate();
      return (HDFSCompactionConfig) config.configHolder;
    }
    
    private static HDFSCompactionConfigFactoryImpl createInstance(String name) {
      HDFSCompactionConfigFactoryImpl impl = new HDFSCompactionConfigFactoryImpl();
      impl.configHolder = AbstractHDFSCompactionConfigHolder.createInstance(name);
      return impl;
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

  public static void assertIsPositive(String name, int count) {
    if (count < 1) {
      throw new IllegalArgumentException(
          LocalizedStrings.DiskWriteAttributesImpl_0_HAS_TO_BE_POSITIVE_NUMBER_AND_THE_VALUE_GIVEN_1_IS_NOT_ACCEPTABLE
              .toLocalizedString(new Object[] { name, count }));
    }
  }
}
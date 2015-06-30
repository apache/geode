/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.hdfs.internal;

import com.gemstone.gemfire.cache.hdfs.HDFSStoreFactory.HDFSCompactionConfigFactory;
import com.gemstone.gemfire.cache.hdfs.internal.HDFSStoreConfigHolder.AbstractHDFSCompactionConfigHolder;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheXml;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

/**
 * Class for authorization and validation of HDFS size Tiered compaction config
 * 
 * @author ashvina
 */
public class SizeTieredHdfsCompactionConfigHolder extends AbstractHDFSCompactionConfigHolder {
  @Override
  public String getCompactionStrategy() {
    return SIZE_ORIENTED;
  }

  @Override
  public HDFSCompactionConfigFactory setMaxInputFileSizeMB(int size) {
    HDFSStoreCreation.assertIsPositive("HDFS_COMPACTION_MAX_INPUT_FILE_SIZE_MB", size);
    this.maxInputFileSizeMB = size;
    return this;
  }

  @Override
  public HDFSCompactionConfigFactory setMinInputFileCount(int count) {
    HDFSStoreCreation.assertIsPositive("HDFS_COMPACTION_MIN_INPUT_FILE_COUNT", count);
    this.minInputFileCount = count;
    return this;
  }

  @Override
  public HDFSCompactionConfigFactory setMaxInputFileCount(int count) {
    HDFSStoreCreation.assertIsPositive("HDFS_COMPACTION_MAX_INPUT_FILE_COUNT", count);
    this.maxInputFileCount = count;
    return this;
  }
  
  @Override
  public HDFSCompactionConfigFactory setMajorCompactionIntervalMins(int count) {
    HDFSStoreCreation.assertIsPositive(CacheXml.HDFS_MAJOR_COMPACTION_INTERVAL, count);
    this.majorCompactionIntervalMins = count;
    return this;
  }
  
  @Override
  public HDFSCompactionConfigFactory setMajorCompactionMaxThreads(int count) {
    HDFSStoreCreation.assertIsPositive(CacheXml.HDFS_MAJOR_COMPACTION_THREADS, count);
    this.majorCompactionConcurrency = count;
    return this;
  }

  @Override
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
    super.validate();
  }
}

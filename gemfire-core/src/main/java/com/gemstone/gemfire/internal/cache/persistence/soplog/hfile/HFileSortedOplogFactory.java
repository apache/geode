/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.persistence.soplog.hfile;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.hbase.io.hfile.BlockCache;
import org.apache.hadoop.hbase.io.hfile.Compression.Algorithm;
import org.apache.hadoop.hbase.io.hfile.HFileDataBlockEncoder;
import org.apache.hadoop.hbase.io.hfile.NoOpDataBlockEncoder;
import org.apache.hadoop.hbase.util.ChecksumType;

import com.gemstone.gemfire.internal.cache.persistence.soplog.HFileStoreStatistics;
import com.gemstone.gemfire.internal.cache.persistence.soplog.SortedOplog;
import com.gemstone.gemfire.internal.cache.persistence.soplog.SortedOplogFactory;
import com.gemstone.gemfire.internal.cache.persistence.soplog.SortedOplogFactory.SortedOplogConfiguration.Checksum;
import com.gemstone.gemfire.internal.cache.persistence.soplog.SortedOplogFactory.SortedOplogConfiguration.Compression;
import com.gemstone.gemfire.internal.cache.persistence.soplog.SortedOplogFactory.SortedOplogConfiguration.KeyEncoding;
import com.gemstone.gemfire.internal.cache.persistence.soplog.SortedOplogStatistics;

/**
 * Creates HFile soplogs.
 * 
 * @author bakera
 */
public class HFileSortedOplogFactory implements SortedOplogFactory {
  private final SortedOplogConfiguration config;
  
  public HFileSortedOplogFactory(String name, BlockCache blockCache, SortedOplogStatistics stats, HFileStoreStatistics storeStats) {
    config = new SortedOplogConfiguration(name, blockCache, stats, storeStats);
  }
  
  @Override
  public SortedOplogConfiguration getConfiguration() {
    return config;
  }

  @Override
  public SortedOplog createSortedOplog(File name) throws IOException {
    return new HFileSortedOplog(name, config);
  }
  
  public static ChecksumType convertChecksum(Checksum type) {
    switch (type) {
    case NONE:  return ChecksumType.NULL;
    
    default:
    case CRC32: return ChecksumType.CRC32;
    }
  }

  public static Algorithm convertCompression(Compression type) {
    switch (type) {
    default:
    case NONE: return Algorithm.NONE;
    }
  }
  
  public static HFileDataBlockEncoder convertEncoding(KeyEncoding type) {
    switch (type) {
    default:
    case NONE: return NoOpDataBlockEncoder.INSTANCE;
    }
  }
}

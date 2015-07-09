/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.hdfs.internal.hoplog.mapred;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.ClientCache;

/**
 * Output format for gemfire. The records provided to writers created by this
 * output format are PUT in a live gemfire cluster.
 * 
 * @author ashvina
 */
public class GFOutputFormat extends
    com.gemstone.gemfire.cache.hdfs.internal.hoplog.mapreduce.GFOutputFormat
    implements OutputFormat<Object, Object> {

  @Override
  public RecordWriter<Object, Object> getRecordWriter(
      FileSystem ignored, JobConf job, String name, Progressable progress)
      throws IOException {
    ClientCache cache = getClientCacheInstance(job);
    return new GFRecordWriter(cache, job);
  }
  
  @Override
  public void checkOutputSpecs(FileSystem ignored, JobConf job)
      throws IOException {
    validateConfiguration(job);
  }

  public class GFRecordWriter implements RecordWriter<Object, Object> {
    private ClientCache clientCache;
    private Region<Object, Object> region;

    public GFRecordWriter(ClientCache cache, Configuration conf) {
      this.clientCache = cache;
      region = getRegionInstance(conf, clientCache);
    }
    
    @Override
    public void write(Object key, Object value) throws IOException {
      executePut(region, key, value);
    }

    @Override
    public void close(Reporter reporter) throws IOException {
      closeClientCache(clientCache);
      // TODO update reporter
    }
  }
}

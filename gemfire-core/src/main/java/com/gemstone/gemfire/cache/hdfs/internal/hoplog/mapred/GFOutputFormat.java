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

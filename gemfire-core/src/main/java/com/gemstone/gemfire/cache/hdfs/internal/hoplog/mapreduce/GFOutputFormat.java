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
package com.gemstone.gemfire.cache.hdfs.internal.hoplog.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionExistsException;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.client.ClientRegionFactory;
import com.gemstone.gemfire.cache.client.ClientRegionShortcut;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.management.internal.cli.converters.ConnectionEndpointConverter;

/**
 * Output format for gemfire. The records provided to writers created by this
 * output format are PUT in a live gemfire cluster.
 * 
 * @author ashvina
 */
public class GFOutputFormat extends OutputFormat<Object, Object> {
  public static final String REGION = "mapreduce.output.gfoutputformat.outputregion";
  public static final String LOCATOR_HOST = "mapreduce.output.gfoutputformat.locatorhost";
  public static final String LOCATOR_PORT = "mapreduce.output.gfoutputformat.locatorport";
  public static final String SERVER_HOST = "mapreduce.output.gfoutputformat.serverhost";
  public static final String SERVER_PORT = "mapreduce.output.gfoutputformat.serverport";

  @Override
  public RecordWriter<Object, Object> getRecordWriter(TaskAttemptContext context)
      throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    ClientCache cache = getClientCacheInstance(conf);
    return new GFRecordWriter(cache, context.getConfiguration());
  }

  public ClientCache getClientCacheInstance(Configuration conf) {
    // if locator host is provided create a client cache instance using
    // connection to locator. If locator is not provided and server host is also
    // not provided, connect using default locator
    ClientCache cache;
    String serverHost = conf.get(SERVER_HOST);
    if (serverHost == null || serverHost.isEmpty()) {
      cache = createGFWriterUsingLocator(conf);
    } else {
      cache = createGFWriterUsingServer(conf);
    }
    return cache;
  }

  /**
   * Creates instance of {@link ClientCache} by connecting to GF cluster through
   * locator
   */
  public ClientCache createGFWriterUsingLocator(Configuration conf) {
    // if locator host is not provided assume localhost
    String locator = conf.get(LOCATOR_HOST,
        ConnectionEndpointConverter.DEFAULT_LOCATOR_HOST);
    // if locator port is not provided assume default locator port 10334
    int port = conf.getInt(LOCATOR_PORT,
        ConnectionEndpointConverter.DEFAULT_LOCATOR_PORT);

    // create gemfire client cache instance
    ClientCacheFactory ccf = new ClientCacheFactory();
    ccf.addPoolLocator(locator, port);
    ClientCache cache = ccf.create();
    return cache;
  }

  /**
   * Creates instance of {@link ClientCache} by connecting to GF cluster through
   * GF server
   */
  public ClientCache createGFWriterUsingServer(Configuration conf) {
    String server = conf.get(SERVER_HOST);
    // if server port is not provided assume default server port, 40404
    int port = conf.getInt(SERVER_PORT, CacheServer.DEFAULT_PORT);

    // create gemfire client cache instance
    ClientCacheFactory ccf = new ClientCacheFactory();
    ccf.addPoolServer(server, port);
    ClientCache cache = ccf.create();
    return cache;
  }

  public Region<Object, Object> getRegionInstance(Configuration conf,
      ClientCache cache) {
    Region<Object, Object> region;

    // create gemfire region in proxy mode
    String regionName = conf.get(REGION);
    ClientRegionFactory<Object, Object> regionFactory = cache
        .createClientRegionFactory(ClientRegionShortcut.PROXY);
    try {
      region = regionFactory.create(regionName);
    } catch (RegionExistsException e) {
      region = cache.getRegion(regionName);
    }

    return region;
  }

  /**
   * Puts a K-V pair in region
   * @param region
   * @param key
   * @param value
   */
  public void executePut(Region<Object, Object> region, Object key, Object value) {
    region.put(key, value);
  }

  /**
   * Closes client cache instance
   * @param clientCache
   */
  public void closeClientCache(ClientCache clientCache) {
    if (clientCache != null && !clientCache.isClosed()) {
      clientCache.close();
    }
  }

  /**
   * Validates correctness and completeness of job's output configuration
   * 
   * @param conf
   * @throws InvalidJobConfException
   */
  protected void validateConfiguration(Configuration conf)
      throws InvalidJobConfException {
    // User must configure the output region name.
    String region = conf.get(REGION);
    if (region == null || region.trim().isEmpty()) {
      throw new InvalidJobConfException("Output Region name not provided.");
    }

    // TODO validate if a client connected to gemfire cluster can be created
  }
  
  @Override
  public void checkOutputSpecs(JobContext context) throws IOException,
      InterruptedException {
    Configuration conf = context.getConfiguration();
    validateConfiguration(conf);
  }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context)
      throws IOException, InterruptedException {
    return new FileOutputCommitter(FileOutputFormat.getOutputPath(context),
        context);
  }

  public class GFRecordWriter extends RecordWriter<Object, Object> {
    private ClientCache clientCache;
    private Region<Object, Object> region;

    public GFRecordWriter(ClientCache cache, Configuration conf) {
      this.clientCache = cache;
      region = getRegionInstance(conf, clientCache);
    }

    @Override
    public void write(Object key, Object value) throws IOException,
        InterruptedException {
      executePut(region, key, value);
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException,
        InterruptedException {
      closeClientCache(clientCache);
    }
  }
}

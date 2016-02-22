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
package com.gemstone.gemfire.cache.hdfs.internal.hoplog;


/**
 * This interface defines all the hoplog configuration related constants. One
 * location simplifies searching for a constant
 * 
 * @author ashvina
 */
public interface HoplogConfig {
  // max number of open files per bucket. by default each region has 113
  // buckets. A typical hdfs deployment has 5 DN each allowing 4096 open
  // files. The intent is to use around 40 % of these and hence the default
  // value is 72
  public static final String BUCKET_MAX_OPEN_HFILES_CONF = "hoplog.bucket.max.open.files";
  public final Integer BUCKET_MAX_OPEN_HFILES_DEFAULT = 72;
  
  public static final String HFILE_BLOCK_SIZE_CONF = "hoplog.hfile.block.size";
  
  // Region maintenance activity interval. default is 2 mins
  public static final String JANITOR_INTERVAL_SECS = "hoplog.janitor.interval.secs";
  public static final long JANITOR_INTERVAL_SECS_DEFAULT = 120l;
  
  // Maximum number of milliseconds to wait for suspension action to complete
  public static final String SUSPEND_MAX_WAIT_MS = "hoplog.suspend.max.wait.ms";
  public static final long SUSPEND_MAX_WAIT_MS_DEFAULT = 1000l;
  
  // Compaction request queue limit configuraiton
  public static final String COMPCATION_QUEUE_CAPACITY = "hoplog.compaction.queue.capacity";
  public static final int COMPCATION_QUEUE_CAPACITY_DEFAULT = 500;
  
  // Compaction request queue limit configuraiton
  public static final String COMPACTION_FILE_RATIO = "hoplog.compaction.file.ratio";
  public static final float COMPACTION_FILE_RATIO_DEFAULT = 1.3f;
  
  //Amount of time before deleting old temporary files
  public static final String TMP_FILE_EXPIRATION = "hoplog.tmp.file.expiration.ms";
  public static final long TMP_FILE_EXPIRATION_DEFAULT = 10 * 60 * 1000;
  
  // If this property is set as true, GF will let DFS client cache FS objects
  public static final String USE_FS_CACHE = "hoplog.use.fs.cache";

  // If set hdfs store will be able to connect to local file System
  public static final String ALLOW_LOCAL_HDFS_PROP = "hoplog.ALLOW_LOCAL_HDFS";
  
  // The following constants are used to read kerberos authentication related
  // configuration. Currently these configurations are provided as client config
  // file while hdfs store is created
  public static final String KERBEROS_PRINCIPAL = "gemfirexd.kerberos.principal";
  public static final String KERBEROS_KEYTAB_FILE= "gemfirexd.kerberos.keytab.file";
  public static final String PERFORM_SECURE_HDFS_CHECK_PROP = "gemfire.PERFORM_SECURE_HDFS_CHECK";
  
  // clean up interval file that exposed to MapReduce jobs
  public static final String CLEAN_UP_INTERVAL_FILE_NAME = "cleanUpInterval";
  // Compression settings
  public static final String COMPRESSION = "hoplog.compression.algorithm";
  public static final String COMPRESSION_DEFAULT = "NONE";
  
}

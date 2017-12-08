/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.management.internal.cli.util;

/***
 * Contains the names for the region attributes used by describe region command
 *
 */
public class RegionAttributesNames {

  public static final String CONCURRENCY_LEVEL = "concurrency-level";
  public static final String DATA_POLICY = "data-policy";
  public static final String ENABLE_ASYNC_CONFLATION = "enable-async-conflation";
  public static final String ENABLE_GATEWAY = "enable-gateway";
  public static final String ENABLE_SUBSCRIPTION_CONFLATION = "enable-subscription-conflation";
  public static final String IGNORE_JTA = "ignore-jta";
  public static final String INITIAL_CAPACITY = "initial-capacity";
  public static final String IS_LOCK_GRANTOR = "is-lock-grantor";
  public static final String LOAD_FACTOR = "load-factor";
  public static final String MULTICAST_ENABLED = "multicast-enabled";
  public static final String SCOPE = "scope";
  public static final String STATISTICS_ENABLED = "statistics-enabled";
  public static final String CLONING_ENABLED = "cloning-enabled";
  public static final String CONCURRENCY_CHECK_ENABLED = "concurrency-checks-enabled";
  public static final String DISK_STORE_NAME = "disk-store-name";
  public static final String CACHE_LISTENERS = "cache-listeners";
  public static final String CACHE_LOADER = "cache-loader";
  public static final String CACHE_WRITER = "cache-writer";
  public static final String GATEWAY_HUB_ID = "gateway-hub-id";
  public static final String INDEX_MAINTENANCE_SYNCHRONOUS = "index-maintenance-synchronous";
  public static final String POOL_NAME = "pool-name";
  public static final String COMPRESSOR = "compressor";
  public static final String OFF_HEAP = "off-heap";
  public static final String ASYNC_EVENT_QUEUE_ID = "async-event-queue-id";
  public static final String GATEWAY_SENDER_ID = "gateway-sender-id";

  // Partition attributes
  public static final String LOCAL_MAX_MEMORY = "local-max-memory";
  public static final String REDUNDANT_COPIES = "redundant-copies";
  public static final String TOTAL_MAX_MEMORY = "total-max-memory";
  public static final String TOTAL_NUM_BUCKETS = "total-num-buckets";
  public static final String COLOCATED_WITH = "colocated with";
  public static final String RECOVERY_DELAY = "recovery-delay";
  public static final String STARTUP_RECOVERY_DELAY = "startup-recovery-delay";
  public static final String PARTITION_RESOLVER = "partition-resolver";
  public static final String PARTITION_LISTENERS = "partition-listeners";



  // Region element attributes
  // region-time-to-live
  public static final String REGION_TIME_TO_LIVE = "region-time-to-live.timeout";
  public static final String REGION_TIME_TO_LIVE_ACTION = "region-time-to-live.action";

  // region-idle-time
  public static final String REGION_IDLE_TIMEOUT = "region-idle-time.timeout";
  public static final String REGION_IDLE_TIMEOUT_ACTION = "region-idle-time.action";

  // entry-time-to-live
  public static final String ENTRY_TIME_TO_LIVE = "entry-time-to-live.timeout";
  public static final String ENTRY_TIME_TO_LIVE_ACTION = "entry-time-to-live.action";

  // entry-idle-time
  public static final String ENTRY_IDLE_TIMEOUT = "entry-idle-time.timeout";
  public static final String ENTRY_IDLE_TIMEOUT_ACTION = "entry-idle-time.action";


  // EVICTION ATTRIBUTES
  public static final String EVICTION_ACTION = "eviction-action";
  public static final String EVICTION_ALGORITHM = "eviction-algorithm";
  public static final String EVICTION_MAX_VALUE = "eviction-maximum-value";
}

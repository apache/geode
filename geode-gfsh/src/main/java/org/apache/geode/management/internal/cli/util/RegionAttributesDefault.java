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

import org.apache.geode.annotations.Immutable;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.ExpirationAction;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Scope;

/***
 * Contains the default values for the region attributes
 *
 */
public class RegionAttributesDefault {
  @Immutable
  public static final Scope SCOPE = Scope.DISTRIBUTED_NO_ACK;
  @Immutable
  public static final DataPolicy DATA_POLICY = DataPolicy.DEFAULT;
  public static final boolean CLONING_ENABLED = false;
  public static final boolean CONCURRENCY_CHECK_ENABLED = true;
  public static final boolean ENABLE_ASYNC_CONFLATION = false;
  public static final boolean ENABLE_SUBSCRIPTION_CONFLATION = false;
  public static final boolean IGNORE_JTA = false;
  public static final boolean INDEX_MAINTENANCE_SYNCHRONOUS = true;
  public static final boolean MULTICAST_ENABLED = false;
  public static final int CONCURRENCY_LEVEL = 16;
  public static final String DISK_STORE_NAME = "";
  public static final int INITIAL_CAPACITY = 16;
  public static final float LOAD_FACTOR = 0.75f;
  public static final String POOL_NAME = "";
  public static final boolean STATISTICS_ENABLED = false;
  public static final boolean IS_LOCK_GRANTOR = false;
  public static final String COMPRESSOR_CLASS_NAME = null;

  public static final int ENTRY_TIME_TO_LIVE = 0;
  public static final int REGION_TIME_TO_LIVE = 0;
  public static final int ENTRY_IDLE_TIMEOUT = 0;
  public static final int REGION_IDLE_TIMEOUT = 0;
  public static final String ENTRY_TIME_TO_LIVE_ACTION = ExpirationAction.INVALIDATE.toString();
  public static final String REGION_TIME_TO_LIVE_ACTION = ExpirationAction.INVALIDATE.toString();
  public static final String ENTRY_IDLE_TIMEOUT_ACTION = ExpirationAction.INVALIDATE.toString();
  public static final String REGION_IDLE_TIMEOUT_ACTION = ExpirationAction.INVALIDATE.toString();

  // PA
  // Partition attributes
  public static final int REDUNDANT_COPIES = 0;
  public static final int TOTAL_NUM_BUCKETS = PartitionAttributesFactory.GLOBAL_MAX_BUCKETS_DEFAULT;
  public static final String COLOCATED_WITH = "";
  public static final long RECOVERY_DELAY = PartitionAttributesFactory.RECOVERY_DELAY_DEFAULT;
  public static final long STARTUP_RECOVERY_DELAY =
      PartitionAttributesFactory.STARTUP_RECOVERY_DELAY_DEFAULT;
  public static final String PARTITION_RESOLVER = "";


  // EVICTION ATTRIBUTES
  public static final String EVICTION_ACTION = EvictionAction.NONE.toString();
  public static final String EVICTION_ALGORITHM = EvictionAction.NONE.toString();
  public static final long EVICTION_MAX_VALUE = 0;

  public static final boolean OFF_HEAP = false;

}

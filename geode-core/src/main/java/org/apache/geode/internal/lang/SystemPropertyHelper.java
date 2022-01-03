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
package org.apache.geode.internal.lang;

import static org.apache.geode.internal.lang.SystemProperty.getProductBooleanProperty;

import org.apache.geode.internal.cache.eviction.LRUListWithAsyncSorting;

/**
 * The SystemPropertyHelper class is an helper class for accessing system properties used in geode.
 * The method name to get the system property should be the same as the system property name.
 *
 * @since Geode 1.4.0
 */
public class SystemPropertyHelper {

  /**
   * When set to "true" enables asynchronous eviction algorithm (defaults to true). For more details
   * see {@link LRUListWithAsyncSorting}.
   *
   * @since Geode 1.4.0
   */
  public static final String EVICTION_SCAN_ASYNC = "EvictionScanAsync";

  /**
   * This property allows the maximum number of threads used for asynchronous eviction scanning to
   * be configured. It defaults to "Math.max((Runtime.getRuntime().availableProcessors() / 4), 1)".
   * For more details see {@link LRUListWithAsyncSorting}.
   *
   * @since Geode 1.4.0
   */
  public static final String EVICTION_SCAN_MAX_THREADS = "EvictionScanMaxThreads";

  /**
   * This property allows configuration of the threshold percentage at which an asynchronous scan is
   * started. If the number of entries that have been recently used since the previous scan divided
   * by total number of entries exceeds the threshold then a scan is started. The default threshold
   * is 25. If the threshold is less than 0 or greater than 100 then the default threshold is used.
   * For more details see {@link LRUListWithAsyncSorting}.
   *
   * @since Geode 1.4.0
   */
  public static final String EVICTION_SCAN_THRESHOLD_PERCENT = "EvictionScanThresholdPercent";

  public static final String EVICTION_SEARCH_MAX_ENTRIES = "lru.maxSearchEntries";

  public static final String EARLY_ENTRY_EVENT_SERIALIZATION = "earlyEntryEventSerialization";

  public static final String DEFAULT_DISK_DIRS_PROPERTY = "defaultDiskDirs";

  public static final String HA_REGION_QUEUE_EXPIRY_TIME_PROPERTY = "MessageTimeToLive";

  public static final String THREAD_ID_EXPIRY_TIME_PROPERTY = "threadIdExpiryTime";

  public static final String PERSISTENT_VIEW_RETRY_TIMEOUT_SECONDS =
      "PERSISTENT_VIEW_RETRY_TIMEOUT_SECONDS";

  public static final String USE_HTTP_SYSTEM_PROPERTY = "useHTTP";

  /**
   * This property allows users to enable retrying when client application encounters
   * PdxSerializationException. The default setting is false, and PdxSerializationException will not
   * be retried. It will cause client application to throw ServerOperationException. When the
   * property is set to true, the client application will automatically retry the operation to
   * another server if encountered PdxSerializationException.
   *
   * @since Geode 1.15.0
   */
  public static final String ENABLE_QUERY_RETRY_ON_PDX_SERIALIZATION_EXCEPTION =
      "enableQueryRetryOnPdxSerializationException";

  /**
   * a comma separated string to list out the packages to scan. If not specified, the entire
   * classpath is scanned.
   *
   * <p>
   * This is used by the FastPathScanner to scan for:
   * 1. XSDRootElement annotation
   *
   * @since Geode 1.7.0
   */
  public static final String PACKAGES_TO_SCAN = "packagesToScan";

  /**
   * This property turns on/off parallel disk store recovery during cluster restart.
   * By default, the value is True, which allows parallel disk store recovery by multiple threads.
   */
  public static final String PARALLEL_DISK_STORE_RECOVERY = "parallelDiskStoreRecovery";

  /**
   * Milliseconds to wait before retrying to get events for a transaction from the
   * gateway sender queue when group-transaction-events is true.
   */
  public static final String GET_TRANSACTION_EVENTS_FROM_QUEUE_WAIT_TIME_MS =
      "get-transaction-events-from-queue-wait-time-ms";

  /**
   * Milliseconds to wait for the client to re-authenticate back before unregister this client
   * proxy. If client re-authenticate back successfully within this period, messages will continue
   * to be delivered to the client
   */
  public static final String RE_AUTHENTICATE_WAIT_TIME = "reauthenticate.wait.time";

  /**
   * As of Geode 1.4.0, a region set operation will be in a transaction even if it is the first
   * operation in the transaction.
   *
   * <p>
   * In previous releases, a region set operation is not in a transaction if it is the first
   * operation of the transaction.
   *
   * <p>
   * Setting this system property to true will restore the previous behavior.
   *
   * @since Geode 1.4.0
   */
  public static boolean restoreSetOperationTransactionBehavior() {
    return getProductBooleanProperty("restoreSetOperationTransactionBehavior").orElse(false);
  }

  /**
   * As of Geode 1.4.0, idle expiration on a replicate or partitioned region will now do a
   * distributed check for a more recent last access time on one of the other copies of the region.
   *
   * <p>
   * This system property can be set to true to turn off this new check and restore the previous
   * behavior of only using the local last access time as the basis for expiration.
   *
   * @since Geode 1.4.0
   */
  public static boolean restoreIdleExpirationBehavior() {
    return getProductBooleanProperty("restoreIdleExpirationBehavior").orElse(false);
  }

}

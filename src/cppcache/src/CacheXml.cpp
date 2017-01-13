/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "CacheXml.hpp"

using namespace gemfire;

CacheXml::CacheXml() {
  /** The name of the <code>cache</code> element */
  CACHE = "cache";
  CLIENT_CACHE = "client-cache";
  PDX = "pdx";

  /** The name of the <code>redundancy-level</code> element */
  REDUNDANCY_LEVEL = "redundancy-level";

  /** The name of the <code>region</code> element */
  REGION = "region";

  /** The name of the <code>root-region</code> element */
  ROOT_REGION = "root-region";

  /** The name of the <code>region-attributes</code> element */
  REGION_ATTRIBUTES = "region-attributes";

  LRU_ENTRIES_LIMIT = "lru-entries-limit";

  DISK_POLICY = "disk-policy";

  ENDPOINTS = "endpoints";

  /** The name of the <code>region-time-to-live</code> element */
  REGION_TIME_TO_LIVE = "region-time-to-live";

  /** The name of the <code>region-idle-time</code> element */
  REGION_IDLE_TIME = "region-idle-time";

  /** The name of the <code>entry-time-to-live</code> element */
  ENTRY_TIME_TO_LIVE = "entry-time-to-live";

  /** The name of the <code>entry-idle-time</code> element */
  ENTRY_IDLE_TIME = "entry-idle-time";

  /** The name of the <code>expiration-attributes</code> element */
  EXPIRATION_ATTRIBUTES = "expiration-attributes";

  /** The name of the <code>cache-loader</code> element */
  CACHE_LOADER = "cache-loader";

  /** The name of the <code>cache-writer</code> element */
  CACHE_WRITER = "cache-writer";

  /** The name of the <code>cache-listener</code> element */
  CACHE_LISTENER = "cache-listener";

  /** The name of the <code>partition-resolver</code> element */
  PARTITION_RESOLVER = "partition-resolver";

  LIBRARY_NAME = "library-name";

  LIBRARY_FUNCTION_NAME = "library-function-name";

  CACHING_ENABLED = "caching-enabled";

  INTEREST_LIST_ENABLED = "interest-list-enabled";

  MAX_DISTRIBUTE_VALUE_LENGTH_WHEN_CREATE =
      "max-distribute-value-length-when-create";
  /** The name of the <code>scope</code> attribute */
  SCOPE = "scope";

  /** The name of the <code>client-notification</code> attribute */
  CLIENT_NOTIFICATION_ENABLED = "client-notification";

  /** The name of the <code>initial-capacity</code> attribute */
  INITIAL_CAPACITY = "initial-capacity";

  /** The name of the <code>initial-capacity</code> attribute */
  CONCURRENCY_LEVEL = "concurrency-level";

  /** The name of the <code>load-factor</code> attribute */
  LOAD_FACTOR = "load-factor";

  /** The name of the <code>statistics-enabled</code> attribute */
  STATISTICS_ENABLED = "statistics-enabled";

  /** The name of the <code>timeout</code> attribute */
  TIMEOUT = "timeout";

  /** The name of the <code>action</code> attribute */
  ACTION = "action";

  /** The name of the <code>local</code> value */
  LOCAL = "local";

  /** The name of the <code>distributed-no-ack</code> value */
  DISTRIBUTED_NO_ACK = "distributed-no-ack";

  /** The name of the <code>distributed-ack</code> value */
  DISTRIBUTED_ACK = "distributed-ack";

  /** The name of the <code>global</code> value */
  GLOBAL = "global";

  /** The name of the <code>invalidate</code> value */
  INVALIDATE = "invalidate";

  /** The name of the <code>destroy</code> value */
  DESTROY = "destroy";

  /** The name of the <code>overflow</code> value */
  OVERFLOWS = "overflows";

  /** The name of the <code>overflow</code> value */
  PERSIST = "persist";

  /** The name of the <code>none</code> value */
  NONE = "none";

  /** The name of the <code>local-invalidate</code> value */
  LOCAL_INVALIDATE = "local-invalidate";

  /** The name of the <code>local-destroy</code> value */
  LOCAL_DESTROY = "local-destroy";

  /** The name of the <code>persistence-manager</code> value */
  PERSISTENCE_MANAGER = "persistence-manager";

  /** The name of the <code>properties</code> value */
  PROPERTIES = "properties";

  /** The name of the <code>property</code> value */
  PROPERTY = "property";

  CONCURRENCY_CHECKS_ENABLED = "concurrency-checks-enabled";

  TOMBSTONE_TIMEOUT = "tombstone-timeout";

  /** Pool elements and attributes */

  POOL_NAME = "pool-name";
  POOL = "pool";
  NAME = "name";
  LOCATOR = "locator";
  SERVER = "server";
  HOST = "host";
  PORT = "port";
  IGNORE_UNREAD_FIELDS = "ignore-unread-fields";
  READ_SERIALIZED = "read-serialized";
  FREE_CONNECTION_TIMEOUT = "free-connection-timeout";
  MULTIUSER_SECURE_MODE = "multiuser-authentication";
  IDLE_TIMEOUT = "idle-timeout";
  LOAD_CONDITIONING_INTERVAL = "load-conditioning-interval";
  MAX_CONNECTIONS = "max-connections";
  MIN_CONNECTIONS = "min-connections";
  PING_INTERVAL = "ping-interval";
  UPDATE_LOCATOR_LIST_INTERVAL = "update-locator-list-interval";
  READ_TIMEOUT = "read-timeout";
  RETRY_ATTEMPTS = "retry-attempts";
  SERVER_GROUP = "server-group";
  SOCKET_BUFFER_SIZE = "socket-buffer-size";
  STATISTIC_INTERVAL = "statistic-interval";
  SUBSCRIPTION_ACK_INTERVAL = "subscription-ack-interval";
  SUBSCRIPTION_ENABLED = "subscription-enabled";
  SUBSCRIPTION_MTT = "subscription-message-tracking-timeout";
  SUBSCRIPTION_REDUNDANCY = "subscription-redundancy";
  THREAD_LOCAL_CONNECTIONS = "thread-local-connections";
  CLONING_ENABLED = "cloning-enabled";
  ID = "id";
  REFID = "refid";
  PR_SINGLE_HOP_ENABLED = "pr-single-hop-enabled";
}

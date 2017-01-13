#ifndef __GEMFIRE_CACHEXML_HPP__
#define __GEMFIRE_CACHEXML_HPP__
/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

#include <gfcpp/gfcpp_globals.hpp>

namespace gemfire {

class CPPCACHE_EXPORT CacheXml {
 public:
  /** The name of the <code>cache</code> element */
  const char* CACHE;

  /** The name of the <code>client-cache</code> element */
  const char* CLIENT_CACHE;

  /** The name of the <code>redundancy-level</code> attribute **/
  const char* REDUNDANCY_LEVEL;

  /** The name of the <code>region</code> element */
  const char* REGION;

  /** The name of the <code>pdx</code> element */
  const char* PDX;

  /** The name of the <code>vm-root-region</code> element */
  const char* ROOT_REGION;

  /** The name of the <code>region-attributes</code> element */
  const char* REGION_ATTRIBUTES;

  /** The name of the <code>key-constraint</code> element */
  //  const char* KEY_CONSTRAINT";

  const char* LRU_ENTRIES_LIMIT;

  /** The name of the <code>lru-eviction-action</code> attribute **/
  const char* DISK_POLICY;

  /** The name of the <code>endpoints</code> attribute **/
  const char* ENDPOINTS;

  /** The name of the <code>region-time-to-live</code> element */
  const char* REGION_TIME_TO_LIVE;

  /** The name of the <code>region-idle-time</code> element */
  const char* REGION_IDLE_TIME;

  /** The name of the <code>entry-time-to-live</code> element */
  const char* ENTRY_TIME_TO_LIVE;

  /** The name of the <code>entry-idle-time</code> element */
  const char* ENTRY_IDLE_TIME;

  /** The name of the <code>expiration-attributes</code> element */
  const char* EXPIRATION_ATTRIBUTES;

  /** The name of the <code>cache-loader</code> element */
  const char* CACHE_LOADER;

  /** The name of the <code>cache-writer</code> element */
  const char* CACHE_WRITER;

  /** The name of the <code>cache-listener</code> element */
  const char* CACHE_LISTENER;

  /** The name of the <code>partition-resolver</code> element */
  const char* PARTITION_RESOLVER;

  const char* LIBRARY_NAME;

  const char* LIBRARY_FUNCTION_NAME;

  const char* CACHING_ENABLED;

  const char* INTEREST_LIST_ENABLED;

  const char* MAX_DISTRIBUTE_VALUE_LENGTH_WHEN_CREATE;

  /** The name of the <code>scope</code> attribute */
  const char* SCOPE;

  /** The name of the <code>client-notification</code> attribute */
  const char* CLIENT_NOTIFICATION_ENABLED;

  /** The name of the <code>keep-alive-timeout</code> attribute */
  //??  const char* KEEP_ALIVE_TIMEOUT;

  /** The name of the <code>initial-capacity</code> attribute */
  const char* INITIAL_CAPACITY;

  /** The name of the <code>initial-capacity</code> attribute */
  const char* CONCURRENCY_LEVEL;

  /** The name of the <code>serialize-values</code> attribute */
  //  const char* SERIALIZE_VALUES;

  /** The name of the <code>load-factor</code> attribute */
  const char* LOAD_FACTOR;

  /** The name of the <code>statistics-enabled</code> attribute */
  const char* STATISTICS_ENABLED;

  /** The name of the <code>timeout</code> attribute */
  const char* TIMEOUT;

  /** The name of the <code>action</code> attribute */
  const char* ACTION;

  /** The name of the <code>local</code> value */
  const char* LOCAL;

  /** The name of the <code>distributed-no-ack</code> value */
  const char* DISTRIBUTED_NO_ACK;

  /** The name of the <code>distributed-ack</code> value */
  const char* DISTRIBUTED_ACK;

  /** The name of the <code>global</code> value */
  const char* GLOBAL;

  /** The name of the <code>invalidate</code> value */

  const char* INVALIDATE;

  /** The name of the <code>destroy</code> value */
  const char* DESTROY;

  /** The name of the <code>overflows</code> value */
  const char* OVERFLOWS;

  /** The name of the <code>persist</code> value */
  const char* PERSIST;

  /** The name of the <code>none</code> value */
  const char* NONE;

  /** The name of the <code>local-invalidate</code> value */
  const char* LOCAL_INVALIDATE;

  /** The name of the <code>local-destroy</code> value */
  const char* LOCAL_DESTROY;

  /** The name of the <code>persistence-manager</code> value */
  const char* PERSISTENCE_MANAGER;

  /** The name of the <code>properties</code> value */
  const char* PROPERTIES;

  /** The name of the <code>property</code> value */
  const char* PROPERTY;

  /** Pool elements and attributes */

  const char* POOL_NAME;
  const char* POOL;
  const char* NAME;
  const char* LOCATOR;
  const char* SERVER;
  const char* HOST;
  const char* PORT;
  const char* IGNORE_UNREAD_FIELDS;
  const char* READ_SERIALIZED;
  const char* FREE_CONNECTION_TIMEOUT;
  const char* IDLE_TIMEOUT;
  const char* LOAD_CONDITIONING_INTERVAL;
  const char* MAX_CONNECTIONS;
  const char* MIN_CONNECTIONS;
  const char* PING_INTERVAL;
  const char* UPDATE_LOCATOR_LIST_INTERVAL;
  const char* READ_TIMEOUT;
  const char* RETRY_ATTEMPTS;
  const char* SERVER_GROUP;
  const char* SOCKET_BUFFER_SIZE;
  const char* STATISTIC_INTERVAL;
  const char* SUBSCRIPTION_ACK_INTERVAL;
  const char* SUBSCRIPTION_ENABLED;
  const char* SUBSCRIPTION_MTT;
  const char* SUBSCRIPTION_REDUNDANCY;
  const char* THREAD_LOCAL_CONNECTIONS;
  const char* CLONING_ENABLED;
  const char* MULTIUSER_SECURE_MODE;
  const char* PR_SINGLE_HOP_ENABLED;
  const char* CONCURRENCY_CHECKS_ENABLED;
  const char* TOMBSTONE_TIMEOUT;

  /** Name of the named region attributes */
  const char* ID;

  /** reference to a named attribute */
  const char* REFID;

 public:
  CacheXml();
};

};      // namespace gemfire
#endif  // ifndef __GEMFIRE_CACHEXML_HPP__

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
package org.apache.geode.internal.cache;

import java.util.Set;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.asyncqueue.AsyncEventQueue;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.internal.cache.eviction.HeapEvictor;
import org.apache.geode.internal.cache.eviction.OffHeapEvictor;
import org.apache.geode.management.internal.RestAgent;

/**
 * Defines methods for InternalCache that are VisibleForTesting.
 */
public interface InternalCacheForTesting {

  /**
   * Get cause for forcibly closing cache.
   */
  @VisibleForTesting
  Throwable getDisconnectCause();

  /**
   * Used by tests to get handle of Rest Agent.
   */
  @VisibleForTesting
  RestAgent getRestAgent();

  @VisibleForTesting
  boolean removeCacheServer(CacheServer cacheServer);

  /**
   * A test-hook allowing you to alter the cache setting established by
   * CacheFactory.setPdxReadSerialized()
   *
   * @deprecated tests using this method should be refactored to not require it
   */
  @Deprecated
  @VisibleForTesting
  void setReadSerializedForTest(boolean value);

  @VisibleForTesting
  Set<AsyncEventQueue> getAsyncEventQueues(boolean visibleOnly);

  @VisibleForTesting
  void closeDiskStores();

  @VisibleForTesting
  HeapEvictor getHeapEvictor();

  @VisibleForTesting
  OffHeapEvictor getOffHeapEvictor();
}

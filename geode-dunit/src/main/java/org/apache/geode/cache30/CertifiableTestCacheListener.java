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

/**
 * Test class used to certify that a particular key has arrived in the cache This class is a great
 * way to reduce the likelihood of a race condition
 */
package org.apache.geode.cache30;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.CacheEvent;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.test.awaitility.GeodeAwaitility;

public class CertifiableTestCacheListener<K, V> extends TestCacheListener<K, V> {
  private final Set<K> destroys = Collections.synchronizedSet(new HashSet<>());
  private final Set<K> creates = Collections.synchronizedSet(new HashSet<>());
  private final Set<K> invalidates = Collections.synchronizedSet(new HashSet<>());
  private final Set<K> updates = Collections.synchronizedSet(new HashSet<>());
  private static final Logger logger = LogService.getLogger();

  /**
   * Clears the state of the listener, for consistent behavior this should only be called when there
   * is no activity on the Region
   */
  public void clearState() {
    destroys.clear();
    creates.clear();
    invalidates.clear();
    updates.clear();
  }

  @Override
  public List<CacheEvent<K, V>> getEventHistory() {
    destroys.clear();
    creates.clear();
    invalidates.clear();
    updates.clear();
    return super.getEventHistory();
  }

  @VisibleForTesting
  public Set<K> getDestroys() {
    return destroys;
  }

  @Override
  public void afterCreate2(EntryEvent<K, V> event) {
    creates.add(event.getKey());
  }

  @Override
  public void afterDestroy2(EntryEvent<K, V> event) {
    destroys.add(event.getKey());
  }

  @Override
  public void afterInvalidate2(EntryEvent<K, V> event) {
    invalidates.add(event.getKey());
  }

  @Override
  public void afterUpdate2(EntryEvent<K, V> event) {
    updates.add(event.getKey());
  }

  public boolean waitForCreated(final K key) {
    GeodeAwaitility.await("Waiting for key creation: " + key)
        .until(() -> creates.contains(key));
    return true;
  }

  public boolean waitForDestroyed(final K key) {
    GeodeAwaitility.await("Waiting for key destroy: " + key)
        .until(() -> destroys.contains(key));
    return true;
  }

  public boolean waitForInvalidated(final K key) {
    GeodeAwaitility.await("Waiting for key invalidate: " + key)
        .until(() -> invalidates.contains(key));
    return true;
  }

  public boolean waitForUpdated(final K key) {
    GeodeAwaitility.await("Waiting for key update: " + key)
        .until(() -> updates.contains(key));
    return true;
  }
}

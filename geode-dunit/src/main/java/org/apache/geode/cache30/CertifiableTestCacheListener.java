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
 * way to reduce the liklihood of a race condition
 */
package org.apache.geode.cache30;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.geode.cache.CacheEvent;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.test.awaitility.GeodeAwaitility;

public class CertifiableTestCacheListener<K, V> extends TestCacheListener<K, V> {
  public final Set<K> destroys = Collections.synchronizedSet(new HashSet<>());
  public final Set<K> creates = Collections.synchronizedSet(new HashSet<>());
  public final Set<K> invalidates = Collections.synchronizedSet(new HashSet<>());
  public final Set<K> updates = Collections.synchronizedSet(new HashSet<>());

  /**
   * Clears the state of the listener, for consistent behavior this should only be called when there
   * is no activity on the Region
   */
  public void clearState() {
    this.destroys.clear();
    this.creates.clear();
    this.invalidates.clear();
    this.updates.clear();
  }

  @Override
  public List<CacheEvent<K, V>> getEventHistory() {
    destroys.clear();
    creates.clear();
    invalidates.clear();
    updates.clear();
    return super.getEventHistory();
  }

  @Override
  public void afterCreate2(EntryEvent<K, V> event) {
    this.creates.add(event.getKey());
  }

  @Override
  public void afterDestroy2(EntryEvent<K, V> event) {
    this.destroys.add(event.getKey());
  }

  @Override
  public void afterInvalidate2(EntryEvent<K, V> event) {
    K key = event.getKey();
    this.invalidates.add(key);
  }

  @Override
  public void afterUpdate2(EntryEvent<K, V> event) {
    this.updates.add(event.getKey());
  }

  private static final String WAIT_PROPERTY = "CertifiableTestCacheListener.maxWaitTime";
  private static final int WAIT_DEFAULT = 30000;

  private static final long MAX_TIME = Integer.getInteger(WAIT_PROPERTY, WAIT_DEFAULT);


  public boolean waitForCreated(final K key) {
    GeodeAwaitility.await("Waiting for key creation: " + key).timeout(MAX_TIME, MILLISECONDS)
        .until(() -> CertifiableTestCacheListener.this.creates.contains(key));
    return true;
  }

  public boolean waitForDestroyed(final K key) {
    GeodeAwaitility.await("Waiting for key destroy: " + key).timeout(MAX_TIME, MILLISECONDS)
        .until(() -> CertifiableTestCacheListener.this.destroys.contains(key));
    return true;
  }

  public boolean waitForInvalidated(final K key) {
    GeodeAwaitility.await("Waiting for key invalidate: " + key).timeout(MAX_TIME, MILLISECONDS)
        .until(() -> CertifiableTestCacheListener.this.invalidates.contains(key));
    return true;
  }

  public boolean waitForUpdated(final K key) {
    GeodeAwaitility.await("Waiting for key update: " + key).timeout(MAX_TIME, MILLISECONDS)
        .until(() -> CertifiableTestCacheListener.this.updates.contains(key));
    return true;
  }
}

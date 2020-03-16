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
package org.apache.geode.cache.query.cq.internal;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.logging.log4j.Logger;

import org.apache.geode.internal.cache.Token;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * Internal CQ cache implementation for CQs operating on partitioned regions.
 */
class ServerCQResultsCachePartitionRegionImpl implements ServerCQResultsCache {
  private static final Logger logger = LogService.getLogger();

  /**
   * To indicate if the CQ results key cache is initialized.
   */
  public volatile boolean cqResultKeysInitialized = false;

  /**
   * This holds the keys that are part of the CQ query results. Using this CQ engine can determine
   * whether to execute query on old value from EntryEvent, which is an expensive operation.
   *
   * NOTE: In case of RR this map is populated and used as intended. In case of PR this map will not
   * be populated. If executeCQ happens after update operations this map will remain empty.
   */
  private final ConcurrentMap<Object, Object> cqResultKeys;

  public ServerCQResultsCachePartitionRegionImpl() {
    cqResultKeys = new ConcurrentHashMap<>();
  }

  @Override
  public void setInitialized() {
    cqResultKeysInitialized = true;
  }

  @Override
  public boolean isInitialized() {
    return cqResultKeysInitialized;
  }

  @Override
  public void add(Object key) {
    cqResultKeys.put(key, TOKEN);
  }

  @Override
  public void remove(Object key, boolean isTokenMode) {
    if (isTokenMode && cqResultKeys.get(key) != Token.DESTROYED) {
      return;
    }

    cqResultKeys.remove(key);
  }

  @Override
  public void invalidate() {
    cqResultKeys.clear();
    cqResultKeysInitialized = false;
  }

  /**
   * Returns if the passed key is part of the CQs result set. This method needs to be called once
   * the CQ result key caching is completed (cqResultsCacheInitialized is true).
   *
   * @return true if key is in the Results Cache.
   */
  @Override
  public boolean contains(Object key) {
    // Handle events that may have been deleted,
    // but added by result caching.
    if (!isInitialized()) {
      logger.warn(
          "The CQ Result key cache is not initialized. This should not happen as the call to isPartOfCqResult() is based on the condition cqResultsCacheInitialized.");
      return false;
    }

    return cqResultKeys.containsKey(key);
  }

  /**
   * Marks the key as destroyed in the CQ Results key cache.
   */
  @Override
  public void markAsDestroyed(Object key) {
    cqResultKeys.put(key, Token.DESTROYED);
  }

  @Override
  public int size() {
    return cqResultKeys.size();
  }

  /**
   * For Test use only.
   *
   * @return CQ Results Cache.
   */
  @Override
  public Set<Object> getKeys() {
    return Collections.synchronizedSet(new HashSet<>(cqResultKeys.keySet()));
  }

  @Override
  public boolean isOldValueRequiredForQueryProcessing(Object key) {
    return !isInitialized() || !contains(key);
  }

  @Override
  public void clear() {
    cqResultKeys.clear();
  }
}

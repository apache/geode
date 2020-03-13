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
import java.util.Set;

/**
 * No op implementation, used when the property CqServiceProvider.MAINTAIN_KEYS is set as false.
 */
class ServerCQResultsCacheNoOpImpl implements ServerCQResultsCache {
  private static final Set<Object> EMPTY_CACHE = Collections.emptySet();

  @Override
  public void setInitialized() {}

  @Override
  public boolean isInitialized() {
    return false;
  }

  @Override
  public void add(Object key) {}

  @Override
  public void remove(Object key, boolean isTokenMode) {}

  @Override
  public void invalidate() {}

  @Override
  public boolean contains(Object key) {
    return false;
  }

  @Override
  public void markAsDestroyed(Object key) {}

  @Override
  public int size() {
    return 0;
  }

  @Override
  public Set<Object> getKeys() {
    return EMPTY_CACHE;
  }

  @Override
  public boolean isOldValueRequiredForQueryProcessing(Object key) {
    return true;
  }

  @Override
  public void clear() {}
}

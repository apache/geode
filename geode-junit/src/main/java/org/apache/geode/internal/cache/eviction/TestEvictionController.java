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
package org.apache.geode.internal.cache.eviction;

import static org.mockito.Mockito.mock;

import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAlgorithm;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.internal.cache.persistence.DiskRegionView;

class TestEvictionController implements EvictionController {

  private final EvictionCounters evictionCounters = mock(EvictionCounters.class);

  @Override
  public int entrySize(Object key, Object value) throws IllegalArgumentException {
    return 1;
  }

  @Override
  public long limit() {
    return 20;
  }

  public boolean usesMem() {
    return false;
  }

  @Override
  public EvictionAlgorithm getEvictionAlgorithm() {
    return EvictionAlgorithm.LRU_ENTRY;
  }

  @Override
  public EvictionCounters getCounters() {
    return this.evictionCounters;
  }

  @Override
  public EvictionAction getEvictionAction() {
    return EvictionAction.DEFAULT_EVICTION_ACTION;
  }

  @Override
  public boolean mustEvict(EvictionCounters stats, InternalRegion region, int delta) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean lruLimitExceeded(EvictionCounters stats, DiskRegionView diskRegionView) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public long getLimit() {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setLimit(int maximum) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void close() {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void closeBucket(BucketRegion bucketRegion) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setPerEntryOverhead(int entryOverhead) {
    throw new UnsupportedOperationException("Not implemented");
  }

}

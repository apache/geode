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
package org.apache.geode.internal.cache.persistence.query.mock;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.geode.internal.cache.CachedDeserializable;
import org.apache.geode.internal.cache.persistence.query.CloseableIterator;
import org.apache.geode.internal.cache.persistence.query.IdentityExtractor;
import org.apache.geode.internal.cache.persistence.query.ResultBag;
import org.apache.geode.internal.cache.persistence.query.SortKeyExtractor;

/**
 * Mock sorted bag implementation. Uses the mock index map internally, for convenience.
 *
 */
public class SortedResultBagImpl implements ResultBag {
  private final IndexMapImpl map;
  private final AtomicLong counter = new AtomicLong();
  private final SortKeyExtractor extractor;
  private final boolean reverse;

  public SortedResultBagImpl(SortKeyExtractor extractor, boolean reverse) {
    this.extractor = extractor == null ? new IdentityExtractor() : extractor;
    map = new IndexMapImpl();
    this.reverse = reverse;
  }

  @Override
  public void add(Object e) {
    map.put(extractor.getSortKey(e), counter.incrementAndGet(), e);

  }

  @Override
  public CloseableIterator<CachedDeserializable> iterator() {
    if (reverse) {
      return map.descendingValueIterator();
    } else {
      return map.valueIterator();
    }
  }

  @Override
  public void close() {
    map.destroy();
  }
}

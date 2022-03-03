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
import org.apache.geode.internal.cache.persistence.query.ResultList;

public class ResultListImpl implements ResultList {
  private final SortedResultMapImpl map;
  private final AtomicLong counter = new AtomicLong();

  public ResultListImpl() {
    map = new SortedResultMapImpl(false);
  }

  @Override
  public void add(Object e) {
    map.put(counter.getAndIncrement(), e);

  }

  @Override
  public CloseableIterator<CachedDeserializable> iterator() {
    return map.valueIterator();
  }

  @Override
  public CloseableIterator<CachedDeserializable> iterator(long start) {
    return map.valueIterator(start, true);
  }

  @Override
  public void close() {
    map.close();
  }



}

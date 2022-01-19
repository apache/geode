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

import java.util.Iterator;

import org.apache.geode.internal.cache.CachedDeserializable;
import org.apache.geode.internal.cache.persistence.query.CloseableIterator;

/**
 * An adapter from a java.util.iterator to a closable iterator.
 *
 * The java.util.iterator is expected to be iterating over a set of CachedDeserializable objects.
 *
 */
class ItrAdapter implements CloseableIterator<CachedDeserializable> {

  private final Iterator<?> iterator;

  public ItrAdapter(Iterator<?> iterator) {
    this.iterator = iterator;
  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override
  public CachedDeserializable next() {
    return (CachedDeserializable) iterator.next();
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();

  }

  @Override
  public void close() {
    // do nothing

  }

}

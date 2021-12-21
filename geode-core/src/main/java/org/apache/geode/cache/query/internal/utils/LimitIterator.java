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
package org.apache.geode.cache.query.internal.utils;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Utility Iterator which provides limit functionality on a given iterator
 *
 * @param <E>
 */
public class LimitIterator<E> implements Iterator<E> {

  private final Iterator<E> rootIter;
  private final int limit;
  private int numIterated = 0;

  public LimitIterator(Iterator<E> rootIter, int limit) {
    this.rootIter = rootIter;
    this.limit = limit;
  }

  @Override
  public boolean hasNext() {
    if (numIterated < limit) {
      return rootIter.hasNext();
    } else {
      return false;
    }
  }

  @Override
  public E next() {
    if (numIterated < limit) {
      ++numIterated;
      return rootIter.next();
    } else {
      throw new NoSuchElementException();
    }
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException("Removal from limit based collection not supported");

  }
}

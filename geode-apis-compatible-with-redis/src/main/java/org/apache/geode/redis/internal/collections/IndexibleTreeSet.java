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
package org.apache.geode.redis.internal.collections;

import java.util.Iterator;
import java.util.TreeSet;

/**
 * An OrderStatisticsSet implemented using TreeSet. {@link #indexOf(Object)} and other
 * rank operations will be O(N) instead of O(log(N))
 * Only for testing and performance comparisons.
 */
class IndexibleTreeSet<E> extends TreeSet<E> implements OrderStatisticsSet<E> {

  @Override
  public E get(int index) {
    Iterator<E> iterator = iterator();
    E value = null;
    for (int i = 0; i <= index; i++) {
      if (!iterator.hasNext()) {
        return null;
      }
      value = iterator.next();
    }

    return value;
  }

  @Override
  public int indexOf(E element) {
    return headSet(element).size();
  }
}

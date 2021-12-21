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

import java.util.Comparator;

import org.apache.geode.internal.cache.CachedDeserializable;

/**
 * Compare two cached deserializable objects by unwrapping the underlying object.
 *
 * If either object is not a cached deserializable, just use the object directly.
 *
 */
class CachedDeserializableComparator implements Comparator<Object> {

  private final Comparator comparator;

  public CachedDeserializableComparator(Comparator<?> comparator) {
    this.comparator = comparator;
  }

  @Override
  public int compare(Object o1, Object o2) {
    if (o1 instanceof CachedDeserializable) {
      o1 = ((CachedDeserializable) o1).getDeserializedForReading();
    }

    if (o2 instanceof CachedDeserializable) {
      o2 = ((CachedDeserializable) o2).getDeserializedForReading();
    }

    return comparator.compare(o1, o2);

  }

}

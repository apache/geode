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
package org.apache.geode.cache.lucene.internal.partition;

import java.util.AbstractMap;
import java.util.Set;

import org.apache.geode.cache.EntryExistsException;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.cache.BucketRegion;

public class BucketTargetingMap<K, V> extends AbstractMap<K, V> {

  private final Region<K, V> region;
  public Object callbackArg;

  public BucketTargetingMap(BucketRegion region, int bucketId) {
    callbackArg = bucketId;
    this.region = region;
  }

  @Override
  public Set<K> keySet() {
    return region.keySet();
  }

  @Override
  public V putIfAbsent(final K key, final V value) {
    try {
      region.create(key, value, callbackArg);
    } catch (EntryExistsException e) {
      return (V) e.getOldValue();
    }
    return null;
  }

  @Override
  public V get(final Object key) {
    return region.get(key, callbackArg);
  }

  @Override
  public V remove(final Object key) {
    try {
      V oldValue = region.get(key, callbackArg);
      region.destroy(key, callbackArg);
      return oldValue;
    } catch (EntryNotFoundException e) {
      return null;
    }
  }

  @Override
  public boolean containsKey(final Object key) {
    return region.get(key, callbackArg) != null;
  }

  @Override
  public V put(final K key, final V value) {
    return region.put(key, value, callbackArg);
  }

  @Override
  public Set<Entry<K, V>> entrySet() {
    return region.entrySet();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }

    final BucketTargetingMap<?, ?> that = (BucketTargetingMap<?, ?>) o;

    if (!region.getFullPath().equals(that.region.getFullPath())) {
      return false;
    }
    return callbackArg.equals(that.callbackArg);

  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + region.hashCode();
    result = 31 * result + callbackArg.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "BucketTargetingMap{" + "region=" + region.getFullPath() + ", callbackArg=" + callbackArg
        + '}';
  }
}

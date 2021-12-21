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
import java.util.Iterator;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.geode.internal.cache.CachedDeserializable;
import org.apache.geode.internal.cache.PreferBytesCachedDeserializable;
import org.apache.geode.internal.cache.persistence.query.CloseableIterator;
import org.apache.geode.internal.cache.persistence.query.ResultMap;

public class SortedResultMapImpl implements ResultMap {
  // This should be <CachedDeserializable, CachedDeserializable>, except
  // that we support retrievals using non CachedDeserializable objects.
  private final ConcurrentSkipListMap<Object, Object> map;

  public SortedResultMapImpl(boolean reverse) {
    Comparator comparator = new CachedDeserializableComparator(new NaturalComparator());
    if (reverse) {
      comparator = new ReverseComparator(comparator);
    }
    map = new ConcurrentSkipListMap(comparator);
  }

  @Override
  public void put(Object key, Object value) {
    map.put(toDeserializable(key), toDeserializable(value));

  }

  @Override
  public void remove(Object key) {
    map.remove(key);

  }

  @Override
  public Entry getEntry(Object key) {
    if (map.containsKey(key)) {
      return new EntryImpl(toDeserializable(key), (CachedDeserializable) map.get(key));
    } else {
      return null;
    }
  }

  @Override
  public CachedDeserializable get(Object key) {
    return (CachedDeserializable) map.get(key);
  }

  @Override
  public CloseableIterator<Entry> iterator(Object start, boolean startInclusive, Object end,
      boolean endInclusive) {
    return new IterImpl(map.subMap(start, startInclusive, end, endInclusive).entrySet().iterator());
  }

  @Override
  public CloseableIterator<Entry> iterator(Object start, boolean startInclusive) {
    // TODO Auto-generated method stub
    return new IterImpl(map.subMap(start, startInclusive).entrySet().iterator());
  }

  @Override
  public CloseableIterator<Entry> iterator() {
    // TODO Auto-generated method stub
    return new IterImpl(map.entrySet().iterator());
  }


  @Override
  public CloseableIterator<CachedDeserializable> keyIterator(Object start, boolean startInclusive,
      Object end, boolean endInclusive) {
    return new ItrAdapter(map.subMap(start, startInclusive, end, endInclusive).keySet().iterator());
  }

  @Override
  public CloseableIterator<CachedDeserializable> keyIterator(Object start, boolean startInclusive) {
    return new ItrAdapter(map.subMap(start, startInclusive).keySet().iterator());
  }

  @Override
  public CloseableIterator<CachedDeserializable> keyIterator() {
    return new ItrAdapter(map.keySet().iterator());
  }

  public CloseableIterator<CachedDeserializable> valueIterator() {
    return new ItrAdapter(map.values().iterator());
  }

  public CloseableIterator<CachedDeserializable> valueIterator(Object start,
      boolean startInclusive) {
    return new ItrAdapter(map.tailMap(start, startInclusive).values().iterator());
  }

  @Override
  public void close() {
    // do nothing
  }

  private static class EntryImpl implements Entry {

    private final CachedDeserializable key;
    private final CachedDeserializable value;

    public EntryImpl(CachedDeserializable key, CachedDeserializable value) {
      this.key = key;
      this.value = value;
    }

    @Override
    public CachedDeserializable getKey() {
      return key;
    }

    @Override
    public CachedDeserializable getValue() {
      return value;
    }


  }

  private CachedDeserializable toDeserializable(Object value) {
    if (value instanceof CachedDeserializable) {
      return (CachedDeserializable) value;
    }

    return new PreferBytesCachedDeserializable(value);
  }

  private static class IterImpl implements CloseableIterator<Entry> {

    private final Iterator<java.util.Map.Entry<Object, Object>> iterator;

    public IterImpl(Iterator<java.util.Map.Entry<Object, Object>> iterator) {
      this.iterator = iterator;
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public Entry next() {
      java.util.Map.Entry<Object, Object> next = iterator.next();
      return new EntryImpl((CachedDeserializable) next.getKey(),
          (CachedDeserializable) next.getValue());
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

  @Override
  public boolean containsKey(Object e) {
    return map.containsKey(e);
  }

}

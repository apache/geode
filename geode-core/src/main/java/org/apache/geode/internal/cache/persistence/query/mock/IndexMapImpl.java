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
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.geode.cache.query.internal.types.ExtendedNumericComparator;
import org.apache.geode.internal.cache.CachedDeserializable;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.PreferBytesCachedDeserializable;
import org.apache.geode.internal.cache.persistence.query.CloseableIterator;
import org.apache.geode.internal.cache.persistence.query.IndexMap;


/**
 * A dummy implementation of an IndexMap. Keeps all of the entries in memory, but in serialized
 * form.
 *
 *
 */
public class IndexMapImpl implements IndexMap {
  ConcurrentSkipListMap<Pair<CachedDeserializable, CachedDeserializable>, CachedDeserializable> map;

  public IndexMapImpl() {
    map = new ConcurrentSkipListMap(new PairComparator(
        new CachedDeserializableComparator(new ExtendedNumericComparator()), new ByteComparator()));
  }

  @Override
  public void put(Object indexKey, Object regionKey, Object value) {
    map.put(new Pair(toDeserializable(indexKey), toDeserializable(regionKey)),
        toDeserializable(value));

  }

  @Override
  public void remove(Object indexKey, Object regionKey) {
    map.remove(new Pair(indexKey, EntryEventImpl.serialize(regionKey)));

  }

  @Override
  public CloseableIterator<IndexEntry> get(Object indexKey) {
    return new Itr(map.subMap(new Pair(indexKey, ByteComparator.MIN_BYTES), true,
        new Pair(indexKey, ByteComparator.MAX_BYTES), true).entrySet().iterator());
  }

  @Override
  public CloseableIterator<CachedDeserializable> getKey(Object indexKey) {

    return new KeyItr(map.subMap(new Pair(indexKey, ByteComparator.MIN_BYTES), true,
        new Pair(indexKey, ByteComparator.MAX_BYTES), true).entrySet().iterator());
  }

  @Override
  public CloseableIterator<IndexEntry> iterator(Object start, boolean startInclusive, Object end,
      boolean endInclusive) {
    byte[] startBytes = startInclusive ? ByteComparator.MIN_BYTES : ByteComparator.MAX_BYTES;
    byte[] endBytes = endInclusive ? ByteComparator.MAX_BYTES : ByteComparator.MIN_BYTES;
    return new Itr(map
        .subMap(new Pair(start, startBytes), startInclusive, new Pair(end, endBytes), endInclusive)
        .entrySet().iterator());
  }

  @Override
  public CloseableIterator<IndexEntry> iterator(Object start, boolean startInclusive) {
    byte[] startBytes = startInclusive ? ByteComparator.MIN_BYTES : ByteComparator.MAX_BYTES;
    return new Itr(map.tailMap(new Pair(start, startBytes), startInclusive).entrySet().iterator());
  }

  @Override
  public CloseableIterator<IndexEntry> iterator() {
    return new Itr(map.entrySet().iterator());
  }

  public CloseableIterator<CachedDeserializable> valueIterator() {
    return new ItrAdapter(map.values().iterator());
  }

  public CloseableIterator<CachedDeserializable> descendingValueIterator() {
    return new ItrAdapter(map.descendingMap().values().iterator());
  }

  @Override
  public CloseableIterator<IndexEntry> descendingIterator(Object end, boolean endInclusive) {
    byte[] endBytes = endInclusive ? ByteComparator.MAX_BYTES : ByteComparator.MIN_BYTES;
    return new Itr(
        map.headMap(new Pair(end, endBytes), endInclusive).descendingMap().entrySet().iterator());
  }

  @Override
  public CloseableIterator<IndexEntry> descendingIterator() {
    return new Itr(map.descendingMap().entrySet().iterator());
  }

  @Override
  public CloseableIterator<CachedDeserializable> keyIterator(Object start, boolean startInclusive,
      Object end, boolean endInclusive) {
    byte[] startBytes = startInclusive ? ByteComparator.MIN_BYTES : ByteComparator.MAX_BYTES;
    byte[] endBytes = endInclusive ? ByteComparator.MAX_BYTES : ByteComparator.MIN_BYTES;
    return new KeyItr(map
        .subMap(new Pair(start, startBytes), startInclusive, new Pair(end, endBytes), endInclusive)
        .entrySet().iterator());
  }

  @Override
  public CloseableIterator<CachedDeserializable> keyIterator(Object start, boolean startInclusive) {
    byte[] startBytes = startInclusive ? ByteComparator.MIN_BYTES : ByteComparator.MAX_BYTES;
    return new KeyItr(
        map.tailMap(new Pair(start, startBytes), startInclusive).entrySet().iterator());
  }

  @Override
  public CloseableIterator<CachedDeserializable> keyIterator() {
    return new KeyItr(map.entrySet().iterator());
  }

  @Override
  public CloseableIterator<CachedDeserializable> descendingKeyIterator(Object end,
      boolean endInclusive) {
    byte[] endBytes = endInclusive ? ByteComparator.MAX_BYTES : ByteComparator.MIN_BYTES;
    return new KeyItr(
        map.headMap(new Pair(end, endBytes), endInclusive).descendingMap().entrySet().iterator());
  }

  @Override
  public CloseableIterator<CachedDeserializable> descendingKeyIterator() {
    return new KeyItr(map.descendingMap().entrySet().iterator());
  }


  @Override
  public long size(Object start, Object end) {
    byte[] startBytes = ByteComparator.MIN_BYTES;
    byte[] endBytes = ByteComparator.MAX_BYTES;
    return map.subMap(new Pair(start, startBytes), new Pair(end, endBytes)).size();
  }

  @Override
  public long sizeToEnd(Object start) {
    byte[] startBytes = ByteComparator.MIN_BYTES;
    return map.tailMap(new Pair(start, startBytes)).size();
  }

  @Override
  public long sizeFromStart(Object end) {
    byte[] endBytes = ByteComparator.MAX_BYTES;
    return map.headMap(new Pair(end, endBytes)).size();
  }

  @Override
  public long size() {
    return map.size();
  }



  @Override
  public void destroy() {
    // do nothing.
  }

  private CachedDeserializable toDeserializable(Object value) {
    if (value instanceof CachedDeserializable) {
      return (CachedDeserializable) value;
    }

    return new PreferBytesCachedDeserializable(value);
  }

  private static class IndexEntryImpl implements IndexEntry {

    private final CachedDeserializable indexKey;
    private final CachedDeserializable regionKey;
    private final CachedDeserializable value;


    public IndexEntryImpl(CachedDeserializable indexKey, CachedDeserializable regionKey,
        CachedDeserializable value) {
      this.indexKey = indexKey;
      this.regionKey = regionKey;
      this.value = value;
    }

    @Override
    public CachedDeserializable getKey() {
      return indexKey;
    }

    @Override
    public CachedDeserializable getRegionKey() {
      return regionKey;
    }

    @Override
    public CachedDeserializable getValue() {
      return value;
    }

  }


  private static class Itr implements CloseableIterator<IndexEntry> {

    private final Iterator<java.util.Map.Entry<Pair<CachedDeserializable, CachedDeserializable>, CachedDeserializable>> iterator;

    public Itr(
        Iterator<java.util.Map.Entry<Pair<CachedDeserializable, CachedDeserializable>, CachedDeserializable>> iterator) {
      this.iterator = iterator;
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public IndexEntry next() {
      java.util.Map.Entry<Pair<CachedDeserializable, CachedDeserializable>, CachedDeserializable> next =
          iterator.next();
      return new IndexEntryImpl(next.getKey().getX(), next.getKey().getY(), next.getValue());
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

  private static class KeyItr implements CloseableIterator<CachedDeserializable> {

    private final Iterator<java.util.Map.Entry<Pair<CachedDeserializable, CachedDeserializable>, CachedDeserializable>> iterator;

    public KeyItr(
        Iterator<java.util.Map.Entry<Pair<CachedDeserializable, CachedDeserializable>, CachedDeserializable>> iterator) {
      this.iterator = iterator;
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public CachedDeserializable next() {
      java.util.Map.Entry<Pair<CachedDeserializable, CachedDeserializable>, CachedDeserializable> next =
          iterator.next();
      return next.getKey().getY();

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

}

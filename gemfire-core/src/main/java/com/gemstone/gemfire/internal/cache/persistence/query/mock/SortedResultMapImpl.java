/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.persistence.query.mock;

import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.ConcurrentSkipListMap;

import com.gemstone.gemfire.internal.cache.CachedDeserializable;
import com.gemstone.gemfire.internal.cache.PreferBytesCachedDeserializable;
import com.gemstone.gemfire.internal.cache.persistence.query.CloseableIterator;
import com.gemstone.gemfire.internal.cache.persistence.query.ResultMap;

public class SortedResultMapImpl implements ResultMap {
  //This should be <CachedDeserializable, CachedDeserializable>, except
  //that we support retrievals using non CachedDeserializable objects.
  private final ConcurrentSkipListMap<Object, Object> map;
  
  public SortedResultMapImpl(boolean reverse) {
    Comparator comparator = new CachedDeserializableComparator(new NaturalComparator());
    if(reverse) {
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
    if(map.containsKey(key)) {
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
  public CloseableIterator<Entry> iterator(Object start,
      boolean startInclusive, Object end, boolean endInclusive) {
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
  public CloseableIterator<CachedDeserializable> keyIterator(Object start,
      boolean startInclusive, Object end, boolean endInclusive) {
    return new ItrAdapter(map.subMap(start, startInclusive, end, endInclusive).keySet().iterator());
  }

  @Override
  public CloseableIterator<CachedDeserializable> keyIterator(Object start,
      boolean startInclusive) {
    return new ItrAdapter(map.subMap(start, startInclusive).keySet().iterator());
  }

  @Override
  public CloseableIterator<CachedDeserializable> keyIterator() {
    return new ItrAdapter(map.keySet().iterator());
  }
  
  public CloseableIterator<CachedDeserializable> valueIterator() {
    return new ItrAdapter(map.values().iterator());
  }
  
  public CloseableIterator<CachedDeserializable> valueIterator(Object start, boolean startInclusive) {
    return new ItrAdapter(map.tailMap(start, startInclusive).values().iterator());
  }
  
  @Override
  public void close() {
    //do nothing
  }

  private static class EntryImpl implements Entry {

    private CachedDeserializable key;
    private CachedDeserializable value;

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
    if(value instanceof CachedDeserializable) {
      return (CachedDeserializable) value;
    }
    
    return new PreferBytesCachedDeserializable(value);
  }
  
  private static class IterImpl implements CloseableIterator<Entry> {

    private Iterator<java.util.Map.Entry<Object, Object>> iterator;

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
      return new EntryImpl((CachedDeserializable) next.getKey(), (CachedDeserializable) next.getValue());
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
      
    }

    @Override
    public void close() {
      //do nothing
      
    }
    
  }
  
  public boolean containsKey(Object e) {
    return map.containsKey(e);
  }

}

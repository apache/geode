/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.cache.operations.internal;

import java.io.Serializable;

import java.util.AbstractCollection;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Map.Entry;

import com.gemstone.gemfire.internal.cache.CachedDeserializable;
import com.gemstone.gemfire.internal.cache.Token;

/**
 * This map only allows updates. No creates or removes.
 * It was adapted from UnmodifiableMap in the jdk's Collections class.
 * It was added to fix bug 51604.
 * It also make sure that customers do not see Token.INVALID and
 * CachedDeserializable to fix bug 51625.
 */
public class UpdateOnlyMap implements Map, Serializable {
  private static final long serialVersionUID = -1034234728574286014L;

  private final Map m;

  public UpdateOnlyMap(Map m) {
    if (m==null) {
      throw new NullPointerException();
    }
    this.m = m;
  }

  /**
   * Only called by internal code
   * to bypass exportValue() method
   * @return internal map
   */
  public Map getInternalMap() {
    return this.m;
  }
  
  public int size()                        {return m.size();}
  public boolean isEmpty()                 {return m.isEmpty();}
  public boolean containsKey(Object key)   {return m.containsKey(key);}
  public boolean containsValue(Object val) {
    return values().contains(val);
  }
  public Object get(Object key) {
    return exportValue(m.get(key));
  }
  
  private static Object exportValue(Object v) {
    Object result;
    if (v == Token.INVALID) {
      result = null;
    } else if (v instanceof CachedDeserializable) {
      result = ((CachedDeserializable) v).getDeserializedForReading();
    } else {
      result = v;
    }
    return result;
  }

  public Object put(Object key, Object value) {
    if (containsKey(key)) {
      return m.put(key,  value);
    } else {
      throw new UnsupportedOperationException("can not add the key \"" + key + "\"");
    }
  }
  public void putAll(Map m) {
    if (m != null) {
      for (Object i: m.entrySet()) {
        Map.Entry me = (Map.Entry) i;
        put(me.getKey(), me.getValue());
      }
    }
  }
  public Object remove(Object key) {
      throw new UnsupportedOperationException();
  }
  public void clear() {
      throw new UnsupportedOperationException();
  }

  private transient Set keySet = null;
  private transient Set entrySet = null;
  private transient Collection values = null;

  public Set keySet() {
    if (keySet==null) {
      keySet = Collections.unmodifiableSet(m.keySet());
    }
    return keySet;
  }

  public Set entrySet() {
    if (entrySet==null) {
      entrySet = Collections.unmodifiableSet(new EntrySet());
    }
    return entrySet;
  }

  private final class EntrySet extends AbstractSet {
    public Iterator iterator() {
        return new EntryIterator();
    }
    @Override
    public int size() {
        return m.size();
    }
  }
  private class EntryIterator implements Iterator {
    private Iterator mIterator = m.entrySet().iterator();

    @Override
    public boolean hasNext() {
        return this.mIterator.hasNext();
    }

    @Override
    public Object next() {
      Entry me = (Entry) this.mIterator.next();
      return new ExportableEntry(me);
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
  private static class ExportableEntry implements Map.Entry {
    private final Map.Entry e;

    ExportableEntry(Map.Entry e) {
      this.e = e;
    }
    public Object getKey() {
      return this.e.getKey();
    }
    public Object getValue() {
      return exportValue(this.e.getValue());
    }
    public Object setValue(Object value) {
      return exportValue(this.e.setValue(value));
    }
    public int hashCode() {
      return Objects.hashCode(getKey()) ^ Objects.hashCode(getValue());
    }
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof Map.Entry)) {
        return false;
      }
      Entry other = (Entry) o;
      return eq(getKey(), other.getKey()) && eq(getValue(), other.getValue());
    }
    public String toString() {
      return getKey() + "=" + getValue();
    }
  }
  private static boolean eq(Object o1, Object o2) {
    return o1==null ? o2==null : o1.equals(o2);
  }
  
  public Collection values() {
    if (values==null) {
      values = Collections.unmodifiableCollection(new Values());
    }
    return values;
  }
  
  private final class Values extends AbstractCollection {
    @Override
    public Iterator iterator() {
      return new ValueIterator();
    }
    @Override
    public int size() {
      return m.size();
    }
  }
  private class ValueIterator implements Iterator {
    private Iterator mIterator = m.values().iterator();

    @Override
    public boolean hasNext() {
        return this.mIterator.hasNext();
    }

    @Override
    public Object next() {
      return exportValue(this.mIterator.next());
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  
  /**
   * equals is over-ridden to make sure it is based on
   * the objects we expose and not the internal CachedDeserializables.
   */
  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }

    if (!(o instanceof Map)) {
      return false;
    }
    Map m = (Map) o;
    if (m.size() != size()) {
      return false;
    }

    try {
      Iterator<Entry> i = entrySet().iterator();
      while (i.hasNext()) {
        Entry e = i.next();
        Object key = e.getKey();
        Object value = e.getValue();
        if (value == null) {
          if (!(m.get(key)==null && m.containsKey(key))) {
            return false;
          }
        } else {
          if (!value.equals(m.get(key))) {
            return false;
          }
        }
      }
    } catch (ClassCastException unused) {
      return false;
    } catch (NullPointerException unused) {
      return false;
    }

    return true;
}

/**
 * hashCode is over-ridden to make sure it is based on
 * the objects we expose and not the internal CachedDeserializables.
 */
@Override
public int hashCode() {
  int h = 0;
  Iterator<Entry> i = entrySet().iterator();
  while (i.hasNext()) {
    h += i.next().hashCode();
  }
  return h;
}

  @Override
  public String toString() {
    Iterator<Entry> i = entrySet().iterator();
    if (! i.hasNext()) {
      return "{}";
    }
    StringBuilder sb = new StringBuilder();
    sb.append('{');
    for (;;) {
      Entry e = i.next();
      Object key = e.getKey();
      Object value = e.getValue();
      sb.append(key   == this ? "(this Map)" : key);
      sb.append('=');
      sb.append(value == this ? "(this Map)" : value);
      if (! i.hasNext()) {
        return sb.append('}').toString();
      }
      sb.append(',').append(' ');
    }
  }
}
/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.cache.operations;

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

import com.gemstone.gemfire.cache.operations.OperationContext;
import com.gemstone.gemfire.internal.cache.CachedDeserializable;
import com.gemstone.gemfire.internal.cache.Token;

/**
 * Encapsulates a {@link com.gemstone.gemfire.cache.operations.OperationContext.OperationCode#PUTALL} operation for both the
 * pre-operation and post-operation cases.
 * 
 * @author Gester Zhou
 * @since 5.7
 */
public class PutAllOperationContext extends OperationContext {

  /** The set of keys for the operation */
  private final UpdateOnlyMap map;
  
  /** True if this is a post-operation context */
  private boolean postOperation = false;
  
  private Object callbackArg;

  /**
   * Constructor for the operation.
   * 
   */
  public PutAllOperationContext(Map map) {
    this.map = new UpdateOnlyMap(map);
  }

  /**
   * Return the operation associated with the <code>OperationContext</code>
   * object.
   * 
   * @return <code>OperationCode.PUTALL</code>.
   */
  @Override
  public OperationCode getOperationCode() {
    return OperationCode.PUTALL;
  }

  /**
   * True if the context is for post-operation.
   */
  @Override
  public boolean isPostOperation() {
    return this.postOperation;
  }

  /**
   * Set the post-operation flag to true.
   */
  protected void setPostOperation() {
    this.postOperation = true;
  }

  /**
   * Returns the map whose keys and values will be put.
   * Note that only the values of this map can be changed.
   * You can not add or remove keys.
   * Any attempt to modify the returned map with an operation
   * that is not supported will throw an UnsupportedOperationException.
   * If the returned map is modified and this is a pre-operation
   * authorization then the modified map is what will be used by the operation.
   */
  public <K,V> Map<K,V> getMap() {
    return this.map;
  }

  /**
   * Set the authorized map.
   * @throws IllegalArgumentException if the given map is null or if its keys are not the same as the original keys.
   * @deprecated use getMap() instead and modify the values in the map it returns
   */
  public void setMap(Map map) {
    if (map == this.map) {
      return;
    }
    if (map == null) {
      throw new IllegalArgumentException("PutAllOperationContext.setMap does not allow a null map.");
    }
    if (map.size() != this.map.size()) {
      throw new IllegalArgumentException("PutAllOperationContext.setMap does not allow the size of the map to be changed.");
    }
    // this.map is a LinkedHashMap and our implementation needs its order to be preserved.
    // So take each entry from the input "map" and update the corresponding entry in the linked "this.map".
    // Note that updates do not change the order of a linked hash map; only inserts do.
    try {
      this.map.putAll(map);
    } catch (UnsupportedOperationException ex) {
      throw new IllegalArgumentException("PutAllOperationContext.setMap " + ex.getMessage() + " to the original keys of the putAll");
    }
  }

  /**
   * Get the callback argument object for this operation.
   * 
   * @return the callback argument object for this operation.
   * @since 8.1
   */
  public Object getCallbackArg() {
    return this.callbackArg;
  }

  /**
   * Set the callback argument object for this operation.
   * 
   * @param callbackArg
   *                the callback argument object for this operation.
   * @since 8.1
   */
  public void setCallbackArg(Object callbackArg) {
    this.callbackArg = callbackArg;
  }
  
  /**
   * This map only allows updates. No creates or removes.
   * It was adapted from UnmodifiableMap in the jdk's Collections class.
   * It was added to fix bug 51604.
   * It also make sure that customers do not see Token.INVALID and
   * CachedDeserializable to fix bug 51625.
   * @author dschneider
   */
  private static class UpdateOnlyMap implements Map, Serializable {
    private static final long serialVersionUID = -1034234728574286014L;

    private final Map m;

    UpdateOnlyMap(Map m) {
      if (m==null) {
        throw new NullPointerException();
      }
      this.m = m;
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
}

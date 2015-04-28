/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * A Hash set where every modification makes an internal copy 
 * of a HashSet. Similar to java.util.concurrent.CopyOnWriteArrayList,
 * except methods provide the access time characteristics of HashSet, instead
 * of ArrayList, for example contains is O(1) instead of O(n).
 * 
 * Also, this class provides a getSnapshot method, which should
 * be used for any thing that needs an unchanging snapshot of this
 * this (For example, any serialization of this class should use getSnapshot).
 * @author dsmith
 *
 */
public class CopyOnWriteHashSet<T> implements Set<T>, Serializable  {
  
  private static final long serialVersionUID = 8591978652141659932L;
  
  private volatile transient Set<T> snapshot = Collections.emptySet();

  public CopyOnWriteHashSet() {
  }

  public CopyOnWriteHashSet(Set<T> copy) {
    this.snapshot = new HashSet<T>(copy);
  }
  
  /**
   * Because I'm lazy, this iterator does not support modification
   * of this set. If you need it, it shouldn't be too hard to implement.
   */
  public Iterator<T> iterator() {
    return Collections.unmodifiableSet(snapshot).iterator();
  }

  public int size() {
    return snapshot.size();
  }

  public boolean add(T e) {
    synchronized(this) {
      Set<T> set = new HashSet<T>(snapshot);
      boolean result = set.add(e);
      snapshot = set;
      return result;
    }
  }

  public boolean addAll(Collection<? extends T> c) {
    synchronized(this) {
      Set<T> set = new HashSet<T>(snapshot);
      boolean result = set.addAll(c);
      snapshot = set;
      return result;
    }
  }

  public void clear() {
    synchronized(this) {
      snapshot = Collections.emptySet();
    }
  }

  public boolean contains(Object o) {
    return snapshot.contains(o);
  }

  public boolean containsAll(Collection<?> c) {
    return snapshot.containsAll(c);
  }

  public boolean isEmpty() {
    return snapshot.isEmpty();
  }

  public boolean remove(Object o) {
    synchronized(this) {
      Set<T> set = new HashSet<T>(snapshot);
      boolean result = set.remove(o);
      snapshot = set;
      return result;
    }
  }

  public boolean retainAll(Collection<?> c) {
    synchronized(this) {
      Set<T> set = new HashSet<T>(snapshot);
      boolean result = set.retainAll(c);
      snapshot = set;
      return result;
    }
  }

  public Object[] toArray() {
    return snapshot.toArray();
  }

  public <T> T[] toArray(T[] a) {
    return snapshot.toArray(a);
  }

  @Override
  public boolean equals(Object o) {
    return snapshot.equals(o);
  }

  @Override
  public int hashCode() {
    return snapshot.hashCode();
  }

  public boolean removeAll(Collection<?> c) {
    synchronized(this) {
      Set<T> set = new HashSet<T>(snapshot);
      boolean result = set.removeAll(c);
      snapshot = set;
      return result;
    }
  }

  @Override
  public String toString() {
    return snapshot.toString();
  }
  
  /**
   * Return a snapshot of the set at this point in time.
   * The snapshot is guaranteed not to change. It is therefore
   * unmodifiable.
   * This will likely be more efficient than copying this
   * set.
   * @return A snapshot of this set.
   */
  public Set<T> getSnapshot() {
    return Collections.unmodifiableSet(snapshot);
  }
  
  private void writeObject(ObjectOutputStream s) throws IOException {
    s.defaultWriteObject();
    s.writeObject(snapshot);
  }
  
  @SuppressWarnings("unchecked")
  private void readObject(ObjectInputStream s)
  throws java.io.IOException, ClassNotFoundException {
    s.defaultReadObject();
    this.snapshot = (Set<T>) s.readObject();
  }
  
}

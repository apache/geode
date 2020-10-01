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
package org.apache.geode.internal;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;
import java.util.function.Consumer;

/**
 * A Hash set where every modification makes an internal copy of a HashSet. Similar to
 * java.util.concurrent.CopyOnWriteArrayList, except methods provide the access time characteristics
 * of HashSet, instead of ArrayList, for example contains is O(1) instead of O(n).
 *
 * Also, this class provides a getSnapshot method, which should be used for any thing that needs an
 * unchanging snapshot of this this (For example, any serialization of this class should use
 * getSnapshot).
 *
 */
public class CopyOnWriteHashSet<T> implements Set<T>, Serializable {

  private static final long serialVersionUID = 8591978652141659932L;

  private transient volatile Set<T> snapshot = Collections.emptySet();

  public CopyOnWriteHashSet() {}

  public CopyOnWriteHashSet(Set<T> copy) {
    this.snapshot = new HashSet<T>(copy);
  }

  /**
   * Create a custom {@link Iterator} implementation for the {@link CopyOnWriteHashSet}
   */
  @Override
  public Iterator<T> iterator() {
    return new Iterator<T>() {

      private Iterator<T> iterator = new LinkedList<>(snapshot).iterator();
      private T currentElement;

      @Override
      public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override
      public T next() {
        currentElement = iterator.next();
        return currentElement;
      }

      @Override
      public void remove() {
        snapshot.remove(currentElement);
      }

      @Override
      public void forEachRemaining(Consumer<? super T> action) {
        iterator.forEachRemaining(action);
      }
    };
  }

  @Override
  public int size() {
    return snapshot.size();
  }

  @Override
  public boolean add(T e) {
    synchronized (this) {
      Set<T> set = new HashSet<T>(snapshot);
      boolean result = set.add(e);
      snapshot = set;
      return result;
    }
  }

  @Override
  public boolean addAll(Collection<? extends T> c) {
    synchronized (this) {
      Set<T> set = new HashSet<T>(snapshot);
      boolean result = set.addAll(c);
      snapshot = set;
      return result;
    }
  }

  @Override
  public void clear() {
    synchronized (this) {
      snapshot = Collections.emptySet();
    }
  }

  @Override
  public boolean contains(Object o) {
    return snapshot.contains(o);
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    return snapshot.containsAll(c);
  }

  @Override
  public boolean isEmpty() {
    return snapshot.isEmpty();
  }

  @Override
  public boolean remove(Object o) {
    synchronized (this) {
      Set<T> set = new HashSet<T>(snapshot);
      boolean result = set.remove(o);
      snapshot = set;
      return result;
    }
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    synchronized (this) {
      Set<T> set = new HashSet<T>(snapshot);
      boolean result = set.retainAll(c);
      snapshot = set;
      return result;
    }
  }

  @Override
  public Object[] toArray() {
    return snapshot.toArray();
  }

  @Override
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

  @Override
  public boolean removeAll(Collection<?> c) {
    synchronized (this) {
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
   * Return a snapshot of the set at this point in time. The snapshot is guaranteed not to change.
   * It is therefore unmodifiable. This will likely be more efficient than copying this set.
   *
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
  private void readObject(ObjectInputStream s) throws java.io.IOException, ClassNotFoundException {
    s.defaultReadObject();
    this.snapshot = (Set<T>) s.readObject();
  }

}

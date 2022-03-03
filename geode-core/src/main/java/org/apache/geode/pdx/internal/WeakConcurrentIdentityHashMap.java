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
package org.apache.geode.pdx.internal;

import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * <p>
 * A map where keys are compared using identity comparison (like IdentityHashMap) but where the
 * presence of an object as a key in the map does not prevent it being garbage collected (like
 * WeakHashMap). This class does not implement the Map interface because it is difficult to ensure
 * correct semantics for iterators over the entrySet().
 * </p>
 *
 * <p>
 * Because we do not implement Map, we do not copy the questionable interface where you can call
 * get(k) or remove(k) for any type of k, which of course can only have an effect if k is of type K.
 * </p>
 *
 * <p>
 * This map does not support null keys.
 * </p>
 * <p>
 * The approach is to wrap each key in a WeakReference and use the wrapped value as a key in an
 * ordinary HashMap. The WeakReference has to be a subclass IdentityWeakReference (IWR) where two
 * IWRs are equal if they refer to the same object. This enables us to find the entry again.
 *
 * <p>
 * Note: this code came from the jdk from the package: com.sun.jmx.mbeanserver. I modified it to use
 * a ConcurrentMap.
 *
 * @since GemFire 6.6
 */
class WeakConcurrentIdentityHashMap<K, V> {
  private WeakConcurrentIdentityHashMap() {}

  static <K, V> WeakConcurrentIdentityHashMap<K, V> make() {
    return new WeakConcurrentIdentityHashMap<>();
  }

  public V get(K key) {
    expunge();
    WeakReference<K> keyref = makeReference(key);
    return map.get(keyref);
  }

  public V put(K key, V value) {
    expunge();
    if (key == null) {
      throw new IllegalArgumentException("Null key");
    }
    WeakReference<K> keyref = makeReference(key, refQueue);
    return map.put(keyref, value);
  }

  public V remove(K key) {
    expunge();
    WeakReference<K> keyref = makeReference(key);
    return map.remove(keyref);
  }

  private void expunge() {
    Reference<? extends K> ref;
    while ((ref = refQueue.poll()) != null) {
      map.remove(ref);
    }
  }

  private WeakReference<K> makeReference(K referent) {
    return new IdentityWeakReference<>(referent);
  }

  private WeakReference<K> makeReference(K referent, ReferenceQueue<K> q) {
    return new IdentityWeakReference<>(referent, q);
  }

  public void clear() {
    expunge();
    map.clear();
  }

  /**
   * WeakReference where equals and hashCode are based on the referent. More precisely, two objects
   * are equal if they are identical or if they both have the same non-null referent. The hashCode
   * is the value the original referent had. Even if the referent is cleared, the hashCode remains.
   * Thus, objects of this class can be used as keys in hash-based maps and sets.
   */
  private static class IdentityWeakReference<T> extends WeakReference<T> {
    IdentityWeakReference(T o) {
      this(o, null);
    }

    IdentityWeakReference(T o, ReferenceQueue<T> q) {
      super(o, q);
      hashCode = (o == null) ? 0 : System.identityHashCode(o);
    }

    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof IdentityWeakReference)) {
        return false;
      }
      IdentityWeakReference wr = (IdentityWeakReference) o;
      Object got = get();
      return (got != null && got == wr.get());
    }

    public int hashCode() {
      return hashCode;
    }

    private final int hashCode;
  }

  private final Map<WeakReference<K>, V> map = new ConcurrentHashMap<>();
  private final ReferenceQueue<K> refQueue = new ReferenceQueue<>();
}

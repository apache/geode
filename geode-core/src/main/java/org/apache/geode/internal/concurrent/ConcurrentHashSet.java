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
package org.apache.geode.internal.concurrent;

import java.io.Serializable;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**This class is similar to HashSet supporting all the feature
 * of ConcurrentHashMap
 * 
 *
 */
public class ConcurrentHashSet<E> extends AbstractSet<E>  
  implements Set<E>, Serializable {

  private static final long serialVersionUID = -3338819662572203596L;
  
  private ConcurrentHashMap<E, Object> map;
  
  // Dummy value to associate with an Object in the backing Map
  private static final Object PRESENT = new Object();

  public ConcurrentHashSet() {
    map = new ConcurrentHashMap<E,Object>();
  }
  
  public ConcurrentHashSet(Collection<? extends E> c) {
    map = new ConcurrentHashMap<E,Object>(Math.max((int) (c.size()/.75f) + 1, 16));
    addAll(c);
  }
  
  public ConcurrentHashSet(int initialCapacity, float loadFactor, int concurrencyLevel) {
    map = new ConcurrentHashMap<E,Object>(initialCapacity, loadFactor, concurrencyLevel);
  }

  public ConcurrentHashSet(int initialCapacity) {
    map = new ConcurrentHashMap<E,Object>(initialCapacity);
  }

  
  public boolean add(E o) {
    return map.put(o, PRESENT)==null;
  }
  
  public void clear() {
    map.clear();
  }
  
  public boolean contains(Object o) {
    return map.containsKey(o);
  }
  
  public boolean isEmpty() {
    return map.isEmpty();
  }
  
  public Iterator<E> iterator() {
    return map.keySet().iterator();
  }
  
  public boolean remove(Object o) {
    return map.remove(o)==PRESENT;
  }
  
  
  public int size() {
    return map.size();
  }
  
}

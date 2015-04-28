/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
package com.gemstone.gemfire.internal.concurrent;

import java.io.Serializable;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**This class is similar to HashSet supporting all the feature
 * of ConcurrentHashMap
 * 
 * @author soubhikc
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

/*
 *  =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *  ========================================================================
 */
package com.gemstone.gemfire.management.internal;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Various uitlity methods for open type conversion
 * 
 * @author rishim
 * 
 */

public class OpenTypeUtil {

  static <K, V> Map<K, V> newMap() {
    return new HashMap<K, V>();
  }

  static <K, V> Map<K, V> newSynchronizedMap() {
    return Collections.synchronizedMap(OpenTypeUtil.<K, V> newMap());
  }

  static <K, V> IdentityHashMap<K, V> newIdentityHashMap() {
    return new IdentityHashMap<K, V>();
  }

  static <K, V> Map<K, V> newSynchronizedIdentityHashMap() {
    Map<K, V> map = newIdentityHashMap();
    return Collections.synchronizedMap(map);
  }

  static <K, V> SortedMap<K, V> newSortedMap() {
    return new TreeMap<K, V>();
  }

  static <K, V> SortedMap<K, V> newSortedMap(Comparator<? super K> comp) {
    return new TreeMap<K, V>(comp);
  }

  static <K, V> Map<K, V> newInsertionOrderMap() {
    return new LinkedHashMap<K, V>();
  }

  static <E> Set<E> newSet() {
    return new HashSet<E>();
  }

  static <E> Set<E> newSet(Collection<E> c) {
    return new HashSet<E>(c);
  }

  static <E> List<E> newList() {
    return new ArrayList<E>();
  }

  static <E> List<E> newList(Collection<E> c) {
    return new ArrayList<E>(c);
  }

  @SuppressWarnings("unchecked")
  public static <T> T cast(Object x) {
    return (T) x;
  }

  /**
   * Utility method to take a string and convert it to normal Java variable name
   * capitalization.
   * 
   * @param name
   *          The string to be made in camel case.
   * @return The camel case version of the string.
   */
  public static String decapitalize(String name) {
    if (name == null || name.length() == 0) {
      return name;
    }
    int offset1 = Character.offsetByCodePoints(name, 0, 1);
    if (offset1 < name.length()
        && Character.isUpperCase(name.codePointAt(offset1)))
      return name;
    return name.substring(0, offset1).toLowerCase() + name.substring(offset1);
  }

  protected static String capitalize(String name) {
    if (name == null || name.length() == 0)
      return name;
    int offset1 = name.offsetByCodePoints(0, 1);
    return name.substring(0, offset1).toUpperCase() + name.substring(offset1);
  }
}

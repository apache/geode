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
package org.apache.geode.management.internal;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Various utility methods for open type conversion
 */
public class OpenTypeUtil {

  static <K, V> Map<K, V> newMap() {
    return new HashMap<>();
  }

  static <K, V> IdentityHashMap<K, V> newIdentityHashMap() {
    return new IdentityHashMap<>();
  }

  static <K, V> SortedMap<K, V> newSortedMap() {
    return new TreeMap<>();
  }

  static <E> Set<E> newSet() {
    return new HashSet<>();
  }

  static <E> Set<E> newSet(Collection<E> c) {
    return new HashSet<>(c);
  }

  static <E> List<E> newList() {
    return new ArrayList<>();
  }

  static <E> List<E> newList(Collection<E> c) {
    return new ArrayList<>(c);
  }

  public static <T> T cast(Object x) {
    return (T) x;
  }

  /**
   * Utility method to take a string and convert it to normal Java variable name capitalization.
   *
   * @param name The string to be made in camel case.
   *
   * @return The camel case version of the string.
   */
  static String toCamelCase(String name) {
    if (name == null || name.isEmpty()) {
      return name;
    }
    int offset1 = Character.offsetByCodePoints(name, 0, 1);
    if (offset1 < name.length() && Character.isUpperCase(name.codePointAt(offset1))) {
      return name;
    }
    return name.substring(0, offset1).toLowerCase() + name.substring(offset1);
  }
}

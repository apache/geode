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

package org.apache.geode.internal.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.apache.geode.internal.lang.Filter;

/**
 * The CollectionUtils class is a utility class for working with the Java Collections framework of
 * classes, data structures and algorithms.
 * <p/>
 *
 * @see org.apache.geode.internal.lang.Filter
 * @see java.util.Arrays
 * @see java.util.Collection
 * @see java.util.Collections
 * @see java.util.Iterator
 * @see java.util.List
 * @see java.util.Map
 * @see java.util.Set
 * @since GemFire 7.0
 */
public abstract class CollectionUtils {

  /**
   * Returns the specified array as a List of elements.
   * <p/>
   *
   * @param <T> the class type of the elements in the array.
   * @param array the object array of elements to convert to a List.
   * @return a List of elements contained in the specified array.
   * @see java.util.Arrays#asList(Object[])
   */
  @SafeVarargs
  public static <T> List<T> asList(final T... array) {
    List<T> arrayList = new ArrayList<T>(array.length);
    Collections.addAll(arrayList, array);
    return arrayList;
  }

  /**
   * Returns the specified array as a Set of elements.
   * <p/>
   *
   * @param <T> the class type of the elements in the array.
   * @param array the object array of elements to convert to a Set.
   * @return a Set of elements contained in the specified array.
   * @see java.util.Arrays#asList(Object[])
   */
  @SafeVarargs
  public static <T> Set<T> asSet(final T... array) {
    Set<T> arraySet = new HashSet<T>(array.length);
    Collections.addAll(arraySet, array);
    return arraySet;
  }

  /**
   * Creates an Properties object initialized with the value from the given Map.
   * <p>
   *
   * @param map the Map supply the keys and value for the Properties object.
   * @return a Properties object initialized with the key and value from the Map.
   * @see java.util.Map
   * @see java.util.Properties
   */
  public static Properties createProperties(final Map<String, String> map) {
    Properties properties = new Properties();

    if (!(map == null || map.isEmpty())) {
      for (Entry<String, String> entry : map.entrySet()) {
        properties.setProperty(entry.getKey(), entry.getValue());
      }
    }

    return properties;
  }

  /**
   * Null-safe implementation for method invocations that return a List Collection. If the returned
   * List is null, then this method will return an empty List in it's place.
   * <p/>
   *
   * @param <T> the class type of the List's elements.
   * @param list the target List to verify as not null.
   * @return the specified List if not null otherwise return an empty List.
   */
  public static <T> List<T> emptyList(final List<T> list) {
    return (list != null ? list : Collections.emptyList());
  }

  /**
   * Null-safe implementation for method invocations that return a Set Collection. If the returned
   * Set is null, then this method will return an empty Set in it's place.
   * <p/>
   *
   * @param <T> the class type of the Set's elements.
   * @param set the target Set to verify as not null.
   * @return the specified Set if not null otherwise return an empty Set.
   */
  public static <T> Set<T> emptySet(final Set<T> set) {
    return (set != null ? set : Collections.emptySet());
  }

  /**
   * Iterates the Collection and finds all object elements that match the Filter criteria.
   * <p/>
   *
   * @param <T> the class type of the Collection elements.
   * @param collection the Collection of elements to iterate and filter.
   * @param filter the Filter applied to the Collection of elements in search of all matching
   *        elements.
   * @return a List of elements from the Collection matching the criteria of the Filter in the order
   *         in which they were found. If no elements match the Filter criteria, then an empty List
   *         is returned.
   */
  public static <T> List<T> findAll(final Collection<T> collection, final Filter<T> filter) {
    final List<T> matches = new ArrayList<T>(collection.size());

    for (final T element : collection) {
      if (filter.accept(element)) {
        matches.add(element);
      }
    }

    return matches;
  }

  /**
   * Iterates the Collection and finds all object elements that match the Filter criteria.
   * <p/>
   *
   * @param <T> the class type of the Collection elements.
   * @param collection the Collection of elements to iterate and filter.
   * @param filter the Filter applied to the Collection of elements in search of the matching
   *        element.
   * @return a single element from the Collection that match the criteria of the Filter. If multiple
   *         elements match the Filter criteria, then this method will return the first one. If no
   *         element of the Collection matches the criteria of the Filter, then this method returns
   *         null.
   */
  public static <T> T findBy(final Collection<T> collection, final Filter<T> filter) {
    for (T element : collection) {
      if (filter.accept(element)) {
        return element;
      }
    }

    return null;
  }

  /**
   * Removes keys from the Map based on a Filter.
   * <p/>
   *
   * @param <K> the Class type of the key.
   * @param <V> the Class type of the value.
   * @param map the Map from which to remove key-value pairs based on a Filter.
   * @param filter the Filter to apply to the Map entries to ascertain their "value".
   * @return the Map with entries filtered by the specified Filter.
   * @see java.util.Map
   * @see java.util.Map.Entry
   * @see org.apache.geode.internal.lang.Filter
   */
  public static <K, V> Map<K, V> removeKeys(final Map<K, V> map,
      final Filter<Map.Entry<K, V>> filter) {
    for (final Iterator<Map.Entry<K, V>> mapEntries = map.entrySet().iterator(); mapEntries
        .hasNext();) {
      if (!filter.accept(mapEntries.next())) {
        mapEntries.remove();
      }
    }

    return map;
  }

  /**
   * Removes keys with null values in the Map.
   * <p/>
   *
   * @param map the Map from which to remove null key-value pairs.
   * @return the Map without any null keys or values.
   * @see #removeKeys
   * @see java.util.Map
   */
  public static <K, V> Map<K, V> removeKeysWithNullValues(final Map<K, V> map) {
    return removeKeys(map, new Filter<Map.Entry<K, V>>() {
      @Override
      public boolean accept(final Map.Entry<K, V> entry) {
        return (entry.getValue() != null);
      }
    });
  }

  /**
   * Add all elements of an {@link Enumeration} to a {@link Collection}.
   *
   * @param collection to add from enumeration.
   * @param enumeration to add to collection.
   * @return true if collection is modified, otherwise false.
   * @since GemFire 8.1
   * @see Collection#addAll(Collection)
   */
  public static <T> boolean addAll(final Collection<T> collection,
      final Enumeration<T> enumeration) {
    if (null == enumeration) {
      return false;
    }

    boolean modified = false;
    while (enumeration.hasMoreElements()) {
      modified |= collection.add(enumeration.nextElement());
    }
    return modified;
  }

  /**
   * Construct a new unmodifiable {@link Iterable} backed by the supplied <code>iterable</code>.
   *
   * {@link Iterable#iterator()} will return an umodifiable {@link Iterator} on which calling
   * {@link Iterator#remove()} will throw {@link UnsupportedOperationException}.
   *
   * @param iterable to wrap as unmodifiable
   * @return unmodifiable {@link Iterable}
   * @since GemFire 8.1
   */
  public static <T> Iterable<T> unmodifiableIterable(final Iterable<T> iterable) {
    return new UnmodifiableIterable<T>(iterable);
  }

  /**
   * Unmodifiable {@link Iterable} in the style of
   * {@link Collections#unmodifiableCollection(Collection)}.
   *
   *
   * @since GemFire 8.1
   */
  private static class UnmodifiableIterable<T> implements Iterable<T> {

    private final Iterable<T> iterable;

    private UnmodifiableIterable(final Iterable<T> iterable) {
      this.iterable = iterable;
    }

    @Override
    public Iterator<T> iterator() {
      return new Iterator<T>() {
        private final Iterator<T> iterator = iterable.iterator();

        @Override
        public boolean hasNext() {
          return iterator.hasNext();
        }

        @Override
        public T next() {
          return iterator.next();
        }

        @Override
        public void remove() {
          throw new UnsupportedOperationException();
        }
      };
    }
  }
}

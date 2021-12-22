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
package org.apache.geode.cache.query.internal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeSet;

import org.junit.Test;

import org.apache.geode.cache.query.internal.types.ObjectTypeImpl;

public class ResultsCollectionWrapperLimitJUnitTest {

  @Test
  public void testConstructorBehaviour() {
    // Create a Collection of unordered data elements
    HashSet unordered = new HashSet();
    for (int i = 1; i < 11; ++i) {
      unordered.add(i);
    }
    ResultsCollectionWrapper wrapper =
        new ResultsCollectionWrapper(new ObjectTypeImpl(Object.class), unordered, 15);
    assertEquals(10, wrapper.size());
    assertFalse(wrapper.iterator() instanceof ResultsCollectionWrapper.LimitIterator);
    try {
      wrapper.add(7);
    } catch (UnsupportedOperationException uoe) {
      // Ok
    }
    wrapper = new ResultsCollectionWrapper(new ObjectTypeImpl(Object.class), unordered, 5);
    assertEquals(5, wrapper.size());
    assertFalse(wrapper.iterator() instanceof ResultsCollectionWrapper.LimitIterator);
    try {
      wrapper.add(7);
    } catch (UnsupportedOperationException uoe) {
      // Ok
    }
    List ordered = new ArrayList();
    for (int i = 1; i < 11; ++i) {
      ordered.add(i);
    }
    wrapper = new ResultsCollectionWrapper(new ObjectTypeImpl(Object.class), ordered, 20);
    assertEquals(10, wrapper.size());
    assertFalse(wrapper.iterator() instanceof ResultsCollectionWrapper.LimitIterator);
    try {
      wrapper.add(7);
    } catch (UnsupportedOperationException uoe) {
      // Ok
    }
    wrapper = new ResultsCollectionWrapper(new ObjectTypeImpl(Object.class), ordered, 5);
    assertEquals(5, wrapper.size());
    assertTrue(wrapper.iterator() instanceof ResultsCollectionWrapper.LimitIterator);
    try {
      wrapper.add(7);
    } catch (UnsupportedOperationException uoe) {
      // Ok
    }
  }

  @Test
  public void testContains() {
    // Create a Collection of unordered data elements
    HashSet unordered = new HashSet();
    for (int i = 1; i < 11; ++i) {
      unordered.add(i);
    }
    ResultsCollectionWrapper wrapper =
        new ResultsCollectionWrapper(new ObjectTypeImpl(Object.class), unordered, 15);
    assertTrue(wrapper.contains(10));
    assertFalse(wrapper.contains(11));
    List ordered = new ArrayList();
    for (int i = 1; i < 11; ++i) {
      ordered.add(i);
    }
    wrapper = new ResultsCollectionWrapper(new ObjectTypeImpl(Object.class), ordered, 20);
    assertTrue(wrapper.contains(10));
    assertFalse(wrapper.contains(11));
    wrapper = new ResultsCollectionWrapper(new ObjectTypeImpl(Object.class), ordered, 5);
    assertTrue(wrapper.contains(1));
  }

  @Test
  public void testContainsAll() {
    List ordered = new ArrayList();
    for (int i = 1; i < 11; ++i) {
      ordered.add(i);
    }
    ResultsCollectionWrapper wrapper =
        new ResultsCollectionWrapper(new ObjectTypeImpl(Object.class), ordered, 5);
    assertFalse(wrapper.containsAll(ordered));
    List newList = new ArrayList();
    for (int i = 0; i < 5; ++i) {
      newList.add(ordered.get(4 - i));
    }
    assertTrue(wrapper.containsAll(newList));
  }

  @Test
  public void testEmpty() {
    List ordered = new ArrayList();
    for (int i = 1; i < 11; ++i) {
      ordered.add(i);
    }
    ResultsCollectionWrapper wrapper =
        new ResultsCollectionWrapper(new ObjectTypeImpl(Object.class), ordered, 0);
    assertEquals(0, wrapper.size());
  }

  @Test
  public void testRemove() {
    List ordered = new ArrayList();
    for (int i = 1; i < 11; ++i) {
      ordered.add(i);
    }
    ResultsCollectionWrapper wrapper =
        new ResultsCollectionWrapper(new ObjectTypeImpl(Object.class), ordered, 5);
    assertFalse(wrapper.remove(6));
    assertTrue(wrapper.remove(4));
    assertEquals(4, wrapper.size());
    List newList = new ArrayList();
    for (int i = 1; i < 6; ++i) {
      newList.add(i);
    }
    newList.remove(3);
    assertTrue(wrapper.containsAll(newList));
  }

  @Test
  public void testRemoveAll() {
    List ordered = new ArrayList();
    for (int i = 1; i < 11; ++i) {
      ordered.add(i);
    }
    ResultsCollectionWrapper wrapper =
        new ResultsCollectionWrapper(new ObjectTypeImpl(Object.class), ordered, 5);
    assertFalse(wrapper.remove(6));
    assertTrue(wrapper.remove(4));
    assertEquals(4, wrapper.size());
    List newList = new ArrayList();
    for (int i = 1; i < 6; ++i) {
      newList.add(i);
    }
    assertTrue(wrapper.removeAll(newList));
    assertTrue(wrapper.size() == 0);
    ordered = new ArrayList();
    for (int i = 1; i < 11; ++i) {
      ordered.add(i);
    }
    wrapper = new ResultsCollectionWrapper(new ObjectTypeImpl(Object.class), ordered, 5);
    newList = new ArrayList();
    for (int i = 2; i < 11; ++i) {
      newList.add(i);
    }
    assertTrue(wrapper.removeAll(newList));
    assertTrue(wrapper.size() == 1);
    Iterator itr = wrapper.iterator();
    assertEquals(itr.next(), 1);
    assertFalse(itr.hasNext());
  }

  @Test
  public void testRetainAll() {
    List ordered = new ArrayList();
    for (int i = 1; i < 11; ++i) {
      ordered.add(i);
    }
    ResultsCollectionWrapper wrapper =
        new ResultsCollectionWrapper(new ObjectTypeImpl(Object.class), ordered, 5);
    List newList = new ArrayList();
    for (int i = 1; i < 6; ++i) {
      newList.add(i);
    }
    assertFalse(wrapper.retainAll(newList));
    assertEquals(5, wrapper.size());
    for (int i = 6; i < 11; ++i) {
      newList.add(i);
    }
    assertFalse(wrapper.retainAll(newList));
    assertEquals(5, wrapper.size());
    for (int i = 1; i < 6; ++i) {
      newList.remove(0);
    }
    assertTrue(wrapper.retainAll(newList));
    assertEquals(0, wrapper.size());
    Iterator itr = wrapper.iterator();
    assertFalse(itr.hasNext());
  }

  @Test
  public void testToArray() {
    List ordered = new ArrayList();
    for (int i = 1; i < 11; ++i) {
      ordered.add(i);
    }
    ResultsCollectionWrapper wrapper =
        new ResultsCollectionWrapper(new ObjectTypeImpl(Object.class), ordered, 5);
    Object[] toArray = wrapper.toArray();
    assertEquals(wrapper.size(), toArray.length);
    Iterator itr = wrapper.iterator();
    for (int i = 1; i < 6; ++i) {
      assertEquals(toArray[i - 1], itr.next());
    }
    assertFalse(itr.hasNext());
  }

  @Test
  public void testToArrayParameterized() {
    List ordered = new ArrayList();
    for (int i = 1; i < 11; ++i) {
      ordered.add(i);
    }
    ResultsCollectionWrapper wrapper =
        new ResultsCollectionWrapper(new ObjectTypeImpl(Object.class), ordered, 5);
    Object[] toArray = new Object[5];
    wrapper.toArray(toArray);
    assertEquals(wrapper.size(), toArray.length);
    Iterator itr = wrapper.iterator();
    for (int i = 1; i < 6; ++i) {
      assertEquals(toArray[i - 1], itr.next());
    }
    assertFalse(itr.hasNext());
  }

  @Test
  public void testAsList() {
    List ordered = new ArrayList();
    for (int i = 1; i < 11; ++i) {
      ordered.add(i);
    }
    ResultsCollectionWrapper wrapper =
        new ResultsCollectionWrapper(new ObjectTypeImpl(Object.class), ordered, 5);
    List toList = wrapper.asList();
    List newList = new ArrayList();
    for (int i = 1; i < 6; ++i) {
      newList.add(i);
    }
    assertEquals(newList.size(), toList.size());
    assertTrue(newList.containsAll(toList));

    ordered = new ArrayList();
    for (int i = 1; i < 11; ++i) {
      ordered.add(i);
    }
    wrapper = new ResultsCollectionWrapper(new ObjectTypeImpl(Object.class), ordered, 2);
    toList = wrapper.asList();
    newList = new ArrayList();
    for (int i = 1; i < 3; ++i) {
      newList.add(i);
    }
    assertEquals(newList.size(), toList.size());
    assertTrue(newList.containsAll(toList));
  }

  @Test
  public void testAsSet() {
    Collection ordered = new TreeSet();
    for (int i = 1; i < 11; ++i) {
      ordered.add(i);
    }
    ResultsCollectionWrapper wrapper =
        new ResultsCollectionWrapper(new ObjectTypeImpl(Object.class), ordered, 5);
    Set toSet = wrapper.asSet();
    Set newSet = new TreeSet();
    for (int i = 1; i < 6; ++i) {
      newSet.add(i);
    }
    assertEquals(newSet.size(), toSet.size());
    assertTrue(newSet.containsAll(toSet));
    ordered = new ArrayList();
    for (int i = 1; i < 11; ++i) {
      ordered.add(i);
    }
    wrapper = new ResultsCollectionWrapper(new ObjectTypeImpl(Object.class), ordered, 6);
    toSet = wrapper.asSet();
    newSet = new TreeSet();
    for (int i = 1; i < 7; ++i) {
      newSet.add(i);
    }
    assertEquals(newSet.size(), toSet.size());
    assertTrue(newSet.containsAll(toSet));
  }

  @Test
  public void testOccurrences() {
    Collection ordered = new TreeSet();
    for (int i = 1; i < 11; ++i) {
      ordered.add(i);
    }
    ResultsCollectionWrapper wrapper =
        new ResultsCollectionWrapper(new ObjectTypeImpl(Object.class), ordered, 5);
    assertEquals(0, wrapper.occurrences(6));
    for (int i = 1; i < 6; ++i) {
      assertEquals(1, wrapper.occurrences(i));
    }
    ordered = new ArrayList();
    for (int i = 1; i < 11; ++i) {
      ordered.add(i);
    }
    wrapper = new ResultsCollectionWrapper(new ObjectTypeImpl(Object.class), ordered, 5);
    assertEquals(0, wrapper.occurrences(6));
    for (int i = 1; i < 6; ++i) {
      assertEquals(1, wrapper.occurrences(i));
    }
  }

  @Test
  public void testLimitIterator() {
    Collection ordered = new TreeSet();
    for (int i = 1; i < 11; ++i) {
      ordered.add(i);
    }
    ResultsCollectionWrapper wrapper =
        new ResultsCollectionWrapper(new ObjectTypeImpl(Object.class), ordered, 5);
    Iterator itr = wrapper.iterator();
    assertEquals(5, wrapper.size());
    while (itr.hasNext()) {
      itr.next();
      itr.remove();
    }
    assertEquals(0, wrapper.size());
    assertFalse(wrapper.iterator().hasNext());
    try {
      wrapper.iterator().next();
      fail("Should have failed as the wrapper size is 0");
    } catch (NoSuchElementException nsee) {
      // Ok
    }
    assertTrue(wrapper.isEmpty());

  }
}

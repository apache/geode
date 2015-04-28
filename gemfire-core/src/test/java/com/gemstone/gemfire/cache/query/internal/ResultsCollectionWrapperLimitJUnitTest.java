/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/**
 * 
 */
package com.gemstone.gemfire.cache.query.internal;

import com.gemstone.gemfire.cache.query.internal.types.ObjectTypeImpl;
import com.gemstone.junit.UnitTest;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeSet;

import org.junit.experimental.categories.Category;

import junit.framework.TestCase;

/**
 * @author Asif
 * 
 */
@Category(UnitTest.class)
public class ResultsCollectionWrapperLimitJUnitTest extends TestCase {

  public ResultsCollectionWrapperLimitJUnitTest(String testName) {
    super(testName);
  }

  public void testConstructorBehaviour() {
    // Create a Collection of unordered data elements
    HashSet unordered = new HashSet();
    for (int i = 1; i < 11; ++i) {
      unordered.add(new Integer(i));
    }
    ResultsCollectionWrapper wrapper = new ResultsCollectionWrapper(
        new ObjectTypeImpl(Object.class), unordered, 15);
    assertEquals(10, wrapper.size());
    assertFalse(wrapper.iterator() instanceof ResultsCollectionWrapper.LimitIterator);
    try {
      wrapper.add(new Integer(7));
    }
    catch (UnsupportedOperationException uoe) {
      // Ok
    }
    wrapper = new ResultsCollectionWrapper(new ObjectTypeImpl(Object.class),
        unordered, 5);
    assertEquals(5, wrapper.size());
    assertFalse(wrapper.iterator() instanceof ResultsCollectionWrapper.LimitIterator);
    try {
      wrapper.add(new Integer(7));
    }
    catch (UnsupportedOperationException uoe) {
      // Ok
    }
    List ordered = new ArrayList();
    for (int i = 1; i < 11; ++i) {
      ordered.add(new Integer(i));
    }
    wrapper = new ResultsCollectionWrapper(new ObjectTypeImpl(Object.class),
        ordered, 20);
    assertEquals(10, wrapper.size());
    assertFalse(wrapper.iterator() instanceof ResultsCollectionWrapper.LimitIterator);
    try {
      wrapper.add(new Integer(7));
    }
    catch (UnsupportedOperationException uoe) {
      // Ok
    }
    wrapper = new ResultsCollectionWrapper(new ObjectTypeImpl(Object.class),
        ordered, 5);
    assertEquals(5, wrapper.size());
    assertTrue(wrapper.iterator() instanceof ResultsCollectionWrapper.LimitIterator);
    try {
      wrapper.add(new Integer(7));
    }
    catch (UnsupportedOperationException uoe) {
      // Ok
    }
  }

  public void testContains() {
    // Create a Collection of unordered data elements
    HashSet unordered = new HashSet();
    for (int i = 1; i < 11; ++i) {
      unordered.add(new Integer(i));
    }
    ResultsCollectionWrapper wrapper = new ResultsCollectionWrapper(
        new ObjectTypeImpl(Object.class), unordered, 15);
    assertTrue(wrapper.contains(new Integer(10)));
    assertFalse(wrapper.contains(new Integer(11)));
    List ordered = new ArrayList();
    for (int i = 1; i < 11; ++i) {
      ordered.add(new Integer(i));
    }
    wrapper = new ResultsCollectionWrapper(new ObjectTypeImpl(Object.class),
        ordered, 20);
    assertTrue(wrapper.contains(new Integer(10)));
    assertFalse(wrapper.contains(new Integer(11)));
    wrapper = new ResultsCollectionWrapper(new ObjectTypeImpl(Object.class),
        ordered, 5);
    assertTrue(wrapper.contains(new Integer(1)));
  }

  public void testContainsAll() {
    List ordered = new ArrayList();
    for (int i = 1; i < 11; ++i) {
      ordered.add(new Integer(i));
    }
    ResultsCollectionWrapper wrapper = new ResultsCollectionWrapper(
        new ObjectTypeImpl(Object.class), ordered, 5);
    assertFalse(wrapper.containsAll(ordered));
    List newList = new ArrayList();
    for (int i = 0; i < 5; ++i) {
      newList.add(ordered.get(4 - i));
    }
    assertTrue(wrapper.containsAll(newList));
  }

  public void testEmpty() {
    List ordered = new ArrayList();
    for (int i = 1; i < 11; ++i) {
      ordered.add(new Integer(i));
    }
    ResultsCollectionWrapper wrapper = new ResultsCollectionWrapper(
        new ObjectTypeImpl(Object.class), ordered, 0);
    assertEquals(0, wrapper.size());
  }

  public void testRemove() {
    List ordered = new ArrayList();
    for (int i = 1; i < 11; ++i) {
      ordered.add(new Integer(i));
    }
    ResultsCollectionWrapper wrapper = new ResultsCollectionWrapper(
        new ObjectTypeImpl(Object.class), ordered, 5);
    assertFalse(wrapper.remove(new Integer(6)));
    assertTrue(wrapper.remove(new Integer(4)));
    assertEquals(4, wrapper.size());
    List newList = new ArrayList();
    for (int i = 1; i < 6; ++i) {
      newList.add(new Integer(i));
    }
    newList.remove(3);
    assertTrue(wrapper.containsAll(newList));
  }

  public void testRemoveAll() {
    List ordered = new ArrayList();
    for (int i = 1; i < 11; ++i) {
      ordered.add(new Integer(i));
    }
    ResultsCollectionWrapper wrapper = new ResultsCollectionWrapper(
        new ObjectTypeImpl(Object.class), ordered, 5);
    assertFalse(wrapper.remove(new Integer(6)));
    assertTrue(wrapper.remove(new Integer(4)));
    assertEquals(4, wrapper.size());
    List newList = new ArrayList();
    for (int i = 1; i < 6; ++i) {
      newList.add(new Integer(i));
    }
    assertTrue(wrapper.removeAll(newList));
    assertTrue(wrapper.size() == 0);
    ordered = new ArrayList();
    for (int i = 1; i < 11; ++i) {
      ordered.add(new Integer(i));
    }
    wrapper = new ResultsCollectionWrapper(new ObjectTypeImpl(Object.class),
        ordered, 5);
    newList = new ArrayList();
    for (int i = 2; i < 11; ++i) {
      newList.add(new Integer(i));
    }
    assertTrue(wrapper.removeAll(newList));
    assertTrue(wrapper.size() == 1);
    Iterator itr = wrapper.iterator();
    assertEquals(itr.next(), new Integer(1));
    assertFalse(itr.hasNext());
  }

  public void testRetainAll() {
    List ordered = new ArrayList();
    for (int i = 1; i < 11; ++i) {
      ordered.add(new Integer(i));
    }
    ResultsCollectionWrapper wrapper = new ResultsCollectionWrapper(
        new ObjectTypeImpl(Object.class), ordered, 5);
    List newList = new ArrayList();
    for (int i = 1; i < 6; ++i) {
      newList.add(new Integer(i));
    }
    assertFalse(wrapper.retainAll(newList));
    assertEquals(5, wrapper.size());
    for (int i = 6; i < 11; ++i) {
      newList.add(new Integer(i));
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

  public void testToArray() {
    List ordered = new ArrayList();
    for (int i = 1; i < 11; ++i) {
      ordered.add(new Integer(i));
    }
    ResultsCollectionWrapper wrapper = new ResultsCollectionWrapper(
        new ObjectTypeImpl(Object.class), ordered, 5);
    Object[] toArray = wrapper.toArray();
    assertEquals(wrapper.size(), toArray.length);
    Iterator itr = wrapper.iterator();
    for (int i = 1; i < 6; ++i) {
      assertEquals(toArray[i - 1], itr.next());
    }
    assertFalse(itr.hasNext());
  }

  public void testToArrayParameterized() {
    List ordered = new ArrayList();
    for (int i = 1; i < 11; ++i) {
      ordered.add(new Integer(i));
    }
    ResultsCollectionWrapper wrapper = new ResultsCollectionWrapper(
        new ObjectTypeImpl(Object.class), ordered, 5);
    Object[] toArray = new Object[5];
    wrapper.toArray(toArray);
    assertEquals(wrapper.size(), toArray.length);
    Iterator itr = wrapper.iterator();
    for (int i = 1; i < 6; ++i) {
      assertEquals(toArray[i - 1], itr.next());
    }
    assertFalse(itr.hasNext());
  }

  public void testAsList() {
    List ordered = new ArrayList();
    for (int i = 1; i < 11; ++i) {
      ordered.add(new Integer(i));
    }
    ResultsCollectionWrapper wrapper = new ResultsCollectionWrapper(
        new ObjectTypeImpl(Object.class), ordered, 5);
    List toList = wrapper.asList();
    List newList = new ArrayList();
    for (int i = 1; i < 6; ++i) {
      newList.add(new Integer(i));
    }
    assertEquals(newList.size(), toList.size());
    assertTrue(newList.containsAll(toList));

    ordered = new ArrayList();
    for (int i = 1; i < 11; ++i) {
      ordered.add(new Integer(i));
    }
    wrapper = new ResultsCollectionWrapper(new ObjectTypeImpl(Object.class),
        ordered, 2);
    toList = wrapper.asList();
    newList = new ArrayList();
    for (int i = 1; i < 3; ++i) {
      newList.add(new Integer(i));
    }
    assertEquals(newList.size(), toList.size());
    assertTrue(newList.containsAll(toList));
  }

  public void testAsSet() {
    Collection ordered = new TreeSet();
    for (int i = 1; i < 11; ++i) {
      ordered.add(new Integer(i));
    }
    ResultsCollectionWrapper wrapper = new ResultsCollectionWrapper(
        new ObjectTypeImpl(Object.class), ordered, 5);
    Set toSet = wrapper.asSet();
    Set newSet = new TreeSet();
    for (int i = 1; i < 6; ++i) {
      newSet.add(new Integer(i));
    }
    assertEquals(newSet.size(), toSet.size());
    assertTrue(newSet.containsAll(toSet));
    ordered = new ArrayList();
    for (int i = 1; i < 11; ++i) {
      ordered.add(new Integer(i));
    }
    wrapper = new ResultsCollectionWrapper(new ObjectTypeImpl(Object.class),
        ordered, 6);
    toSet = wrapper.asSet();
    newSet = new TreeSet();
    for (int i = 1; i < 7; ++i) {
      newSet.add(new Integer(i));
    }
    assertEquals(newSet.size(), toSet.size());
    assertTrue(newSet.containsAll(toSet));
  }

  public void testOccurences() {
    Collection ordered = new TreeSet();
    for (int i = 1; i < 11; ++i) {
      ordered.add(new Integer(i));
    }
    ResultsCollectionWrapper wrapper = new ResultsCollectionWrapper(
        new ObjectTypeImpl(Object.class), ordered, 5);
    assertEquals(0, wrapper.occurrences(new Integer(6)));
    for (int i = 1; i < 6; ++i) {
      assertEquals(1, wrapper.occurrences(new Integer(i)));
    }
    ordered = new ArrayList();
    for (int i = 1; i < 11; ++i) {
      ordered.add(new Integer(i));
    }
    wrapper = new ResultsCollectionWrapper(new ObjectTypeImpl(Object.class),
        ordered, 5);
    assertEquals(0, wrapper.occurrences(new Integer(6)));
    for (int i = 1; i < 6; ++i) {
      assertEquals(1, wrapper.occurrences(new Integer(i)));
    }
  }

  public void testLimitIterator() {
    Collection ordered = new TreeSet();
    for (int i = 1; i < 11; ++i) {
      ordered.add(new Integer(i));
    }
    ResultsCollectionWrapper wrapper = new ResultsCollectionWrapper(
        new ObjectTypeImpl(Object.class), ordered, 5);
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
    }
    catch (NoSuchElementException nsee) {
      // Ok
    }
    assertTrue(wrapper.isEmpty());

  }
}

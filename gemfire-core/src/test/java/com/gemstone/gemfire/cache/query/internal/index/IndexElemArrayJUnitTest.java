/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.query.internal.index;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Iterator;

import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.junit.UnitTest;

@Category(UnitTest.class)
public class IndexElemArrayJUnitTest {
  
  private IndexElemArray list;

  @After
  public void tearDown() {
    System.clearProperty("index_elemarray_size");
  }
  
  @Test
  public void testFunctionality() throws Exception {
    System.setProperty("index_elemarray_size", "7");
    list = new IndexElemArray();
    boundaryCondition();
    add();
    clearAndAdd();
    removeFirst();
    clearAndAdd();
    removeLast();
    clearAndAdd();
    remove();
    clearAndAdd();
    iterate();
    clearAndAdd();
  }

  private void add() {
    Object objBefore = list.getElementData();
    insert(7);
    Object objAfter = list.getElementData();
    assertSame("Before: " + Arrays.asList((Object[])objBefore) + " After:" + Arrays.asList((Object[])objAfter), objBefore, objAfter);

    assertEquals(7, list.size());
    for (int i = 0; i < 7; i++) {
      assertEquals(i + 1, list.get(i));
    }
    list.add(8);
    objAfter = list.getElementData();
    assertNotSame("Before: " + Arrays.asList((Object[])objBefore) + " After:" + Arrays.asList((Object[])objAfter), objBefore, objAfter);
    
    assertEquals(8, list.size());
    for (int i = 0; i < 8; i++) {
      assertEquals(i + 1, list.get(i));
    }
  }

  private void insert(int num) {
    for (int i = 1; i <= num; i++) {
      list.add(i);
    }
  }

  private void removeFirst() {
    list.remove(1);
    assertEquals(6, list.size());
    for (int i = 0; i < 6; i++) {
      assertEquals(i + 2, list.get(i));
    }
  }

  private void removeLast() {
    list.remove(7);
    assertEquals(6, list.size());
    for (int i = 0; i < 6; i++) {
      assertEquals(i + 1, list.get(i));
    }
  }

  private void remove() {
    list.remove(4);
    assertEquals(6, list.size());
    int temp[] = { 1, 2, 3, 5, 6, 7 };
    for (int i = 0; i < 6; i++) {
      assertEquals(temp[i], list.get(i));
    }
  }

  private void clearAndAdd() {
    list.clear();
    insert(7);
  }

  private void iterate() {
    Iterator itr = list.iterator();
    int i = 1;
    while (itr.hasNext()) {
      assertEquals(i++, itr.next());
    }
  }
  
  private void boundaryCondition() {
    try {
      Object o = list.get(2);
      fail("get() Should have thrown IndexOutOfBoundsException");
    } catch (IndexOutOfBoundsException expected) {
      // ok
    }
  }
}

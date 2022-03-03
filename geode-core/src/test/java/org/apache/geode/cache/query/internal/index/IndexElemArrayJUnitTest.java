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
package org.apache.geode.cache.query.internal.index;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.IntStream;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.query.MultithreadedTester;

public class IndexElemArrayJUnitTest {

  private IndexElemArray list;

  @Before
  public void setUp() throws Exception {
    list = new IndexElemArray(7);
  }

  @Test
  public void testFunctionality() throws Exception {
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

  @Test
  public void overflowFromAddThrowsException() {
    // no exception should be thrown by this loop
    for (int i = 1; i < 256; i++) {
      list.add(i);
    }
    try {
      list.add(256);
      fail("list should have thrown an exception when full");
    } catch (IllegalStateException e) {
      // expected
    }
  }

  @Test(expected = IllegalStateException.class)
  public void overflowFromAddAllThrowsException() {
    Object[] array = new Object[256];
    Arrays.fill(array, new Object());
    List<Object> data = Arrays.asList(array);
    list.addAll(data);
  }

  @Test
  public void sizeAfterOverflowFromAddIsCorrect() {
    for (int i = 1; i < 256; i++) {
      list.add(i);
    }
    try {
      list.add(256);
    } catch (IllegalStateException e) {
      assertThat(list.size()).isEqualTo(255);
    }
  }

  @Test
  public void sizeAfterOverflowFromAddAllIsCorrect() {
    for (int i = 1; i < 256; i++) {
      list.add(i);
    }
    try {
      list.add(new Object());
    } catch (IllegalStateException e) {
      assertThat(list.size()).isEqualTo(255);
    }
  }

  @Test
  public void listCanBeIteratedOverFullRange() {
    for (int i = 1; i < 256; i++) {
      list.add(i);
    }
    for (int i = 1; i < 256; i++) {
      assertThat(list.get(i - 1)).isEqualTo(i);
    }
  }


  /**
   * This tests concurrent modification of IndexElemArray and to make sure elementData and size are
   * updated atomically. Ticket# GEODE-106.
   */
  @Test
  public void testFunctionalityUsingMultiThread() throws Exception {
    Collection<Callable> callables = new ConcurrentLinkedQueue<>();
    IntStream.range(0, 1000).parallel().forEach(i -> {
      callables.add(() -> {
        if (i % 3 == 0) {
          return add(new Random().nextInt(4));
        } else if (i % 3 == 1) {
          return remove(new Random().nextInt(4));
        } else {
          return iterateList();
        }
      });
    });

    Collection<Object> results = MultithreadedTester.runMultithreaded(callables);
    results.forEach(result -> {
      // There should not be any Exception here.
      // E.g. ArrayIndexOutOfBoundsException when multiple threads are acting.
      assertTrue(result.getClass().getName() + " was not an expected result",
          result instanceof Integer);
    });
  }

  private Integer add(Integer i) {
    list.add(i);
    return i;
  }

  private Integer remove(Integer i) {
    list.remove(i);
    return i;
  }

  private Integer iterateList() {
    Iterator iter = list.iterator();
    if (iter.hasNext()) {
      iter.next();
    }
    return list.size();
  }

  private void add() {
    Object objBefore = list.getElementData();
    insert(7);
    Object objAfter = list.getElementData();
    assertSame("Before: " + Arrays.asList((Object[]) objBefore) + " After:"
        + Arrays.asList((Object[]) objAfter), objBefore, objAfter);

    assertEquals(7, list.size());
    for (int i = 0; i < 7; i++) {
      assertEquals(i + 1, list.get(i));
    }
    list.add(8);
    objAfter = list.getElementData();
    assertNotSame("Before: " + Arrays.asList((Object[]) objBefore) + " After:"
        + Arrays.asList((Object[]) objAfter), objBefore, objAfter);

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
    int[] temp = {1, 2, 3, 5, 6, 7};
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

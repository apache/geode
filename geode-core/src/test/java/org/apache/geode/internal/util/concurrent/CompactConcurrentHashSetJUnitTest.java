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
package org.apache.geode.internal.util.concurrent;

import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.internal.concurrent.CompactConcurrentHashSet2;

public class CompactConcurrentHashSetJUnitTest {

  private static final int RANGE = 100000;

  private Random random;

  @Before
  public void setUp() throws Exception {
    random = new Random();
  }

  @Test
  public void testEquals() { // TODO: reduce test runtime
    Set s1, s2;
    s1 = new CompactConcurrentHashSet2();
    s2 = new HashSet();
    for (int i = 0; i < 10000; i++) {
      int nexti = random.nextInt(RANGE);
      s1.add(nexti);
      s2.add(nexti);
      assertTrue("expected s1 and s2 to be equal", s1.equals(s2));
      assertTrue("expected s1 and s2 to be equal", s2.equals(s1));
    }
    assertTrue(s1.hashCode() != 0);
    s2 = new CompactConcurrentHashSet2(2);
    s2.addAll(s1);
    assertTrue("expected s1 and s2 to be equal", s1.equals(s2));
    assertTrue("expected s1 and s2 to be equal", s2.equals(s1));
  }

  @Test
  public void testIterator() {
    Set<Integer> s1;
    s1 = new CompactConcurrentHashSet2<>();
    for (int i = 0; i < 10000; i++) {
      int nexti = random.nextInt(RANGE);
      s1.add(nexti);
    }
    for (Iterator<Integer> it = s1.iterator(); it.hasNext();) {
      Integer i = it.next();
      assertTrue(s1.contains(i));
      it.remove();
      if (s1.contains(i)) {
        assertTrue(!s1.contains(i));
      }
    }
    assertTrue(s1.size() == 0);
  }

  @Test
  public void testSize() {
    Set<Integer> s1, s2;
    s1 = new CompactConcurrentHashSet2<>();
    s2 = new HashSet<>();
    for (int i = 0; i < 10000; i++) {
      int nexti = random.nextInt(RANGE);
      s1.add(nexti);
      s2.add(nexti);
    }
    int size = s2.size(); // trust HashSet.size()
    assertTrue(s1.size() == size);
    s2 = new CompactConcurrentHashSet2<>(s1);
    assertTrue(s2.size() == size);
    int i = size - 1;
    for (Iterator<Integer> it = s2.iterator(); it.hasNext(); i--) {
      s1.remove(it.next());
      assertTrue(s1.size() == i);
    }
    assertTrue(s1.size() == 0);
    assertTrue(s1.isEmpty());
    assertTrue(!s2.isEmpty());
    s2.clear();
    assertTrue(s2.isEmpty());
  }

}

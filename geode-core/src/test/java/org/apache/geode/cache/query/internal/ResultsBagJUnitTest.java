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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.DataInputStream;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

import org.junit.Test;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.internal.types.ObjectTypeImpl;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.serialization.KnownVersion;

/**
 * Test ResultsBag, including null elements
 */
public class ResultsBagJUnitTest {

  @Test
  public void testDuplicates() {
    ResultsBag bag = new ResultsBag();
    bag.add("one");
    bag.add("two");
    assertEquals(2, bag.size());
    bag.add("two");
    assertEquals(3, bag.size());
    assertEquals(1, bag.occurrences("one"));
    assertEquals(2, bag.occurrences("two"));

    assertTrue(bag.remove("two"));
    assertEquals(1, bag.occurrences("two"));
    assertTrue(bag.remove("one"));
    assertEquals(0, bag.occurrences("one"));
    assertTrue(!bag.remove("one"));
    assertEquals(0, bag.occurrences("one"));
  }

  @Test
  public void testIteration() {
    ResultsBag bag = new ResultsBag();
    bag.add(1);
    bag.add(2);
    bag.add(2);

    int numOnes = 0;
    int numTwos = 0;
    Integer one = 1;
    Integer two = 2;
    for (Object n : bag) {
      if (one.equals(n)) {
        numOnes++;
      } else if (two.equals(n)) {
        numTwos++;
      } else {
        fail(n + " did not equal 1 or 2");
      }
    }
    assertEquals(1, numOnes);
    assertEquals(2, numTwos);
  }

  @Test
  public void testSerializingSetViewWithNulls() throws Exception {
    ResultsBag bag = new ResultsBag();
    bag.add(4);
    bag.add(2);
    bag.add(42);
    bag.add(null);
    bag.add(null);
    bag.add(null);

    assertEquals(6, bag.size());
    assertEquals(1, bag.occurrences(4));
    assertEquals(3, bag.occurrences(null));

    Set set = bag.asSet();
    assertEquals(4, set.size());
    assertTrue(set.contains(4));
    assertTrue(set.contains(null));

    ResultsCollectionWrapper w =
        new ResultsCollectionWrapper(new ObjectTypeImpl(Integer.class), set);

    HeapDataOutputStream hdos = new HeapDataOutputStream(KnownVersion.CURRENT);
    DataSerializer.writeObject(w, hdos);
    DataInputStream in = new DataInputStream(hdos.getInputStream());
    SelectResults setCopy = DataSerializer.readObject(in);

    assertEquals(4, setCopy.size());
    assertTrue(setCopy.contains(4));
    assertTrue(setCopy.contains(null));
  }

  @Test
  public void testNulls() {
    ResultsBag bag = new ResultsBag();
    assertTrue(bag.isEmpty());
    bag.add(null);
    assertTrue(!bag.isEmpty());
    assertEquals(1, bag.size());
    assertEquals(1, bag.occurrences(null));

    bag.add(1);
    assertEquals(2, bag.size());
    bag.add(2);
    assertEquals(3, bag.size());
    bag.add(2);
    assertEquals(4, bag.size());

    bag.add(null);
    assertEquals(5, bag.size());
    assertEquals(2, bag.occurrences(null));

    int numNulls = 0;
    int numOnes = 0;
    int numTwos = 0;
    Integer one = 1;
    Integer two = 2;
    for (Object n : bag) {
      if (one.equals(n)) {
        numOnes++;
      } else if (two.equals(n)) {
        numTwos++;
      } else if (n == null) {
        numNulls++;
      } else {
        fail(n + " was not null and did not equal 1 or 2");
      }
    }
    assertEquals(1, numOnes);
    assertEquals(2, numTwos);
    assertEquals(2, numNulls);

    // make sure toString doesn't blow up with nulls
    String s = bag.toString();
    assertTrue("toString didn't contain 'null': '" + s + "'", s.indexOf("null") > 0);

    assertTrue(bag.remove(null));
    assertEquals(1, bag.occurrences(null));
    assertTrue(bag.remove(null));
    assertEquals(0, bag.occurrences(null));
    assertTrue(!bag.remove(null));
    assertEquals(0, bag.occurrences(null));
  }

  @Test
  public void testIterationNullRemoval() {
    ResultsBag bag = new ResultsBag();
    bag.add(null);
    bag.add(null);

    bag.add(1);
    bag.add(2);
    bag.add(2);
    assertEquals(5, bag.size());

    for (Iterator itr = bag.iterator(); itr.hasNext();) {
      Object n = itr.next();
      if (n == null) {
        itr.remove();
      }
    }
    assertEquals(3, bag.size());
    assertEquals(0, bag.occurrences(null));
  }

  @Test
  public void testIterationRemoval() {
    ResultsBag bag = new ResultsBag();

    bag.add(1);
    bag.add(2);
    bag.add(2);
    bag.add(3);
    bag.add(3);
    bag.add(4);

    assertEquals(6, bag.size());

    Iterator itr = bag.iterator();
    for (int i = 0; i < 3; i++) {
      itr.next();
      itr.remove();
    }
    assertEquals(3, bag.size());

    for (int i = 0; i < 3; i++) {
      itr.next();
      itr.remove();
    }
    assertTrue(bag.isEmpty());
    assertEquals(0, bag.size());
  }

  @Test
  public void testNoSuchElementException() {
    ResultsBag bag = new ResultsBag();

    bag.add(1);
    bag.add(2);
    bag.add(2);
    bag.add(3);
    bag.add(3);
    bag.add(4);

    assertEquals(6, bag.size());

    Iterator itr = bag.iterator();
    for (int i = 0; i < 6; i++) {
      itr.next();
    }
    try {
      itr.next();
      fail("should have thrown a NoSuchElementException");
    } catch (NoSuchElementException expected) {
      // pass
    }

    // test with removes
    itr = bag.iterator();
    for (int i = 0; i < 6; i++) {
      itr.next();
      itr.remove();
    }
    assertEquals(0, bag.size());
    try {
      itr.next();
      fail("should have thrown a NoSuchElementException");
    } catch (NoSuchElementException expected) {
      // pass
    }

    // test with nulls
    bag = new ResultsBag();

    bag.add(1);
    bag.add(2);
    bag.add(2);
    bag.add(null);
    bag.add(null);
    bag.add(null);
    bag.add(3);
    bag.add(3);
    bag.add(4);

    assertEquals(9, bag.size());

    itr = bag.iterator();
    for (int i = 0; i < 9; i++) {
      itr.next();
    }
    try {
      itr.next();
      fail("should have thrown a NoSuchElementException");
    } catch (NoSuchElementException expected) {
      // pass
    }

    // test with removes
    itr = bag.iterator();
    for (int i = 0; i < 9; i++) {
      itr.next();
      itr.remove();
    }
    assertEquals(0, bag.size());
    try {
      itr.next();
      fail("should have thrown a NoSuchElementException");
    } catch (NoSuchElementException expected) {
      // pass
    }
  }
}

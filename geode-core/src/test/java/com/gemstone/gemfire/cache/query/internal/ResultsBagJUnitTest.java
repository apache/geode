/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.cache.query.internal;

import java.util.*;
import java.io.*;

import org.junit.experimental.categories.Category;

import junit.framework.*;

import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.internal.types.ObjectTypeImpl;
import com.gemstone.gemfire.internal.HeapDataOutputStream;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

/**
 * Test ResultsBag, including null elements
 */
@Category(UnitTest.class)
public class ResultsBagJUnitTest extends TestCase {

  public ResultsBagJUnitTest(String testName) {
    super(testName);
  }
  
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
  
  public void testIteration() {
    ResultsBag bag = new ResultsBag();
    bag.add(new Integer(1));
    bag.add(new Integer(2));
    bag.add(new Integer(2));
    
    int numOnes = 0;
    int numTwos = 0;
    Integer one = new Integer(1);
    Integer two = new Integer(2);
    for (Iterator itr = bag.iterator(); itr.hasNext(); ) {
      Object n = itr.next();
      if (one.equals(n)) {
        numOnes++;
      }
      else if (two.equals(n)) {
        numTwos++;
      }
      else {
        fail(n + " did not equal 1 or 2");
      }
    }
    assertEquals(1, numOnes);
    assertEquals(2, numTwos);
  }
  
  public void testSerializingSetViewWithNulls()
  throws ClassNotFoundException, IOException {
    ResultsBag bag = new ResultsBag();
    bag.add(new Integer(4));
    bag.add(new Integer(2));
    bag.add(new Integer(42));
    bag.add(null);
    bag.add(null);
    bag.add(null);
    
    assertEquals(6, bag.size());
    assertEquals(1, bag.occurrences(new Integer(4)));
    assertEquals(3, bag.occurrences(null));
    
    Set set = bag.asSet();
    assertEquals(4, set.size());
    assertTrue(set.contains(new Integer(4)));
    assertTrue(set.contains(null));    
    
    ResultsCollectionWrapper w
      = new ResultsCollectionWrapper(new ObjectTypeImpl(Integer.class),
                                     set);
    
    HeapDataOutputStream hdos = new HeapDataOutputStream(Version.CURRENT);
    DataSerializer.writeObject(w, hdos);
    DataInputStream in = new DataInputStream(hdos.getInputStream());
    SelectResults setCopy = (SelectResults)DataSerializer.readObject(in);
    
    assertEquals(4, setCopy.size());
    assertTrue(setCopy.contains(new Integer(4)));
    assertTrue(setCopy.contains(null));
  }
    
  public void testNulls() {
    ResultsBag bag = new ResultsBag();
    assertTrue(bag.isEmpty());
    bag.add(null);
    assertTrue(!bag.isEmpty());
    assertEquals(1, bag.size());
    assertEquals(1, bag.occurrences(null));
    
    bag.add(new Integer(1));
    assertEquals(2, bag.size());
    bag.add(new Integer(2));
    assertEquals(3, bag.size());
    bag.add(new Integer(2));
    assertEquals(4, bag.size());
    
    bag.add(null);
    assertEquals(5, bag.size());
    assertEquals(2, bag.occurrences(null));
    
    int numNulls = 0;
    int numOnes = 0;
    int numTwos = 0;
    Integer one = new Integer(1);
    Integer two = new Integer(2);
    for (Iterator itr = bag.iterator(); itr.hasNext(); ) {
      Object n = itr.next();
      if (one.equals(n)) {
        numOnes++;
      }
      else if (two.equals(n)) {
        numTwos++;
      }
      else if (n == null) {
        numNulls++;
      }
      else {
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
  
  public void testIterationNullRemoval() {
    ResultsBag bag = new ResultsBag();
    bag.add(null);
    bag.add(null);

    bag.add(new Integer(1));
    bag.add(new Integer(2));
    bag.add(new Integer(2));
    assertEquals(5, bag.size());
    
    for (Iterator itr = bag.iterator(); itr.hasNext(); ) {
      Object n = itr.next();
      if (n == null) {
        itr.remove();
      }
    }
    assertEquals(3, bag.size());
    assertEquals(0, bag.occurrences(null));
  }
  
  public void testIterationRemoval() {
    ResultsBag bag = new ResultsBag();
    
    bag.add(new Integer(1));
    bag.add(new Integer(2));
    bag.add(new Integer(2));
    bag.add(new Integer(3));
    bag.add(new Integer(3));
    bag.add(new Integer(4));
    
    assertEquals(6, bag.size());
    
//    Integer one = new Integer(1);
//    Integer two = new Integer(2);
    Iterator itr = bag.iterator();
    for (int i = 0 ; i < 3; i++) {
      itr.next();
      itr.remove();
    }
    assertEquals(3, bag.size());
    
    for (int i = 0 ; i < 3; i++) {
      itr.next();
      itr.remove();
    }
    assertTrue(bag.isEmpty());
    assertEquals(0, bag.size());
  }
  
  public void testNoSuchElementException() {
    ResultsBag bag = new ResultsBag();
    
    bag.add(new Integer(1));
    bag.add(new Integer(2));
    bag.add(new Integer(2));
    bag.add(new Integer(3));
    bag.add(new Integer(3));
    bag.add(new Integer(4));
    
    assertEquals(6, bag.size());
    
    Iterator itr = bag.iterator();
    for (int i = 0 ; i < 6; i++) {
      itr.next();
    }
    try {
      itr.next();
      fail("should have thrown a NoSuchElementException");
    }
    catch (NoSuchElementException e) {
      // pass
    }
    
    // test with removes
    itr = bag.iterator();
    for (int i = 0 ; i < 6; i++) {
      itr.next();
      itr.remove();
    }
    assertEquals(0, bag.size());
    try {
      itr.next();
      fail("should have thrown a NoSuchElementException");
    }
    catch (NoSuchElementException e) {
      // pass
    }
    
    // test with nulls
    bag = new ResultsBag();
    
    bag.add(new Integer(1));
    bag.add(new Integer(2));
    bag.add(new Integer(2));
    bag.add(null);
    bag.add(null);
    bag.add(null);
    bag.add(new Integer(3));
    bag.add(new Integer(3));
    bag.add(new Integer(4));
    
    assertEquals(9, bag.size());
    
    itr = bag.iterator();
    for (int i = 0 ; i < 9; i++) {
      itr.next();
    }
    try {
      itr.next();
      fail("should have thrown a NoSuchElementException");
    }
    catch (NoSuchElementException e) {
      // pass
    }
    
    // test with removes
    itr = bag.iterator();
    for (int i = 0 ; i < 9; i++) {
      itr.next();
      itr.remove();
    }
    assertEquals(0, bag.size());
    try {
      itr.next();
      fail("should have thrown a NoSuchElementException");
    }
    catch (NoSuchElementException e) {
      // pass
    }
  }
}

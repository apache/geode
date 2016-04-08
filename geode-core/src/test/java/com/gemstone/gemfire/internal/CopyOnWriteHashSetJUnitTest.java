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
package com.gemstone.gemfire.internal;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.test.junit.categories.UnitTest;

import junit.framework.TestCase;

/**
 *
 */
@Category(UnitTest.class)
public class CopyOnWriteHashSetJUnitTest extends TestCase {
  
  public void testSnapshot() {
    CopyOnWriteHashSet<String> set = new CopyOnWriteHashSet<String>();
    set.add("a");
    Set<String> snap = set.getSnapshot();
    Set<String> copy = new HashSet<String>(set);
    set.add("b");
    
    assertEquals(copy, snap);
  }
  
  public void testAllMethods() throws Exception {
    
    CopyOnWriteHashSet<String> set = new CopyOnWriteHashSet<String>();
    assertTrue(set.add("a"));
    assertFalse(set.add("a"));
    Iterator itr = set.iterator();
    assertTrue(itr.hasNext());
    assertEquals("a", itr.next());
    assertFalse(itr.hasNext());
    assertEquals(1, set.size());
    
    assertTrue(set.addAll(Arrays.asList(new String[] {"b", "c", "d"})));
    assertTrue(set.contains("b"));
    assertTrue(set.contains("c"));
    assertTrue(set.contains("d"));
    
    assertTrue(set.retainAll(Arrays.asList(new String[] {"a", "b", "c"})));
    assertFalse(set.retainAll(Arrays.asList(new String[] {"a", "b", "c"})));
    
    HashSet<String> test = new HashSet<String>();
    test.addAll(Arrays.asList(new String[] {"a", "b", "c"}));
    assertEquals(test, set);
    assertEquals(set, test);
    assertEquals(test.toString(), set.toString());
    assertEquals(Arrays.asList(test.toArray()), Arrays.asList(set.toArray()));
    assertEquals(Arrays.asList(test.toArray(new String[0])), Arrays.asList(set.toArray(new String[0])));
    
    assertTrue(set.containsAll(test));
    assertTrue(set.containsAll(test));
    
    set.remove("b");
    
    assertFalse(set.containsAll(test));
    
    set.clear();
    
    set.addAll(Arrays.asList(new String[] {"b", "c", "d"}));
    
    assertTrue(set.removeAll(Arrays.asList(new String[] {"b", "c"})));
    assertFalse(set.removeAll(Arrays.asList(new String[] {"b", "c"})));
    
    assertEquals(new HashSet(Arrays.asList(new String[] {"d"})), set);
    
    ByteArrayOutputStream boas = new ByteArrayOutputStream();
    ObjectOutputStream out = new ObjectOutputStream(boas);
    out.writeObject(set);
    ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(boas.toByteArray()));
    Set<String> result = (Set<String>) in.readObject();
    assertEquals(set, result);
    assertTrue(result instanceof CopyOnWriteHashSet);
  }

}

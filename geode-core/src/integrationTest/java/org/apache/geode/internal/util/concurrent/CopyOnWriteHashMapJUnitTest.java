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
/*
 * Written by Doug Lea with assistance from members of JCP JSR-166 Expert Group and released to the
 * public domain, as explained at http://creativecommons.org/licenses/publicdomain Other
 * contributors include Andrew Wright, Jeffrey Hayes, Pat Fisher, Mike Judd.
 */
package org.apache.geode.internal.util.concurrent;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import org.apache.geode.internal.util.concurrent.cm.ConcurrentHashMapJUnitTest;
import org.apache.geode.util.JSR166TestCase;

/**
 * Adopted from the JSR166 test cases. {@link ConcurrentHashMapJUnitTest}
 */
public class CopyOnWriteHashMapJUnitTest extends JSR166TestCase { // TODO: reformat

  /**
   * Create a map from Integers 1-5 to Strings "A"-"E".
   */
  private CopyOnWriteHashMap map5() {
    CopyOnWriteHashMap map = newMap();
    assertTrue(map.isEmpty());
    map.put(one, "A");
    map.put(two, "B");
    map.put(three, "C");
    map.put(four, "D");
    map.put(five, "E");
    assertFalse(map.isEmpty());
    assertEquals(5, map.size());
    return map;
  }

  protected CopyOnWriteHashMap newMap() {
    return new CopyOnWriteHashMap();
  }

  /**
   * clear removes all pairs
   */
  @Test
  public void testClear() {
    CopyOnWriteHashMap map = map5();
    map.clear();
    assertEquals(map.size(), 0);
  }

  /**
   * Maps with same contents are equal
   */
  @Test
  public void testEquals() {
    CopyOnWriteHashMap map1 = map5();
    CopyOnWriteHashMap map2 = map5();
    assertEquals(map1, map2);
    assertEquals(map2, map1);
    map1.clear();
    assertFalse(map1.equals(map2));
    assertFalse(map2.equals(map1));
  }

  /**
   * containsKey returns true for contained key
   */
  @Test
  public void testContainsKey() {
    CopyOnWriteHashMap map = map5();
    assertTrue(map.containsKey(one));
    assertFalse(map.containsKey(zero));
  }

  /**
   * containsValue returns true for held values
   */
  @Test
  public void testContainsValue() {
    CopyOnWriteHashMap map = map5();
    assertTrue(map.containsValue("A"));
    assertFalse(map.containsValue("Z"));
  }

  /**
   * get returns the correct element at the given key, or null if not present
   */
  @Test
  public void testGet() {
    CopyOnWriteHashMap map = map5();
    assertEquals("A", map.get(one));
    CopyOnWriteHashMap empty = newMap();
    assertNull(map.get("anything"));
  }

  /**
   * isEmpty is true of empty map and false for non-empty
   */
  @Test
  public void testIsEmpty() {
    CopyOnWriteHashMap empty = newMap();
    CopyOnWriteHashMap map = map5();
    assertTrue(empty.isEmpty());
    assertFalse(map.isEmpty());
  }

  /**
   * keySet returns a Set containing all the keys
   */
  @Test
  public void testKeySet() {
    CopyOnWriteHashMap map = map5();
    Set s = map.keySet();
    assertEquals(5, s.size());
    assertTrue(s.contains(one));
    assertTrue(s.contains(two));
    assertTrue(s.contains(three));
    assertTrue(s.contains(four));
    assertTrue(s.contains(five));
  }

  /**
   * keySet.toArray returns contains all keys
   */
  @Test
  public void testKeySetToArray() {
    CopyOnWriteHashMap map = map5();
    Set s = map.keySet();
    Object[] ar = s.toArray();
    assertTrue(s.containsAll(Arrays.asList(ar)));
    assertEquals(5, ar.length);
    ar[0] = m10;
    assertFalse(s.containsAll(Arrays.asList(ar)));
  }

  /**
   * Values.toArray contains all values
   */
  @Test
  public void testValuesToArray() {
    CopyOnWriteHashMap map = map5();
    Collection v = map.values();
    Object[] ar = v.toArray();
    ArrayList s = new ArrayList(Arrays.asList(ar));
    assertEquals(5, ar.length);
    assertTrue(s.contains("A"));
    assertTrue(s.contains("B"));
    assertTrue(s.contains("C"));
    assertTrue(s.contains("D"));
    assertTrue(s.contains("E"));
  }

  /**
   * entrySet.toArray contains all entries
   */
  @Test
  public void testEntrySetToArray() {
    CopyOnWriteHashMap map = map5();
    Set s = map.entrySet();
    Object[] ar = s.toArray();
    assertEquals(5, ar.length);
    for (int i = 0; i < 5; ++i) {
      assertTrue(map.containsKey(((Map.Entry) (ar[i])).getKey()));
      assertTrue(map.containsValue(((Map.Entry) (ar[i])).getValue()));
    }
  }

  /**
   * values collection contains all values
   */
  @Test
  public void testValues() {
    CopyOnWriteHashMap map = map5();
    Collection s = map.values();
    assertEquals(5, s.size());
    assertTrue(s.contains("A"));
    assertTrue(s.contains("B"));
    assertTrue(s.contains("C"));
    assertTrue(s.contains("D"));
    assertTrue(s.contains("E"));
  }

  /**
   * entrySet contains all pairs
   */
  @Test
  public void testEntrySet() {
    CopyOnWriteHashMap map = map5();
    Set s = map.entrySet();
    assertEquals(5, s.size());
    for (final Object o : s) {
      Map.Entry e = (Map.Entry) o;
      assertTrue((e.getKey().equals(one) && e.getValue().equals("A"))
          || (e.getKey().equals(two) && e.getValue().equals("B"))
          || (e.getKey().equals(three) && e.getValue().equals("C"))
          || (e.getKey().equals(four) && e.getValue().equals("D"))
          || (e.getKey().equals(five) && e.getValue().equals("E")));
    }
  }

  /**
   * putAll adds all key-value pairs from the given map
   */
  @Test
  public void testPutAll() {
    CopyOnWriteHashMap empty = newMap();
    CopyOnWriteHashMap map = map5();
    empty.putAll(map);
    assertEquals(5, empty.size());
    assertTrue(empty.containsKey(one));
    assertTrue(empty.containsKey(two));
    assertTrue(empty.containsKey(three));
    assertTrue(empty.containsKey(four));
    assertTrue(empty.containsKey(five));
  }

  /**
   * putIfAbsent works when the given key is not present
   */
  @Test
  public void testPutIfAbsent() {
    CopyOnWriteHashMap map = map5();
    map.putIfAbsent(six, "Z");
    assertTrue(map.containsKey(six));
  }

  /**
   * putIfAbsent does not add the pair if the key is already present
   */
  @Test
  public void testPutIfAbsent2() {
    CopyOnWriteHashMap map = map5();
    assertEquals("A", map.putIfAbsent(one, "Z"));
  }

  /**
   * replace fails when the given key is not present
   */
  @Test
  public void testReplace() {
    CopyOnWriteHashMap map = map5();
    assertNull(map.replace(six, "Z"));
    assertFalse(map.containsKey(six));
  }

  /**
   * replace succeeds if the key is already present
   */
  @Test
  public void testReplace2() {
    CopyOnWriteHashMap map = map5();
    assertNotNull(map.replace(one, "Z"));
    assertEquals("Z", map.get(one));
  }


  /**
   * replace value fails when the given key not mapped to expected value
   */
  @Test
  public void testReplaceValue() {
    CopyOnWriteHashMap map = map5();
    assertEquals("A", map.get(one));
    assertFalse(map.replace(one, "Z", "Z"));
    assertEquals("A", map.get(one));
  }

  /**
   * replace value succeeds when the given key mapped to expected value
   */
  @Test
  public void testReplaceValue2() {
    CopyOnWriteHashMap map = map5();
    assertEquals("A", map.get(one));
    assertTrue(map.replace(one, "A", "Z"));
    assertEquals("Z", map.get(one));
  }


  /**
   * remove removes the correct key-value pair from the map
   */
  @Test
  public void testRemove() {
    CopyOnWriteHashMap map = map5();
    map.remove(five);
    assertEquals(4, map.size());
    assertFalse(map.containsKey(five));
  }

  /**
   * remove(key,value) removes only if pair present
   */
  @Test
  public void testRemove2() {
    CopyOnWriteHashMap map = map5();
    map.remove(five, "E");
    assertEquals(4, map.size());
    assertFalse(map.containsKey(five));
    map.remove(four, "A");
    assertEquals(4, map.size());
    assertTrue(map.containsKey(four));

  }

  /**
   * size returns the correct values
   */
  @Test
  public void testSize() {
    CopyOnWriteHashMap map = map5();
    CopyOnWriteHashMap empty = newMap();
    assertEquals(0, empty.size());
    assertEquals(5, map.size());
  }

  /**
   * toString contains toString of elements
   */
  @Test
  public void testToString() {
    CopyOnWriteHashMap map = map5();
    String s = map.toString();
    for (int i = 1; i <= 5; ++i) {
      assertTrue(s.indexOf(String.valueOf(i)) >= 0);
    }
  }

  // Exception tests

  /**
   * get(null) throws NPE
   */
  @Test
  public void testGetNull() {
    CopyOnWriteHashMap c = newMap();
    assertNull(c.get(null));
  }

  /**
   * containsKey(null) throws NPE
   */
  @Test
  public void testContainsKeyNull() {
    CopyOnWriteHashMap c = newMap();
    assertFalse(c.containsKey(null));
  }

  /**
   * containsValue(null) throws NPE
   */
  @Test
  public void testContainsValue_NullPointerException() {
    CopyOnWriteHashMap c = newMap();
    assertFalse(c.containsValue(null));
  }

  /**
   * put(null,x) throws NPE
   */
  @Test
  public void testPut1NullKey() {
    CopyOnWriteHashMap c = newMap();
    c.put(null, "whatever");
    assertTrue(c.containsKey(null));
    assertEquals("whatever", c.get(null));
  }

  /**
   * put(x, null) throws NPE
   */
  @Test
  public void testPut2NullValue() {
    CopyOnWriteHashMap c = newMap();
    c.put("whatever", null);
    assertTrue(c.containsKey("whatever"));
    assertEquals(null, c.get("whatever"));
  }

  /**
   * putIfAbsent(null, x) throws NPE
   */
  @Test
  public void testPutIfAbsent1NullKey() {
    CopyOnWriteHashMap c = newMap();
    c.putIfAbsent(null, "whatever");
    assertTrue(c.containsKey(null));
    assertEquals("whatever", c.get(null));
  }

  /**
   * replace(null, x) throws NPE
   */
  @Test
  public void testReplace_NullPointerException() {
    CopyOnWriteHashMap c = newMap();
    assertNull(c.replace(null, "whatever"));
  }

  /**
   * replace(null, x, y) throws NPE
   */
  @Test
  public void testReplaceValueNullKey() {
    CopyOnWriteHashMap c = newMap();
    c.replace(null, one, "whatever");
    assertFalse(c.containsKey(null));
  }

  /**
   * putIfAbsent(x, null) throws NPE
   */
  @Test
  public void testPutIfAbsent2_NullPointerException() {
    CopyOnWriteHashMap c = newMap();
    c.putIfAbsent("whatever", null);
    assertTrue(c.containsKey("whatever"));
    assertNull(c.get("whatever"));
  }


  /**
   * replace(x, null) throws NPE
   */
  @Test
  public void testReplace2Null() {
    CopyOnWriteHashMap c = newMap();
    c.replace("whatever", null);
    assertFalse(c.containsKey("whatever"));
    assertNull(c.get("whatever"));
  }

  /**
   * replace(x, null, y) throws NPE
   */
  @Test
  public void testReplaceValue2Null() {
    CopyOnWriteHashMap c = newMap();
    c.replace("whatever", null, "A");
    assertFalse(c.containsKey("whatever"));
  }

  /**
   * replace(x, y, null) throws NPE
   */
  @Test
  public void testReplaceValue3Null() {
    CopyOnWriteHashMap c = newMap();
    c.replace("whatever", one, null);
    assertFalse(c.containsKey("whatever"));
  }


  /**
   * remove(null) throws NPE
   */
  @Test
  public void testRemoveNull() {
    CopyOnWriteHashMap c = newMap();
    c.put("sadsdf", "asdads");
    c.remove(null);
  }

  /**
   * remove(null, x) throws NPE
   */
  @Test
  public void testRemove2_NullPointerException() {
    CopyOnWriteHashMap c = newMap();
    c.put("sadsdf", "asdads");
    assertFalse(c.remove(null, "whatever"));
  }

  /**
   * remove(x, null) returns false
   */
  @Test
  public void testRemove3() {
    try {
      CopyOnWriteHashMap c = newMap();
      c.put("sadsdf", "asdads");
      assertFalse(c.remove("sadsdf", null));
    } catch (NullPointerException e) {
      fail();
    }
  }

  /**
   * A deserialized map equals original
   */
  @Test
  public void testSerialization() throws Exception {
    CopyOnWriteHashMap q = map5();

    ByteArrayOutputStream bout = new ByteArrayOutputStream(10000);
    ObjectOutputStream out = new ObjectOutputStream(new BufferedOutputStream(bout));
    out.writeObject(q);
    out.close();

    ByteArrayInputStream bin = new ByteArrayInputStream(bout.toByteArray());
    ObjectInputStream in = new ObjectInputStream(new BufferedInputStream(bin));
    CopyOnWriteHashMap r = (CopyOnWriteHashMap) in.readObject();
    assertEquals(q.size(), r.size());
    assertTrue(q.equals(r));
    assertTrue(r.equals(q));
  }
}

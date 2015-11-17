/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 * Other contributors include Andrew Wright, Jeffrey Hayes,
 * Pat Fisher, Mike Judd.
 */
package com.gemstone.gemfire.internal.util.concurrent;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.internal.util.concurrent.cm.ConcurrentHashMapJUnitTest;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;
import com.gemstone.gemfire.util.JSR166TestCase;

/**
 * Adopted from the JSR166 test cases. {@link ConcurrentHashMapJUnitTest}
 */
@Category(IntegrationTest.class)
public class CopyOnWriteHashMapJUnitTest extends JSR166TestCase{

    public CopyOnWriteHashMapJUnitTest(String name) {
      super(name);
    }

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
     *  clear removes all pairs
     */
    public void testClear() {
        CopyOnWriteHashMap map = map5();
        map.clear();
        assertEquals(map.size(), 0);
    }

    /**
     *  Maps with same contents are equal
     */
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
     *  containsKey returns true for contained key
     */
    public void testContainsKey() {
        CopyOnWriteHashMap map = map5();
        assertTrue(map.containsKey(one));
        assertFalse(map.containsKey(zero));
    }

    /**
     *  containsValue returns true for held values
     */
    public void testContainsValue() {
        CopyOnWriteHashMap map = map5();
        assertTrue(map.containsValue("A"));
        assertFalse(map.containsValue("Z"));
    }

    /**
     *  get returns the correct element at the given key,
     *  or null if not present
     */
    public void testGet() {
        CopyOnWriteHashMap map = map5();
        assertEquals("A", (String)map.get(one));
        CopyOnWriteHashMap empty = newMap();
        assertNull(map.get("anything"));
    }

    /**
     *  isEmpty is true of empty map and false for non-empty
     */
    public void testIsEmpty() {
        CopyOnWriteHashMap empty = newMap();
        CopyOnWriteHashMap map = map5();
        assertTrue(empty.isEmpty());
        assertFalse(map.isEmpty());
    }

    /**
     *   keySet returns a Set containing all the keys
     */
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
     *  keySet.toArray returns contains all keys
     */
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
     *  Values.toArray contains all values
     */
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
     *  entrySet.toArray contains all entries
     */
    public void testEntrySetToArray() {
        CopyOnWriteHashMap map = map5();
        Set s = map.entrySet();
        Object[] ar = s.toArray();
        assertEquals(5, ar.length);
        for (int i = 0; i < 5; ++i) {
            assertTrue(map.containsKey(((Map.Entry)(ar[i])).getKey()));
            assertTrue(map.containsValue(((Map.Entry)(ar[i])).getValue()));
        }
    }

    /**
     * values collection contains all values
     */
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
    public void testEntrySet() {
        CopyOnWriteHashMap map = map5();
        Set s = map.entrySet();
        assertEquals(5, s.size());
        Iterator it = s.iterator();
        while (it.hasNext()) {
            Map.Entry e = (Map.Entry) it.next();
            assertTrue(
                       (e.getKey().equals(one) && e.getValue().equals("A")) ||
                       (e.getKey().equals(two) && e.getValue().equals("B")) ||
                       (e.getKey().equals(three) && e.getValue().equals("C")) ||
                       (e.getKey().equals(four) && e.getValue().equals("D")) ||
                       (e.getKey().equals(five) && e.getValue().equals("E")));
        }
    }

    /**
     *   putAll  adds all key-value pairs from the given map
     */
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
     *   putIfAbsent works when the given key is not present
     */
    public void testPutIfAbsent() {
        CopyOnWriteHashMap map = map5();
        map.putIfAbsent(six, "Z");
        assertTrue(map.containsKey(six));
    }

    /**
     *   putIfAbsent does not add the pair if the key is already present
     */
    public void testPutIfAbsent2() {
        CopyOnWriteHashMap map = map5();
        assertEquals("A", map.putIfAbsent(one, "Z"));
    }

    /**
     *   replace fails when the given key is not present
     */
    public void testReplace() {
        CopyOnWriteHashMap map = map5();
        assertNull(map.replace(six, "Z"));
        assertFalse(map.containsKey(six));
    }

    /**
     *   replace succeeds if the key is already present
     */
    public void testReplace2() {
        CopyOnWriteHashMap map = map5();
        assertNotNull(map.replace(one, "Z"));
        assertEquals("Z", map.get(one));
    }


    /**
     * replace value fails when the given key not mapped to expected value
     */
    public void testReplaceValue() {
        CopyOnWriteHashMap map = map5();
        assertEquals("A", map.get(one));
        assertFalse(map.replace(one, "Z", "Z"));
        assertEquals("A", map.get(one));
    }

    /**
     * replace value succeeds when the given key mapped to expected value
     */
    public void testReplaceValue2() {
        CopyOnWriteHashMap map = map5();
        assertEquals("A", map.get(one));
        assertTrue(map.replace(one, "A", "Z"));
        assertEquals("Z", map.get(one));
    }


    /**
     *   remove removes the correct key-value pair from the map
     */
    public void testRemove() {
        CopyOnWriteHashMap map = map5();
        map.remove(five);
        assertEquals(4, map.size());
        assertFalse(map.containsKey(five));
    }

    /**
     * remove(key,value) removes only if pair present
     */
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
     *   size returns the correct values
     */
    public void testSize() {
        CopyOnWriteHashMap map = map5();
        CopyOnWriteHashMap empty = newMap();
        assertEquals(0, empty.size());
        assertEquals(5, map.size());
    }

    /**
     * toString contains toString of elements
     */
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
    public void testGetNull() {
      CopyOnWriteHashMap c = newMap();
      assertNull(c.get(null));
    }

    /**
     * containsKey(null) throws NPE
     */
    public void testContainsKeyNull() {
      CopyOnWriteHashMap c = newMap();
      assertFalse(c.containsKey(null));
    }

    /**
     * containsValue(null) throws NPE
     */
    public void testContainsValue_NullPointerException() {
      CopyOnWriteHashMap c = newMap();
      assertFalse(c.containsValue(null));
    }

    /**
     * put(null,x) throws NPE
     */
    public void testPut1NullKey() {
      CopyOnWriteHashMap c = newMap();
      c.put(null, "whatever");
      assertTrue(c.containsKey(null));
      assertEquals("whatever", c.get(null));
    }

    /**
     * put(x, null) throws NPE
     */
    public void testPut2NullValue() {
      CopyOnWriteHashMap c = newMap();
      c.put("whatever", null);
      assertTrue(c.containsKey("whatever"));
      assertEquals(null, c.get("whatever"));
    }

    /**
     * putIfAbsent(null, x) throws NPE
     */
    public void testPutIfAbsent1NullKey() {
      CopyOnWriteHashMap c = newMap();
      c.putIfAbsent(null, "whatever");
      assertTrue(c.containsKey(null));
      assertEquals("whatever", c.get(null));
    }

    /**
     * replace(null, x) throws NPE
     */
    public void testReplace_NullPointerException() {
      CopyOnWriteHashMap c = newMap();
      assertNull(c.replace(null, "whatever"));
    }

    /**
     * replace(null, x, y) throws NPE
     */
    public void testReplaceValueNullKey() {
      CopyOnWriteHashMap c = newMap();
      c.replace(null, one, "whatever");
      assertFalse(c.containsKey(null));
    }

    /**
     * putIfAbsent(x, null) throws NPE
     */
    public void testPutIfAbsent2_NullPointerException() {
      CopyOnWriteHashMap c = newMap();
      c.putIfAbsent("whatever", null);
      assertTrue(c.containsKey("whatever"));
      assertNull(c.get("whatever"));
    }


    /**
     * replace(x, null) throws NPE
     */
    public void testReplace2Null() {
      CopyOnWriteHashMap c = newMap();
      c.replace("whatever", null);
      assertFalse(c.containsKey("whatever"));
      assertNull(c.get("whatever"));
    }

    /**
     * replace(x, null, y) throws NPE
     */
    public void testReplaceValue2Null() {
      CopyOnWriteHashMap c = newMap();
      c.replace("whatever", null, "A");
      assertFalse(c.containsKey("whatever"));
    }

    /**
     * replace(x, y, null) throws NPE
     */
    public void testReplaceValue3Null() {
      CopyOnWriteHashMap c = newMap();
      c.replace("whatever", one, null);
      assertFalse(c.containsKey("whatever"));
    }


    /**
     * remove(null) throws NPE
     */
    public void testRemoveNull() {
      CopyOnWriteHashMap c = newMap();
      c.put("sadsdf", "asdads");
      c.remove(null);
    }

    /**
     * remove(null, x) throws NPE
     */
    public void testRemove2_NullPointerException() {
      CopyOnWriteHashMap c = newMap();
      c.put("sadsdf", "asdads");
      assertFalse(c.remove(null, "whatever"));
    }

    /**
     * remove(x, null) returns false
     */
    public void testRemove3() {
        try {
            CopyOnWriteHashMap c = newMap();
            c.put("sadsdf", "asdads");
            assertFalse(c.remove("sadsdf", null));
        } catch(NullPointerException e){
            fail();
        }
    }

    /**
     * A deserialized map equals original
     */
    public void testSerialization() throws Exception {
        CopyOnWriteHashMap q = map5();

        ByteArrayOutputStream bout = new ByteArrayOutputStream(10000);
        ObjectOutputStream out = new ObjectOutputStream(new BufferedOutputStream(bout));
        out.writeObject(q);
        out.close();

        ByteArrayInputStream bin = new ByteArrayInputStream(bout.toByteArray());
        ObjectInputStream in = new ObjectInputStream(new BufferedInputStream(bin));
        CopyOnWriteHashMap r = (CopyOnWriteHashMap)in.readObject();
        assertEquals(q.size(), r.size());
        assertTrue(q.equals(r));
        assertTrue(r.equals(q));
    }
}

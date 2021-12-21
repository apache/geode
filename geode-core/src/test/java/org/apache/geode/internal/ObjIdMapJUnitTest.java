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
package org.apache.geode.internal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.junit.Test;


/**
 * This class tests the functionality of the {@link ObjIdMap} class.
 */
public class ObjIdMapJUnitTest {

  @Test
  public void testSimplePut() {
    ObjIdMap map = new ObjIdMap();
    int key = 4;
    Object value = new Integer(key);
    map.put(key, value);
    assertSame(value, map.get(key));
  }

  @Test
  public void testGetNotThere() {
    ObjIdMap map = new ObjIdMap();
    int key = 4;
    assertSame(null, map.get(key));
  }

  @Test
  public void testSimpleContainsKey() {
    ObjIdMap map = new ObjIdMap();
    int key = 4;
    Object value = new Integer(key);
    map.put(key, value);
    assertTrue(map.containsKey(key));
  }

  @Test
  public void testSimpleRemove() {
    ObjIdMap map = new ObjIdMap();
    int key = 4;
    Object value = new Integer(key);
    map.put(key, value);
    assertSame(value, map.remove(key));
  }

  @Test
  public void testSimpleValues() {
    ObjIdMap map = new ObjIdMap();
    for (int i = 0; i < 20; i++) {
      map.put(i, new Integer(i));
    }

    Object[] values = map.values();
    assertEquals(20, values.length);
    for (int i = 0; i < 20; i++) {
      boolean found = false;
      for (int j = 0; j < values.length; j++) {
        if (values[j].equals(new Integer(i))) {
          found = true;
          break;
        }
      }

      assertTrue("Didn't find " + i, found);
    }
  }

  @Test
  public void testRandomMap() {
    final ObjIdMap map = new ObjIdMap();
    final int size = 1000;

    // ----------------------
    // This test naively assumed that a list of 1000 ints or 1000 longs would
    // yield 1000 distinct values. This has been rewritten to ensure
    // this invariant...
    // ----------------------
    Random random = new Random();

    // Loop until we have 1000 keys. This addresses the slight
    // possibility of duplicates...
    HashSet<Integer> keySet = new HashSet<Integer>();
    while (keySet.size() != size) {
      int key = Math.abs(random.nextInt());
      keySet.add(new Integer(key));
    }

    // Loop until we have 1000 values
    HashSet<Long> valueSet = new HashSet<Long>();
    while (valueSet.size() != size) {
      long value = Math.abs(random.nextLong());
      valueSet.add(new Long(value));
    }

    Iterator<Integer> keyIt = keySet.iterator();
    Iterator<Long> valueIt = valueSet.iterator();
    int[] keys = new int[size];
    Long[] values = new Long[size];
    for (int i = 0; i < size; i++) {
      keys[i] = keyIt.next().intValue();
      values[i] = valueIt.next();
    }

    // ----------------------
    // Now back to your regularly scheduled program...
    // ----------------------

    // Now populate the map...
    for (int i = 0; i < size; i++) {
      map.put(keys[i], values[i]);
    }
    assertEquals("Map is not correct size", size, map.size());

    for (int i = 0; i < size; i++) {
      int key = keys[i];
      assertTrue("Map does not contain key", map.containsKey(key));
      assertEquals("Map has wrong value for key", values[i], map.get(key));
    }

    Object[] valueArray = map.values();
    assertEquals("Value array for map is wrong size", size, valueArray.length);

    for (int i = 0; i < size; i++) {
      boolean found = false;
      int key = keys[i];
      Object value = values[i];
      for (int j = 0; j < valueArray.length; j++) {
        if (valueArray[j].equals(value)) {
          found = true;
          break;
        }
      }

      assertTrue("Didn't find " + key, found);
    }
  }

  @Test
  public void testRandomGrowRemoveRelease() {
    ObjIdMap map = new ObjIdMap();
    Random random = new Random(System.currentTimeMillis());
    List saver = new ArrayList();
    BitSet bits = new BitSet();
    int maxSize = 10000;
    boolean growing = true;
    int numAdds = 0;
    int numRemoves = 0;
    int numReleases = 0;
    int numChecks = 0;

    while (growing || saver.size() > 0) {
      int op = random.nextInt(8);
      // System.out.println("saver size: " + saver.size() + ", map size: " + map.size());
      switch (op) {
        case 0: // Add new value - more likely to occur than removes
        case 1:
        case 2:
          if (!growing) {
            break;
          }
          int key = Math.abs(random.nextInt(10 * maxSize));
          if (bits.get(key)) {
            // We don't want the newValue in the saver List twice
            // because it is only in the map once.
            continue;
          }
          Integer newValue = new Integer(key);
          numAdds++;
          map.put(key, new WeakReference(newValue));
          saver.add(newValue);
          bits.set(key);
          if (saver.size() >= maxSize) {
            growing = false;
          }
          break;
        case 3: // Explicitly remove random entry
          if (saver.size() == 0) {
            break;
          }
          numRemoves++;
          key = random.nextInt(saver.size());
          Integer value = (Integer) saver.remove(key);
          bits.clear(value.intValue());
          assertNotNull(map.remove(value.intValue()));
          break;
        case 4: // Release reference to random entry
          if (saver.size() == 0) {
            break;
          }
          numReleases++;
          key = random.nextInt(saver.size());
          value = (Integer) saver.remove(key);
          bits.clear(value.intValue());
          break;
        case 5: // Validate random entry
        case 6:
        case 7:
          if (saver.size() == 0) {
            break;
          }
          numChecks++;
          Integer valueToCheck = (Integer) saver.get(random.nextInt(saver.size()));
          WeakReference ref = (WeakReference) map.get(valueToCheck.intValue());
          assertTrue(ref != null);
          assertEquals(valueToCheck, ref.get());
          break;
        default:
          fail("Bad op: " + op);
      }
    }
    System.out.println("map size: " + map.size() + ", numAdds: " + numAdds + ", numRemoves: "
        + numRemoves + ", numReleases: " + numReleases + ", numChecks: " + numChecks);
  }

  @Test
  public void testIterator() {
    int size = 10;
    ObjIdMap map = new ObjIdMap();
    for (int i = 0; i < size; i++) {
      map.put(i, new Integer(i));
    }
    assertEquals(size, map.size());

    boolean[] found = new boolean[size];

    ObjIdMap.EntryIterator iter = map.iterator();
    for (ObjIdMap.Entry e = iter.next(); e != null; e = iter.next()) {
      int key = e.key;
      // System.out.println(key);
      assertFalse("Already saw " + key, found[key]);
      found[key] = true;
    }

    for (int i = 0; i < found.length; i++) {
      assertTrue(i + " not found", found[i]);
    }

  }

}

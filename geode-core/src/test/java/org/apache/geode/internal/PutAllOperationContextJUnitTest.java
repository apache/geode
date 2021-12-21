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
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import org.apache.geode.cache.operations.PutAllOperationContext;
import org.apache.geode.internal.cache.CachedDeserializable;
import org.apache.geode.internal.cache.CachedDeserializableFactory;
import org.apache.geode.internal.cache.Token;

public class PutAllOperationContextJUnitTest {

  @Test
  public void testIllegalMapMods() {
    LinkedHashMap<String, String> m = new LinkedHashMap<>();
    m.put("1", "1");
    m.put("2", "2");
    m.put("3", "3");
    PutAllOperationContext paoc = new PutAllOperationContext(m);
    Map<String, String> opMap = paoc.getMap();
    try {
      paoc.setMap(null);
      fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException expected) {
    }
    try {
      opMap.remove("1");
      fail("expected UnsupportedOperationException");
    } catch (UnsupportedOperationException expected) {
    }
    try {
      opMap.put("4", "4");
      fail("expected UnsupportedOperationException");
    } catch (UnsupportedOperationException expected) {
    }
    { // change order and make sure paoc map order is unchanged
      LinkedHashMap<String, String> m2 = new LinkedHashMap<>();
      m2.put("1", "1");
      try {
        paoc.setMap(m2);
        fail("expected IllegalArgumentException");
      } catch (IllegalArgumentException expected) {
      }
    }
    { // change order and make sure paoc map order is unchanged
      LinkedHashMap<String, String> m2 = new LinkedHashMap<>();
      try {
        paoc.setMap(m2);
        fail("expected IllegalArgumentException");
      } catch (IllegalArgumentException expected) {
      }
    }
    { // change order and make sure paoc map order is unchanged
      LinkedHashMap<String, String> m2 = new LinkedHashMap<>();
      m2.put("4", "4");
      m2.put("1", "1");
      m2.put("2", "2");
      try {
        paoc.setMap(m2);
        fail("expected IllegalArgumentException");
      } catch (IllegalArgumentException expected) {
      }
    }
  }

  /**
   * Make sure that we do not expose the internal Token.INVALID to customers
   */
  @Test
  public void testInvalidToken() {
    LinkedHashMap<String, Object> m = new LinkedHashMap<>();
    m.put("INVALID_TOKEN", Token.INVALID);
    PutAllOperationContext paoc = new PutAllOperationContext(m);
    Map<String, Object> opMap = paoc.getMap();
    assertEquals(1, opMap.size());
    assertEquals(true, opMap.containsKey("INVALID_TOKEN"));
    assertEquals(null, opMap.get("INVALID_TOKEN"));
    assertEquals(true, opMap.containsValue(null));
    assertEquals(false, opMap.containsValue("junk"));
    Collection<Object> values = opMap.values();
    assertEquals(1, values.size());
    assertEquals(null, values.iterator().next());
    Set<Map.Entry<String, Object>> entries = opMap.entrySet();
    assertEquals(1, entries.size());
    Map.Entry me = entries.iterator().next();
    assertEquals("INVALID_TOKEN", me.getKey());
    assertEquals(null, me.getValue());
    assertEquals(Token.INVALID, m.get("INVALID_TOKEN"));
  }

  /**
   * Make sure that we do not expose the internal CachedDeserializable to customers
   */
  @Test
  public void testCachedDeserializable() {
    LinkedHashMap<String, Object> m = new LinkedHashMap<>();
    Object v = Integer.valueOf(99);
    CachedDeserializable cd = CachedDeserializableFactory.create(v, 24, null);
    m.put("cd", cd);
    PutAllOperationContext paoc = new PutAllOperationContext(m);
    Map<String, Object> opMap = paoc.getMap();
    assertEquals(1, opMap.size());
    assertEquals(true, opMap.containsKey("cd"));
    assertEquals(v, opMap.get("cd"));
    assertEquals(true, opMap.containsValue(v));
    assertEquals(false, opMap.containsValue("junk"));
    Collection<Object> values = opMap.values();
    assertEquals(1, values.size());
    assertEquals(v, values.iterator().next());
    Set<Map.Entry<String, Object>> entries = opMap.entrySet();
    assertEquals(1, entries.size());
    Map.Entry me = entries.iterator().next();
    assertEquals("cd", me.getKey());
    assertEquals(v, me.getValue());
    assertEquals(cd, m.get("cd"));
    String opMapStr = opMap.toString();
    assertEquals("expected " + opMapStr + " to not contain CachedDeserializable", false,
        opMapStr.contains("CachedDeserializable"));
    HashMap<String, Object> hm = new HashMap<>(opMap);
    assertEquals(hm, opMap);
    assertEquals(opMap, hm);
    assertEquals(hm.hashCode(), opMap.hashCode());
  }

  @Test
  public void testLegalMapMods() {
    LinkedHashMap<String, String> m = new LinkedHashMap<>();
    m.put("1", "1");
    m.put("2", "2");
    m.put("3", "3");
    PutAllOperationContext paoc = new PutAllOperationContext(m);
    Map<String, String> opMap = paoc.getMap();
    assertEquals(m, opMap);

    { // change order and make sure paoc map order is unchanged
      LinkedHashMap<String, String> m2 = new LinkedHashMap<>();
      m2.put("3", "3");
      m2.put("1", "1");
      m2.put("2", "2");
      paoc.setMap(m2);
      assertEquals(Arrays.asList("1", "2", "3"), new ArrayList<>(opMap.keySet()));
      assertEquals(m, opMap);
    }
    assertEquals(false, opMap.isEmpty());
    assertEquals(3, opMap.size());
    assertEquals(true, opMap.containsKey("1"));
    assertEquals(false, opMap.containsKey("4"));
    assertEquals("1", opMap.get("1"));
    assertEquals(Arrays.asList("1", "2", "3"), new ArrayList<>(opMap.values()));
    opMap.put("1", "1b");
    opMap.put("2", "2b");
    opMap.put("3", "3b");
    m = new LinkedHashMap<>();
    m.put("1", "1b");
    m.put("2", "2b");
    m.put("3", "3b");
    assertEquals(m, opMap);
    m.put("2", "2c");
    paoc.setMap(m);
    assertEquals(m, opMap);
    for (Map.Entry<String, String> me : opMap.entrySet()) {
      if (me.getKey().equals("1")) {
        me.setValue("1d");
      }
    }
    m.put("1", "1d");
    assertEquals(m, opMap);

    paoc.setMap(opMap);

    // check that none of updates changed to key order
    assertEquals(Arrays.asList("1", "2", "3"), new ArrayList<>(opMap.keySet()));

    opMap.toString();
  }

}

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
package org.apache.geode.experimental.driver;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.test.junit.categories.ClientServerTest;

@Category({ClientServerTest.class})
public class RegionIntegrationTest extends IntegrationTestBase {
  /** a JSON document */
  private static final String jsonDocument =
      "{\"name\":\"Charlemagne\",\"age\":1276,\"nationality\":\"french\",\"emailAddress\":\"none\"}";

  @Test
  public void putNewValueShouldIncrementSize() throws Exception {
    Region<String, String> region = driver.getRegion("region");

    region.put("key", "value");
    assertEquals(1, region.size());
  }

  @Test
  public void getShouldReturnPutValue() throws Exception {
    Region<String, String> region = driver.getRegion("region");

    region.put("key", "value");
    assertEquals("value", region.get("key"));

    region.remove("key");
    assertFalse(serverRegion.containsKey("key"));
    assertNull(region.get("key"));
  }

  @Test
  public void putWithIntegerKey() throws Exception {
    Region<Integer, Integer> region = driver.getRegion("region");
    region.put(37, 42);
    assertTrue(serverRegion.containsKey(37));
    assertEquals(42, region.get(37).intValue());
  }

  @Test
  public void removeWithIntegerKey() throws Exception {
    Region<Integer, Integer> region = driver.getRegion("region");
    region.put(37, 42);
    region.remove(37);
    assertFalse(serverRegion.containsKey(37));
    assertNull(region.get(37));
  }

  @Test
  public void putWithJSONKeyAndValue() throws Exception {
    Region<JSONWrapper, JSONWrapper> region = driver.getRegion("region");
    JSONWrapper document = JSONWrapper.wrapJSON(jsonDocument);
    region.put(document, document);
    JSONWrapper value = region.get(document);
    assertEquals(document, value);

    assertEquals(1, serverRegion.size());
    org.apache.geode.cache.Region.Entry entry =
        (org.apache.geode.cache.Region.Entry) serverRegion.entrySet().iterator().next();
    assertTrue(PdxInstance.class.isAssignableFrom(entry.getKey().getClass()));
    assertTrue(PdxInstance.class.isAssignableFrom(entry.getValue().getClass()));
  }

  @Test
  public void putIfAbsent() throws Exception {
    Region<JSONWrapper, JSONWrapper> region = driver.getRegion("region");
    JSONWrapper document = JSONWrapper.wrapJSON(jsonDocument);

    assertNull(region.putIfAbsent(document, document));

    JSONWrapper value = region.get(document);
    assertEquals(document, value);
    assertEquals(1, serverRegion.size());

    assertEquals(document, region.putIfAbsent(document, JSONWrapper.wrapJSON("{3 : 2}")));
    value = region.get(document);
    assertEquals(document, value);
    assertEquals(1, serverRegion.size());

    org.apache.geode.cache.Region.Entry entry =
        (org.apache.geode.cache.Region.Entry) serverRegion.entrySet().iterator().next();

    assertTrue(PdxInstance.class.isAssignableFrom(entry.getKey().getClass()));
    assertTrue(PdxInstance.class.isAssignableFrom(entry.getValue().getClass()));
  }

  @Test
  public void removeWithJSONKey() throws Exception {
    Region<JSONWrapper, JSONWrapper> region = driver.getRegion("region");
    JSONWrapper document = JSONWrapper.wrapJSON(jsonDocument);
    region.put(document, document);
    region.remove(document);
    assertEquals(0, serverRegion.size());
    assertNull(region.get(document));
  }

  @Test
  public void keySetTest() throws Exception {
    Region<String, String> region = driver.getRegion("region");
    Map<String, String> testMap = new HashMap<>();
    testMap.put("Key1", "foo");
    testMap.put("Key2", "foo");
    testMap.put("Key3", "foo");
    region.putAll(testMap);
    assertArrayEquals(testMap.keySet().stream().sorted().toArray(),
        region.keySet().stream().sorted().toArray());
  }

  @Test(expected = IOException.class)
  public void putWithBadJSONKeyAndValue() throws IOException {
    Region<JSONWrapper, JSONWrapper> region = driver.getRegion("region");
    JSONWrapper document = JSONWrapper.wrapJSON("{\"name\":\"Johan\"]");
    region.put(document, document);
  }

  @Test(expected = IOException.class)
  public void putAllWithBadJSONKeyAndValue() throws IOException {
    Region<JSONWrapper, JSONWrapper> region = driver.getRegion("region");
    Map<JSONWrapper, JSONWrapper> putAllMap = new HashMap<>();
    JSONWrapper document = JSONWrapper.wrapJSON("{\"name\":\"Johan\"]");
    putAllMap.put(document, document);
    region.putAll(putAllMap);
  }

  @Test(expected = IOException.class)
  public void getWithBadJSONKey() throws IOException {
    Region<JSONWrapper, JSONWrapper> region = driver.getRegion("region");
    region.get(JSONWrapper.wrapJSON("{\"name\":\"Johan\"]"));
  }

  @Test(expected = IOException.class)
  public void getAllWithBadJSONKeyAndValue() throws IOException {
    Region<JSONWrapper, JSONWrapper> region = driver.getRegion("region");
    Set<JSONWrapper> getAllKeys = new HashSet<>();
    JSONWrapper document = JSONWrapper.wrapJSON("{\"name\":\"Johan\"]");
    getAllKeys.add(document);
    Map<JSONWrapper, JSONWrapper> result = region.getAll(getAllKeys);
    assertTrue("missing key " + document + " in " + result, result.containsKey(document));
    assertNull(result.get(document));
  }

  @Test(expected = IOException.class)
  public void removeWithBadJSONKey() throws IOException {
    Region<JSONWrapper, JSONWrapper> region = driver.getRegion("region");
    region.remove(JSONWrapper.wrapJSON("{\"name\":\"Johan\"]"));
  }

  @Test
  public void clearNonEmptyRegion() throws Exception {
    Region<String, String> region = driver.getRegion("region");

    region.put("key", "value");
    assertEquals(1, region.size());

    region.clear();
    assertEquals(0, region.size());
  }
}

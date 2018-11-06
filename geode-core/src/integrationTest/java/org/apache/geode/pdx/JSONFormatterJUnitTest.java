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
package org.apache.geode.pdx;

import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.text.SimpleDateFormat;
import java.util.List;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.json.JSONException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.pdx.internal.PdxInstanceImpl;
import org.apache.geode.pdx.internal.PeerTypeRegistration;
import org.apache.geode.test.junit.categories.SerializationTest;

@Category({SerializationTest.class})
public class JSONFormatterJUnitTest {
  public static final String REGION_NAME = "primitiveKVStore";
  private Cache cache;
  private Region<Object, Object> region;

  @Before
  public void setUp() throws Exception {
    this.cache = new CacheFactory().set(MCAST_PORT, "0").setPdxReadSerialized(true).create();

    region = cache.createRegionFactory().setDataPolicy(DataPolicy.PARTITION).create(REGION_NAME);

  }

  @After
  public void tearDown() {
    this.cache.close();
  }

  @Test
  public void ValidatePdxInstanceToJsonConversion() {
    TestObjectForJSONFormatter actualTestObject = new TestObjectForJSONFormatter();
    actualTestObject.defaultInitialization();

    // Testcase-1: PdxInstance to Json conversion
    // put Object and getvalue as Pdxinstance
    region.put("201", actualTestObject);
    Object receivedObject = region.get("201");

    assertTrue("receivedObject is expected to be of type PdxInstance",
        receivedObject instanceof PdxInstance);

    // PdxInstance->Json conversion
    PdxInstance pi = (PdxInstance) receivedObject;
    String json = JSONFormatter.toJSON(pi);

    JSONFormatVerifyUtility.verifyJsonWithJavaObject(json, actualTestObject);
  }

  @Test
  public void verifyJsonToPdxInstanceConversion() throws JSONException, JsonProcessingException {
    TestObjectForJSONFormatter expectedTestObject = new TestObjectForJSONFormatter();
    expectedTestObject.defaultInitialization();

    // 1.gets pdxInstance using region.put() and region.get()
    region.put("501", expectedTestObject);
    Object receivedObject = region.get("501");
    assertTrue("receivedObject is expected to be of type PdxInstance",
        receivedObject instanceof PdxInstance);

    // 2. Get the JSON string from actualTestObject using jackson ObjectMapper.
    ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.setDateFormat(new SimpleDateFormat("MM/dd/yyyy"));
    objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    String json = objectMapper.writeValueAsString(expectedTestObject);

    String jsonWithClassType = expectedTestObject.addClassTypeToJson(json);

    // 3. Get PdxInstance from the Json String and Validate pi.getObject() API.
    PdxInstance receivedPdxInstance = JSONFormatter.fromJSON(jsonWithClassType);
    // Note: expectedPI will contains those fields that are part of toData()
    // expectedPI.className = "org.apache.geode.pdx.TestObjectForJSONFormatter"
    // actualPI will contains all the fields that are member of the class.
    // actualPI..className = __GEMFIRE_JSON
    // and hence actualPI.equals(expectedPI) will returns false.

    Object actualTestObject = receivedPdxInstance.getObject();

    assertTrue(actualTestObject instanceof TestObjectForJSONFormatter);
    assertEquals(actualTestObject, expectedTestObject);

  }

  @Test
  public void verifyJsonToPdxInstanceConversionWithJSONFormatter() {
    TestObjectForJSONFormatter expectedTestObject = new TestObjectForJSONFormatter();
    expectedTestObject.defaultInitialization();

    // 1.gets pdxInstance using R.put() and R.get()
    region.put("501", expectedTestObject);
    Object receivedObject = region.get("501");
    assertEquals("receivedObject is expected to be of type PdxInstance", PdxInstanceImpl.class,
        receivedObject.getClass());

    PdxInstance expectedPI = (PdxInstance) receivedObject;

    String json = JSONFormatter.toJSON(expectedPI);

    String jsonWithClassType = expectedTestObject.addClassTypeToJson(json);

    // 3. Get PdxInstance from the Json String and Validate pi.getObject() API.
    PdxInstance actualPI = JSONFormatter.fromJSON(jsonWithClassType);
    // Note: expectedPI will contains those fields that are part of toData()
    // expectedPI.className = "org.apache.geode.pdx.TestObjectForJSONFormatter"
    // actualPI will contains all the fields that are member of the class.
    // actualPI..className = __GEMFIRE_JSON
    // and hence actualPI.equals(expectedPI) will returns false.

    Object actualTestObject = actualPI.getObject();

    assertEquals("receivedObject is expected to be of type PdxInstance",
        TestObjectForJSONFormatter.class, actualTestObject.getClass());

    assertEquals("actualTestObject and expectedTestObject should be equal", expectedTestObject,
        actualTestObject);
  }

  /**
   * this test validates json document, where field has value and null Then it verifies we create
   * only one pdx type id for that
   */
  @Test
  public void testJSONStringAsPdxObject() {
    int pdxTypes = 0;

    if (cache.getRegion(PeerTypeRegistration.REGION_FULL_PATH) != null) {
      pdxTypes = cache.getRegion(PeerTypeRegistration.REGION_FULL_PATH).keySet().size();
    }

    String js = "{name:\"ValueExist\", age:14}";

    region.put(1, JSONFormatter.fromJSON(js));

    String js2 = "{name:null, age:14}";

    region.put(2, JSONFormatter.fromJSON(js2));

    assertEquals(pdxTypes + 1,
        cache.getRegion(PeerTypeRegistration.REGION_FULL_PATH).keySet().size());
  }

  @Test
  public void testJSONStringSortedFields() {
    try {
      System.setProperty(JSONFormatter.SORT_JSON_FIELD_NAMES_PROPERTY, "true");

      String js = "{b:\"b\", age:14, c:\"c' go\", bb:23}";

      region.put(1, JSONFormatter.fromJSON(js));

      PdxInstance ret = (PdxInstance) region.get(1);
      List<String> fieldNames = ret.getFieldNames();

      assertEquals("There should be four fields", 4, fieldNames.size());

      boolean sorted = true;
      for (int i = 0; i < fieldNames.size() - 1; i++) {
        if (fieldNames.get(i).compareTo(fieldNames.get(i + 1)) >= 0) {
          sorted = false;
        }
      }

      assertTrue("Json fields should be sorted", sorted);

      // Now do put with another jsonstring with same fields but different order
      // then verify we don't create another pdxtype

      int pdxTypes = 0;

      if (cache.getRegion(PeerTypeRegistration.REGION_FULL_PATH) != null) {
        pdxTypes = cache.getRegion(PeerTypeRegistration.REGION_FULL_PATH).keySet().size();
      }

      String js2 = "{c:\"c' go\", bb:23, b:\"b\", age:14 }";
      region.put(2, JSONFormatter.fromJSON(js2));

      assertEquals(pdxTypes,
          cache.getRegion(PeerTypeRegistration.REGION_FULL_PATH).keySet().size());

    } finally {
      System.setProperty(JSONFormatter.SORT_JSON_FIELD_NAMES_PROPERTY, "false");
    }
  }
}

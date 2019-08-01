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
import static org.apache.geode.test.util.ResourceUtils.createTempFileFromResource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileNotFoundException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import util.TestException;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.pdx.internal.PdxInstanceImpl;
import org.apache.geode.pdx.internal.PdxType;
import org.apache.geode.pdx.internal.PeerTypeRegistration;
import org.apache.geode.test.junit.categories.SerializationTest;

@Category({SerializationTest.class})
public class JSONFormatterJUnitTest {
  public static final String REGION_NAME = "primitiveKVStore";

  private Cache cache;

  private Region<Object, Object> region;

  @Before
  public void setUp() throws Exception {
    this.cache = new CacheFactory().set(MCAST_PORT, "0")
        .set("log-level", "WARN").setPdxReadSerialized(true).create();

    region = cache.createRegionFactory().setDataPolicy(DataPolicy.PARTITION)
        .create(REGION_NAME);

  }

  @After
  public void tearDown() {
    this.cache.close();
  }

  @Test
  public void ValidatePdxInstanceToJsonConversion() throws Exception {
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
  public void verifyJsonToPdxInstanceConversion() throws Exception {
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
    objectMapper
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

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
  public void verifyJsonToPdxInstanceConversionWithJSONFormatter()
      throws Exception {
    TestObjectForJSONFormatter expectedTestObject = new TestObjectForJSONFormatter();
    expectedTestObject.defaultInitialization();

    // 1.gets pdxInstance using R.put() and R.get()
    region.put("501", expectedTestObject);
    Object receivedObject = region.get("501");
    assertEquals("receivedObject is expected to be of type PdxInstance",
        PdxInstanceImpl.class,
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

    assertEquals("actualTestObject and expectedTestObject should be equal",
        expectedTestObject,
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
      pdxTypes = cache.getRegion(PeerTypeRegistration.REGION_FULL_PATH).keySet()
          .size();
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
        pdxTypes = cache.getRegion(PeerTypeRegistration.REGION_FULL_PATH)
            .keySet().size();
      }

      String js2 = "{c:\"c' go\", bb:23, b:\"b\", age:14 }";
      region.put(2, JSONFormatter.fromJSON(js2));

      assertEquals(pdxTypes,
          cache.getRegion(PeerTypeRegistration.REGION_FULL_PATH).keySet().size());

    } finally {
      System.setProperty(JSONFormatter.SORT_JSON_FIELD_NAMES_PROPERTY, "false");
    }
  }

  @Test
  public void testReorderJSONFieldsDoesNotCauseManyTypeIdConflicts() {
    final int entries = 10000;
    final float toleratedCollisonFraction = 0.01f;

    List<String> jsonLines = loadJSONDocument();

    for (int i = 0; i < entries; ++i) {
      randomizeFieldsAndCreatePdx(jsonLines);
    }

    int collisons = calculateNumberOfCollisions();

    float collisonRate = (float) collisons / (float) entries;

    assertThat(collisonRate)
        .withFailMessage("PdxTypeId collision rate too high. Expected %f but was %f.",
            toleratedCollisonFraction, collisonRate)
        .isLessThan(toleratedCollisonFraction);
  }

  @Test
  public void testCounterInJSONFieldNameDoesNotCauseManyTypeIdConflicts() {
    final int entries = 10000;
    final float toleratedCollisonFraction = 0.01f;

    List<String> jsonLines = loadJSONDocument();

    String jsonString = "";
    String counterField = "";
    String fieldToReplace = jsonLines.get(1);
    for (int i = 0; i < entries; ++i) {
      jsonString = buildJSONString(jsonLines);
      counterField = "\"counter" + i + "\": " + i + ",";
      JSONFormatter.fromJSON(jsonString.replace(fieldToReplace, counterField));
    }

    int collisons = calculateNumberOfCollisions();

    float collisonRate = (float) collisons / (float) entries;

    assertThat(collisonRate)
        .withFailMessage("PdxTypeId collision rate too high. Expected %f but was %f.",
            toleratedCollisonFraction, collisonRate)
        .isLessThan(toleratedCollisonFraction);
  }

  @Test
  public void testSortingJSONFieldsPreventsTypeIDCollision() {
    String initialState =
        String.valueOf(
            Boolean.getBoolean(JSONFormatter.SORT_JSON_FIELD_NAMES_PROPERTY));
    try {
      final int unsortedEntries = 100;
      final int sortedEntries = 10000;
      final int jsonFields = 40;

      List<String> jsonLines = generateJSONWithCollidingFieldNames(jsonFields);

      System.setProperty(JSONFormatter.SORT_JSON_FIELD_NAMES_PROPERTY, "false");

      // Show that not sorting leads to 100% collision rate. There are 1 fewer collisions than there
      // are entries because the first entry cannot have a TypeId collision
      for (int i = 0; i < unsortedEntries; ++i) {
        randomizeFieldsAndCreatePdx(jsonLines);
      }
      int unsortedCollisions = calculateNumberOfCollisions();
      assertThat(unsortedCollisions).withFailMessage(
          "Unexpected number of PdxTypeId collisions. Expected %d but was actually %d.",
          unsortedEntries - 1, unsortedCollisions).isEqualTo(unsortedEntries - 1);

      System.setProperty(JSONFormatter.SORT_JSON_FIELD_NAMES_PROPERTY, "true");

      // Show that sorting leads to no additional collisions beyond the single probable collision
      // caused by the first sorted entry
      for (int i = 0; i < sortedEntries; ++i) {
        randomizeFieldsAndCreatePdx(jsonLines);
      }
      int sortedCollisions = calculateNumberOfCollisions();
      assertThat(sortedCollisions)
          .withFailMessage("Too many PdxTypeId collisions. Expected %d but was %d.",
              unsortedCollisions + 1, sortedCollisions)
          .isLessThanOrEqualTo(unsortedCollisions + 1);
    } finally {
      System.setProperty(JSONFormatter.SORT_JSON_FIELD_NAMES_PROPERTY,
          initialState);
    }
  }

  public List<String> loadJSONDocument() {
    File source = loadTestResource(
        "/org/apache/geode/internal/pdx/jsonStrings/testJSON.txt");
    assertThat(source.exists());
    return getJSONLines(source);
  }

  private File loadTestResource(String fileName) {
    String filePath = createTempFileFromResource(getClass(), fileName)
        .getAbsolutePath();
    assertThat(filePath).isNotNull();

    return new File(filePath);
  }

  private List<String> getJSONLines(File source) {
    try {
      Scanner scanner = new Scanner(source);
      List<String> jsonLines = new ArrayList<>();
      while (scanner.hasNext()) {
        jsonLines.add(scanner.nextLine());
      }
      return jsonLines;
    } catch (FileNotFoundException e) {
      throw new TestException(e.getMessage());
    }
  }

  private String buildJSONString(List<String> jsonLines) {
    StringBuilder jsonString = new StringBuilder();
    for (int i = 0; i < jsonLines.size(); ++i) {
      String line = jsonLines.get(i);
      jsonString.append(line + "\n");
    }
    return jsonString.toString();
  }

  private void randomizeFieldsAndCreatePdx(List<String> jsonLines) {
    String jsonString = "";
    // First and last lines of the JSON document cannot be reordered due to containing "{" and "}"
    Collections.shuffle(jsonLines.subList(1, jsonLines.size() - 1));
    jsonString = buildJSONString(jsonLines);
    JSONFormatter.fromJSON(jsonString);
  }

  private int calculateNumberOfCollisions() {
    Collection pdxTypes = null;

    if (cache.getRegion(PeerTypeRegistration.REGION_FULL_PATH) != null) {
      pdxTypes = cache.getRegion(PeerTypeRegistration.REGION_FULL_PATH)
          .values();
    }
    assertThat(pdxTypes).isNotNull();

    int collisions = 0;
    for (Object object : pdxTypes) {
      PdxType pdxType = (PdxType) object;

      // Ideally this would be a method call to PeerTypeRegistration to avoid code duplication
      int calculatedId =
          pdxType.hashCode() & PeerTypeRegistration.PLACE_HOLDER_FOR_TYPE_ID;

      if (pdxType.getTypeId() != calculatedId) {
        collisions++;
      }
    }
    return collisions;
  }

  private List<String> generateJSONWithCollidingFieldNames(int jsonFields) {
    List<String> jsonLines = generateCollidingStrings(jsonFields);
    assertThat(jsonLines).isNotNull();

    // Format generated strings into JSON document
    for (int i = 0; i < jsonLines.size(); ++i) {
      String field = "\"" + jsonLines.get(i) + "\": " + i;
      // Do not add a comma to the last field, but do add a closing }
      if (i != jsonLines.size() - 1) {
        field += ",";
      } else {
        field += "}";
      }
      jsonLines.set(i, field);
    }
    jsonLines.add(0, "{");
    return jsonLines;
  }

  /**
   * This method produces Strings with identical hashcode() values
   */
  private List<String> generateCollidingStrings(int numberOfStrings) {
    final String[] baseStrings = {"Aa", "BB", "C#"};

    int firstHash = baseStrings[0].hashCode();
    for (String element : baseStrings) {
      assertThat(element.hashCode() == firstHash);
    }

    // By definition, it requires at least two strings for there to be a collision
    if (numberOfStrings < 2) {
      return new ArrayList<>();
    }

    int elementsPerString =
        (int) Math.ceil(Math.log(numberOfStrings) / Math.log(baseStrings.length));

    List<String> result = Arrays.asList("", "", "");

    for (int i = 0; i < elementsPerString; ++i) {
      List<String> partialResult = new ArrayList();
      for (String string : result) {
        for (String element : baseStrings) {
          partialResult.add(string + element);
        }
      }
      result = partialResult;
    }
    return result.subList(0, numberOfStrings);
  }
}

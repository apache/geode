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

import static org.assertj.core.api.Assertions.assertThat;

import junitparams.Parameters;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.test.junit.categories.RestAPITest;
import org.apache.geode.test.junit.runners.GeodeParamsRunner;

/**
 * A test class to document and make clear what JSONFormatter will and won't parse as far as simple
 * examples.
 */
@Category({RestAPITest.class})
@RunWith(GeodeParamsRunner.class)
public class JSONFormatterBasicJUnitTest {
  // This is needed because the JsonFormatter needs to access the PDX region, which requires a
  // running Cache.
  private static Cache cache;

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @BeforeClass
  public static void setUp() throws Exception {
    cache = new CacheFactory().create();
  }

  @AfterClass
  public static void tearDown() {
    cache.close();
  }

  @Test(expected = JSONFormatterException.class)
  public void emptyListFailsToParse() {
    JSONFormatter.fromJSON("[]");
  }

  @Test(expected = JSONFormatterException.class)
  public void listOfASingleNumberFailsToParse() {
    JSONFormatter.fromJSON("[1]");
  }

  @Test(expected = JSONFormatterException.class)
  public void numberFailsToParse() {
    JSONFormatter.fromJSON("1");
  }

  @Test(expected = JSONFormatterException.class)
  public void emptyInputFailsToParse() {
    JSONFormatter.fromJSON("");
  }

  @Test(expected = JSONFormatterException.class)
  public void emptyInputInStringFailsToParse() {
    JSONFormatter.fromJSON("\"\"");
  }

  @Test(expected = JSONFormatterException.class)
  public void arbitraryInputFailsToParse() {
    JSONFormatter.fromJSON("hi");
  }

  @Test(expected = JSONFormatterException.class)
  public void simpleStringFailsToParse() {
    JSONFormatter.fromJSON("\"hi\"");
  }

  @Test(expected = JSONFormatterException.class)
  public void nullFailsToParse() {
    JSONFormatter.fromJSON("null");
  }

  @Test(expected = JSONFormatterException.class)
  public void falseFailsToParse() {
    JSONFormatter.fromJSON("false");
  }

  @Test(expected = JSONFormatterException.class)
  public void trueFailsToParse() {
    JSONFormatter.fromJSON("true");
  }

  @Test
  public void emptyObjectParses() {
    JSONFormatter.fromJSON("{}");
  }

  @Test
  @Parameters({"true", "false"})
  public void simpleObjectAsStringParses(String usePdxInstanceSortedHelper) {
    System.setProperty(JSONFormatter.SORT_JSON_FIELD_NAMES_PROPERTY, usePdxInstanceSortedHelper);
    String testField = "a";
    String jsonObjectString = "{\"a\":2}";

    PdxInstance pdxInstance = JSONFormatter.fromJSON(jsonObjectString);
    String deserializedJsonObjectString = JSONFormatter.toJSON(pdxInstance);

    assertThat(pdxInstance.hasField(testField)).isTrue();
    assertThat(deserializedJsonObjectString).isEqualTo(jsonObjectString);
  }

  @Test
  @Parameters({"true", "false"})
  public void simpleObjectAsBytesParses(String usePdxInstanceSortedHelper) {
    System.setProperty(JSONFormatter.SORT_JSON_FIELD_NAMES_PROPERTY, usePdxInstanceSortedHelper);
    String testField = "a";
    String jsonObjectString = "{\"a\":2}";

    PdxInstance pdxInstance = JSONFormatter.fromJSON(jsonObjectString);
    byte[] deserializedJsonObjectString = JSONFormatter.toJSONByteArray(pdxInstance);

    assertThat(pdxInstance.hasField(testField)).isTrue();
    assertThat(deserializedJsonObjectString).isEqualTo(jsonObjectString.getBytes());
  }

  @Test
  @Parameters({"true", "false"})
  public void simpleObjectAsStringParsesWithIdentityField(String usePdxInstanceSortedHelper) {
    System.setProperty(JSONFormatter.SORT_JSON_FIELD_NAMES_PROPERTY, usePdxInstanceSortedHelper);
    String identityField = "a";
    String nonIdentityField = "b";
    String nonExistentField = "c";
    String jsonObjectString = "{\"a\":2,\"b\":3}";

    PdxInstance pdxInstance = JSONFormatter.fromJSON(jsonObjectString, identityField);
    String deserializedJsonObjectString = JSONFormatter.toJSON(pdxInstance);

    assertThat(pdxInstance.isIdentityField(identityField)).isTrue();
    assertThat(pdxInstance.isIdentityField(nonIdentityField)).isFalse();
    assertThat(pdxInstance.isIdentityField(nonExistentField)).isFalse();
    assertThat(pdxInstance.hasField(identityField)).isTrue();
    assertThat(pdxInstance.hasField(nonIdentityField)).isTrue();
    assertThat(pdxInstance.hasField(nonExistentField)).isFalse();
    assertThat(deserializedJsonObjectString).isEqualTo(jsonObjectString);
  }

  @Test
  @Parameters({"true", "false"})
  public void simpleObjectAsBytesParsesWithIdentityField(String usePdxInstanceSortedHelper) {
    System.setProperty(JSONFormatter.SORT_JSON_FIELD_NAMES_PROPERTY, usePdxInstanceSortedHelper);
    String identityField = "a";
    String nonIdentityField = "b";
    String nonExistentField = "c";
    String jsonObjectString = "{\"a\":2,\"b\":3}";

    PdxInstance pdxInstance = JSONFormatter.fromJSON(jsonObjectString.getBytes(), identityField);
    byte[] deserializedJsonObjectString = JSONFormatter.toJSONByteArray(pdxInstance);

    assertThat(pdxInstance.isIdentityField(identityField)).isTrue();
    assertThat(pdxInstance.isIdentityField(nonIdentityField)).isFalse();
    assertThat(pdxInstance.isIdentityField(nonExistentField)).isFalse();
    assertThat(pdxInstance.hasField(identityField)).isTrue();
    assertThat(pdxInstance.hasField(nonIdentityField)).isTrue();
    assertThat(pdxInstance.hasField(nonExistentField)).isFalse();
    assertThat(deserializedJsonObjectString).isEqualTo(jsonObjectString.getBytes());
  }
}

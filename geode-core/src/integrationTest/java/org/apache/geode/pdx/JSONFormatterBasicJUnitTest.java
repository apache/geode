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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.test.junit.categories.RestAPITest;

/**
 * A test class to document and make clear what JSONFormatter will and won't parse as far as simple
 * examples.
 */
@Category({RestAPITest.class})
public class JSONFormatterBasicJUnitTest {
  // This is needed because the JsonFormatter needs to access the PDX region, which requires a
  // running Cache.
  private static Cache cache;

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
  public void simpleObjectParses() {
    JSONFormatter.fromJSON("{\"a\": 2}");
  }
}

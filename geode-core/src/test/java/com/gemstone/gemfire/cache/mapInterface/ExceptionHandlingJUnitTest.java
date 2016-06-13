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
package com.gemstone.gemfire.cache.mapInterface;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;
import org.junit.*;
import org.junit.experimental.categories.Category;

import java.util.HashMap;
import java.util.Properties;

import static com.gemstone.gemfire.distributed.ConfigurationProperties.LOCATORS;
import static com.gemstone.gemfire.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.fail;

@Category(IntegrationTest.class)
public class ExceptionHandlingJUnitTest {

  private static DistributedSystem distributedSystem = null;
  private static Region testRegion = null;
  private Object returnObject = null;
  private boolean done = false;

  @BeforeClass
  public static void caseSetUp() throws Exception {
    Properties properties = new Properties();
    properties.setProperty(MCAST_PORT, "0");
    properties.setProperty(LOCATORS, "");
    distributedSystem = DistributedSystem.connect(properties);
    Cache cache = CacheFactory.create(distributedSystem);
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.GLOBAL);
    RegionAttributes regionAttributes = factory.create();
    testRegion = cache.createRegion("TestRegion", regionAttributes);
  }
  
  @AfterClass
  public static void caseTearDown() {
    distributedSystem.disconnect();
    distributedSystem = null;
    testRegion = null;
  }

  @Before
  public void setUp() throws Exception {
    testRegion.clear();
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testNullPointerWithContainsValue() {
    boolean caught = false;
    try {
      testRegion.containsValue(null);
    }
    catch (NullPointerException ex) {
      caught = true;
    }
    if (!caught) {
      fail("Nullpointer exception not thrown");
    }
  }

  @Test
  public void _testNullPointerWithGet() {
    boolean caught = false;
    try {
      testRegion.get(null);
    }
    catch (NullPointerException ex) {
      caught = true;
    }
    if (!caught) {
      fail("Nullpointer exception not thrown");
    }
  }

  @Test
  public void testNullPointerWithRemove() {
    boolean caught = false;
    try {
      testRegion.remove(null);
    }
    catch (NullPointerException ex) {
      caught = true;
    }
    if (!caught) {
      fail("Nullpointer exception not thrown");
    }
  }

  @Test
  public void _testNullPointerWithPut() {
    boolean caught = false;
    try {
      testRegion.put(null,null);
    }
    catch (NullPointerException ex) {
      caught = true;
    }
    if (!caught) {
      fail("Nullpointer exception not thrown");
    }
  }

  @Test
  public void testNullPointerWithPutAll() {
    boolean caught = false;
    try {
      testRegion.putAll(null);
    }
    catch (NullPointerException ex) {
      caught = true;
    }
    if (!caught) {
      fail("Nullpointer exception not thrown");
    }
  }

  @Test
  public void testPutAllNullValue() {
    boolean caught = false;
    try {
      HashMap map = new HashMap();
      map.put("key1", "key1value");
      map.put("key2", null);
      testRegion.putAll(map);
    }
    catch (NullPointerException ex) {
      caught = true;
    }
    if (!caught) {
      fail("Nullpointer exception not thrown");
    }
  }
  
  @Test
  public void testNullPointerWithContainsKey() {
    boolean caught = false;
    try {
      testRegion.containsKey(null);
    }
    catch (NullPointerException ex) {
      caught = true;
    }
    if (!caught) {
      fail("Nullpointer exception not thrown");
    }
  }
}

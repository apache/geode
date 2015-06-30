/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.mapInterface;

import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Properties;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class ExceptionHandlingJUnitTest {

  private static DistributedSystem distributedSystem = null;
  private static Region testRegion = null;
  private Object returnObject = null;
  private boolean done = false;

  @BeforeClass
  public static void caseSetUp() throws Exception {
    Properties properties = new Properties();
    properties.setProperty("mcast-port", "0");
    properties.setProperty("locators", "");
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

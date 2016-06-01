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

import static com.gemstone.gemfire.distributed.DistributedSystemConfigProperties.LOCATORS;
import static com.gemstone.gemfire.distributed.DistributedSystemConfigProperties.MCAST_PORT;
import static org.junit.Assert.fail;

@Category(IntegrationTest.class)
public class MapFunctionalJUnitTest {

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
  public void testContainsValuePositive() {
    testRegion.put("Test", "test");
    if(!testRegion.containsValue("test")) {
      fail("contains value failed, value is present but contains value returned false");
    }
  }
  
  @Test
  public void testContainsValueNegative() {
    if(testRegion.containsValue("test123")) {
      fail("Value is not present but contains value returned true");
    }
  }
  
  @Test
  public void testIsEmptyPositive() {
    testRegion.clear();
    if(!testRegion.isEmpty()) {
      fail("region is empty but isEmpty returns false");
    }
  }
  
  @Test
  public void testIsEmptyNegative() {
    testRegion.put("test","test");
    if(testRegion.isEmpty()) {
      fail("region is not empty but isEmpty returns true");
    }
  }
  
  @Test
  public void testPut() {
    testRegion.put("test", "test");
    if(!testRegion.get("test").equals("test")){
      fail("put not successfull");
    }
  }
  
  @Test
  public void testPutAll() {
    HashMap map = new HashMap();
    for(int i=0 ; i < 5 ; i++) {
      map.put(new Integer(i), new Integer(i));
    }
    testRegion.putAll(map);
    if(!testRegion.containsKey(new Integer(4))|| !testRegion.containsValue(new Integer(4))) {
      fail("Put all did not put in all the keys");
    }
  }
  
  @Test
  public void testRemove() {
    testRegion.put("Test","test");
    testRegion.remove("Test");
    if(testRegion.containsKey("Test")) {
      fail("remove did not remove the key");
    }
  }
  
  @Test
  public void testRemoveReturnKey() {
    testRegion.put("Test","test");
    if(!testRegion.remove("Test").equals("test")) {    
      fail("remove did not return the correct value");
    }
  }
  
  @Test
  public void testSize() {
    testRegion.put("1", "1");
    testRegion.put("2", "2");
    testRegion.put("3", "3");
    if(testRegion.size()!=3) {
      fail("size is not returning the correct size of the region");
    }
  }
  
  @Test
  public void testPutReturnsObject() {
    testRegion.put("Test", "test");
    if(!testRegion.put("Test","test123").equals("test")) {
      fail("put does not return the correct object");
    }
  }
  
  @Test
  public void testReturningOldValuePositive() {
    testRegion.put("test", "test123");
    if(!testRegion.put("test", "test567").equals("test123")) {
      fail("old value was not returned inspite of property being set to return null on put");
    }
  }
}

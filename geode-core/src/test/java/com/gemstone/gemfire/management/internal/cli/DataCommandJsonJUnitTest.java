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
package com.gemstone.gemfire.management.internal.cli;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.management.internal.cli.dto.Car;
import com.gemstone.gemfire.management.internal.cli.util.JsonUtil;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class DataCommandJsonJUnitTest {

  @Test
  public void testCollectionTypesInJson(){    
    String json = "{'attributes':{'power':'90hp'},'make':'502.1825','model':'502.1825','colors':['red','white','blue'],'attributeSet':['red','white','blue'], 'attributeArray':['red','white','blue']}";
    Car car = (Car)JsonUtil.jsonToObject(json, Car.class);    
    assertNotNull(car.getAttributeSet());
    assertTrue(car.getAttributeSet() instanceof HashSet);
    assertEquals(3, car.getAttributeSet().size());
    
    assertNotNull(car.getColors());
    assertTrue(car.getColors() instanceof ArrayList);
    assertEquals(3, car.getColors().size());
    
    assertNotNull(car.getAttributes());
    assertTrue(car.getAttributes() instanceof HashMap);
    assertEquals(1, car.getAttributes().size());
    assertTrue(car.getAttributes().containsKey("power"));
    
    assertNotNull(car.getAttributeArray());
    assertTrue(car.getAttributeArray() instanceof String[]);
    assertEquals(3, car.getAttributeArray().length);
  }
}

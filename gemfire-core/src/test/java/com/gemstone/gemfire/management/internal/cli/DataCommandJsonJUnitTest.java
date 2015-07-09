package com.gemstone.gemfire.management.internal.cli;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import org.junit.experimental.categories.Category;

import junit.framework.TestCase;

import com.gemstone.gemfire.management.internal.cli.dto.Car;
import com.gemstone.gemfire.management.internal.cli.util.JsonUtil;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

/**
 * 
 * @author tushark
 *
 */
@Category(UnitTest.class)
public class DataCommandJsonJUnitTest  extends TestCase{

  
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

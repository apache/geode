/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.pdx;

import static org.junit.Assert.fail;

import java.text.SimpleDateFormat;

import org.json.JSONException;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class PdxFormatterPutGetJUnitTest {

  private GemFireCacheImpl c;
  private final String PRIMITIVE_KV_STORE_REGION = "primitiveKVStore";
    
  public PdxFormatterPutGetJUnitTest() {
    super();
  }

  @Before
  public void setUp() throws Exception {
    this.c = (GemFireCacheImpl) new CacheFactory().set("mcast-port", "0").setPdxReadSerialized(true).create();
    
    //start cache-server
    CacheServer server = c.addCacheServer();
    final int serverPort = 40405;
    server.setPort(serverPort);
    server.start();
    
    // Create region, primitiveKVStore
    final AttributesFactory<Object, Object> af1 = new AttributesFactory<Object, Object>();
    af1.setDataPolicy(DataPolicy.PARTITION);
    final RegionAttributes<Object, Object> rAttributes = af1.create();
    c.createRegion(PRIMITIVE_KV_STORE_REGION, rAttributes);
  }

  @After
  public void tearDown() {
    //shutdown and clean up the manager node.
    this.c.close();
  }
  
  private void ValidatePdxInstanceToJsonConversion(){
    
    Cache c = CacheFactory.getAnyInstance();
    Region region = c.getRegion("primitiveKVStore");
    
    TestObjectForPdxFormatter actualTestObject = new TestObjectForPdxFormatter();
    actualTestObject.defaultInitialization();
    
    //Testcase-1: PdxInstance to Json conversion
    //put Object and getvalue as Pdxinstance
    region.put("201", actualTestObject);
    Object receivedObject = region.get("201");
    
    //PdxInstance->Json conversion
    if(receivedObject instanceof PdxInstance){
      PdxInstance pi = (PdxInstance)receivedObject;
      String json = JSONFormatter.toJSON(pi);
      
      verifyJsonWithJavaObject(json, actualTestObject);
    }else {
      fail("receivedObject is expected to be of type PdxInstance");
    }
    
  }
  
  //Testcase-2: validate Json->PdxInstance-->Java conversion
  private void VarifyJsonToPdxInstanceConversion(){
    TestObjectForPdxFormatter expectedTestObject = new TestObjectForPdxFormatter();
    expectedTestObject.defaultInitialization();
    Cache c = CacheFactory.getAnyInstance();
    Region region = c.getRegion("primitiveKVStore");
    
    //1.gets pdxInstance using R.put() and R.get()
    region.put("501", expectedTestObject);
    Object receivedObject = region.get("501");
    if(receivedObject instanceof PdxInstance){
      PdxInstance expectedPI = (PdxInstance)receivedObject;
      
      //2. Get the JSON string from actualTestObject using jackson ObjectMapper.
      ObjectMapper objectMapper = new ObjectMapper();
      objectMapper.setDateFormat(new SimpleDateFormat("MM/dd/yyyy"));
      objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
      
      String json;
      try {
        json = objectMapper.writeValueAsString(expectedTestObject);
        String jsonWithClassType = expectedTestObject.addClassTypeToJson(json);
        
        //3. Get PdxInstance from the Json String and Validate pi.getObject() API. 
        PdxInstance actualPI = JSONFormatter.fromJSON(jsonWithClassType);
        //Note: expectedPI will contains those fields that are part of toData()
        //      expectedPI.className = "com.gemstone.gemfire.pdx.TestObjectForPdxFormatter"
        //      actualPI will contains all the fields that are member of the class.
        //      actualPI..className = __GEMFIRE_JSON
        //      and hence actualPI.equals(expectedPI) will returns false.
        
        Object actualTestObject = actualPI.getObject();
        if(actualTestObject instanceof TestObjectForPdxFormatter){
          boolean isObjectEqual = actualTestObject.equals(expectedTestObject);
          Assert.assertTrue(isObjectEqual, "actualTestObject and expectedTestObject should be equal");
        }else {
          fail("actualTestObject is expected to be of type PdxInstance");
        }
      } catch (JsonProcessingException e1) {
        fail("JsonProcessingException occurred:" + e1.getMessage());
      } catch (JSONException e) {
        fail("JSONException occurred:" + e.getMessage());
      }
    }else {
      fail("receivedObject is expected to be of type PdxInstance");
    }
  }
 
  private void verifyJsonWithJavaObject (String json, TestObjectForPdxFormatter testObject) {
    try {  
      JSONObject jsonObject = new JSONObject(json);
      
      //Testcase-1: Validate json string against the pdxInstance.
      //validation for primitive types
      junit.framework.Assert.assertEquals("VerifyPdxInstanceToJson: Int type values are not matched",
          testObject.getP_int(), jsonObject.getInt(testObject.getP_intFN()));
      junit.framework.Assert.assertEquals("VerifyPdxInstanceToJson: long type values are not matched",
          testObject.getP_long(), jsonObject.getLong(testObject.getP_longFN()));
      
      //validation for wrapper types
      junit.framework.Assert.assertEquals("VerifyPdxInstanceToJson: Boolean type values are not matched",
          testObject.getW_bool().booleanValue(), jsonObject.getBoolean(testObject.getW_boolFN()));
      junit.framework.Assert.assertEquals("VerifyPdxInstanceToJson: Float type values are not matched",
          testObject.getW_double().doubleValue(), jsonObject.getDouble(testObject.getW_doubleFN()));
      junit.framework.Assert.assertEquals("VerifyPdxInstanceToJson: bigDec type values are not matched",
          testObject.getW_bigDec().longValue(), jsonObject.getLong(testObject.getW_bigDecFN()));
      
      //vlidation for array types
      junit.framework.Assert.assertEquals("VerifyPdxInstanceToJson: Byte[] type values are not matched",
          (int)testObject.getW_byteArray()[1], jsonObject.getJSONArray(testObject.getW_byteArrayFN()).getInt(1));
      junit.framework.Assert.assertEquals("VerifyPdxInstanceToJson: Double[] type values are not matched",
          testObject.getW_doubleArray()[0], jsonObject.getJSONArray(testObject.getW_doubleArrayFN()).getDouble(0));
      junit.framework.Assert.assertEquals("VerifyPdxInstanceToJson: String[] type values are not matched",
          testObject.getW_strArray()[2], jsonObject.getJSONArray(testObject.getW_strArrayFN()).getString(2));
      
      //validation for collection types
      junit.framework.Assert.assertEquals("VerifyPdxInstanceToJson: list type values are not matched", 
          testObject.getC_list().get(0), 
          jsonObject.getJSONArray(testObject.getC_listFN()).getString(0));
      
      junit.framework.Assert.assertEquals("VerifyPdxInstanceToJson: stack type values are not matched", 
          testObject.getC_stack().get(2), 
          jsonObject.getJSONArray(testObject.getC_stackFN()).getString(2));
      
      //validation for Map
      junit.framework.Assert.assertEquals("VerifyPdxInstanceToJson: Map type values are not matched", 
          testObject.getM_empByCity().get("Ahmedabad").get(0).getFname(), 
          jsonObject.getJSONObject(testObject.getM_empByCityFN()).getJSONArray("Ahmedabad").getJSONObject(0).getString("fname"));
      
      //validation Enum
      junit.framework.Assert.assertEquals("VerifyPdxInstanceToJson: Enum type values are not matched", 
          testObject.getDay().toString(), 
          jsonObject.getString(testObject.getDayFN()));
      
    } catch (JSONException e) {
      fail("Error in VerifyPdxInstanceToJson, Malformed json, can not create JSONArray from it");
    }
  }
  
  @Test
  public void testPdxFormatterAPIs() {
    ValidatePdxInstanceToJsonConversion();
    VarifyJsonToPdxInstanceConversion();
  }
}



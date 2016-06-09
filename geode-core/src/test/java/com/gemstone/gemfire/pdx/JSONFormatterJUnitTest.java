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
package com.gemstone.gemfire.pdx;

import static com.gemstone.gemfire.distributed.DistributedSystemConfigProperties.*;
import static org.junit.Assert.*;

import java.text.SimpleDateFormat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

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
public class JSONFormatterJUnitTest {

  private GemFireCacheImpl c;
  private final String PRIMITIVE_KV_STORE_REGION = "primitiveKVStore";
    
  @Before
  public void setUp() throws Exception {
    this.c = (GemFireCacheImpl) new CacheFactory().set(MCAST_PORT, "0").setPdxReadSerialized(true).create();
    
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
    
    TestObjectForJSONFormatter actualTestObject = new TestObjectForJSONFormatter();
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
  private void verifyJsonToPdxInstanceConversion(){
    TestObjectForJSONFormatter expectedTestObject = new TestObjectForJSONFormatter();
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
        //      expectedPI.className = "com.gemstone.gemfire.pdx.TestObjectForJSONFormatter"
        //      actualPI will contains all the fields that are member of the class.
        //      actualPI..className = __GEMFIRE_JSON
        //      and hence actualPI.equals(expectedPI) will returns false.

        Object actualTestObject = actualPI.getObject();
        if(actualTestObject instanceof TestObjectForJSONFormatter){
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

  private void verifyJsonWithJavaObject (String json, TestObjectForJSONFormatter testObject) {
    try {
      JSONObject jsonObject = new JSONObject(json);

      //Testcase-1: Validate json string against the pdxInstance.
      //validation for primitive types
      assertEquals("VerifyPdxInstanceToJson: Int type values are not matched",
          testObject.getP_int(), jsonObject.getInt(testObject.getP_intFN()));
      assertEquals("VerifyPdxInstanceToJson: long type values are not matched",
          testObject.getP_long(), jsonObject.getLong(testObject.getP_longFN()));

      //validation for wrapper types
      assertEquals("VerifyPdxInstanceToJson: Boolean type values are not matched",
          testObject.getW_bool().booleanValue(), jsonObject.getBoolean(testObject.getW_boolFN()));
      assertEquals("VerifyPdxInstanceToJson: Float type values are not matched",
          testObject.getW_double().doubleValue(), jsonObject.getDouble(testObject.getW_doubleFN()), 0);
      assertEquals("VerifyPdxInstanceToJson: bigDec type values are not matched",
          testObject.getW_bigDec().longValue(), jsonObject.getLong(testObject.getW_bigDecFN()));

      //vlidation for array types
      assertEquals("VerifyPdxInstanceToJson: Byte[] type values are not matched",
          (int)testObject.getW_byteArray()[1], jsonObject.getJSONArray(testObject.getW_byteArrayFN()).getInt(1));
      assertEquals("VerifyPdxInstanceToJson: Double[] type values are not matched",
          testObject.getW_doubleArray()[0], jsonObject.getJSONArray(testObject.getW_doubleArrayFN()).getDouble(0), 0);
      assertEquals("VerifyPdxInstanceToJson: String[] type values are not matched",
          testObject.getW_strArray()[2], jsonObject.getJSONArray(testObject.getW_strArrayFN()).getString(2));

      //validation for collection types
      assertEquals("VerifyPdxInstanceToJson: list type values are not matched",
          testObject.getC_list().get(0),
          jsonObject.getJSONArray(testObject.getC_listFN()).getString(0));

      assertEquals("VerifyPdxInstanceToJson: stack type values are not matched",
          testObject.getC_stack().get(2),
          jsonObject.getJSONArray(testObject.getC_stackFN()).getString(2));

      //validation for Map
      assertEquals("VerifyPdxInstanceToJson: Map type values are not matched",
          testObject.getM_empByCity().get("Ahmedabad").get(0).getFname(),
          jsonObject.getJSONObject(testObject.getM_empByCityFN()).getJSONArray("Ahmedabad").getJSONObject(0).getString("fname"));

      //validation Enum
      assertEquals("VerifyPdxInstanceToJson: Enum type values are not matched",
          testObject.getDay().toString(),
          jsonObject.getString(testObject.getDayFN()));

    } catch (JSONException e) {
      throw new AssertionError("Error in VerifyPdxInstanceToJson, Malformed json, can not create JSONArray from it", e);
    }
  }

  @Test
  public void testJSONFormatterAPIs() {
    ValidatePdxInstanceToJsonConversion();
    verifyJsonToPdxInstanceConversion();
  }
}



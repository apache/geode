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


import java.io.IOException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;

public class JSONFormatVerifyUtility {
  public static void verifyJsonWithJavaObject(String json, TestObjectForJSONFormatter testObject)
      throws IOException {
    JsonNode jsonObject = new ObjectMapper().readTree(json);

    // Testcase-1: Validate json string against the pdxInstance.
    // validation for primitive types
    Assert.assertEquals("VerifyPdxInstanceToJson: Int type values are not matched",
        testObject.getP_int(),
        jsonObject.get(testObject.getP_intFN()).asInt());
    Assert.assertEquals("VerifyPdxInstanceToJson: long type values are not matched",
        testObject.getP_long(), jsonObject.get(testObject.getP_longFN()).asLong());

    // validation for wrapper types
    Assert.assertEquals("VerifyPdxInstanceToJson: Boolean type values are not matched",
        testObject.getW_bool().booleanValue(),
        jsonObject.get(testObject.getW_boolFN()).asBoolean());
    Assert.assertEquals("VerifyPdxInstanceToJson: Float type values are not matched",
        testObject.getW_double().doubleValue(),
        jsonObject.get(testObject.getW_doubleFN()).asDouble(),
        0);
    Assert.assertEquals("VerifyPdxInstanceToJson: bigDec type values are not matched",
        testObject.getW_bigDec().longValue(), jsonObject.get(testObject.getW_bigDecFN()).asLong());

    // validation for array types
    Assert.assertEquals("VerifyPdxInstanceToJson: Byte[] type values are not matched",
        (int) testObject.getW_byteArray()[1],
        jsonObject.get(testObject.getW_byteArrayFN()).get(1).asInt());
    Assert.assertEquals("VerifyPdxInstanceToJson: Double[] type values are not matched",
        testObject.getW_doubleArray()[0],
        jsonObject.get(testObject.getW_doubleArrayFN()).get(0).asDouble(0), 0);
    Assert.assertEquals("VerifyPdxInstanceToJson: String[] type values are not matched",
        testObject.getW_strArray()[2],
        jsonObject.get(testObject.getW_strArrayFN()).get(2).textValue());

    // validation for collection types
    Assert.assertEquals("VerifyPdxInstanceToJson: list type values are not matched",
        testObject.getC_list().get(0),
        jsonObject.get(testObject.getC_listFN()).get(0).textValue());

    Assert.assertEquals("VerifyPdxInstanceToJson: stack type values are not matched",
        testObject.getC_stack().get(2),
        jsonObject.get(testObject.getC_stackFN()).get(2).textValue());

    // validation for Map
    Assert.assertEquals("VerifyPdxInstanceToJson: Map type values are not matched",
        testObject.getM_empByCity().get("Ahmedabad").get(0).getFname(),
        jsonObject.get(testObject.getM_empByCityFN()).get("Ahmedabad")
            .get(0).get("fname").asText());

    // validation Enum
    Assert.assertEquals("VerifyPdxInstanceToJson: Enum type values are not matched",
        testObject.getDay().toString(), jsonObject.get(testObject.getDayFN()).asText());
  }
}

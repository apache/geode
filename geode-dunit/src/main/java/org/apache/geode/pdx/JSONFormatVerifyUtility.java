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

import org.json.JSONObject;
import org.junit.Assert;

public class JSONFormatVerifyUtility {
  static void verifyJsonWithJavaObject(String json, TestObjectForJSONFormatter testObject) {
    JSONObject jsonObject = new JSONObject(json);

    // Testcase-1: Validate json string against the pdxInstance.
    // validation for primitive types
    Assert.assertEquals("VerifyPdxInstanceToJson: Int type values are not matched",
        testObject.getP_int(),
        jsonObject.getInt(testObject.getP_intFN()));
    Assert.assertEquals("VerifyPdxInstanceToJson: long type values are not matched",
        testObject.getP_long(), jsonObject.getLong(testObject.getP_longFN()));

    // validation for wrapper types
    Assert.assertEquals("VerifyPdxInstanceToJson: Boolean type values are not matched",
        testObject.getW_bool().booleanValue(), jsonObject.getBoolean(testObject.getW_boolFN()));
    Assert.assertEquals("VerifyPdxInstanceToJson: Float type values are not matched",
        testObject.getW_double().doubleValue(), jsonObject.getDouble(testObject.getW_doubleFN()),
        0);
    Assert.assertEquals("VerifyPdxInstanceToJson: bigDec type values are not matched",
        testObject.getW_bigDec().longValue(), jsonObject.getLong(testObject.getW_bigDecFN()));

    // vlidation for array types
    Assert.assertEquals("VerifyPdxInstanceToJson: Byte[] type values are not matched",
        (int) testObject.getW_byteArray()[1],
        jsonObject.getJSONArray(testObject.getW_byteArrayFN()).getInt(1));
    Assert.assertEquals("VerifyPdxInstanceToJson: Double[] type values are not matched",
        testObject.getW_doubleArray()[0],
        jsonObject.getJSONArray(testObject.getW_doubleArrayFN()).getDouble(0), 0);
    Assert.assertEquals("VerifyPdxInstanceToJson: String[] type values are not matched",
        testObject.getW_strArray()[2],
        jsonObject.getJSONArray(testObject.getW_strArrayFN()).getString(2));

    // validation for collection types
    Assert.assertEquals("VerifyPdxInstanceToJson: list type values are not matched",
        testObject.getC_list().get(0),
        jsonObject.getJSONArray(testObject.getC_listFN()).getString(0));

    Assert.assertEquals("VerifyPdxInstanceToJson: stack type values are not matched",
        testObject.getC_stack().get(2),
        jsonObject.getJSONArray(testObject.getC_stackFN()).getString(2));

    // validation for Map
    Assert.assertEquals("VerifyPdxInstanceToJson: Map type values are not matched",
        testObject.getM_empByCity().get("Ahmedabad").get(0).getFname(),
        jsonObject.getJSONObject(testObject.getM_empByCityFN()).getJSONArray("Ahmedabad")
            .getJSONObject(0).getString("fname"));

    // validation Enum
    Assert.assertEquals("VerifyPdxInstanceToJson: Enum type values are not matched",
        testObject.getDay().toString(), jsonObject.getString(testObject.getDayFN()));
  }
}

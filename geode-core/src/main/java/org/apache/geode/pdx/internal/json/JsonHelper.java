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
package org.apache.geode.pdx.internal.json;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;

import com.fasterxml.jackson.core.JsonGenerator;

public class JsonHelper {

  public static void getJsonFromPrimitiveBoolArray(JsonGenerator jg, boolean[] array, String pf)
      throws IOException {
    jg.writeStartArray();
    for (boolean obj : array) {
      jg.writeBoolean(obj);
    }
    jg.writeEndArray();
  }

  public static void getJsonFromPrimitiveByteArray(JsonGenerator jg, byte[] array, String pf)
      throws IOException {
    jg.writeStartArray();
    for (byte obj : array) {
      jg.writeNumber(obj);
    }
    jg.writeEndArray();
  }

  public static void getJsonFromPrimitiveShortArray(JsonGenerator jg, short[] array, String pf)
      throws IOException {
    jg.writeStartArray();
    for (short obj : array) {
      jg.writeNumber(obj);
    }
    jg.writeEndArray();
  }

  public static void getJsonFromPrimitiveIntArray(JsonGenerator jg, int[] array, String pf)
      throws IOException {
    jg.writeStartArray();
    for (int obj : array) {
      jg.writeNumber(obj);
    }
    jg.writeEndArray();
  }

  public static void getJsonFromPrimitiveLongArray(JsonGenerator jg, long[] array, String pf)
      throws IOException {
    jg.writeStartArray();
    for (long obj : array) {
      jg.writeNumber(obj);
    }
    jg.writeEndArray();
  }

  public static void getJsonFromPrimitiveFloatArray(JsonGenerator jg, float[] array, String pf)
      throws IOException {
    jg.writeStartArray();
    for (float obj : array) {
      jg.writeNumber(obj);
    }
    jg.writeEndArray();
  }

  public static void getJsonFromPrimitiveDoubleArray(JsonGenerator jg, double[] array, String pf)
      throws IOException {
    jg.writeStartArray();
    for (double obj : array) {
      jg.writeNumber(obj);
    }
    jg.writeEndArray();
  }


  public static void getJsonFromWrapperBoolArray(JsonGenerator jg, Boolean[] array, String pf)
      throws IOException {
    jg.writeStartArray();
    for (Boolean obj : array) {
      jg.writeBoolean(obj);
    }
    jg.writeEndArray();
  }

  public static void getJsonFromWrapperByteArray(JsonGenerator jg, Byte[] array, String pf)
      throws IOException {
    jg.writeStartArray();
    for (Byte obj : array) {
      jg.writeNumber(obj);
    }
    jg.writeEndArray();
  }

  public static void getJsonFromWrapperShortArray(JsonGenerator jg, Short[] array, String pf)
      throws IOException {
    jg.writeStartArray();
    for (Short obj : array) {
      jg.writeNumber(obj);
    }
    jg.writeEndArray();
  }

  public static void getJsonFromWrapperIntArray(JsonGenerator jg, Integer[] array, String pf)
      throws IOException {
    jg.writeStartArray();
    for (Integer obj : array) {
      jg.writeNumber(obj);
    }
    jg.writeEndArray();
  }

  public static void getJsonFromWrapperLongArray(JsonGenerator jg, Long[] array, String pf)
      throws IOException {
    jg.writeStartArray();
    for (Long obj : array) {
      jg.writeNumber(obj);
    }
    jg.writeEndArray();
  }

  public static void getJsonFromWrapperFloatArray(JsonGenerator jg, Float[] array, String pf)
      throws IOException {
    jg.writeStartArray();
    for (Float obj : array) {
      jg.writeNumber(obj);
    }
    jg.writeEndArray();
  }

  public static void getJsonFromWrapperDoubleArray(JsonGenerator jg, Double[] array, String pf)
      throws IOException {
    jg.writeStartArray();
    for (Double obj : array) {
      jg.writeNumber(obj);
    }
    jg.writeEndArray();
  }

  public static void getJsonFromBigIntArray(JsonGenerator jg, BigInteger[] array, String pf)
      throws IOException {
    jg.writeStartArray();
    for (BigInteger obj : array) {
      jg.writeNumber(obj);
    }
    jg.writeEndArray();
  }

  public static void getJsonFromBigDecimalArray(JsonGenerator jg, BigDecimal[] array, String pf)
      throws IOException {
    jg.writeStartArray();
    for (BigDecimal obj : array) {
      jg.writeNumber(obj);
    }
    jg.writeEndArray();
  }

  public static void getJsonFromStringArray(JsonGenerator jg, String[] array, String pf)
      throws IOException {
    jg.writeStartArray();
    for (String obj : array) {
      jg.writeString(obj);
    }
    jg.writeEndArray();
  }

}

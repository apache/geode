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
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonGenerator.Feature;

import org.apache.geode.annotations.internal.MutableForTesting;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.pdx.JSONFormatter;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.pdx.internal.EnumInfo;
import org.apache.geode.pdx.internal.EnumInfo.PdxInstanceEnumInfo;

/*
 * This class converts a PdxInstance into a JSON document.
 */
public class PdxToJSON {

  @MutableForTesting
  public static boolean PDXTOJJSON_UNQUOTEFIELDNAMES =
      Boolean.getBoolean("pdxToJson.unQuoteFieldNames");
  private PdxInstance m_pdxInstance;

  public PdxToJSON(PdxInstance pdx) {
    m_pdxInstance = pdx;
  }

  public String getJSON() {
    JsonFactory jf = new JsonFactory();
    HeapDataOutputStream hdos = new HeapDataOutputStream(KnownVersion.CURRENT);
    try {
      JsonGenerator jg = jf.createJsonGenerator(hdos, JsonEncoding.UTF8);
      enableDisableJSONGeneratorFeature(jg);
      getJSONString(jg, m_pdxInstance);
      jg.close();
      return new String(hdos.toByteArray());
    } catch (IOException e) {
      throw new RuntimeException(e.getMessage());
    } finally {
      hdos.close();
    }
  }

  public byte[] getJSONByteArray() {
    JsonFactory jf = new JsonFactory();
    HeapDataOutputStream hdos = new HeapDataOutputStream(KnownVersion.CURRENT);
    try {
      JsonGenerator jg = jf.createJsonGenerator(hdos, JsonEncoding.UTF8);
      enableDisableJSONGeneratorFeature(jg);
      getJSONString(jg, m_pdxInstance);
      jg.close();
      return hdos.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException(e.getMessage());
    } finally {
      hdos.close();
    }
  }

  private void enableDisableJSONGeneratorFeature(JsonGenerator jg) {
    jg.enable(Feature.ESCAPE_NON_ASCII);
    jg.disable(Feature.AUTO_CLOSE_TARGET);
    if (PDXTOJJSON_UNQUOTEFIELDNAMES) {
      jg.disable(Feature.QUOTE_FIELD_NAMES);
    }
  }

  protected String convertPdxToJson(final PdxInstance pdxObj) {
    return (pdxObj != null ? JSONFormatter.toJSON(pdxObj) : null);
  }

  private void writeValue(JsonGenerator jg, Object value, String pf)
      throws JsonGenerationException, IOException {

    if (value == null) {
      jg.writeNull();
    } else if (value.getClass().equals(Boolean.class)) {
      boolean b = (Boolean) value;
      jg.writeBoolean(b);
    } else if (value.getClass().equals(Byte.class)) {
      Byte b = (Byte) value;
      jg.writeNumber(b);
    } else if (value.getClass().equals(Short.class)) {
      Short b = (Short) value;
      jg.writeNumber(b);
    } else if (value.getClass().equals(Integer.class)) {
      int i = (Integer) value;
      jg.writeNumber(i);
    } else if (value.getClass().equals(Long.class)) {
      long i = (Long) value;
      jg.writeNumber(i);
    } else if (value.getClass().equals(BigInteger.class)) {
      BigInteger i = (BigInteger) value;
      jg.writeNumber(i);
    } else if (value.getClass().equals(Float.class)) {
      float i = (Float) value;
      jg.writeNumber(i);
    } else if (value.getClass().equals(BigDecimal.class)) {
      BigDecimal i = (BigDecimal) value;
      jg.writeNumber(i);
    } else if (value.getClass().equals(Double.class)) {
      double d = (Double) value;
      jg.writeNumber(d);
    } else if (value.getClass().equals(String.class)) {
      String s = (String) value;
      jg.writeString(s);
    } else if (value.getClass().isArray()) {
      getJSONStringFromArray(jg, value, pf);
    } else if (value.getClass().equals(EnumInfo.class)) {
      jg.writeString(value.toString());
    } else if (value.getClass().equals(PdxInstanceEnumInfo.class)) {
      jg.writeString(value.toString());
    } else {
      if (value instanceof PdxInstance) {
        getJSONString(jg, (PdxInstance) value);
      } else if (value instanceof Collection) {
        getJSONStringFromCollection(jg, (Collection<?>) value, pf);
      } else if (value instanceof Map) {
        getJSONStringFromMap(jg, (Map) value, pf);
      } else {
        throw new IllegalStateException(
            "PdxInstance returns unknwon pdxfield " + pf + " for type " + value);
      }
    }
  }

  private void getJSONStringFromMap(JsonGenerator jg, Map map, String pf)
      throws JsonGenerationException, IOException {

    jg.writeStartObject();

    Iterator iter = map.entrySet().iterator();
    while (iter.hasNext()) {
      Map.Entry entry = (Map.Entry) iter.next();

      // Iterate over Map and write key-value
      jg.writeFieldName(entry.getKey().toString()); // write Key in a Map
      writeValue(jg, entry.getValue(), pf); // write value in a Map
    }
    jg.writeEndObject();
  }

  private String getJSONString(JsonGenerator jg, PdxInstance pdxInstance)
      throws JsonGenerationException, IOException {
    jg.writeStartObject();

    List<String> pdxFields = pdxInstance.getFieldNames();

    for (String pf : pdxFields) {
      Object value = pdxInstance.getField(pf);
      jg.writeFieldName(pf);
      writeValue(jg, value, pf);
    }
    jg.writeEndObject();
    return null;
  }

  private void getJSONStringFromArray(JsonGenerator jg, Object value, String pf)
      throws JsonGenerationException, IOException {

    if (value.getClass().getName().equals("[Z")) {
      JsonHelper.getJsonFromPrimitiveBoolArray(jg, (boolean[]) value, pf);
    } else if (value.getClass().getName().equals("[B")) {
      JsonHelper.getJsonFromPrimitiveByteArray(jg, (byte[]) value, pf);
    } else if (value.getClass().getName().equals("[S")) {
      JsonHelper.getJsonFromPrimitiveShortArray(jg, (short[]) value, pf);
    } else if (value.getClass().getName().equals("[I")) {
      JsonHelper.getJsonFromPrimitiveIntArray(jg, (int[]) value, pf);
    } else if (value.getClass().getName().equals("[J")) {
      JsonHelper.getJsonFromPrimitiveLongArray(jg, (long[]) value, pf);
    } else if (value.getClass().getName().equals("[F")) {
      JsonHelper.getJsonFromPrimitiveFloatArray(jg, (float[]) value, pf);
    } else if (value.getClass().getName().equals("[D")) {
      JsonHelper.getJsonFromPrimitiveDoubleArray(jg, (double[]) value, pf);
    } else if (value.getClass().equals(Boolean[].class)) {
      JsonHelper.getJsonFromWrapperBoolArray(jg, (Boolean[]) value, pf);
    } else if (value.getClass().equals(Byte[].class)) {
      JsonHelper.getJsonFromWrapperByteArray(jg, (Byte[]) value, pf);
    } else if (value.getClass().equals(Short[].class)) {
      JsonHelper.getJsonFromWrapperShortArray(jg, (Short[]) value, pf);
    } else if (value.getClass().equals(Integer[].class)) {
      JsonHelper.getJsonFromWrapperIntArray(jg, (Integer[]) value, pf);
    } else if (value.getClass().equals(Long[].class)) {
      JsonHelper.getJsonFromWrapperLongArray(jg, (Long[]) value, pf);
    } else if (value.getClass().equals(Float[].class)) {
      JsonHelper.getJsonFromWrapperFloatArray(jg, (Float[]) value, pf);
    } else if (value.getClass().equals(Double[].class)) {
      JsonHelper.getJsonFromWrapperDoubleArray(jg, (Double[]) value, pf);
    } else if (value.getClass().equals(BigInteger[].class)) {
      JsonHelper.getJsonFromBigIntArray(jg, (BigInteger[]) value, pf);
    } else if (value.getClass().equals(BigDecimal[].class)) {
      JsonHelper.getJsonFromBigDecimalArray(jg, (BigDecimal[]) value, pf);
    } else if (value.getClass().equals(String[].class)) {
      JsonHelper.getJsonFromStringArray(jg, (String[]) value, pf);
    } else if (value.getClass().equals(Object[].class)) {
      jg.writeStartArray();
      Object[] array = (Object[]) value;
      for (Object obj : array) {
        writeValue(jg, obj, pf);
      }
      jg.writeEndArray();
    } else {
      throw new IllegalStateException(
          "PdxInstance returns unknown pdxfield " + pf + " for type " + value);
    }
  }

  private void getJSONStringFromCollection(JsonGenerator jg, Collection<?> coll, String pf)
      throws JsonGenerationException, IOException {
    jg.writeStartArray();

    for (Object obj : coll) {
      writeValue(jg, obj, pf);
    }
    jg.writeEndArray();
  }
}

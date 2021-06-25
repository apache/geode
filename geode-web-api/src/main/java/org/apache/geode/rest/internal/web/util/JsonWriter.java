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

package org.apache.geode.rest.internal.web.util;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import org.springframework.hateoas.Link;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.util.CollectionUtils;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.Struct;
import org.apache.geode.cache.query.internal.StructImpl;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.pdx.internal.EnumInfo;
import org.apache.geode.pdx.internal.EnumInfo.PdxInstanceEnumInfo;

/**
 * The JsonWriter class is an utility to write various java types as a JSON string.
 * <p/>
 *
 * @since GemFire 8.0
 */
public class JsonWriter {

  public static void writeLinkAsJson(JsonGenerator generator, Link value, String pdxField)
      throws JsonGenerationException, IOException {
    generator.writeArrayFieldStart("links");

    generator.writeStartObject();

    generator.writeFieldName("rel");
    generator.writeString(value.getRel().value());

    generator.writeFieldName("href");
    generator.writeString(value.getHref());

    generator.writeFieldName("type");

    HttpHeaders header = new HttpHeaders();
    header.setContentType(MediaType.APPLICATION_JSON);
    List<Charset> arg = new ArrayList<Charset>();
    arg.add(Charset.defaultCharset());
    header.setAcceptCharset(arg);

    generator.writeString(
        header.getContentType() + ", charset=" + header.getAcceptCharset().get(0).displayName());

    generator.writeEndObject();
    generator.writeEndArray();
  }

  public static void writeQueryListAsJson(JsonGenerator generator, String pdxField,
      Region<String, String> region) throws JsonGenerationException, IOException {
    generator.writeStartObject();
    generator.writeFieldName(pdxField);

    generator.writeStartArray();

    for (Map.Entry<String, String> entry : region.entrySet()) {
      generator.writeStartObject();

      generator.writeFieldName("id");
      generator.writeString(entry.getKey());

      generator.writeFieldName("oql");
      generator.writeString(entry.getValue());

      generator.writeEndObject();
    }

    generator.writeEndArray();
    generator.writeEndObject();
  }

  public static void writeQueryAsJson(JsonGenerator generator, String queryId, String query)
      throws JsonGenerationException, IOException {
    generator.writeStartObject();

    generator.writeFieldName("id");
    generator.writeString(queryId);

    generator.writeFieldName("oql");
    generator.writeString(query);

    generator.writeEndObject();
  }

  public static void writeListAsJson(JsonGenerator generator, Map map, String name,
      String fieldName) throws JsonGenerationException, IOException {

    generator.writeStartObject();
    generator.writeFieldName(name);

    // introspect the Map and write its value into desired format
    generator.writeStartArray();
    Iterator iter = (Iterator) map.entrySet().iterator();
    while (iter.hasNext()) {

      Map.Entry entry = (Map.Entry) iter.next();
      generator.writeStartObject();
      // Iterate over Map and write key-value
      generator.writeFieldName(fieldName);
      generator.writeString(entry.getKey().toString());

      writeValueAsJson(generator, entry.getValue(), name);
      generator.writeEndObject();
    }

    generator.writeEndArray();
    generator.writeEndObject();
  }

  public static void writeCollectionAsJson(JsonGenerator generator, /* List */ Collection<?> coll)
      throws JsonGenerationException, IOException {
    generator.writeStartArray();

    for (Object obj : coll) {
      writeValueAsJson(generator, obj, null);
    }
    generator.writeEndArray();
  }

  public static void writeStructAsJson(JsonGenerator generator, StructImpl element)
      throws JsonGenerationException, IOException {
    generator.writeStartObject();

    String[] fieldNames = element.getFieldNames();
    for (int fieldNamesIndex = 0; fieldNamesIndex < fieldNames.length; fieldNamesIndex++) {
      // Iterate over Map and write key-value
      generator.writeFieldName(fieldNames[fieldNamesIndex]); // write Key in a Map
      Object value = element.get(fieldNames[fieldNamesIndex]);
      writeValueAsJson(generator, value, null); // write value in a Map
    }
    generator.writeEndObject();
  }

  public static String writePdxInstanceAsJson(JsonGenerator generator, PdxInstance pdxInstance)
      throws JsonGenerationException, IOException {
    generator.writeStartObject();

    List<String> pdxFields = pdxInstance.getFieldNames();

    for (String pdxField : pdxFields) {
      Object value = pdxInstance.getField(pdxField);
      generator.writeFieldName(pdxField);
      writeValueAsJson(generator, value, pdxField);
    }
    generator.writeEndObject();
    return null;
  }

  public static void writeArrayAsJson(JsonGenerator generator, Object value, String pdxField)
      throws JsonGenerationException, IOException {

    if (value.getClass().getName().equals("[Z")) {
      writePrimitiveBoolArrayAsJson(generator, (boolean[]) value);
    } else if (value.getClass().getName().equals("[B")) {
      writePrimitiveByteArrayAsJson(generator, (byte[]) value);
    } else if (value.getClass().getName().equals("[S")) {
      writePrimitiveShortArrayAsJson(generator, (short[]) value);
    } else if (value.getClass().getName().equals("[I")) {
      writePrimitiveIntArrayAsJson(generator, (int[]) value);
    } else if (value.getClass().getName().equals("[J")) {
      writePrimitiveLongArrayAsJson(generator, (long[]) value);
    } else if (value.getClass().getName().equals("[F")) {
      writePrimitiveFloatArrayAsJson(generator, (float[]) value);
    } else if (value.getClass().getName().equals("[D")) {
      writePrimitiveDoubleArrayAsJson(generator, (double[]) value);
    } else if (value.getClass().equals(Boolean[].class)) {
      writeWrapperBoolArrayAsJson(generator, (Boolean[]) value);
    } else if (value.getClass().equals(Byte[].class)) {
      writeWrapperByteArrayAsJson(generator, (Byte[]) value);
    } else if (value.getClass().equals(Short[].class)) {
      writeWrapperShortArrayAsJson(generator, (Short[]) value);
    } else if (value.getClass().equals(Integer[].class)) {
      writeWrapperIntArrayAsJson(generator, (Integer[]) value);
    } else if (value.getClass().equals(Long[].class)) {
      writeWrapperLongArrayAsJson(generator, (Long[]) value);
    } else if (value.getClass().equals(Float[].class)) {
      writeWrapperFloatArrayAsJson(generator, (Float[]) value);
    } else if (value.getClass().equals(Double[].class)) {
      writeWrapperDoubleArrayAsJson(generator, (Double[]) value);
    } else if (value.getClass().equals(BigInteger[].class)) {
      writeBigIntArrayAsJson(generator, (BigInteger[]) value);
    } else if (value.getClass().equals(BigDecimal[].class)) {
      writeBigDecimalArrayAsJson(generator, (BigDecimal[]) value);
    } else if (value.getClass().equals(String[].class)) {
      writeStringArrayAsJson(generator, (String[]) value);
    } else if (value.getClass().equals(Object[].class)) {
      writeObjectArrdyAsJson(generator, (Object[]) value, pdxField);
    } else if (value.getClass().isArray()) {
      throw new IllegalStateException(
          "The pdx field " + pdxField + " is an array whose component type "
              + value.getClass().getComponentType()
              + " can not be converted to JSON.");
    } else {
      throw new IllegalStateException(
          "Expected an array for pdx field " + pdxField + ", but got an object of type "
              + value.getClass());
    }
  }

  public static void writePrimitiveBoolArrayAsJson(JsonGenerator generator, boolean[] array)
      throws JsonGenerationException, IOException {
    generator.writeStartArray();
    for (boolean obj : array) {
      generator.writeBoolean(obj);
    }
    generator.writeEndArray();
  }

  public static void writePrimitiveByteArrayAsJson(JsonGenerator generator, byte[] array)
      throws JsonGenerationException, IOException {
    generator.writeStartArray();
    for (byte obj : array) {
      generator.writeNumber(obj);
    }
    generator.writeEndArray();
  }

  public static void writePrimitiveShortArrayAsJson(JsonGenerator generator, short[] array)
      throws JsonGenerationException, IOException {
    generator.writeStartArray();
    for (short obj : array) {
      generator.writeNumber(obj);
    }
    generator.writeEndArray();
  }

  public static void writePrimitiveIntArrayAsJson(JsonGenerator generator, int[] array)
      throws JsonGenerationException, IOException {
    generator.writeStartArray();
    for (int obj : array) {
      generator.writeNumber(obj);
    }
    generator.writeEndArray();
  }

  public static void writePrimitiveLongArrayAsJson(JsonGenerator generator, long[] array)
      throws JsonGenerationException, IOException {
    generator.writeStartArray();
    for (long obj : array) {
      generator.writeNumber(obj);
    }
    generator.writeEndArray();
  }

  public static void writePrimitiveFloatArrayAsJson(JsonGenerator generator, float[] array)
      throws JsonGenerationException, IOException {
    generator.writeStartArray();
    for (float obj : array) {
      generator.writeNumber(obj);
    }
    generator.writeEndArray();
  }

  public static void writePrimitiveDoubleArrayAsJson(JsonGenerator generator, double[] array)
      throws JsonGenerationException, IOException {
    generator.writeStartArray();
    for (double obj : array) {
      generator.writeNumber(obj);
    }
    generator.writeEndArray();
  }


  public static void writeWrapperBoolArrayAsJson(JsonGenerator generator, Boolean[] array)
      throws JsonGenerationException, IOException {
    generator.writeStartArray();
    for (Boolean obj : array) {
      generator.writeBoolean(obj.booleanValue());
    }
    generator.writeEndArray();
  }

  public static void writeWrapperByteArrayAsJson(JsonGenerator generator, Byte[] array)
      throws JsonGenerationException, IOException {
    generator.writeStartArray();
    for (Byte obj : array) {
      generator.writeNumber(obj.byteValue());
    }
    generator.writeEndArray();
  }

  public static void writeWrapperShortArrayAsJson(JsonGenerator generator, Short[] array)
      throws JsonGenerationException, IOException {
    generator.writeStartArray();
    for (Short obj : array) {
      generator.writeNumber(obj.shortValue());
    }
    generator.writeEndArray();
  }

  public static void writeWrapperIntArrayAsJson(JsonGenerator generator, Integer[] array)
      throws JsonGenerationException, IOException {
    generator.writeStartArray();
    for (Integer obj : array) {
      generator.writeNumber(obj.intValue());
    }
    generator.writeEndArray();
  }

  public static void writeWrapperLongArrayAsJson(JsonGenerator generator, Long[] array)
      throws JsonGenerationException, IOException {
    generator.writeStartArray();
    for (Long obj : array) {
      generator.writeNumber(obj.longValue());
    }
    generator.writeEndArray();
  }

  public static void writeWrapperFloatArrayAsJson(JsonGenerator generator, Float[] array)
      throws JsonGenerationException, IOException {
    generator.writeStartArray();
    for (Float obj : array) {
      generator.writeNumber(obj.floatValue());
    }
    generator.writeEndArray();
  }

  public static void writeWrapperDoubleArrayAsJson(JsonGenerator generator, Double[] array)
      throws JsonGenerationException, IOException {
    generator.writeStartArray();
    for (Double obj : array) {
      generator.writeNumber(obj.doubleValue());
    }
    generator.writeEndArray();
  }

  public static void writeBigIntArrayAsJson(JsonGenerator generator, BigInteger[] array)
      throws JsonGenerationException, IOException {
    generator.writeStartArray();
    for (BigInteger obj : array) {
      generator.writeNumber(obj);
    }
    generator.writeEndArray();
  }

  public static void writeBigDecimalArrayAsJson(JsonGenerator generator, BigDecimal[] array)
      throws JsonGenerationException, IOException {
    generator.writeStartArray();
    for (BigDecimal obj : array) {
      generator.writeNumber(obj);
    }
    generator.writeEndArray();
  }

  public static void writeStringArrayAsJson(JsonGenerator generator, String[] array)
      throws JsonGenerationException, IOException {
    generator.writeStartArray();
    for (String obj : array) {
      generator.writeString(obj);
    }
    generator.writeEndArray();
  }

  public static void writeObjectArrayAsJson(JsonGenerator generator, Object[] array,
      String pdxField) throws JsonGenerationException, IOException {
    generator.writeStartArray();

    if (ArrayUtils.isNotEmpty(array)) {
      for (Object obj : array) {
        writeValueAsJson(generator, obj, pdxField);
      }
    }

    generator.writeEndArray();
  }

  public static void writeRegionDetailsAsJson(JsonGenerator generator, Region<?, ?> region)
      throws JsonGenerationException, IOException {
    generator.writeStartObject();

    generator.writeFieldName("name");
    generator.writeString(region.getName());

    generator.writeFieldName("type");

    if (region.getAttributes().getDataPolicy() != null) {
      generator.writeString(region.getAttributes().getDataPolicy().toString());
    } else {
      generator.writeNull();
    }

    generator.writeFieldName("key-constraint");
    if (region.getAttributes().getKeyConstraint() != null) {
      generator.writeString(region.getAttributes().getKeyConstraint().getName());
    } else {
      generator.writeNull();
    }

    generator.writeFieldName("value-constraint");
    if (region.getAttributes().getValueConstraint() != null) {
      generator.writeString(region.getAttributes().getValueConstraint().getName());
    } else {
      generator.writeNull();
    }

    generator.writeEndObject();

  }

  public static void writeRegionSetAsJson(JsonGenerator generator, Set<Region<?, ?>> regions)
      throws JsonGenerationException, IOException {
    generator.writeStartArray();

    if (!CollectionUtils.isEmpty(regions)) {

      for (final Region<?, ?> region : regions) {
        writeRegionDetailsAsJson(generator, region);
      }
    }
    generator.writeEndArray();
  }

  public static void writeMapAsJson(JsonGenerator generator, Map map, String pdxField)
      throws JsonGenerationException, IOException {

    generator.writeStartObject();

    Iterator iter = (Iterator) map.entrySet().iterator();
    while (iter.hasNext()) {
      Map.Entry entry = (Map.Entry) iter.next();

      // Iterate over Map and write key-value
      generator.writeFieldName(entry.getKey().toString()); // write Key in a Map
      writeValueAsJson(generator, entry.getValue(), pdxField); // write value in a Map
    }
    generator.writeEndObject();
  }

  public static void writeValueAsJson(JsonGenerator generator, Object value, String pdxField)
      throws JsonGenerationException, IOException {

    if (value == null) {
      generator.writeNull();
    } else if (value.getClass().equals(Boolean.class)) {
      boolean b = (Boolean) value;
      generator.writeBoolean(b);
    } else if (value.getClass().equals(Byte.class)) {
      Byte b = (Byte) value;
      generator.writeNumber(b);
    } else if (value.getClass().equals(Short.class)) {
      Short b = (Short) value;
      generator.writeNumber(b);
    } else if (value.getClass().equals(Integer.class)) {
      int i = (Integer) value;
      generator.writeNumber(i);
    } else if (value.getClass().equals(Long.class)) {
      long i = (Long) value;
      generator.writeNumber(i);
    } else if (value.getClass().equals(BigInteger.class)) {
      BigInteger i = (BigInteger) value;
      generator.writeNumber(i);
    } else if (value.getClass().equals(Float.class)) {
      float i = (Float) value;
      generator.writeNumber(i);
    } else if (value.getClass().equals(BigDecimal.class)) {
      BigDecimal i = (BigDecimal) value;
      generator.writeNumber(i);
    } else if (value.getClass().equals(Double.class)) {
      double d = (Double) value;
      generator.writeNumber(d);
    } else if (value.getClass().equals(String.class)) {
      String s = (String) value;
      generator.writeString(s);
    } else if (value.getClass().isArray()) {
      writeArrayAsJson(generator, value, pdxField);
    } else if (value.getClass().equals(Link.class)) {
      writeLinkAsJson(generator, (Link) value, pdxField);
      // } else if (value.getClass().equals(Date.class)) {
      // generator.writeObject((Date) value);
    } else if (value.getClass().equals(EnumInfo.class)) {
      /*
       * try { generator.writeString(((EnumInfo)value).getEnum().name()); } catch
       * (ClassNotFoundException e) { throw new
       * IllegalStateException("PdxInstance returns unknwon pdxfield " + pdxField + " for type " +
       * value); }
       */
      generator.writeString(value.toString());
    } else if (value.getClass().equals(PdxInstanceEnumInfo.class)) {
      generator.writeString(value.toString());
    } else {
      if (value instanceof Struct) {
        writeStructAsJson(generator, (StructImpl) value);
      } else if (value instanceof PdxInstance) {
        writePdxInstanceAsJson(generator, (PdxInstance) value);
      } else if (value instanceof Collection) {
        writeCollectionAsJson(generator, (Collection<?>) value);
      } else if (value instanceof Map) {
        writeMapAsJson(generator, (Map) value, pdxField);
      } else {
        generator.writeObject(value);
      }
    }
  }
}

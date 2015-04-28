/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.pdx.internal.json;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;

public class JsonHelper {
  
  public static void getJsonFromPrimitiveBoolArray(JsonGenerator jg, boolean[] array, String pf) throws JsonGenerationException, IOException{
    jg.writeStartArray();
    for (boolean obj : array)
    {
      jg.writeBoolean(obj); 
    }
    jg.writeEndArray();
  }
  
  public static void getJsonFromPrimitiveByteArray(JsonGenerator jg, byte[] array, String pf) throws JsonGenerationException, IOException{
    jg.writeStartArray();
    for (byte obj : array)
    {
      jg.writeNumber(obj);   
    }
    jg.writeEndArray();
  }

  public static void getJsonFromPrimitiveShortArray(JsonGenerator jg, short[] array, String pf) throws JsonGenerationException, IOException{
    jg.writeStartArray();
    for (short obj : array)
    {
      jg.writeNumber(obj);   
    }
    jg.writeEndArray();
  }

  public static void getJsonFromPrimitiveIntArray(JsonGenerator jg, int[] array, String pf) throws JsonGenerationException, IOException{
    jg.writeStartArray();
    for (int obj : array)
    {
      jg.writeNumber(obj);   
    }
    jg.writeEndArray();
  }
  
  public static void getJsonFromPrimitiveLongArray(JsonGenerator jg, long[] array, String pf) throws JsonGenerationException, IOException{
    jg.writeStartArray();
    for (long obj : array)
    {
      jg.writeNumber(obj);   
    }
    jg.writeEndArray();
  }
  
  public static void getJsonFromPrimitiveFloatArray(JsonGenerator jg, float[] array, String pf) throws JsonGenerationException, IOException{
    jg.writeStartArray();
    for (float obj : array)
    {
      jg.writeNumber(obj);   
    }
    jg.writeEndArray();
  }
  
  public static void getJsonFromPrimitiveDoubleArray(JsonGenerator jg, double[] array, String pf) throws JsonGenerationException, IOException{
    jg.writeStartArray();
    for (double obj : array)
    {
      jg.writeNumber(obj);   
    }
    jg.writeEndArray();
  }
  
  
  public static void getJsonFromWrapperBoolArray(JsonGenerator jg, Boolean[] array, String pf) throws JsonGenerationException, IOException{
    jg.writeStartArray();
    for (Boolean obj : array)
    {
      jg.writeBoolean(obj.booleanValue());   
    }
    jg.writeEndArray();
  }
  
  public static void getJsonFromWrapperByteArray(JsonGenerator jg, Byte[] array, String pf) throws JsonGenerationException, IOException{
    jg.writeStartArray();
    for (Byte obj : array)
    {
      jg.writeNumber(obj.byteValue());   
    }
    jg.writeEndArray();
  }

  public static void getJsonFromWrapperShortArray(JsonGenerator jg, Short[] array, String pf) throws JsonGenerationException, IOException{
    jg.writeStartArray();
    for (Short obj : array)
    {
      jg.writeNumber(obj.shortValue());   
    }
    jg.writeEndArray();
  }

  public static void getJsonFromWrapperIntArray(JsonGenerator jg, Integer[] array, String pf) throws JsonGenerationException, IOException{
    jg.writeStartArray();
    for (Integer obj : array)
    {
      jg.writeNumber(obj.intValue());   
    }
    jg.writeEndArray();
  }
  
  public static void getJsonFromWrapperLongArray(JsonGenerator jg, Long[] array, String pf) throws JsonGenerationException, IOException{
    jg.writeStartArray();
    for (Long obj : array)
    {
      jg.writeNumber(obj.longValue());   
    }
    jg.writeEndArray();
  }
  
  public static void getJsonFromWrapperFloatArray(JsonGenerator jg, Float[] array, String pf) throws JsonGenerationException, IOException{
    jg.writeStartArray();
    for (Float obj : array)
    {
      jg.writeNumber(obj.floatValue());   
    }
    jg.writeEndArray();
  }
  
  public static void getJsonFromWrapperDoubleArray(JsonGenerator jg, Double[] array, String pf) throws JsonGenerationException, IOException{
    jg.writeStartArray();
    for (Double obj : array)
    {
      jg.writeNumber(obj.doubleValue());   
    }
    jg.writeEndArray();
  }

  public static void getJsonFromBigIntArray(JsonGenerator jg, BigInteger[] array, String pf) throws JsonGenerationException, IOException{
    jg.writeStartArray();
    for (BigInteger obj : array)
    {
      jg.writeNumber(obj);   
    }
    jg.writeEndArray();
  }
  
  public static void getJsonFromBigDecimalArray(JsonGenerator jg, BigDecimal[] array, String pf) throws JsonGenerationException, IOException{
    jg.writeStartArray();
    for (BigDecimal obj : array)
    {
      jg.writeNumber(obj);   
    }
    jg.writeEndArray();
  }
  
  public static void getJsonFromStringArray(JsonGenerator jg, String[] array, String pf) throws JsonGenerationException, IOException{
    jg.writeStartArray();
    for (String obj : array)
    {
      jg.writeString(obj);   
    }
    jg.writeEndArray();
  }
  
}

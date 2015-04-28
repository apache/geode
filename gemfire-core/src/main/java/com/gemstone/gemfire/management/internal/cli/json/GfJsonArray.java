/*
 * =========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.json;

import java.util.Collection;
import java.util.Map;

import com.gemstone.gemfire.management.internal.cli.LogWrapper;
import org.json.JSONArray;
import org.json.JSONException;

/**
 * Wrapper over JSONArray.
 * 
 * @author Abhishek Chaudhari
 * 
 * @since 7.0
 */
public class GfJsonArray {
  private JSONArray jsonArray;
  
  public GfJsonArray() {
    this.jsonArray = new JSONArray();
  }
  
  /**
   * 
   * @param array
   * @throws GfJsonException If not an array.
   */
  public GfJsonArray(Object array) throws GfJsonException {
    try {
      if (array instanceof JSONArray) {
        this.jsonArray = (JSONArray) array;
      } else {
        this.jsonArray = new JSONArray(array);
      }
    } catch (JSONException e) {
      throw new GfJsonException(e.getMessage());
    }
  }

  /**
   * 
   * @param source
   * @throws GfJsonException If there is a syntax error.
   */
  public GfJsonArray(String source) throws GfJsonException {
    try {
      this.jsonArray = new JSONArray(source);
    } catch (JSONException e) {
      throw new GfJsonException(e.getMessage());
    }
  }

  /**
   * Get the object value associated with an index.
   * 
   * @param index
   * @return An object value.
   * @throws GfJsonException If there is no value for the index.
   */
  public Object get(int index) throws GfJsonException {
    try {
      return this.jsonArray.get(index);
    } catch (JSONException e) {
      throw new GfJsonException(e.getMessage());
    }
  }
  
  public GfJsonObject getJSONObject(int index) throws GfJsonException {
    try {
      return new GfJsonObject(jsonArray.getJSONObject(index));
    } catch (JSONException e) {
      throw new GfJsonException(e.getMessage());
    }
  }

  public GfJsonArray put(Object value) {
    this.jsonArray.put(extractInternalForGfJsonOrReturnSame(value));
    
    return this;
  }

  /**
   * 
   * @param index
   * @param value
   * @return this GfJsonArray
   * @throws GfJsonException
   *           If the index is negative or if the the value is an invalid
   *           number.
   */
  public GfJsonArray put(int index, Object value) throws GfJsonException {
    try {
      this.jsonArray.put(index, extractInternalForGfJsonOrReturnSame(value));
    } catch (JSONException e) {
      throw new GfJsonException(e.getMessage());
    }
    return this;
  }

  public GfJsonArray put(Collection<?> value) {
    this.jsonArray.put(value);
    return this;
  }

  /**
   * 
   * @param index
   * @param value
   * @return this GfJsonArray
   * @throws GfJsonException
   *           If the index is negative or if the value is not finite.
   */
  public GfJsonArray put(int index, Collection<?> value)
      throws GfJsonException {
    try {
      this.jsonArray.put(index, value);
    } catch (JSONException e) {
      throw new GfJsonException(e.getMessage());
    }
    return this;
  }

  public GfJsonArray put(Map<?, ?> value) {
    this.jsonArray.put(value);
    return this;
  }

  /**
   * 
   * @param index
   * @param value
   * @return this GfJsonArray
   * @throws GfJsonException
   *           If the index is negative or if the the value is an invalid
   *           number.
   */
  public GfJsonArray put(int index, Map<?, ?> value)
      throws GfJsonException {
    try {
      this.jsonArray.put(index, value);
    } catch (JSONException e) {
      throw new GfJsonException(e.getMessage());
    }
    return this;
  }
  
  public int size() {
    return jsonArray.length();
  }
  
  @Override
  public String toString() {
    return jsonArray.toString();
  }

  /**
   * 
   * @param indentFactor
   * @return this GfJsonArray
   * @throws GfJsonException
   *           If the object contains an invalid number.
   */
  public String toIndentedString(int indentFactor) throws GfJsonException {
    try {
      return jsonArray.toString(indentFactor);
    } catch (JSONException e) {
      throw new GfJsonException(e.getMessage());
    }
  }
  
  public static byte[] toByteArray(GfJsonArray jsonArray) throws GfJsonException {
    byte[] byteArray = null;
    if (jsonArray != null) {
      int length = jsonArray.size();
      
      byteArray = new byte[length];
      for (int i = 0; i < length; i++) {
        try {
          byteArray[i] = Byte.valueOf(String.valueOf(jsonArray.get(i)));
        } catch (NumberFormatException e) {
          throw e;
        } catch (GfJsonException e) {
          throw new GfJsonException(e.getMessage());
        }
      }
    }
    
    return byteArray ;
  }
  
  public static String[] toStringArray(GfJsonArray jsonArray) {
    String[] stringArray = null;
    if (jsonArray != null) {
      int length = jsonArray.size();
      stringArray = new String[length];
      for (int i = 0; i < length; i++) {
        try {
          stringArray[i] = String.valueOf(jsonArray.get(i));
        } catch (GfJsonException e) {
          LogWrapper.getInstance().info("", e);
          stringArray = null;
        }
      }
    }
    
    return stringArray ;
  }

  public JSONArray getInternalJsonArray() {
    return jsonArray;
  }
  
  private static Object extractInternalForGfJsonOrReturnSame(Object value) {
    Object returnedValue = value;
    if (value instanceof GfJsonObject) {
      returnedValue = ((GfJsonObject)value).getInternalJsonObject();
    } else if (value instanceof GfJsonArray) {
      returnedValue = ((GfJsonArray)value).getInternalJsonArray();
    } else if (value == null) {
      returnedValue = GfJsonObject.NULL;
    }
    
    return returnedValue;
  }
}

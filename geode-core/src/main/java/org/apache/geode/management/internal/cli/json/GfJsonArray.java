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
package org.apache.geode.management.internal.cli.json;

import java.util.Collection;
import java.util.Map;

import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;

import org.apache.geode.internal.logging.LogService;

/**
 * Wrapper over JSONArray.
 *
 *
 * @since GemFire 7.0
 */
public class GfJsonArray {
  private static final Logger logger = LogService.getLogger();

  private JSONArray jsonArray;

  public GfJsonArray() {
    this.jsonArray = new JSONArray();
  }

  /**
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
   * Get the object value associated with an index.
   *
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
   * @return this GfJsonArray
   * @throws GfJsonException If the index is negative or if the the value is an invalid number.
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
   * @return this GfJsonArray
   * @throws GfJsonException If the index is negative or if the value is not finite.
   */
  public GfJsonArray put(int index, Collection<?> value) throws GfJsonException {
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
   * @return this GfJsonArray
   * @throws GfJsonException If the index is negative or if the the value is an invalid number.
   */
  public GfJsonArray put(int index, Map<?, ?> value) throws GfJsonException {
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

  public static byte[] toByteArray(GfJsonArray jsonArray) throws GfJsonException {
    byte[] byteArray = null;
    if (jsonArray != null) {
      int length = jsonArray.size();

      byteArray = new byte[length];
      for (int i = 0; i < length; i++) {
        try {
          byteArray[i] = Byte.valueOf(String.valueOf(jsonArray.get(i)));
        } catch (GfJsonException e) {
          throw new GfJsonException(e.getMessage());
        }
      }
    }

    return byteArray;
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
          logger.info("", e);
          stringArray = null;
        }
      }
    }

    return stringArray;
  }

  public JSONArray getInternalJsonArray() {
    return jsonArray;
  }

  private static Object extractInternalForGfJsonOrReturnSame(Object value) {
    Object returnedValue = value;
    if (value instanceof GfJsonObject) {
      returnedValue = ((GfJsonObject) value).getInternalJsonObject();
    } else if (value instanceof GfJsonArray) {
      returnedValue = ((GfJsonArray) value).getInternalJsonArray();
    } else if (value == null) {
      returnedValue = GfJsonObject.NULL;
    }

    return returnedValue;
  }
}

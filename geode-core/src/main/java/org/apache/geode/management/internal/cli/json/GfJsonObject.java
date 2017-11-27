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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Wrapper over JSONObject.
 *
 *
 * @since GemFire 7.0
 */
public class GfJsonObject {
  public static final Object NULL = JSONObject.NULL;

  private JSONObject jsonObject;

  public GfJsonObject() {
    this.jsonObject = new JSONObject();
  }

  @SuppressWarnings("rawtypes")
  public GfJsonObject(Object bean, boolean checkCyclicDep) {
    if (checkCyclicDep) {
      JSONObject.cyclicDepChkEnabled.set(true);
      JSONObject.cyclicDependencySet.set(new HashSet());
    }
    if (bean instanceof JSONObject) {
      this.jsonObject = (JSONObject) bean;
    } else {
      this.jsonObject = new JSONObject(bean);
    }
    if (checkCyclicDep) {
      JSONObject.cyclicDepChkEnabled.set(false);
      JSONObject.cyclicDependencySet.set(null);
    }
  }

  public GfJsonObject(GfJsonObject gfJsonObject) {
    this.jsonObject = gfJsonObject.jsonObject;
  }

  public GfJsonObject(Map<?, ?> map) {
    this.jsonObject = new JSONObject(map);
  }

  public GfJsonObject(Object bean) {
    if (bean instanceof JSONObject) {
      this.jsonObject = (JSONObject) bean;
    } else {
      this.jsonObject = new JSONObject(bean);
    }
  }

  /**
   *
   * @param source A string beginning with { (left brace) and ending with } (right brace).
   * @throws GfJsonException - If there is a syntax error in the source string or a duplicated key.
   */
  public GfJsonObject(String source) throws GfJsonException {
    try {
      this.jsonObject = new JSONObject(source);
    } catch (JSONException e) {
      throw new GfJsonException(e.getMessage());
    }
  }

  /**
   *
   * @param key
   * @param value
   * @return this GfJsonObject
   * @throws GfJsonException If the key is null OR if the value is non-finite number
   */
  public GfJsonObject accumulate(String key, Object value) throws GfJsonException {
    try {
      if (jsonObject.has(key)) {
        jsonObject.append(key, value);
      } else {
        // first time always add JSONArray for accumulate - for convenience
        jsonObject.put(key, new JSONArray().put(value));
      }
    } catch (JSONException e) {
      throw new GfJsonException(e.getMessage());
    }
    return this;
  }

  public GfJsonObject accumulateAsJSONObject(String key, Object value) throws GfJsonException {
    JSONObject val = new JSONObject(value);
    try {
      if (jsonObject.has(key)) {
        jsonObject.append(key, val);
      } else {
        // first time always add JSONArray for accumulate - for convenience
        jsonObject.put(key, new JSONArray().put(val));
      }
    } catch (JSONException e) {
      throw new GfJsonException(e.getMessage());
    }
    return this;
  }

  /**
   *
   * @param key
   * @param value
   * @return this GfJsonObject
   * @throws GfJsonException - If the key is null or if the current value associated with the key is
   *         not a JSONArray.
   */
  public GfJsonObject append(String key, Object value) throws GfJsonException {
    try {
      jsonObject.append(key, value);
    } catch (JSONException e) {
      throw new GfJsonException(e.getMessage());
    }
    return this;
  }

  public Object get(String key) {
    return jsonObject.opt(key);
  }

  public String getString(String key) {
    return jsonObject.optString(key);
  }

  public int getInt(String key) {
    return jsonObject.optInt(key);
  }

  public long getLong(String key) {
    return jsonObject.optLong(key);
  }

  public double getDouble(String key) {
    return jsonObject.optDouble(key);
  }

  public boolean getBoolean(String key) {
    return jsonObject.optBoolean(key);
  }

  public GfJsonObject getJSONObject(String key) {
    Object opt = jsonObject.opt(key);
    if (opt instanceof GfJsonObject) {
      return (GfJsonObject) opt;
    }

    if (opt == null) {
      return null;
    }

    return new GfJsonObject(opt);
  }

  public JSONObject getInternalJsonObject() {
    return jsonObject;
  }

  /**
   *
   * @param key
   * @return this GfJsonObject
   * @throws GfJsonException If there is a syntax error while preparing GfJsonArray.
   */
  public GfJsonArray getJSONArray(String key) throws GfJsonException {
    JSONArray jsonArray = jsonObject.optJSONArray(key);
    if (jsonArray == null) {
      return null;
    }
    return new GfJsonArray(jsonArray);
  }

  /**
   *
   * @return A GfJsonArray containing the key strings, or null if the internal JSONObject is empty.
   * @throws GfJsonException If there is a syntax error while preparing GfJsonArray.
   */
  public GfJsonArray names() throws GfJsonException {
    GfJsonArray gfJsonArray = new GfJsonArray();
    JSONArray names = jsonObject.names();
    if (names != null) {
      gfJsonArray = new GfJsonArray(names);
    }

    return gfJsonArray;
  }

  /**
   *
   * @param key
   * @param value
   * @return this GfJsonObject object
   * @throws GfJsonException If the value is non-finite number or if the key is null.
   */
  public GfJsonObject put(String key, Object value) throws GfJsonException {
    try {
      jsonObject.put(key, extractInternalForGfJsonOrReturnSame(value));
    } catch (JSONException e) {
      throw new GfJsonException(e.getMessage());
    }
    return this;
  }

  public GfJsonObject putAsJSONObject(String key, Object value) throws GfJsonException {
    try {
      Object internalJsonObj = extractInternalForGfJsonOrReturnSame(value);
      if (internalJsonObj == value) {
        GfJsonObject jsonObj = new GfJsonObject(value);
        internalJsonObj = jsonObj.getInternalJsonObject();
      }
      jsonObject.put(key, internalJsonObj);
    } catch (JSONException e) {
      throw new GfJsonException(e.getMessage());
    }
    return this;
  }

  /**
   *
   * @param key
   * @param value
   * @return this GfJsonObject
   * @throws GfJsonException If the value is a non-finite number.
   */
  public GfJsonObject putOpt(String key, Object value) throws GfJsonException {
    try {
      jsonObject.putOpt(key, extractInternalForGfJsonOrReturnSame(value));
    } catch (JSONException e) {
      throw new GfJsonException(e.getMessage());
    }
    return this;
  }

  /**
   *
   * @param key
   * @param value
   * @return this GfJsonObject
   * @throws GfJsonException If the value is a non-finite number.
   */
  public GfJsonObject put(String key, Collection<?> value) throws GfJsonException {
    try {
      jsonObject.putOpt(key, value);
    } catch (JSONException e) {
      throw new GfJsonException(e.getMessage());
    }
    return this;
  }

  public GfJsonObject putJSONArray(String key, GfJsonArray value) throws GfJsonException {
    try {
      jsonObject.putOpt(key, value.getInternalJsonArray());
    } catch (JSONException e) {
      throw new GfJsonException(e.getMessage());
    }
    return this;
  }

  public GfJsonObject put(String key, Map<?, ?> value) throws GfJsonException {
    try {
      jsonObject.put(key, value);
    } catch (JSONException e) {
      throw new GfJsonException(e.getMessage());
    }
    return this;
  }

  public static String quote(String string) {
    return JSONObject.quote(string);
  }

  public Object remove(String key) {
    return jsonObject.remove(key);
  }

  public boolean has(String key) {
    return jsonObject.has(key);
  }

  @SuppressWarnings("unchecked")
  public Iterator<String> keys() {
    return jsonObject.keys();
  }

  /**
   * @return the column size of this GfJsonObject
   */
  public int size() {
    return jsonObject.length();
  }

  public String toString() {
    return jsonObject.toString();
  }

  public String getType() {
    return jsonObject.optString("type-class");
  }

  /**
   *
   * @param indentFactor
   * @return this GfJsonObject
   * @throws GfJsonException If the object contains an invalid number.
   */
  public String toIndentedString(int indentFactor) throws GfJsonException {
    try {
      return jsonObject.toString(indentFactor);
    } catch (JSONException e) {
      throw new GfJsonException(e.getMessage());
    }
  }

  private static Object extractInternalForGfJsonOrReturnSame(Object value) {
    Object returnedValue = value;
    if (value instanceof GfJsonObject) {
      returnedValue = ((GfJsonObject) value).getInternalJsonObject();
    } else if (value instanceof GfJsonArray) {
      returnedValue = ((GfJsonArray) value).getInternalJsonArray();
    } else if (value == null) {
      returnedValue = NULL;
    }

    return returnedValue;
  }

  public static GfJsonObject getGfJsonErrorObject(String errorMessage) {
    Map<String, String> errorMap = new HashMap<String, String>();
    errorMap.put("error", errorMessage);
    return new GfJsonObject(errorMap);
  }

  public static boolean isJSONKind(Object object) {
    return object instanceof JSONObject;
  }

}

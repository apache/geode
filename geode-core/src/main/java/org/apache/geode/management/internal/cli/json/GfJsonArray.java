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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.logging.log4j.Logger;

import org.apache.geode.internal.logging.LogService;

/**
 * Wrapper over JSONArray.
 *
 *
 * @since GemFire 7.0
 */
public class GfJsonArray extends AbstractJSONFormatter {
  private static final Logger logger = LogService.getLogger();

  private final ArrayNode jsonArray;

  public GfJsonArray() {
    super(-1, -1, false);
    this.jsonArray = mapper.createArrayNode();
  }

  /**
   * @throws GfJsonException If not an array.
   */
  public GfJsonArray(Object array) throws GfJsonException {
    super(-1, -1, false);
    try {
      if (array instanceof ArrayNode) {
        this.jsonArray = (ArrayNode) array;
      } else {
        this.jsonArray = mapper.valueToTree(array);
      }
      if (this.jsonArray == null) {
        throw new IllegalArgumentException("Array translated into null JSON node: " + array);
      }
    } catch (IllegalArgumentException e) {
      throw new GfJsonException(e);
    }
  }

  void postCreateMapper() {
    mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
    mapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
  }

  /**
   * Get the object value associated with an index.
   *
   * @return An object value.
   * @throws GfJsonException If there is no value for the index.
   */
  public String getString(int index) throws GfJsonException {
    try {
      JsonNode node = jsonArray.get(index);
      if (node.textValue() != null) {
        return node.asText();
      } else {
        return node.toString();
      }
    } catch (IllegalArgumentException e) {
      throw new GfJsonException(e);
    }
  }

  public GfJsonObject getInternalJsonObject(int index) throws GfJsonException {
    try {
      return new GfJsonObject(jsonArray.get(index));
    } catch (IllegalArgumentException e) {
      throw new GfJsonException(e);
    }
  }

  public GfJsonArray put(Object value) {
    this.jsonArray.add(toJsonNode(value));
    return this;
  }

  public int length() {
    return this.jsonArray.size();
  }

  /**
   * @return this GfJsonArray
   * @throws GfJsonException If the index is negative or if the the value is an invalid number.
   */
  public GfJsonArray put(int index, Object value) throws GfJsonException {
    JsonNode json = null;
    try {
      json = toJsonNode(value);
      this.jsonArray.set(index, json);
    } catch (IndexOutOfBoundsException e) {
      while (this.jsonArray.size() < index) {
        this.jsonArray.add((JsonNode) null);
      }
      this.jsonArray.add(json);
    } catch (IllegalArgumentException e) {
      throw new GfJsonException(e);
    }
    return this;
  }

  /**
   * @return this GfJsonArray
   * @throws GfJsonException If the index is negative or if the the value is an invalid number.
   */
  public GfJsonArray put(int index, Map<?, ?> value) throws GfJsonException {
    try {
      this.jsonArray.set(index, toJsonNode(value));
    } catch (IllegalArgumentException e) {
      throw new GfJsonException(e);
    }
    return this;
  }

  public int size() {
    return jsonArray.size();
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
          byteArray[i] = Byte.valueOf(String.valueOf(jsonArray.getString(i)));
        } catch (GfJsonException e) {
          throw new GfJsonException(e.getMessage());
        }
      }
    }

    return byteArray;
  }

  public List<String> toStringList() {
    List<String> stringArray = null;
    int length = jsonArray.size();
    stringArray = new ArrayList<>(length);
    for (int i = 0; i < length; i++) {
      try {
        stringArray.add(getString(i));
      } catch (GfJsonException e) {
        logger.info("", e);
        stringArray = null;
      }
    }
    return stringArray;
  }

  public ArrayNode getInternalJsonArray() {
    return jsonArray;
  }

  private JsonNode toJsonNode(Object value) {
    if (value instanceof GfJsonObject) {
      return ((GfJsonObject) value).getInternalJsonObject();
    }
    if (value instanceof GfJsonArray) {
      return ((GfJsonArray) value).getInternalJsonArray();
    } else if (value == null) {
      return mapper.valueToTree(GfJsonObject.NULL);
    }
    return mapper.valueToTree(value);
  }

  public GfJsonArray getJSONArray(int i) throws GfJsonException {
    return new GfJsonArray(jsonArray.get(i));
  }
}

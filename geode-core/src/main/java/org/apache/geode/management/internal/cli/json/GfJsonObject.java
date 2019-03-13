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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Simiulation of org.json.JSONObject based on Jackson Databind
 */
public class GfJsonObject extends AbstractJSONFormatter {
  @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
  public static final Object NULL = new Object() {
    @Override
    public boolean equals(Object o) {
      if (Objects.isNull(o)) {
        return true;
      }
      return o == this;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    @Override
    public String toString() {
      return "null";
    }
  };

  private ObjectNode rootNode;

  public GfJsonObject() {
    super(-1, -1, false);
    this.rootNode = mapper.createObjectNode();
  }

  public GfJsonObject(Map<?, ?> map) {
    super(-1, -1, false);
    this.rootNode = mapper.valueToTree(map);
  }

  public GfJsonObject(Object bean) {
    super(-1, -1, false);
    if (bean instanceof ObjectNode) {
      this.rootNode = (ObjectNode) bean;
    } else {
      this.rootNode = mapper.valueToTree(bean);
    }
  }

  /**
   *
   * @param source A string beginning with { (left brace) and ending with } (right brace).
   * @throws GfJsonException - If there is a syntax error in the source string or a duplicated key.
   */
  public GfJsonObject(String source) throws GfJsonException {
    super(-1, -1, false);
    try {
      this.rootNode = (ObjectNode) mapper.readTree(source);
    } catch (IOException e) {
      throw new GfJsonException(e.getMessage());
    }
    if (rootNode == null) {
      throw new GfJsonException("Unable to parse JSON document");
    }
  }

  void postCreateMapper() {
    mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
    mapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
  }

  /**
   *
   * @return this GfJsonObject
   * @throws GfJsonException If the key is null OR if the value is non-finite number
   */
  public GfJsonObject accumulate(String key, Object value) throws GfJsonException {
    try {
      if (rootNode.has(key)) {
        append(key, value);
      } else {
        // first time always add JSONArray for accumulate - for convenience
        ArrayNode array = mapper.createArrayNode();
        array.add(mapper.valueToTree(value));
        rootNode.set(key, array);
      }
    } catch (RuntimeException e) {
      throw new GfJsonException(e);
    }
    return this;
  }

  /**
   *
   * @return this GfJsonObject
   * @throws GfJsonException - If the key is null or if the current value associated with the key is
   *         not a JSONArray.
   */
  public GfJsonObject append(String key, Object value) throws GfJsonException {
    try {
      JsonNode current = rootNode.get(key);
      if (current instanceof ArrayNode) {
        ArrayNode array = (ArrayNode) current;
        array.add(mapper.valueToTree(value));
      } else if (current == null) {
        ArrayNode array = mapper.createArrayNode();
        array.add(mapper.valueToTree(value));
        rootNode.set(key, array);
      } else {
        throw new GfJsonException("Cannot append to a non-array field");
      }
    } catch (RuntimeException e) {
      throw new GfJsonException(e);
    }
    return this;
  }

  /**
   * return the Jackson JsonNode associated with the given key
   */
  public JsonNode get(String key) {
    return rootNode.get(key);
  }

  public String getString(String key) {
    JsonNode node = rootNode.get(key);
    if (node == null) {
      return null; // "null";
    }
    if (node.textValue() != null) {
      return node.textValue();
    }
    return node.toString();
  }

  public int getInt(String key) {
    return rootNode.get(key).asInt();
  }

  public long getLong(String key) {
    return rootNode.get(key).asLong();
  }

  public double getDouble(String key) {
    return rootNode.get(key).asDouble();
  }

  public boolean getBoolean(String key) {
    return rootNode.get(key).asBoolean();
  }

  public GfJsonObject getJSONObject(String key) {
    Object value = rootNode.get(key);
    if (value == null) {
      return null;
    }
    return new GfJsonObject(value);
  }

  public JsonNode getInternalJsonObject() {
    return rootNode;
  }

  /**
   *
   * @return this GfJsonObject
   * @throws GfJsonException If there is a syntax error while preparing GfJsonArray.
   */
  public GfJsonArray getJSONArray(String key) throws GfJsonException {
    JsonNode node = rootNode.get(key);
    if (node == null) {
      return null;
    }
    if (!(node instanceof ArrayNode)) {
      // convert from list format to array format
      ArrayNode newNode = mapper.createArrayNode();
      for (int i = 0; i < node.size(); i++) {
        newNode.add(node.get("" + i));
      }
      rootNode.set(key, newNode);
      return new GfJsonArray(newNode);
    }
    return new GfJsonArray(node);
  }

  /**
   *
   * @return A GfJsonArray containing the key strings, or null if the internal JSONObject is empty.
   * @throws GfJsonException If there is a syntax error while preparing GfJsonArray.
   */
  public GfJsonArray names() throws GfJsonException {
    int size = rootNode.size();
    if (size == 0) {
      return new GfJsonArray();
    }
    String[] fieldNames = new String[rootNode.size()];
    Iterator<String> fieldNameIter = rootNode.fieldNames();
    int i = 0;
    while (fieldNameIter.hasNext()) {
      fieldNames[i++] = fieldNameIter.next();
    }
    return new GfJsonArray(fieldNames);
  }

  /**
   *
   * @return this GfJsonObject object
   * @throws GfJsonException If the value is non-finite number or if the key is null.
   */
  public GfJsonObject put(String key, Object value) throws GfJsonException {
    try {
      rootNode.set(key, toJsonNode(value));
    } catch (IllegalArgumentException e) {
      throw new GfJsonException(e);
    }
    return this;
  }

  public GfJsonObject putAsJSONObject(String key, Object value) throws GfJsonException {
    try {
      JsonNode internalJsonObj = toJsonNode(value);
      rootNode.set(key, internalJsonObj);
    } catch (IllegalArgumentException e) {
      throw new GfJsonException(e);
    }
    return this;
  }

  /**
   *
   * @return this GfJsonObject
   * @throws GfJsonException If the value is a non-finite number.
   */
  public GfJsonObject putOpt(String key, Object value) throws GfJsonException {
    if (key == null || value == null) {
      return this;
    }
    try {
      rootNode.set(key, toJsonNode(value));
    } catch (IllegalArgumentException e) {
      throw new GfJsonException(e);
    }
    return this;
  }

  // /**
  // *
  // * @return this GfJsonObject
  // * @throws GfJsonException If the value is a non-finite number.
  // */
  // public GfJsonObject put(String key, Collection<?> value) throws GfJsonException {
  // if (key == null || value == null) {
  // return this;
  // }
  // try {
  // rootNode.set(key, toJsonNode(value));
  // } catch (IllegalArgumentException e) {
  // throw new GfJsonException(e);
  // }
  // return this;
  // }
  //
  // public GfJsonObject put(String key, Map<?, ?> value) throws GfJsonException {
  // try {
  // rootNode.set(key, toJsonNode(value));
  // } catch (IllegalArgumentException e) {
  // throw new GfJsonException(e);
  // }
  // return this;
  // }

  // public static String quote(String string) {
  // return JSONObject.quote(string);
  // }

  // public Object remove(String key) {
  // return rootNode.remove(key);
  // }

  public boolean has(String key) {
    return rootNode.has(key);
  }

  public Iterator<String> keys() {
    return rootNode.fieldNames();
  }

  /**
   * @return the column size of this GfJsonObject
   */
  public int size() {
    return rootNode.size();
  }

  public String toString() {
    return rootNode.toString();
  }

  // public String getType() {
  // return rootNode.optString("type-class");
  // }

  /**
   *
   * @return this GfJsonObject
   * @throws GfJsonException If the object contains an invalid number.
   */
  public String toIndentedString() throws GfJsonException {
    try {
      return rootNode.toString();
    } catch (Exception e) {
      throw new GfJsonException(e);
    }
  }

  public List<String> getArrayValues(String key) {
    List<String> result = new ArrayList<>();
    if (rootNode.has(key)) {
      JsonNode node = rootNode.get(key);
      if (!(node instanceof ArrayNode)) {
        throw new IllegalStateException("requested field is not an array: " + key);
      }
      ArrayNode array = (ArrayNode) node;
      for (int i = 0; i < array.size(); i++) {
        JsonNode valueNode = array.get(i);
        result.add(valueNode.textValue() == null ? valueNode.toString() : valueNode.asText());
      }
    }
    return result;
  }

  private JsonNode toJsonNode(Object value) {
    if (value instanceof JsonNode) {
      return (JsonNode) value;
    }
    if (value instanceof GfJsonObject) {
      return ((GfJsonObject) value).getInternalJsonObject();
    }
    if (value instanceof GfJsonArray) {
      return ((GfJsonArray) value).getInternalJsonArray();
    }
    if (value == null) {
      return mapper.valueToTree(NULL);
    }
    return mapper.valueToTree(value);
  }

  public static GfJsonObject getGfJsonErrorObject(String errorMessage) {
    Map<String, String> errorMap = new HashMap<String, String>();
    errorMap.put("error", errorMessage);
    return new GfJsonObject(errorMap);
  }

}

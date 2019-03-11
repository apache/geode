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
package org.apache.geode.management.internal.cli.util;

import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.beanutils.ConvertUtils;

import org.apache.geode.management.internal.cli.json.GfJsonException;
import org.apache.geode.management.internal.cli.json.GfJsonObject;

/**
 * This class contains utility methods for JSON (http://www.json.org/) which is used by classes used
 * for the Command Line Interface (CLI).
 *
 *
 * @since GemFire 7.0
 */
public class JsonUtil {

  /**
   * Converts given JSON String in to a Object. Refer http://www.json.org/ to construct a JSON
   * format.
   *
   * @param jsonString jsonString to be converted in to a Map.
   * @return an object constructed from given JSON String
   *
   * @throws IllegalArgumentException if the specified JSON string can not be converted in to an
   *         Object
   */
  public static <T> T jsonToObject(String jsonString, Class<T> klass) {
    T objectFromJson = null;
    try {
      GfJsonObject jsonObject = new GfJsonObject(jsonString);
      objectFromJson = klass.newInstance();
      Method[] declaredMethods = klass.getMethods();
      Map<String, Method> methodsMap = new HashMap<String, Method>();
      for (Method method : declaredMethods) {
        methodsMap.put(method.getName(), method);
      }

      int noOfFields = jsonObject.size();
      Iterator<String> keys = jsonObject.keys();

      while (keys.hasNext()) {
        String key = keys.next();
        Method method = methodsMap.get("set" + capitalize(key));
        if (method != null) {
          Class<?>[] parameterTypes = method.getParameterTypes();
          if (parameterTypes.length == 1) {
            Class<?> parameterType = parameterTypes[0];

            Object value = jsonObject.get(key);
            if (isPrimitiveOrWrapper(parameterType)) {
              value = ConvertUtils.convert(getPrimitiveOrWrapperValue(parameterType, value),
                  parameterType);
            }
            // Bug #51175
            else if (isArray(parameterType)) {
              value = toArray(value, parameterType);
            } else if (isList(parameterType)) {
              value = toList(value, parameterType);
            } else if (isMap(parameterType)) {
              value = toMap(value, parameterType);
            } else if (isSet(parameterType)) {
              value = toSet(value, parameterType);
            } else {
              value = jsonToObject(value.toString(), parameterType);
            }
            method.invoke(objectFromJson, new Object[] {value});
            noOfFields--;
          }

        }
      }

      if (noOfFields != 0) {
        throw new IllegalArgumentException(
            "Not enough setter methods for fields in given JSON String : " + jsonString
                + " in class : " + klass);
      }

    } catch (InstantiationException e) {
      throw new IllegalArgumentException("Couldn't convert JSON to Object of type " + klass, e);
    } catch (IllegalAccessException e) {
      throw new IllegalArgumentException("Couldn't convert JSON to Object of type " + klass, e);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Couldn't convert JSON to Object of type " + klass, e);
    } catch (InvocationTargetException e) {
      throw new IllegalArgumentException("Couldn't convert JSON to Object of type " + klass, e);
    } catch (GfJsonException e) {
      throw new IllegalArgumentException(e);
    }
    return objectFromJson;
  }

  private static void throwUnsupportedType(String containerType, JsonNodeType valueType)
      throws GfJsonException {
    throw new GfJsonException(String.format(
        "Only primitive types are supported in %s type for input commands but found %s",
        containerType, valueType.toString()));
  }

  private static Object toArray(Object value, Class<?> parameterType) throws GfJsonException {
    Class arrayComponentType = parameterType.getComponentType();
    if (isPrimitiveOrWrapper(arrayComponentType)) {
      if (value instanceof ArrayNode) {
        try {
          ArrayNode jsonArray = (ArrayNode) value;
          Object jArray = Array.newInstance(arrayComponentType, jsonArray.size());
          for (int i = 0; i < jsonArray.size(); i++) {
            JsonNode elem = jsonArray.get(i);
            switch (elem.getNodeType()) {
              case ARRAY:
                throwUnsupportedType("array", elem.getNodeType());
              case BINARY:
                throwUnsupportedType("array", elem.getNodeType());
              case BOOLEAN:
                Array.set(jArray, i, jsonArray.get(i).booleanValue());
              case MISSING:
                throwUnsupportedType("array", elem.getNodeType());
              case NULL:
                throwUnsupportedType("array", elem.getNodeType());
              case NUMBER:
                if (elem.isIntegralNumber()) {
                  Array.set(jArray, i, elem.longValue());
                } else {
                  Array.set(jArray, i, elem.doubleValue());
                }
                break;
              case OBJECT:
                throwUnsupportedType("array", elem.getNodeType());
              case POJO:
                throwUnsupportedType("array", elem.getNodeType());
              case STRING:
                if (elem.textValue() != null) {
                  Array.set(jArray, i, elem.textValue());
                } else {
                  Array.set(jArray, i, elem.toString());
                }
            }
          }
          return jArray;
        } catch (ArrayIndexOutOfBoundsException e) {
          throw new GfJsonException(e);
        } catch (IllegalArgumentException e) {
          throw new GfJsonException(e);
        }
      } else {
        throw new GfJsonException("Expected JSONArray for array type");
      }
    } else
      throw new GfJsonException(
          "Array contains non-primitive element. Non-primitive elements are not supported in json array");
  }



  /**
   * This is used in Put command this method uses HashSet as default implementation
   *
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  private static Object toSet(Object value, Class<?> parameterType) throws GfJsonException {
    try {
      ArrayNode array = (ArrayNode) value;
      Set set = new HashSet();
      for (int i = 0; i < array.size(); i++) {
        JsonNode elem = array.get(i);
        switch (elem.getNodeType()) {
          case ARRAY:
            throwUnsupportedType("list", elem.getNodeType());
          case BINARY:
            throwUnsupportedType("list", elem.getNodeType());
          case BOOLEAN:
            set.add(elem.booleanValue());
          case MISSING:
            throwUnsupportedType("list", elem.getNodeType());
          case NULL:
            throwUnsupportedType("list", elem.getNodeType());
          case NUMBER:
            if (elem.isIntegralNumber()) {
              set.add(elem.longValue());
            } else {
              set.add(elem.doubleValue());
            }
            break;
          case OBJECT:
            throwUnsupportedType("list", elem.getNodeType());
          case POJO:
            throwUnsupportedType("list", elem.getNodeType());
          case STRING:
            if (elem.textValue() != null) {
              set.add(elem.textValue());
            } else {
              set.add(elem.toString());
            }
        }
      }
      return set;
    } catch (Exception e) {
      throw new GfJsonException(e);
    }
  }

  private static Object toMap(Object value, Class<?> parameterType) throws GfJsonException {
    try {
      if (value instanceof ObjectNode) {
        ObjectNode obj = (ObjectNode) value;
        Iterator iterator = obj.fieldNames();
        Map map = new HashMap();
        while (iterator.hasNext()) {
          String key = (String) iterator.next();
          JsonNode elem = obj.get(key);
          switch (elem.getNodeType()) {
            case ARRAY:
              throwUnsupportedType("map", elem.getNodeType());
            case BINARY:
              throwUnsupportedType("map", elem.getNodeType());
            case BOOLEAN:
              map.put(key, elem.booleanValue());
            case MISSING:
              throwUnsupportedType("map", elem.getNodeType());
            case NULL:
              throwUnsupportedType("map", elem.getNodeType());
            case NUMBER:
              if (elem.isIntegralNumber()) {
                map.put(key, elem.longValue());
              } else {
                map.put(key, elem.doubleValue());
              }
              break;
            case OBJECT:
              throwUnsupportedType("map", elem.getNodeType());
            case POJO:
              throwUnsupportedType("map", elem.getNodeType());
            case STRING:
              if (elem.textValue() != null) {
                map.put(key, elem.textValue());
              } else {
                map.put(key, elem.toString());
              }
          }
        }
        return map;
      } else
        throw new GfJsonException(
            "Expected JSONObject for Map. Retrieved type is " + value.getClass());
    } catch (Exception e) {
      throw new GfJsonException(e);
    }
  }

  private static Object toList(Object value, Class<?> parameterType) throws GfJsonException {
    try {
      ArrayNode array = (ArrayNode) value;
      List list = new ArrayList();
      for (int i = 0; i < array.size(); i++) {
        JsonNode elem = array.get(i);
        switch (elem.getNodeType()) {
          case ARRAY:
            throwUnsupportedType("list", elem.getNodeType());
          case BINARY:
            throwUnsupportedType("list", elem.getNodeType());
          case BOOLEAN:
            list.add(elem.booleanValue());
          case MISSING:
            throwUnsupportedType("list", elem.getNodeType());
          case NULL:
            throwUnsupportedType("list", elem.getNodeType());
          case NUMBER:
            if (elem.isIntegralNumber()) {
              list.add(elem.longValue());
            } else {
              list.add(elem.doubleValue());
            }
            break;
          case OBJECT:
            throwUnsupportedType("list", elem.getNodeType());
          case POJO:
            throwUnsupportedType("list", elem.getNodeType());
          case STRING:
            if (elem.textValue() != null) {
              list.add(elem.textValue());
            } else {
              list.add(elem.toString());
            }
        }
      }
      return list;
    } catch (Exception e) {
      throw new GfJsonException(e);
    }
  }

  public static String capitalize(String str) {
    String capitalized = str;
    if (str == null || str.isEmpty()) {
      return capitalized;
    }
    capitalized = String.valueOf(str.charAt(0)).toUpperCase() + str.substring(1);

    return capitalized;
  }

  private static boolean isArray(Class<?> parameterType) {
    return parameterType.isArray();
  }

  public static boolean isList(Class<?> klass) {
    return klass.isAssignableFrom(List.class);
  }

  public static boolean isSet(Class<?> klass) {
    return klass.isAssignableFrom(Set.class);
  }

  public static boolean isMap(Class<?> klass) {
    return klass.isAssignableFrom(Map.class);
  }

  public static boolean isPrimitiveOrWrapper(Class<?> klass) {
    return klass.isAssignableFrom(Byte.class) || klass.isAssignableFrom(byte.class)
        || klass.isAssignableFrom(Short.class) || klass.isAssignableFrom(short.class)
        || klass.isAssignableFrom(Integer.class) || klass.isAssignableFrom(int.class)
        || klass.isAssignableFrom(Long.class) || klass.isAssignableFrom(long.class)
        || klass.isAssignableFrom(Float.class) || klass.isAssignableFrom(float.class)
        || klass.isAssignableFrom(Double.class) || klass.isAssignableFrom(double.class)
        || klass.isAssignableFrom(Boolean.class) || klass.isAssignableFrom(boolean.class)
        || klass.isAssignableFrom(String.class) || klass.isAssignableFrom(Character.class)
        || klass.isAssignableFrom(char.class);
  }

  public static Object getPrimitiveOrWrapperValue(Class<?> klass, Object value)
      throws IllegalArgumentException {
    if (klass.isAssignableFrom(Byte.class) || klass.isAssignableFrom(byte.class)) {
      return value;
    } else if (klass.isAssignableFrom(Short.class) || klass.isAssignableFrom(short.class)) {
      return value;
    } else if (klass.isAssignableFrom(Integer.class) || klass.isAssignableFrom(int.class)) {
      return value;
    } else if (klass.isAssignableFrom(Long.class) || klass.isAssignableFrom(long.class)) {
      return value;
    } else if (klass.isAssignableFrom(Float.class) || klass.isAssignableFrom(float.class)) {
      return value;
    } else if (klass.isAssignableFrom(Double.class) || klass.isAssignableFrom(double.class)) {
      return value;
    } else if (klass.isAssignableFrom(Boolean.class) || klass.isAssignableFrom(boolean.class)) {
      return value;
    } else if (klass.isAssignableFrom(String.class)) {
      return String.valueOf(value);
    } else if (klass.isAssignableFrom(Character.class)) {
      // Need to take care of converting between string to char values
      if (value instanceof String) {
        String str = (String) value;
        if (str.length() == 1)
          return new Character(str.charAt(0));
        else if (str.length() > 1 || str.length() == 0) {
          throw new IllegalArgumentException(
              "Expected Character value but found String with length " + str.length());
        }
      } else if (value instanceof Character) {
        return value;
      } else {
        throw new IllegalArgumentException(
            "Expected Character value but found " + value.getClass());
      }
    } else if (klass.isAssignableFrom(char.class)) {
      // Need to take care of converting between string to char values
      if (value instanceof String) {
        String str = (String) value;
        if (str.length() == 1)
          return str.charAt(0);
        else if (str.length() > 1 || str.length() == 0) {
          throw new IllegalArgumentException(
              "Expected Character value but found String with length " + str.length());
        }
      } else if (value instanceof Character) {
        return ((Character) value).charValue();
      } else {
        throw new IllegalArgumentException(
            "Expected Character value but found " + value.getClass());
      }
    } else {
      return null;
    }
    return value;
  }
}

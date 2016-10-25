/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.management.internal.cli.util;

import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.geode.internal.ClassPathLoader;
import org.apache.geode.management.internal.cli.json.GfJsonArray;
import org.apache.geode.management.internal.cli.json.GfJsonException;
import org.apache.geode.management.internal.cli.json.GfJsonObject;
import org.apache.geode.management.internal.cli.result.CliJsonSerializable;
import org.apache.geode.management.internal.cli.result.CliJsonSerializableFactory;
import org.apache.geode.management.internal.cli.result.ResultDataException;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * This class contains utility methods for JSON (http://www.json.org/) which is 
 * used by classes used for the Command Line Interface (CLI).
 * 
 * 
 * @since GemFire 7.0
 */
public class JsonUtil {

  /**
   * Converts given JSON String in to a Map. 
   * Refer http://www.json.org/ to construct a JSON format.
   * 
   * @param jsonString
   *          jsonString to be converted in to a Map.
   * @return a Map created from
   * 
   * @throws IllegalArgumentException
   *           if the specified JSON string can not be converted in to a Map
   */
  public static Map<String, String> jsonToMap(String jsonString) {
    Map<String, String> jsonMap = new TreeMap<String, String>();
    try {
      GfJsonObject jsonObject = new GfJsonObject(jsonString);
      Iterator<String> keys = jsonObject.keys();
      
      while (keys.hasNext()) {
        String key = keys.next();
        jsonMap.put(key, jsonObject.getString(key));
      }
      
    } catch (GfJsonException e) {
      throw new IllegalArgumentException("Could not convert jsonString : '"+jsonString+"' to map.");
    }
    return jsonMap;
  }
  
  /**
   * Converts given Map in to a JSON string representing a Map. 
   * Refer http://www.json.org/ for more.
   * 
   * @param properties a Map of Strings to be converted in to JSON String
   * @return a JSON string representing the specified Map.
   */
  public static String mapToJson(Map<String, String> properties) {
    return new GfJsonObject(properties).toString();
  }
  
  /**
   * Converts given Object in to a JSON string representing an Object. 
   * Refer http://www.json.org/ for more.
   * 
   * @param object an Object to be converted in to JSON String
   * @return a JSON string representing the specified object.
   */
  public static String objectToJson(Object object) {    
    return new GfJsonObject(object).toString();
  }  

  /**
   * Converts given Object in to a JSON string representing an Object. 
   * If object contains an attribute which itself is another object
   * it will be displayed as className if its json representation
   * exceeds the length
   * 
   * @param object an Object to be converted in to JSON String
   * @return a JSON string representing the specified object.
   */
  public static String objectToJsonNested(Object object, int length) {
    return objectToJsonNestedChkCDep(object, length, false);    
  }
  
  public static String objectToJsonNestedChkCDep(Object object, int length) {    
   return objectToJsonNestedChkCDep(object, length, true);
  }
  
  private static String objectToJsonNestedChkCDep(Object object, int length, boolean checkCyclicDep) {    
    GfJsonObject jsonObject = new GfJsonObject(object,checkCyclicDep);
    Iterator<String> iterator = jsonObject.keys();
    while(iterator.hasNext()){
      String key = iterator.next();
      Object value = jsonObject.get(key);
      if(value!=null && !isPrimitiveOrWrapper(value.getClass())){
        GfJsonObject jsonified = new GfJsonObject(value);
        String stringified = jsonified.toString();
        try{
        if(stringified.length()>length){          
          jsonObject.put(key,jsonified.getType());
        }else{
          jsonObject.put(key, stringified);
        }
        }catch (GfJsonException e) {          
          e.printStackTrace();          
        }
      }
    }    
    return jsonObject.toString();
  }
  
  /**
   * Converts given JSON String in to a Object. 
   * Refer http://www.json.org/ to construct a JSON format.
   * 
   * @param jsonString
   *          jsonString to be converted in to a Map.
   * @return an object constructed from given JSON String
   * 
   * @throws IllegalArgumentException
   *           if the specified JSON string can not be converted in to an Object
   */
  public static <T> T jsonToObject(String jsonString, Class<T> klass) {
    T objectFromJson = null;
    try {      
      GfJsonObject jsonObject = new GfJsonObject(jsonString);
      objectFromJson = klass.newInstance();
      Method[] declaredMethods = klass.getDeclaredMethods();
      Map<String, Method> methodsMap = new HashMap<String, Method>();
      for (Method method : declaredMethods) {
        methodsMap.put(method.getName(), method);
      }
      
      int noOfFields = jsonObject.size();
      Iterator<String> keys = jsonObject.keys();
      
      while (keys.hasNext()) {
        String key = keys.next();
        Method method = methodsMap.get("set"+capitalize(key));
        if (method != null) {
          Class<?>[] parameterTypes = method.getParameterTypes();
          if (parameterTypes.length == 1) {
            Class<?> parameterType = parameterTypes[0];
            
            Object value = jsonObject.get(key);
            if (isPrimitiveOrWrapper(parameterType)) {
              value = getPrimitiveOrWrapperValue(parameterType, value);
            }
            // Bug #51175
            else if (isArray(parameterType)){
              value = toArray(value,parameterType);
            }
            else if (isList(parameterType)) {
              value = toList(value, parameterType);
            } else if (isMap(parameterType)) {
              value = toMap(value, parameterType);
            } else if (isSet(parameterType)) {
              value = toSet(value, parameterType);
            } else {
              value = jsonToObject(value.toString(), parameterType);
            }
            method.invoke(objectFromJson, new Object[] { value });
            noOfFields--;
          }
          
        }
      }
      
      if (noOfFields != 0) {
        throw new IllegalArgumentException("Not enough setter methods for fields in given JSON String : "+jsonString+" in class : "+klass);
      }
      
    } catch (InstantiationException e) {
      throw new IllegalArgumentException("Couldn't convert JSON to Object of type "+klass, e);
    } catch (IllegalAccessException e) {
      throw new IllegalArgumentException("Couldn't convert JSON to Object of type "+klass, e);
    } catch (GfJsonException e) {
      throw new IllegalArgumentException("Couldn't convert JSON to Object of type "+klass, e);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Couldn't convert JSON to Object of type "+klass, e);
    } catch (InvocationTargetException e) {
      throw new IllegalArgumentException("Couldn't convert JSON to Object of type "+klass, e);
    }
    return objectFromJson;
  }  
  
  private static Object toArray(Object value, Class<?> parameterType)
      throws GfJsonException {
    Class arrayComponentType = parameterType.getComponentType();
    if (isPrimitiveOrWrapper(arrayComponentType)) {
      if(value instanceof JSONArray){
        try {
          JSONArray jsonArray = (JSONArray) value;
          Object jArray = Array.newInstance(arrayComponentType,
              jsonArray.length());
          for (int i = 0; i < jsonArray.length(); i++) {
            Array.set(jArray, i, jsonArray.get(i));
          }
          return jArray;
        } catch (ArrayIndexOutOfBoundsException e) {
          throw new GfJsonException(e);
        } catch (IllegalArgumentException e) {
          throw new GfJsonException(e);
        } catch (JSONException e) {
          throw new GfJsonException(e);
        }
      }else {
        throw new GfJsonException(
        "Expected JSONArray for array type");
      }
    } else
      throw new GfJsonException(
          "Array contains non-primitive element. Non-primitive elements are not supported in json array");
  }

  

  /**
   * This is used in Put command this method uses HashSet as default implementation
   * @param value
   * @param parameterType 
   * @return setValue
   * @throws GfJsonException 
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  private static Object toSet(Object value, Class<?> parameterType)
      throws GfJsonException {
    try {
      JSONArray array = (JSONArray) value;
      Set set = new HashSet();
      for (int i = 0; i < array.length(); i++) {
        Object element = array.get(i);
        if (isPrimitiveOrWrapper(element.getClass())) {
          set.add(element);
        } else
          throw new GfJsonException(
              "Only primitive types are supported in set type for input commands");
      }
      return set;
    } catch (JSONException e) {
      throw new GfJsonException(e);
    }
  }

  private static Object toMap(Object value, Class<?> parameterType)
      throws GfJsonException {
    try {
      if (value instanceof JSONObject) {
        JSONObject obj = (JSONObject) value;
        Iterator iterator = obj.keys();
        Map map = new HashMap();
        while (iterator.hasNext()) {
          String key = (String) iterator.next();
          Object elem;
          elem = obj.get(key);
          if (isPrimitiveOrWrapper(elem.getClass())) {
            map.put(key, elem);
          } else
            throw new GfJsonException(
                "Only primitive types are supported in map type for input commands");
        }
        return map;
      } else
        throw new GfJsonException(
            "Expected JSONObject for Map. Retrieved type is "
                + value.getClass());
    } catch (JSONException e) {
      throw new GfJsonException(e);
    }
  }

  private static Object toList(Object value, Class<?> parameterType) throws GfJsonException {
    try {
      JSONArray array = (JSONArray) value;
      List list = new ArrayList();      
      for (int i = 0; i < array.length(); i++) {
        Object element = array.get(i);
        if(isPrimitiveOrWrapper(element.getClass())){
          list.add(element);
        }
        else throw new GfJsonException("Only primitive types are supported in set type for input commands");        
      }
      return list;
    } catch (JSONException e) {
      throw new GfJsonException(e);
    }
  }

  public static Object jsonToObject(String jsonString) {
    Object objectFromJson = null;
    try {
      GfJsonObject jsonObject = new GfJsonObject(jsonString);
      
      Iterator<String> keys = jsonObject.keys();
      
      Object[] arr = new Object[jsonObject.size()];
      int i = 0;
      
      while(keys.hasNext()) {
        String key = keys.next();
        Class<?> klass = ClassPathLoader.getLatest().forName(key);
        arr[i++] = jsonToObject((String)jsonObject.get(key).toString(), klass);
      }
      
      if (arr.length == 1) {
        objectFromJson = arr[0];
      } else {
        objectFromJson = arr;
      }
    } catch (GfJsonException e) {
      throw new IllegalArgumentException("Couldn't convert JSON to Object.", e);
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException("Couldn't convert JSON to Object.", e);
    }
    
    return objectFromJson;
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
  
  public static boolean isList(Class<?> klass){
    return klass.isAssignableFrom(List.class);      
  }
  
  public static boolean isSet(Class<?> klass){
    return klass.isAssignableFrom(Set.class);      
  }
  
  public static boolean isMap(Class<?> klass){
    return klass.isAssignableFrom(Map.class);      
  }  
  
  public static boolean isPrimitiveOrWrapper(Class<?> klass) {
    return klass.isAssignableFrom(Byte.class)
        || klass.isAssignableFrom(byte.class)
        || klass.isAssignableFrom(Short.class)
        || klass.isAssignableFrom(short.class)
        || klass.isAssignableFrom(Integer.class)
        || klass.isAssignableFrom(int.class)
        || klass.isAssignableFrom(Long.class)
        || klass.isAssignableFrom(long.class)
        || klass.isAssignableFrom(Float.class)
        || klass.isAssignableFrom(float.class)
        || klass.isAssignableFrom(Double.class)
        || klass.isAssignableFrom(double.class)
        || klass.isAssignableFrom(Boolean.class)
        || klass.isAssignableFrom(boolean.class)
        || klass.isAssignableFrom(String.class)
        || klass.isAssignableFrom(Character.class)
        || klass.isAssignableFrom(char.class);
  }
  
  public static Object getPrimitiveOrWrapperValue(Class<?> klass, Object value) throws IllegalArgumentException {
    if (klass.isAssignableFrom(Byte.class) || klass.isAssignableFrom(byte.class)) {
      return value;
    } else if (klass.isAssignableFrom(Short.class) || klass.isAssignableFrom(short.class)) {
      return value;
    } else if (klass.isAssignableFrom(Integer.class) || klass.isAssignableFrom(int.class)) {
      return value;
    } else if (klass.isAssignableFrom(Long.class) || klass.isAssignableFrom(long.class)) {
      return value;
    } else if (klass.isAssignableFrom(Float.class) || klass.isAssignableFrom(Float.class)) {
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
        else if (str.length() > 1 || str.length()==0) {
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
        else if (str.length() > 1 || str.length()==0) {
          throw new IllegalArgumentException(
              "Expected Character value but found String with length " + str.length());
        }
      } else if (value instanceof Character) {
        return ((Character) value).charValue();
      } else {
        throw new IllegalArgumentException(
            "Expected Character value but found " + value.getClass());
      }
    }else {
      return null;
    }
    return value;    
  }
  
  public static int getInt(GfJsonObject jsonObject, String byName) {
    return jsonObject.getInt(byName);
  }
  
  public static long getLong(GfJsonObject jsonObject, String byName) {
    return jsonObject.getLong(byName);
  }
  
  public static double getDouble(GfJsonObject jsonObject, String byName) {
    return jsonObject.getDouble(byName);
  }
  
  public static boolean getBoolean(GfJsonObject jsonObject, String byName) {
    return jsonObject.getBoolean(byName);
  }
  
  public static String getString(GfJsonObject jsonObject, String byName) {
    return jsonObject.getString(byName);
  }
  
  public static GfJsonObject getJSONObject(GfJsonObject jsonObject, String byName) {
    return jsonObject.getJSONObject(byName);
  }
  
  public static String[] getStringArray(GfJsonObject jsonObject, String byName) {
    String[] stringArray = null;
    try {
      GfJsonArray jsonArray = jsonObject.getJSONArray(byName);
      stringArray = GfJsonArray.toStringArray(jsonArray);
    } catch (GfJsonException e) {
      throw new ResultDataException(e.getMessage());
    }
    return stringArray;
  }
  
  public static byte[] getByteArray(GfJsonObject jsonObject, String byName) {
    byte[] byteArray = null;
    try {
      GfJsonArray jsonArray = jsonObject.getJSONArray(byName);
      byteArray = GfJsonArray.toByteArray(jsonArray);
    } catch (GfJsonException e) {
      throw new ResultDataException(e.getMessage());
    }
    return byteArray;
  }
  
  public static List<CliJsonSerializable> getList(GfJsonObject jsonObject, String byName) {
    List<CliJsonSerializable> cliJsonSerializables = Collections.emptyList();
    try {
      GfJsonArray cliJsonSerializableArray = jsonObject.getJSONArray(byName);
      int size = cliJsonSerializableArray.size();
      if (size > 0) {
        cliJsonSerializables = new ArrayList<CliJsonSerializable>();
      }
      for (int i = 0; i < size; i++) {
        GfJsonObject cliJsonSerializableState = cliJsonSerializableArray.getJSONObject(i);
        int jsId = cliJsonSerializableState.getInt(CliJsonSerializable.JSID);
        CliJsonSerializable cliJsonSerializable = CliJsonSerializableFactory.getCliJsonSerializable(jsId);
        cliJsonSerializable.fromJson(cliJsonSerializableState);
        cliJsonSerializables.add(cliJsonSerializable);
      }
    } catch (GfJsonException e) {
      throw new ResultDataException(e.getMessage());
    }
    return cliJsonSerializables;
  }
  
  public static Set<CliJsonSerializable> getSet(GfJsonObject jsonObject, String byName) {
    Set<CliJsonSerializable> cliJsonSerializables = Collections.emptySet();
    try {
      GfJsonArray cliJsonSerializableArray = jsonObject.getJSONArray(byName);
      int size = cliJsonSerializableArray.size();
      if (size > 0) {
        cliJsonSerializables = new HashSet<CliJsonSerializable>();
      }
      for (int i = 0; i < size; i++) {
        GfJsonObject cliJsonSerializableState = cliJsonSerializableArray.getJSONObject(i);
        int jsId = cliJsonSerializableState.getInt(CliJsonSerializable.JSID);
        CliJsonSerializable cliJsonSerializable = CliJsonSerializableFactory.getCliJsonSerializable(jsId);
        cliJsonSerializable.fromJson(cliJsonSerializableState);
        cliJsonSerializables.add(cliJsonSerializable);
      }
    } catch (GfJsonException e) {
      throw new ResultDataException(e.getMessage());
    }
    return cliJsonSerializables;
  }
  
  
  // For testing purpose
  public static void main(String[] args) {
    System.out.println(capitalize("key"));
    System.out.println(capitalize("Key"));
    
    String str = "{\"org.apache.geode.management.internal.cli.JsonUtil$Employee\":{\"id\":1234,\"name\":\"Foo BAR\",\"department\":{\"id\":456,\"name\":\"support\"}}}";
    Object jsonToObject = jsonToObject(str);
    System.out.println(jsonToObject);
    
    str = "{\"id\":1234,\"name\":\"Foo BAR\",\"department\":{\"id\":456,\"name\":\"support\"}}";
    Object jsonToObject2 = jsonToObject(str, Employee.class);
    System.out.println(jsonToObject2);
  }
  
  public static class Employee {
    private int id;
    private String name;
    private Department department;
    
    public int getId() {
      return id;
    }
    public void setId(int id) {
      this.id = id;
    }
    public String getName() {
      return name;
    }
    public void setName(String name) {
      this.name = name;
    }
    public Department getDepartment() {
      return department;
    }
    public void setDepartment(Department department) {
      this.department = department;
    }
    @Override
    public String toString() {
      return "Employee [id=" + id + ", name=" + name + ", department="
          + department + "]";
    }
  }
  
  public static class Department {
    private int id;
    private String name;
    
    public int getId() {
      return id;
    }
    public void setId(int id) {
      this.id = id;
    }
    public String getName() {
      return name;
    }
    public void setName(String name) {
      this.name = name;
    }
    @Override
    public String toString() {
      return "Department [id=" + id + ", name=" + name + "]";
    }
  }

}

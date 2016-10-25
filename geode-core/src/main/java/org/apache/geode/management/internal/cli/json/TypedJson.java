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
package org.apache.geode.management.internal.cli.json;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.Struct;
import org.apache.geode.cache.query.internal.StructImpl;
import org.apache.geode.pdx.PdxInstance;

/**
 * A limited functionality JSON parser. Its a DSF based JSON parser. It does not
 * create Object maps and serialize them like JSONObject. It just traverses an Object graph in
 * depth first search manner and appends key values to a String writer.
 * Hence we prevent creating a lot of garbage.
 * 
 * Although it has limited functionality,still a simple use of add() method
 * should suffice for most of the simple JSON use cases.
 * 
 * 
 */
public class TypedJson {

  /**
   * Limit of collection length to be serialized in JSON format.
   */
  public static int DEFAULT_COLLECTION_ELEMENT_LIMIT = 100;

  public static final Object NULL = GfJsonObject.NULL;

  /**
   * If Integer of Float is NAN
   */
  static final String NONFINITE = "Non-Finite";
  
  Map<Object,List<Object>> forbidden = new java.util.IdentityHashMap<Object,List<Object>>();

  boolean commanate;

  private Map<String, List<Object>> map;
  
  private int queryCollectionsDepth;

  public TypedJson(String key, Object value, int queryCollectionsDepth) {
    List<Object> list = new ArrayList<Object>();
    this.map = new LinkedHashMap<String, List<Object>>();
    if (value != null) {
      list.add(value);
    }
    this.map.put(key, list);
    this.queryCollectionsDepth = queryCollectionsDepth;
  }

  public TypedJson(int queryCollectionsDepth) {
    this.map = new LinkedHashMap<String, List<Object>>();
    this.queryCollectionsDepth = queryCollectionsDepth;
  }

  public TypedJson() {
    this.map = new LinkedHashMap<String, List<Object>>();
    this.queryCollectionsDepth = DEFAULT_COLLECTION_ELEMENT_LIMIT;
  }
  
  public TypedJson(String key, Object value) {
    List<Object> list = new ArrayList<Object>();
    this.map = new LinkedHashMap<String, List<Object>>();
    if (value != null) {
      list.add(value);
    }
    this.map.put(key, list);
    this.queryCollectionsDepth = DEFAULT_COLLECTION_ELEMENT_LIMIT;
  }
  
  void bfs(Writer w, Object root) throws IOException {
    if(root == null || isPrimitiveOrWrapper(root.getClass())){
      return;
    }
    LinkedList<Object> queue = new LinkedList<Object>();
    Map seen = new java.util.IdentityHashMap();

    seen.put(root, null);

    // Adds to end of queue
    queue.addFirst(root);

    while (!queue.isEmpty()) {

      // removes from front of queue
      Object r = queue.pollFirst();
      List<Object> childrens = getChildrens(w, r);
      // Visit child first before grand child
      for (Object n : childrens) {
        
        if(n == null){
          continue;
        }
        if (!isPrimitiveOrWrapper(n.getClass())) {
          if (!seen.containsKey(n)) {
            queue.addFirst(n);
            seen.put(n, null);
          } else {
            List<Object> list = forbidden.get(r);
            if(list != null){
              list.add(n);
              forbidden.put(r, list);
            }else{
              List<Object> newList = new ArrayList<Object>();
              newList.add(n);
              forbidden.put(r, newList);
            }
            
          }
        }

      }
    }
  }

  List<Object> getChildrens(Writer w, Object object) throws IOException {
    if (isSpecialObject(object)) {
      return this.visitSpecialObjects(w, object, false);
    } else {
      return this.visitChildrens(w, object, false);
    }
  }
 

  /**
   * 
   * User can build on this object by adding Objects against a key.
   * 
   * TypedJson result = new TypedJson(); result.add(KEY,object); If users add
   * more objects against the same key the newly added object will be appended
   * to the existing key forming an array of objects.
   * 
   * If the KEY is a new one then it will be a key map value.
   * 
   * @param key
   *          Key against which an object will be added
   * @param value
   *          Object to be added
   * @return TypedJson object
   */
  public TypedJson add(String key, Object value) {
    List<Object> list = this.map.get(key);
    if (list != null) {
      list.add(value);
    } else {
      list = new ArrayList<Object>();
      list.add(value);
      this.map.put(key, list);
    }
    return this;
  }

  public String toString() {
    StringWriter w = new StringWriter();
    synchronized (w.getBuffer()) {
      try {
        return this.write(w).toString();
      } catch (Exception e) {
        return null;
      }
    }
  }

  public int length() {
    return this.map.size();
  }

  Writer write(Writer writer) throws GfJsonException {
    try {
      boolean addComma = false;
      final int length = this.length();
      Iterator<String> keys = map.keySet().iterator();
      writer.write('{');

      if (length == 1) {
        Object key = keys.next();
        writer.write(quote(key.toString()));
        writer.write(':');

        writeList(writer, this.map.get(key));
      } else if (length != 0) {
        while (keys.hasNext()) {
          Object key = keys.next();
          if (addComma) {
            writer.write(',');
          }
          writer.write(quote(key.toString()));
          writer.write(':');

          writeList(writer, this.map.get(key));
          commanate = false;
          addComma = true;
        }

      }

      writer.write('}');

      return writer;
    } catch (IOException exception) {
      throw new GfJsonException(exception);
    }
  }

  Writer writeList(Writer writer, List<Object> myArrayList) throws GfJsonException {
    try {
      boolean addComma = false;
      int length = myArrayList.size();

      if (length == 0) {
        writer.write(']');
        writeValue(writer, null);
        writer.write(']');
      }
      if (length == 1) {
        writer.write('[');
        writeValue(writer, myArrayList.get(0));
        writer.write(']');

      } else if (length != 0) {
        writer.write('[');
        for (int i = 0; i < length; i += 1) {
          if (addComma) {
            writer.write(',');
          }
          writeValue(writer, myArrayList.get(i));
          commanate = false;
          addComma = true;
        }
        writer.write(']');
      }

      return writer;
    } catch (IOException e) {
      throw new GfJsonException(e);
    }
  }

  static String quote(String string) {
    StringWriter sw = new StringWriter();
    synchronized (sw.getBuffer()) {
      try {
        return quote(string, sw).toString();
      } catch (IOException ignored) {
        // will never happen - we are writing to a string writer
        return "";
      }
    }
  }

  static boolean shouldVisitChildren(Object object) {
    Class type = object.getClass();
    if (isPrimitiveOrWrapper(type)) {
      return false;
    }
    if (isSpecialObject(object)) {
      return false;
    }
    return true;
  }

  static boolean isPrimitiveOrWrapper(Class<?> klass) {
    return klass.isAssignableFrom(Byte.class) || klass.isAssignableFrom(byte.class)
        || klass.isAssignableFrom(Short.class) || klass.isAssignableFrom(short.class)
        || klass.isAssignableFrom(Integer.class) || klass.isAssignableFrom(int.class)
        || klass.isAssignableFrom(Long.class) || klass.isAssignableFrom(long.class)
        || klass.isAssignableFrom(Float.class) || klass.isAssignableFrom(float.class)
        || klass.isAssignableFrom(Double.class) || klass.isAssignableFrom(double.class)
        || klass.isAssignableFrom(Boolean.class) || klass.isAssignableFrom(boolean.class)
        || klass.isAssignableFrom(String.class) || klass.isAssignableFrom(char.class)
        || klass.isAssignableFrom(Character.class) || klass.isAssignableFrom(java.sql.Date.class)
        || klass.isAssignableFrom(java.util.Date.class) || klass.isAssignableFrom(java.math.BigDecimal.class);
  }

  static boolean isSpecialObject(Object object) {
    Class type = object.getClass();
    if (type.isArray() || type.isEnum()) {
      return true;
    }
    if ((object instanceof Collection) || (object instanceof Map) || (object instanceof PdxInstance)
        || (object instanceof Struct) || (object instanceof Region.Entry) ){
      return true;
    }
    return false;
  }

  final void writeVal(Writer w, Object value) throws IOException {
    w.write('{');
    addVal(w, value);
    w.write('}');
  }

  void addVal(Writer w, Object object) {
    if (object == null) {
      return;
    }
    if (shouldVisitChildren(object)) {
      visitChildrens(w, object, true);
    }

  }

  void writeKeyValue(Writer w, Object key, Object value, Class type) throws IOException {
    if (commanate) {
      w.write(",");
    }

    if (value == null || value.equals(null)) {
      w.write(quote(key.toString()));
      w.write(':');
      w.write("null");
      commanate = true;
      return;
    }
    Class clazz = value.getClass();
    w.write(quote(key.toString()));
    w.write(':');

    if (type != null) {
      writeType(w, type, value);
    }

    if (isPrimitiveOrWrapper(clazz)) {      
      writePrimitives(w, value);
      commanate = true;
    } else if (isSpecialObject(value)) {
      commanate = false;
      visitSpecialObjects(w, value, true);
      commanate = true;
    } else {
      commanate = false;
      writeVal(w, value);
      commanate = true;
    }
    endType(w, clazz);
    return;
  }

  void writePrimitives(Writer w, Object value) throws IOException {
    if (value instanceof Number) {
      w.write(numberToString((Number) value));
      return;
    }

    if (value instanceof String || value instanceof Character || value instanceof java.sql.Date
        || value instanceof java.util.Date) {
      w.write(quote(value.toString()));
      return;
    }
    w.write(value.toString());
  }

  void writeArray(Writer w, Object object) throws IOException {

    if (commanate) {
      w.write(",");
    }
    w.write('[');
    int length = Array.getLength(object);
    int elements = 0;
    for (int i = 0; i < length && elements < queryCollectionsDepth; i += 1) {
      Object item = Array.get(object, i);
      if (i != 0) {
        w.write(",");
      }
      if(item != null){
        Class clazz = item.getClass();

        if (isPrimitiveOrWrapper(clazz)) {
          writePrimitives(w, item);
        } else if (isSpecialObject(item)) {
          visitSpecialObjects(w, item, true);
        } else {
          writeVal(w, item);
        }
      }else{
        w.write("null");
      }
      elements++;
      commanate = false;
    }
    w.write(']');
    commanate = true;
    return;
  }
  
  List<Object> getArrayChildren(Object object){
    List<Object> items = new ArrayList<Object>();
    int length = Array.getLength(object);
    int elements = 0;
    for (int i = 0; i < length && elements < queryCollectionsDepth; i += 1) {
      Object item = Array.get(object, i);
      items.add(item);
      
    }
    return items;
  }

  void writeEnum(Writer w, Object object) throws IOException {
    if (commanate) {
      w.write(",");
    }
    w.write(quote(object.toString()));
    commanate = true;
    return;
  }

  void writeTypedJson(Writer w, TypedJson object) throws IOException {

    if (commanate) {
      w.write(",");
    }
    w.write(quote(object.toString()));
    commanate = true;
    return;
  }

  void writeValue(Writer w, Object value) {
    try {
      if (value == null || value.equals(null)) {
        w.write("null");
        return;
      }
      this.bfs(w, value);
      Class rootClazz = value.getClass();
      writeType(w, rootClazz, value);

      if (isPrimitiveOrWrapper(rootClazz)) {
        writePrimitives(w, value);
      } else if (isSpecialObject(value)) {
        visitSpecialObjects(w, value, true);
      } else {
        writeVal(w, value);
      }
      endType(w, rootClazz);
    } catch (IOException e) {
    }

  }

  void startKey(Writer writer, String key) throws IOException {
    if (key != null) {
      writer.write('{');
      writer.write(quote(key.toString()));
      writer.write(':');
    }
  }

  void endKey(Writer writer, String key) throws IOException {
    if (key != null) {
      writer.write('}');
    }

  }

  List<Object> visitSpecialObjects(Writer w, Object object, boolean write) throws IOException {
    
    List<Object> elements = new ArrayList<Object>();
    
    Class clazz = object.getClass();

    if (clazz.isArray()) {
      if (write) {
        writeArray(w, object);
      } else {
        return getArrayChildren(object);
      }
    }
    
    if (clazz.isEnum()) {
      if (write) {
        writeEnum(w, object);
      }else{
        elements.add(object);
      }      
      return elements;
    }

    if (object instanceof TypedJson) {
      this.writeTypedJson(w, (TypedJson) object);
      return elements;
    }

    if (object instanceof Collection) {
      Collection collection = (Collection) object;
      Iterator iter = collection.iterator();
      int i = 0;
      if(write)w.write('{');
      while (iter.hasNext() && i < queryCollectionsDepth) {
        Object item = iter.next();
        if(write){
          writeKeyValue(w, i, item, item !=null ? item.getClass() : null);
        }else{
          elements.add(item);
        }
        
        i++;
      }
      if(write)w.write('}');
      return elements;
    }

    if (object instanceof Map) {
      Map map = (Map) object;
      Iterator it = map.entrySet().iterator();
      int i = 0;
      if(write)w.write('{');
      while (it.hasNext() && i < queryCollectionsDepth) {
        Map.Entry e = (Map.Entry) it.next();
        Object value = e.getValue();
        if(write){
          writeKeyValue(w, e.getKey(), value, value !=null ? value.getClass(): null);
        }else{
          elements.add(value);
        }
      
        i++;
      }
      if(write)w.write('}');
      return elements;
    }

    if (object instanceof PdxInstance) {
      PdxInstance pdxInstance = (PdxInstance) object;
      if(write)w.write('{');
      for (String field : pdxInstance.getFieldNames()) {
        Object fieldValue = pdxInstance.getField(field);
        if(write){
          writeKeyValue(w, field, fieldValue, fieldValue !=null ? fieldValue.getClass() : null);
        }else{
          elements.add(fieldValue);
        }
        

      }
      if(write)w.write('}');
      return elements;
    }

    if (object instanceof Struct) {
      StructImpl impl = (StructImpl) object;
      String fields[] = impl.getFieldNames();
      Object[] values = impl.getFieldValues();

      if(write)w.write('{');
      for (int i = 0; i < fields.length; i++) {
        Object fieldValue = values[i];
        if(write){
          writeKeyValue(w, fields[i], fieldValue, fieldValue !=null ? fieldValue.getClass() : null);
        }else{
          elements.add(fieldValue);
        }
       
      }
      if(write)w.write('}');
      return elements;
    }
    

    if (object instanceof Region.Entry) {
      Region.Entry entry = (Region.Entry) object;
      Object key = entry.getKey();
      Object value = entry.getValue();

     
      if(write){
        w.write('{');
        writeKeyValue(w, key, value, value !=null ? value.getClass() : null);
        w.write('}');
      }else{
        elements.add(value);
      }
     
     
      return elements;
    }
    
    
    return elements;
  }

  void writeType(Writer w, Class clazz, Object value) throws IOException {
    if (clazz != TypedJson.class) {
      w.write('[');
      w.write(quote(internalToExternal(clazz, value)));
      w.write(",");
    }
  }
  /**
   * Handle some special GemFire classes. We don't want to expose some of the internal classes.
   * Hence corresponding interface or external classes should be shown.
   * 
   */
  String internalToExternal(Class clazz, Object value){
    if(value != null && value instanceof Region.Entry){
      return Region.Entry.class.getCanonicalName();
    }
    if(value != null && value instanceof PdxInstance){
      return PdxInstance.class.getCanonicalName();
    }
    return clazz.getCanonicalName();
  }

  void endType(Writer w, Class clazz) throws IOException {
    if (clazz != TypedJson.class) {
      w.write(']');
    }

  }

  List<Object> visitChildrens(Writer w, Object object, boolean write) {
    
    List<Object> elements = new ArrayList<Object>();
 
    Method[] methods = getMethods(object);
    
    for (int i = 0; i < methods.length; i += 1) {
      try {
        Method method = methods[i];
        if (Modifier.isPublic(method.getModifiers()) && !Modifier.isStatic(method.getModifiers())) {
          String name = method.getName();
          String key = "";
          if (name.startsWith("get")) {
            if ("getClass".equals(name) || "getDeclaringClass".equals(name)) {
              key = "";
            } else {
              key = name.substring(3);
            }
          } else if (name.startsWith("is")) {
            key = name.substring(2);
          }
          if (key.length() > 0 && Character.isUpperCase(key.charAt(0)) && method.getParameterTypes().length == 0) {
            if (key.length() == 1) {
              key = key.toLowerCase();
            } else if (!Character.isUpperCase(key.charAt(1))) {
              key = key.substring(0, 1).toLowerCase() + key.substring(1);
            }
            method.setAccessible(true);
            Object result = method.invoke(object, (Object[]) null);
            if(write){
              List<Object> forbiddenList = forbidden.get(object);
              if(forbiddenList != null && forbiddenList.contains(result)){
                writeKeyValue(w, key, result.getClass().getCanonicalName(), method.getReturnType());
              }else{
                writeKeyValue(w, key, result, method.getReturnType());
              }
            }else{
              elements.add(result);
            }

            
          }
        }
      } catch (Exception ignore) {
      }
    }
    return elements;
  }
  
  
  /**
   * This method returns method declared in a Class as well as all the super classes in the hierarchy.
   * If class is a system class it wont include super class methods 
   */
  Method[] getMethods(Object object){
    Class klass = object.getClass();

    // If klass is a System class then set includeSuperClass to false.

    boolean includeSuperClass = klass.getClassLoader() != null;

    Method[] decMethods = klass.getDeclaredMethods();
    Map<String, Method> decMethodMap = new HashMap<String,Method>();
    for(Method method : decMethods){
      decMethodMap.put(method.getName(), method);
    }
    
    if(includeSuperClass){
      Method[] allMethods = klass.getMethods();
      List<Method> allMethodList = Arrays.asList(allMethods);
      for(Method method : allMethodList){
        if(decMethodMap.get(method.getName()) != null){
          //skip. This will ensure overriden methods wont be added again.
        }else{
          decMethodMap.put(method.getName(), method);
        }
      }
    }
    
    Method[] methodArr = new Method[decMethodMap.size()];
    return decMethodMap.values().toArray(methodArr);
  }


  /**
   * Produce a string from a Number.
   * 
   * @param number
   *          A Number
   * @return A String.
   */
  public static String numberToString(Number number) {
    if (number == null) {
      return "";
    }
    if (number != null) {
      if (number instanceof Double) {
        if (((Double) number).isInfinite() || ((Double) number).isNaN()) {
          return "Non-Finite";
        }
      } else if (number instanceof Float) {
        if (((Float) number).isInfinite() || ((Float) number).isNaN()) {
          return "Non-Finite";
        }
      }
    }

    // Shave off trailing zeros and decimal point, if possible.

    String string = number.toString();
    if (string.indexOf('.') > 0 && string.indexOf('e') < 0 && string.indexOf('E') < 0) {
      while (string.endsWith("0")) {
        string = string.substring(0, string.length() - 1);
      }
      if (string.endsWith(".")) {
        string = string.substring(0, string.length() - 1);
      }
    }
    return string;
  }

  public static Writer quote(String string, Writer w) throws IOException {
    if (string == null || string.length() == 0) {
      w.write("\"\"");
      return w;
    }

    char b;
    char c = 0;
    String hhhh;
    int i;
    int len = string.length();

    w.write('"');
    for (i = 0; i < len; i += 1) {
      b = c;
      c = string.charAt(i);
      switch (c) {
      case '\\':
      case '"':
        w.write('\\');
        w.write(c);
        break;
      case '/':
        if (b == '<') {
          w.write('\\');
        }
        w.write(c);
        break;
      case '\b':
        w.write("\\b");
        break;
      case '\t':
        w.write("\\t");
        break;
      case '\n':
        w.write("\\n");
        break;
      case '\f':
        w.write("\\f");
        break;
      case '\r':
        w.write("\\r");
        break;
      default:
        if (c < ' ' || (c >= '\u0080' && c < '\u00a0') || (c >= '\u2000' && c < '\u2100')) {
          hhhh = "000" + Integer.toHexString(c);
          w.write("\\u" + hhhh.substring(hhhh.length() - 4));
        } else {
          w.write(c);
        }
      }
    }
    w.write('"');
    return w;
  }
}

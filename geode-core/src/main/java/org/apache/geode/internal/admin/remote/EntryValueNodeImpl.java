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

package org.apache.geode.internal.admin.remote;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;

import org.apache.geode.InternalGemFireException;
import org.apache.geode.internal.admin.EntryValueNode;

/**
 * This class holds the metadata for a single object field in a value stored in the cache. They are
 * built during construction of {@link ObjectDetailsResponse} instances and returned to the console.
 * This class does not implement {@link org.apache.geode.DataSerializable} since that mechanism gets
 * confused by the often cyclical refrences between instances of this class.
 */
public class EntryValueNodeImpl implements EntryValueNode, Externalizable/* , DataSerializable */ {

  private Object primitiveVal;
  private String type;
  private String name;
  private boolean primitive;
  private EntryValueNodeImpl[] fields;
  private static ThreadLocal recursionSet = new ThreadLocal();

  public static EntryValueNodeImpl createFromValueRoot(Object value, boolean logicalInspection) {
    recursionSet.set(new IdentityHashMap());
    EntryValueNodeImpl retVal = null;
    if (value != null) {
      retVal = createFromObject(constructKeyDisplay(value), value, logicalInspection);
    }
    Map map = (Map) recursionSet.get();
    map.clear();
    recursionSet.set(null);

    return retVal;
  }

  private static EntryValueNodeImpl createFromPrimitive(String fieldName, String type,
      Object primitiveWrapper) {
    EntryValueNodeImpl node = new EntryValueNodeImpl();
    node.name = fieldName;
    node.type = type;
    node.primitiveVal = primitiveWrapper;
    node.primitive = true;
    return node;
  }

  private static EntryValueNodeImpl createFromNullField(String fieldName, Class fieldType) {
    EntryValueNodeImpl node = new EntryValueNodeImpl();
    node.name = fieldName;
    if (fieldType.isArray()) {
      node.type = "array of" + fieldType.getComponentType().getName();
    } else {
      node.type = fieldType.getName();
    }
    node.primitiveVal = "null";
    node.primitive = true;
    return node;
  }

  private static EntryValueNodeImpl createFromArray(String fieldName, Object arrayObj,
      Class arrayClass) {
    EntryValueNodeImpl node = new EntryValueNodeImpl();
    Map map = (Map) recursionSet.get();
    map.put(arrayObj, node);

    node.name = fieldName;
    Class compType = arrayClass.getComponentType();
    String elType = compType.getName();
    node.type = "array of " + elType;
    node.primitiveVal = arrayObj.toString();
    node.primitive = false;
    // if (arrayObj != null) (cannot be null)
    {
      EntryValueNodeImpl[] children;
      if (arrayObj instanceof Object[]) {
        Object[] array = (Object[]) arrayObj;
        children = new EntryValueNodeImpl[array.length];
        for (int i = 0; i < array.length; i++) {
          if (array[i] != null) {
            children[i] = createFromObject("[" + i + "]", array[i], false);
          } else {
            children[i] = createFromNullField("[" + i + "]", compType);
          }
        }
        node.fields = children;
      } else if (arrayObj instanceof int[]) {
        int[] array = (int[]) arrayObj;
        children = new EntryValueNodeImpl[array.length];
        for (int i = 0; i < array.length; i++) {
          children[i] = createFromPrimitive("[" + i + "]", elType, Integer.valueOf(array[i]));
        }
        node.fields = children;
      } else if (arrayObj instanceof boolean[]) {
        boolean[] array = (boolean[]) arrayObj;
        children = new EntryValueNodeImpl[array.length];
        for (int i = 0; i < array.length; i++) {
          children[i] = createFromPrimitive("[" + i + "]", elType, Boolean.valueOf(array[i]));
        }
        node.fields = children;
      } else if (arrayObj instanceof char[]) {
        char[] array = (char[]) arrayObj;
        children = new EntryValueNodeImpl[array.length];
        for (int i = 0; i < array.length; i++) {
          children[i] = createFromPrimitive("[" + i + "]", elType, new Character(array[i]));
        }
        node.fields = children;
      } else if (arrayObj instanceof double[]) {
        double[] array = (double[]) arrayObj;
        children = new EntryValueNodeImpl[array.length];
        for (int i = 0; i < array.length; i++) {
          children[i] = createFromPrimitive("[" + i + "]", elType, Double.valueOf(array[i]));
        }
        node.fields = children;
      } else if (arrayObj instanceof long[]) {
        long[] array = (long[]) arrayObj;
        children = new EntryValueNodeImpl[array.length];
        for (int i = 0; i < array.length; i++) {
          children[i] = createFromPrimitive("[" + i + "]", elType, Long.valueOf(array[i]));
        }
        node.fields = children;
      } else if (arrayObj instanceof float[]) {
        float[] array = (float[]) arrayObj;
        children = new EntryValueNodeImpl[array.length];
        for (int i = 0; i < array.length; i++) {
          children[i] = createFromPrimitive("[" + i + "]", elType, new Float(array[i]));
        }
        node.fields = children;
      } else if (arrayObj instanceof byte[]) {
        byte[] array = (byte[]) arrayObj;
        children = new EntryValueNodeImpl[array.length];
        for (int i = 0; i < array.length; i++) {
          children[i] = createFromPrimitive("[" + i + "]", elType, new Byte(array[i]));
        }
        node.fields = children;
      } else if (arrayObj instanceof short[]) {
        short[] array = (short[]) arrayObj;
        children = new EntryValueNodeImpl[array.length];
        for (int i = 0; i < array.length; i++) {
          children[i] = createFromPrimitive("[" + i + "]", elType, new Short(array[i]));
        }
        node.fields = children;
      }
    }
    return node;
  }

  private static EntryValueNodeImpl createFromObject(String fieldName, Object obj,
      boolean logicalInspection) {
    Map map = (Map) recursionSet.get();
    EntryValueNodeImpl stored = (EntryValueNodeImpl) map.get(obj);
    if (stored != null) {
      return stored;
    }
    Class clazz = obj.getClass();
    if (clazz.isArray()) {
      return createFromArray(fieldName, obj, clazz);
    }

    EntryValueNodeImpl node = new EntryValueNodeImpl();
    map.put(obj, node);

    node.name = fieldName;
    node.type = clazz.getName();
    if (isWrapperOrString(obj)) {
      node.primitiveVal = obj;
      node.primitive = true;
      return node;
    } else {
      node.primitiveVal = obj.toString();
      node.primitive = false;
    }

    if (logicalInspection && hasLogicalView(obj)) {
      int retryCount = 0;
      boolean retry;
      List elements = new ArrayList();
      do {

        // if (cancelled) { return; }

        retry = false;
        try {
          if (obj instanceof Map) {
            Map theMap = (Map) obj;
            Set entries = theMap.entrySet();
            if (entries != null) {
              Iterator it = entries.iterator();
              while (it.hasNext()) {
                // if (cancelled) { return; }
                Map.Entry entry = (Map.Entry) it.next();
                Object key = entry.getKey();
                Object value = entry.getValue();
                if (key != null) {
                  elements.add(
                      createFromObject("key->" + constructKeyDisplay(key), key, logicalInspection));
                } else {
                  elements
                      .add(createFromNullField("key->" + constructKeyDisplay(key), Object.class));
                }
                if (value != null) {
                  elements.add(createFromObject("value->" + constructKeyDisplay(value), value,
                      logicalInspection));
                } else {
                  elements.add(
                      createFromNullField("value->" + constructKeyDisplay(value), Object.class));
                }
              }
            }
          } else if (obj instanceof List) {
            java.util.List list = (List) obj;
            ListIterator it = list.listIterator();
            while (it.hasNext()) {
              // if (cancelled) { return; }
              Object element = it.next();
              elements
                  .add(createFromObject(constructKeyDisplay(element), element, logicalInspection));
            }
          } else if (obj instanceof Collection) {
            Collection coll = (Collection) obj;
            Iterator it = coll.iterator();
            while (it.hasNext()) {
              // if (cancelled) { return; }
              Object element = it.next();
              elements
                  .add(createFromObject(constructKeyDisplay(element), element, logicalInspection));
            }
          }
        } catch (ConcurrentModificationException ex) {
          elements = new ArrayList();
          retryCount++;
          if (retryCount <= 5) {
            retry = true;
          }
        }
      } while (retry);
      // if (cancelled) { return; }

      node.fields = (EntryValueNodeImpl[]) elements.toArray(new EntryValueNodeImpl[0]);

    } else { // physical inspection

      Field[] fields = clazz.getDeclaredFields();
      try {
        AccessibleObject.setAccessible(fields, true);
      } catch (SecurityException se) {
        throw new InternalGemFireException(
            "Unable to set accessibility of Field objects during cache value display construction",
            se);
      }
      List fieldList = new ArrayList();
      for (int i = 0; i < fields.length; i++) {
        int mods = fields[i].getModifiers();
        if ((mods & Modifier.STATIC) != 0) {
          continue;
        }
        Object fieldVal = null;
        try {
          fieldVal = fields[i].get(obj);
        } catch (Exception e) {
          throw new InternalGemFireException(
              "Unable to build cache value display",
              e);
        }
        String name = fields[i].getName();
        if (fieldVal == null) {
          fieldList.add(createFromNullField(name, fields[i].getType()));

        } else if (isWrapperOrString(fieldVal)) {
          fieldList.add(createFromPrimitive(name, fields[i].getType().getName(), fieldVal));
        } else {
          fieldList.add(createFromObject(name, fieldVal, logicalInspection));
        }
      }
      node.fields = (EntryValueNodeImpl[]) fieldList.toArray(new EntryValueNodeImpl[0]);
    }
    return node;
  }

  private static boolean isWrapperOrString(Object test) {
    return (test instanceof Number || test instanceof String || test instanceof Boolean
        || test instanceof Character);

  }

  private static boolean hasLogicalView(Object obj) {
    return (obj instanceof Map || obj instanceof List || obj instanceof Collection);
  }

  public boolean isPrimitiveOrString() {
    return primitive;
  }

  public String getName() {
    return name;
  }

  public String getType() {
    return type;
  }

  public EntryValueNode[] getChildren() {
    if (fields != null) {
      return fields;
    } else {
      return new EntryValueNodeImpl[0];
    }
  }

  public Object getPrimitiveValue() {
    return primitiveVal;
  }

  public static String constructKeyDisplay(Object toDisplay) {
    if (toDisplay == null)
      return "null";
    else if (toDisplay instanceof String)
      return (String) toDisplay;
    else if (toDisplay instanceof Number)
      return toDisplay.toString();
    else if (toDisplay instanceof Character)
      return toDisplay.toString();
    else if (toDisplay instanceof Boolean)
      return toDisplay.toString();
    else {
      String className = toDisplay.getClass().getName();
      className = className.substring(className.lastIndexOf(".") + 1);
      char c = className.charAt(0);
      c = Character.toLowerCase(c);
      if (c == 'a' || c == 'e' || c == 'i' || c == 'o' || c == 'u') {
        return "an " + className;
      } else {
        return "a " + className;
      }
    }
  }

  public void writeExternal(ObjectOutput out) throws IOException {
    out.writeObject(primitiveVal);
    out.writeObject(type);
    out.writeObject(name);
    out.writeObject(fields);
    out.writeBoolean(primitive);
  }

  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    this.primitiveVal = in.readObject();
    this.type = (String) in.readObject();
    this.name = (String) in.readObject();
    this.fields = (EntryValueNodeImpl[]) in.readObject();
    this.primitive = in.readBoolean();
  }

  // public void toData(DataOutput out) throws IOException {
  // Helper.writeObject(primitive, out);
  // Helper.writeString(type, out);
  // Helper.writeString(name, out);
  // Helper.writeObject(fields, out);
  // }

  // public void fromData(DataInput in) throws IOException, ClassNotFoundException {
  // this.primitive = Helper.readObject(in);
  // this.type = Helper.readString(in);
  // this.name = Helper.readString(in);
  // this.fields = (EntryValueNodeImpl[])Helper.readObject(in);
  // }

}

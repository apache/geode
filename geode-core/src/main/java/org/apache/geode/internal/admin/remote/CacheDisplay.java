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


import org.apache.geode.internal.admin.GemFireVM;

public class CacheDisplay {
  public static Object getCachedObjectDisplay(Object obj, int inspectionType) {
    switch (inspectionType) {
      case GemFireVM.LIGHTWEIGHT_CACHE_VALUE:
        if (obj == null) {
          return "null";
        }
        String toString = obj.toString();
        Class clazz = obj.getClass();
        String name = null;
        if (clazz.isArray()) {
          return getArrayDisplayName(obj);
        } else {
          name = getClassName(clazz);
        }
        // if (toString.indexOf('@') >= 0) { //probably Object.toString()
        // return "a " + name;
        // } else {
        return name + " \"" + toString + "\"";
      // }
      case GemFireVM.PHYSICAL_CACHE_VALUE:
        Object physicalVal = EntryValueNodeImpl.createFromValueRoot(obj, false);
        return (physicalVal == null) ? "null" : physicalVal;
      case GemFireVM.LOGICAL_CACHE_VALUE:
        Object logicalVal = EntryValueNodeImpl.createFromValueRoot(obj, true);
        return (logicalVal == null) ? "null" : logicalVal;
      default:
        throw new IllegalArgumentException(
            "Invalid inspectionType passed to CacheDisplay.getCachedObjectDisplay");
    }
  }

  private static String getArrayDisplayName(Object instance) {
    if (instance instanceof Object[]) {
      String className = instance.getClass().getName();
      return "an array of " + getClassName(className.substring(2, className.length() - 1))
          + " with " + ((Object[]) instance).length + " elements";
    } else if (instance instanceof int[]) {
      return "an array of int with " + ((int[]) instance).length + " elements";
    } else if (instance instanceof double[]) {
      return "an array of double with " + ((double[]) instance).length + " elements";
    } else if (instance instanceof char[]) {
      return "an array of char with " + ((char[]) instance).length + " elements";
    } else if (instance instanceof byte[]) {
      return "an array of byte with " + ((byte[]) instance).length + " elements";
    } else if (instance instanceof boolean[]) {
      return "an array of boolean with " + ((boolean[]) instance).length + " elements";
    } else if (instance instanceof long[]) {
      return "an array of long with " + ((long[]) instance).length + " elements";
    } else if (instance instanceof float[]) {
      return "an array of float with " + ((float[]) instance).length + " elements";
    } else if (instance instanceof short[]) {
      return "an array of short with " + ((short[]) instance).length + " elements";
    } else
      return null;
  }

  private static String getClassName(Class clazz) {
    return getClassName(clazz.getName());
  }

  private static String getClassName(String name) {
    return (name.length() > 64) ? name.substring(name.lastIndexOf(".") + 1) : name;
  }
}

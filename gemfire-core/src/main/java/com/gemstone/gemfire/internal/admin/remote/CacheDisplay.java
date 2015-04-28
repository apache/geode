/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
   
   
package com.gemstone.gemfire.internal.admin.remote;


import com.gemstone.gemfire.internal.admin.GemFireVM;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

public final class CacheDisplay {
  public static Object getCachedObjectDisplay(Object obj, int inspectionType) {
    switch(inspectionType) {
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
      //if (toString.indexOf('@') >= 0) { //probably Object.toString()
      //  return "a " + name;
      //} else {
      return name + " \"" + toString + "\"";
      //}
    case GemFireVM.PHYSICAL_CACHE_VALUE:
      Object physicalVal = EntryValueNodeImpl.createFromValueRoot(obj, false);
      return (physicalVal == null) ? "null" :  physicalVal;
    case GemFireVM.LOGICAL_CACHE_VALUE:
      Object logicalVal = EntryValueNodeImpl.createFromValueRoot(obj, true);
      return (logicalVal == null) ? "null" : logicalVal;
    default:
      throw new IllegalArgumentException(LocalizedStrings.CacheDisplay_INVALID_INSPECTIONTYPE_PASSED_TO_CACHEDISPLAYGETCACHEDOBJECTDISPLAY.toLocalizedString());
    }
  }

  private static String getArrayDisplayName(Object instance) {
    if (instance instanceof Object[]) {
      String className = instance.getClass().getName();
      return "an array of " + getClassName(className.substring(2, className.length()-1)) + " with " + ((Object[])instance).length + " elements";
    } else if (instance instanceof int[]) {
      return "an array of int with " + ((int[])instance).length + " elements";
    } else if (instance instanceof double[]) {
      return "an array of double with " + ((double[])instance).length + " elements";
    } else if (instance instanceof char[]) {
      return "an array of char with " + ((char[])instance).length + " elements";
    } else if (instance instanceof byte[]) {
      return "an array of byte with " + ((byte[])instance).length + " elements";
    } else if (instance instanceof boolean[]) {
      return "an array of boolean with " + ((boolean[])instance).length + " elements";
    } else if (instance instanceof long[]) {
      return "an array of long with " + ((long[])instance).length + " elements";
    } else if (instance instanceof float[]) {
      return "an array of float with " + ((float[])instance).length + " elements";
    } else if (instance instanceof short[]) {
      return "an array of short with " + ((short[])instance).length + " elements";
    } else return null;
  }

  private static String getClassName(Class clazz) {
    return getClassName(clazz.getName());
  }

  private static String getClassName(String name) {
    return (name.length() > 64) ? name.substring(name.lastIndexOf(".")+1) : name;
  }
}

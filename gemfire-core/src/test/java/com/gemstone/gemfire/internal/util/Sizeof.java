/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.util;
/**
 * This class provides static methods for computing the size of an object.
 * @author Sudhir Menon
 *
 */

import java.lang.reflect.*;

import com.gemstone.gemfire.cache.util.ObjectSizer;

/**
 * @deprecated Use {@link ObjectSizer#DEFAULT} instead. That class should be more
 * accurate, but I didn't want to replace all uses of this thing yet because I'm
 * afraid of breaking tests which depend on the behavior of this class.
 *
 */
public class Sizeof {

  private static final int SZ_REF = 4;

  public static int sizeof(boolean b) {
    return 1;
  }

  public static int sizeof(byte b) {
    return 1;
  }

  public static int sizeof(char c) {
    return 2;
  }

  public static int sizeof(short s) {
    return 2;
  }

  public static int sizeof(int i) {
    return 4;
  }

  public static int sizeof(long l) {
    return 8;
  }

  public static int sizeof(float f) {
    return 4;
  }

  public static int sizeof(double d) {
    return 8;
  }

  public static int size_inst(Class c) {
    Field flds[] = c.getDeclaredFields();
    int sz = 0;

    for (int i = 0; i < flds.length; i++) {
      Field f = flds[i];
      if (!c.isInterface() &&
          (f.getModifiers() & Modifier.STATIC) != 0) {
        continue;
      }
      sz += size_prim(f.getType());
    }

    if (c.getSuperclass() != null)
      sz += size_inst(c.getSuperclass());

    Class cv[] = c.getInterfaces();
    for (int i = 0; i < cv.length; i++)
      sz += size_inst(cv[i]);
    return sz;
  }

  private static int size_prim(Class t) {
    if (t == Boolean.TYPE)
      return 1;
    else if (t == Byte.TYPE)
      return 1;
    else if (t == Character.TYPE)
      return 2;
    else if (t == Short.TYPE)
      return 2;
    else if (t == Integer.TYPE)
      return 4;
    else if (t == Long.TYPE)
      return 8;
    else if (t == Float.TYPE)
      return 4;
    else if (t == Double.TYPE)
      return 8;
    else if (t == Void.TYPE)
      return 0;
    else
      return SZ_REF;
  }

  private static int size_arr(Object obj, Class c) {
    Class ct = c.getComponentType();
    int len = Array.getLength(obj);

    if (ct.isPrimitive()) {
      return len * size_prim(ct);
    }
    else {
      int sz = 0;
      for (int i = 0; i < len; i++) {
        sz += SZ_REF;
        Object obj2 = Array.get(obj, i);
        if (obj2 == null)
                continue;
        Class c2 = obj2.getClass();
        if (!c2.isArray())
                continue;
        sz += size_arr(obj2, c2);
      }
      return sz;
    }
  }

  public static int sizeof(Object obj) {
    if (obj == null)
      return 0;
    Class c = obj.getClass();
    if (c.isArray())
      return size_arr(obj, c);
    else
      return size_inst(c);
  }
  
  public static int sizeof(String str) {
    if (str == null)
      return 0;
    return (str.length() * 2);
  }
  
  public static int sizeof(byte[] ba) {
    if (ba == null)
      return 0;
    return ba.length;
  }
  
  public static int sizeof(int[] ia) {
    if (ia == null)
      return 0;
    return ia.length * 4;
  }
  
  public static int sizeof(long[] la) {
    if (la == null)
      return 0;
    return la.length * 8;
  }
  
}

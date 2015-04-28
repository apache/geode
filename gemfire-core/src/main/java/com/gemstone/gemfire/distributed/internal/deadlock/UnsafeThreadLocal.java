/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.distributed.internal.deadlock;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Most of this thread local is safe to use, except for the getValue(Thread)
 * method. That is not guaranteed to be correct. But for our deadlock detection
 * tool I think it's good enough, and this class provides a very low overhead
 * way for us to record what thread holds a particular resource.
 * 
 * @author dsmith
 * 
 */
public class UnsafeThreadLocal<T> extends ThreadLocal<T> {
  /**
   * Dangerous method. Uses reflection to extract the thread local for a given
   * thread.
   * 
   * Unlike get(), this method does not set the initial value if none is found
   * 
   * @throws NoSuchMethodException
   * @throws SecurityException
   */
  public T get(Thread thread) {
    return (T) get(this, thread);
  }

  private static Object get(ThreadLocal threadLocal, Thread thread) {
    try {
      Object threadLocalMap = invokePrivate(threadLocal, "getMap",
          new Class[] { Thread.class }, new Object[] { thread });

      if (threadLocalMap != null) {
        Object entry = invokePrivate(threadLocalMap, "getEntry",
            new Class[] { ThreadLocal.class }, new Object[] { threadLocal });
        if (entry != null)
          return getPrivate(entry, "value");
      }
      return null;
    } catch (Exception e) {
      throw new RuntimeException("Unable to extract thread local", e);
    }
  }

  private static Object getPrivate(Object object, String fieldName)
      throws SecurityException, NoSuchFieldException, IllegalArgumentException,
      IllegalAccessException {
    Field field = object.getClass().getDeclaredField(fieldName);
    field.setAccessible(true);
    return field.get(object);
  }

  private static Object invokePrivate(Object object, String methodName,
      Class[] argTypes, Object[] args) throws SecurityException,
      NoSuchMethodException, IllegalArgumentException, IllegalAccessException,
      InvocationTargetException {

    Method method = null;
    Class clazz = object.getClass();
    while (method == null) {
      try {
        method = clazz.getDeclaredMethod(methodName, argTypes);
      } catch (NoSuchMethodException e) {
        clazz = clazz.getSuperclass();
        if (clazz == null) {
          throw e;
        }
      }
    }
    method.setAccessible(true);
    Object result = method.invoke(object, args);
    return result;
  }

}

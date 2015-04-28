/*=========================================================================
 * Copyright (c) 2003-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
   
package com.gemstone.gemfire.internal;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

public class ClassLoadUtil  {
  
  static Map primTypes;
  static Map wrapperTypes;
  
  static {
    
    if (primTypes == null) {
      Map ptMap = new HashMap();
      ptMap.put(boolean.class.getName(), boolean.class);
      ptMap.put(char.class.getName(), char.class);
      ptMap.put(byte.class.getName(), byte.class);
      ptMap.put(short.class.getName(), short.class);
      ptMap.put(int.class.getName(), int.class);
      ptMap.put(long.class.getName(), long.class);
      ptMap.put(float.class.getName(), float.class);
      ptMap.put(double.class.getName(), double.class);
      ptMap.put(void.class.getName(), void.class);
      // Do this at the end to support multiple threads without synchronizing
      primTypes = ptMap;
    }
    
    if (wrapperTypes == null) {
      Map wtMap = new HashMap();
      wtMap.put(boolean.class.getName(), Boolean.class);
      wtMap.put(char.class.getName(), Character.class);
      wtMap.put(byte.class.getName(), Byte.class);
      wtMap.put(short.class.getName(), Short.class);
      wtMap.put(int.class.getName(), Integer.class);
      wtMap.put(long.class.getName(), Long.class);
      wtMap.put(float.class.getName(), Float.class);
      wtMap.put(double.class.getName(), Double.class);
      // Do this at the end to support multiple threads without synchronizing
      wrapperTypes = wtMap;
    }
  }
  /**
   * Resolve the class from the given name.  Supports primitive
   * types, too.
   */
  public static Class classFromName(String className) 
  throws ClassNotFoundException 
  {
    Class result = checkForPrimType(className);
    if (result == null) {
      result = ClassPathLoader.getLatest().forName(className);
    }
    return result;
  }

  /**
   * Resolve the method from the given qualified name.
   */
  public static Method methodFromName(String fullyQualifiedMethodName,
      Class[] parameterTypes) throws ClassNotFoundException,
      NoSuchMethodException, SecurityException {
    int classIndex = fullyQualifiedMethodName.lastIndexOf('.');
    if (classIndex <= 0) {
      throw new ClassNotFoundException("Static creation function ["
          + fullyQualifiedMethodName + "] should be fully qualified");
    }
    String className = fullyQualifiedMethodName.substring(0, classIndex);
    if (checkForPrimType(className) != null) {
      throw new NoSuchMethodException(className
          + " cannot be one of the primitive types");
    }
    String methodName = fullyQualifiedMethodName.substring(classIndex + 1);
    Class result = ClassPathLoader.getLatest().forName(className);
    return result.getMethod(methodName, parameterTypes);
  }

  /**
   * Resolve the method from the given qualified name. Only zero argument
   * methods are supported.
   */
  public static Method methodFromName(String fullyQualifiedMethodName)
      throws ClassNotFoundException, NoSuchMethodException, SecurityException {
    return methodFromName(fullyQualifiedMethodName, (Class[])null);
  }

  /**
   * If the argument className is the name of a primitive type (including
   * "void"), return the primitive type class (ex, boolean.class).  Otherwise,
   * return null.
   */
  public static Class checkForPrimType(String className) {
    
    return (Class)primTypes.get(className);
  }

  /**
   * If the argument className is the name of a primitive type (not including
   * "void"), return the wrapper class for that type (ex, Boolean.class).  
   * Otherwise, return null.
   */
  public static Class checkForWrapperType(String className) {
    
    return (Class)wrapperTypes.get(className);
  }

}

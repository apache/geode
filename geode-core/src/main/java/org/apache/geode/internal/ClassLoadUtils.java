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

package org.apache.geode.internal;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.classloader.internal.ClassPathLoader;

public class ClassLoadUtils {

  @Immutable
  private static final Map<String, Class> PRIMITIVE_TYPES = primitiveTypes();

  private static Map primitiveTypes() {
    Map<String, Class> ptMap = new HashMap<>();
    ptMap.put(boolean.class.getName(), boolean.class);
    ptMap.put(char.class.getName(), char.class);
    ptMap.put(byte.class.getName(), byte.class);
    ptMap.put(short.class.getName(), short.class);
    ptMap.put(int.class.getName(), int.class);
    ptMap.put(long.class.getName(), long.class);
    ptMap.put(float.class.getName(), float.class);
    ptMap.put(double.class.getName(), double.class);
    ptMap.put(void.class.getName(), void.class);
    return Collections.unmodifiableMap(ptMap);
  }

  /**
   * Resolve the class from the given name. Supports primitive types, too.
   */
  public static Class classFromName(String className) throws ClassNotFoundException {
    Class result = checkForPrimType(className);
    if (result == null) {
      result = ClassPathLoader.getLatest().forName(className);
    }
    return result;
  }

  /**
   * Resolve the method from the given qualified name. Only zero argument methods are supported.
   */
  public static Method methodFromName(String fullyQualifiedMethodName)
      throws ClassNotFoundException, NoSuchMethodException, SecurityException {
    int classIndex = fullyQualifiedMethodName.lastIndexOf('.');
    if (classIndex <= 0) {
      throw new ClassNotFoundException(
          "Static creation function [" + fullyQualifiedMethodName + "] should be fully qualified");
    }
    String className = fullyQualifiedMethodName.substring(0, classIndex);
    if (checkForPrimType(className) != null) {
      throw new NoSuchMethodException(className + " cannot be one of the primitive types");
    }
    String methodName = fullyQualifiedMethodName.substring(classIndex + 1);
    Class<?> result = ClassPathLoader.getLatest().forName(className);
    return result.getMethod(methodName);
  }

  /**
   * If the argument className is the name of a primitive type (including "void"), return the
   * primitive type class (ex, boolean.class). Otherwise, return null.
   */
  private static Class checkForPrimType(String className) {
    return PRIMITIVE_TYPES.get(className);
  }

}

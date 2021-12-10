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

package org.apache.geode.internal.lang.utils;

import org.apache.geode.annotations.Immutable;

/**
 * The ClassUtils class is an abstract utility class for working with and invoking methods on Class
 * objects.
 * <p/>
 *
 * @see java.lang.Class
 * @see java.lang.Object
 * @since GemFire 7.0
 */
public abstract class ClassUtils {

  @Immutable
  public static final Class[] EMPTY_CLASS_ARRAY = new Class[0];

  /**
   * Attempts to load the specified class by fully qualified name. If the class could not be found,
   * then this method handles the ClassNotFoundException and throws the specified RuntimeException
   * instead.
   * </p>
   *
   * @param className a String indicating the fully qualified name of the class to load.
   * @param e the RuntimeException to throw in place of the ClassNotFoundException if the class
   *        could not be found and loaded.
   * @return a Class object representing the specified class by name.
   * @throws NullPointerException if either className or RuntimeException is null.
   * @see java.lang.Class#forName(String)
   */
  public static Class forName(final String className, final RuntimeException e) {
    try {
      return Class.forName(className);
    } catch (ClassNotFoundException ignore) {
      throw e;
    } catch (NoClassDefFoundError ignore) {
      throw e;
    }
  }

  /**
   * Gets the Class type for the specified Object, or returns null if the Object reference is null.
   * <p/>
   *
   * @param obj the Object who's class type is determined.
   * @return the Class type of the Object parameter, or null if the Object reference is null.
   * @see java.lang.Object#getClass()
   */
  public static Class getClass(final Object obj) {
    return (obj == null ? null : obj.getClass());
  }

  /**
   * Gets the name of the Object's Class type or null if the Object reference is null.
   * <p/>
   *
   * @param obj the Object's who's class name is returned or null if the Object reference is null.
   * @return a String value specifying the name of the Object's class type.
   * @see #getClass(Object)
   * @see java.lang.Class#getName()
   */
  public static String getClassName(final Object obj) {
    final Class objType = getClass(obj);
    return (objType == null ? null : objType.getName());
  }

  /**
   * Determine whether the specified class is on the classpath. <
   * <p/>
   *
   * @param className a String value specifying the fully-qualified name of the class.
   * @return a boolean value indicating whether the specified class is on the classpath.
   * @see #forName(String, RuntimeException)
   */
  public static boolean isClassAvailable(final String className) {
    try {
      forName(className,
          new IllegalArgumentException(String.format("Class (%1$s) is not available!", className)));
      return true;
    } catch (IllegalArgumentException ignore) {
      // NOTE the Exception could be logged at fine/debug level if we used category-based logging
      return false;
    }
  }

  /**
   * Determines whether the specified Object parameter is an instance of, or is
   * assignment-compatible with the given Class type. Note, this method is null-safe for both Class
   * and Object value references.
   * <p/>
   *
   * @param type the Class type used in an instanceof determination with the given Object.
   * @param obj the Object being determined for assignment-compatibility with the specified Class
   *        type.
   * @return a boolean value indicating if the given Object is an instance of the specified Class
   *         type.
   * @see java.lang.Class#isInstance(Object)
   */
  public static boolean isInstanceOf(final Class type, final Object obj) {
    return (type != null && type.isInstance(obj));
  }

  /**
   * Determines whether the specified Object is not a instance of the following Class types. The
   * Object is considered not an instance of the Class types if the condition holds for all Class
   * types.
   * <p/>
   *
   * @param obj the Object who's Class type is in question.
   * @param types an array of Class types that the Object is being tested as an instance of.
   * @return a boolean value of true if the Object is not an instance of any of the specified Class
   *         types.
   * @see java.lang.Class#isInstance(Object)
   */
  public static boolean isNotInstanceOf(final Object obj, final Class... types) {
    boolean condition = true;

    if (types != null) {
      for (int index = 0; index < types.length && condition; index++) {
        condition &= !types[index].isInstance(obj);
      }
    }

    return condition;
  }

}

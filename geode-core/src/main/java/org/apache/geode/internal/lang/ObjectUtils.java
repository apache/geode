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

package org.apache.geode.internal.lang;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * The ObjectUtils class is an abstract utility class for working with and invoking methods on Objects.
 * <p/>
 * @see java.lang.Object
 * @since GemFire 6.8
 */
@SuppressWarnings("unused")
public abstract class ObjectUtils {

  public static final Object[] EMPTY_OBJECT_ARRAY = new Object[0];

  /**
   * Gets the first non-null value in an array of values.  If the array is null, then null is returned, otherwise the
   * first non-null array element is returned.  If the array is not null and all the array elements are null, then null
   * is still returned.
   * <p/>
   * @param <T> a type parameter specifying the array element type.
   * @param values the array of values being iterated for the first non-null value.
   * @return the first non-null value from the array of values, otherwise return null if either the array is null
   * or all the elements of the array are null.
   */
  public static <T> T defaultIfNull(T... values) {
    if (values != null) {
      for (T value : values) {
        if (value != null) {
          return value;
        }
      }
    }

    return null;
  }

  /**
   * Determines whether 2 Objects are equal in value.  The Objects are equal if and only if neither are null and are
   * equal according to the equals method of the Object's class type.
   * <p/>
   * @param obj1 the first Object in the equality comparison.
   * @param obj2 the second Object in the equality comparison.
   * @return a boolean value indicating whether the 2 Objects are equal in value.
   * @see java.lang.Object#equals(Object)
   */
  public static boolean equals(final Object obj1, final Object obj2) {
    return (obj1 != null && obj1.equals(obj2));
  }

  /**
   * Determines whether 2 Objects are equal in value by ignoring nulls.  If both Object references are null, then they
   * are considered equal, or neither must be null and the Objects must be equal in value as determined by their equals
   * method.
   * <p/>
   * @param obj1 the first Object in the equality comparison.
   * @param obj2 the second Object in the equality comparison.
   * @return a boolean value indicating whether the 2 Objects are equal in value.  If both Object references are null,
   * then they are considered equal.
   * @see java.lang.Object#equals(Object)
   */
  public static boolean equalsIgnoreNull(final Object obj1, final Object obj2) {
    return (obj1 == null ? obj2 == null : obj1.equals(obj2));
  }

  /**
   * A null-safe computation of the specified Object's hash value.  If the Object reference is null, then this method
   * returns 0 and will be consistent with the equalsIgnoreNull equality comparison.
   * <p/>
   * @param obj the Object who's hash value will be computed.
   * @return an integer signifying the hash value of the Object or 0 if the Object reference is null.
   * @see java.lang.Object#hashCode()
   */
  public static int hashCode(final Object obj) {
    return (obj == null ? 0 : obj.hashCode());
  }

  /**
   * Null-safe implementation of the Object.toString method.
   * </p>
   * @param obj the Object on which to call toString.
   * @return the String representation of the specified Object or null if the Object reference is null.
   * @see java.lang.Object#toString()
   */
  public static String toString(final Object obj) {
    return (obj == null ? null : obj.toString());
  }

  /**
   * Gets the Class types of all arguments in the Object array.
   * <p/>
   * @param args the Object array of arguments to determine the Class types for.
   * @return a Class array containing the Class types of each argument in the arguments Object array.
   * @see #invoke(Object, String, Object...)
   */
  static Class[] getArgumentTypes(final Object... args) {
    Class[] argTypes = null;

    if (args != null) {
      int index = 0;
      argTypes = new Class[args.length];
      for (Object arg : args) {
        argTypes[index++] = ClassUtils.getClass(arg);
      }
    }

    return argTypes;
  }

  /**
   * Invokes a method by name on the specified Object using Java Reflection.
   * <p/>
   * @param obj the Object in which to invoke the method on.
   * @param methodName a String value indication the name of the method to invoke.
   * @param <T> a generic type parameter for the method return value.
   * @return a value of the method invocation on Object cast to the generized type.
   * @see #invoke(Object, String, Class[], Object...)
   */
  public static <T> T invoke (final Object obj, final String methodName) {
    return invoke(obj, methodName, (Class<?>[]) null, (Object[]) null);
  }

  /**
   * Invokes a method by name on the specified Object using Java Reflection.
   * <p/>
   * @param obj the Object in which to invoke the method on.
   * @param methodName a String value indication the name of the method to invoke.
   * @param arguments the Object arguments to the method based on its parameters.
   * @param <T> a generic type parameter for the method return value.
   * @return a value of the method invocation on Object cast to the generized type.
   * @see #getArgumentTypes(Object...)
   * @see #invoke(Object, String, Class[], Object...)
   */
  public static <T> T invoke(final Object obj, final String methodName, final Object... arguments) {
    return invoke(obj, methodName, getArgumentTypes(arguments), arguments);
  }

  /**
   * Invokes a method by name on the specified Object using Java Reflection.
   * <p/>
   * @param obj the Object in which to invoke the method on.
   * @param methodName a String value indication the name of the method to invoke.
   * @param parameterTypes the Class types of parameters indicating the exact method to invoke (parameters in number,
   * order and type) if the method is overloaded.
   * @param arguments the Object arguments to the method based on its parameters.
   * @param <T> a generic type parameter for the method return value.
   * @return a value of the method invocation on Object cast to the generized type.
   */
  @SuppressWarnings("unchecked")
  public static <T> T invoke(final Object obj, final String methodName, final Class<?>[] parameterTypes, final Object... arguments) {
    assert obj != null : String.format("The Object to invoke method (%1$s) on cannot be null!", methodName);
    assert methodName != null : String.format("The name of the method to invoke on Object of type (%1$s) cannot be null", obj.getClass().getName());

    try {
      final Method method = obj.getClass().getMethod(methodName, parameterTypes);
      method.setAccessible(true);
      return (T) method.invoke(obj, arguments);
    }
    catch (NoSuchMethodException e) {
      throw new RuntimeException(String.format("Method (%1$s) does not exist on Object of type (%2$s)!",
        methodName, obj.getClass().getName()), e);
    }
    catch (InvocationTargetException e) {
      throw new RuntimeException(String.format("The invocation of method (%1$s) on an Object of type (%2$s) failed!",
        methodName, obj.getClass().getName()), e);
    }
    catch (IllegalAccessException e) {
      throw new RuntimeException(String.format("The method (%1$s) on an Object of type (%2$s) is not accessible!",
        methodName, obj.getClass().getName()), e);
    }
  }

}

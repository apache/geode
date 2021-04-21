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
 *
 */

package org.apache.geode.gradle.testing.process;

import java.lang.reflect.Field;
import java.util.Objects;
import java.util.function.BiFunction;

/**
 * Utility methods to retrieve and set otherwise inaccessible fields via reflection.
 */
public class Reflection {
  /**
   * Returns a {@link Field} that describes the named field in the given owner.
   */
  public static Field getField(Object owner, String fieldName) {
    Objects.requireNonNull(owner);
    try {
      return owner.getClass().getDeclaredField(fieldName);
    } catch (NoSuchFieldException e) {
      String message = String.format("Getting %s declaration for %s", fieldName, owner);
      throw new RuntimeException(message, e);
    }
  }

  /**
   * Returns the value of the named field from the given owner.
   */
  public static Object getFieldValue(Object owner, String fieldName) {
    return withAccessibleField(owner, fieldName, getValue());
  }

  /**
   * Sets the value of the named field in the given owner.
   */
  public static void setFieldValue(Object owner, String fieldName, Object value) {
    withAccessibleField(owner, fieldName, setValue(value));
  }

  /**
   * Makes a field temporarily accessible and applies the operation to it.
   */
  private static Object withAccessibleField(Object owner, String fieldName,
      BiFunction<Object, Field, Object> operation) {
    Field field = getField(owner, fieldName);
    boolean accessible = field.isAccessible();
    try {
      field.setAccessible(true);
      return operation.apply(owner, field);
    } finally {
      field.setAccessible(accessible);
    }
  }

  /**
   * Creates a function that extracts the value of a field from an owner.
   */
  private static BiFunction<Object, Field, Object> getValue() {
    return (owner, field) -> {
      try {
        return field.get(owner);
      } catch (IllegalAccessException | IllegalArgumentException e) {
        String message = String.format("Getting %s (%s) value for %s",
            field.getName(), field.isAccessible(), owner);
        throw new RuntimeException(message, e);
      }
    };
  }

  /**
   * Creates a function that sets a field of an owner to the given value.
   */
  private static BiFunction<Object, Field, Object> setValue(Object value) {
    return (owner, field) -> {
      try {
        field.set(owner, value);
      } catch (IllegalAccessException | IllegalArgumentException e) {
        String message = String.format("Setting %s (%s) value for %s",
            field.getName(), field.isAccessible(), owner);
        throw new RuntimeException(message, e);
      }
      return null;
    };
  }
}

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
package com.gemstone.gemfire.test.junit.rules.serializable;

import java.lang.reflect.Field;

/**
 * Provides support for serialization of private fields by reflection.
 */
public class FieldSerializationUtils {

  protected FieldSerializationUtils() {
  }

  public static Object readField(final Class targetClass, final Object targetInstance, final String fieldName) {
    try {
      Field field = targetClass.getDeclaredField(fieldName);
      field.setAccessible(true);
      return field.get(targetInstance);
    } catch (IllegalAccessException | NoSuchFieldException e) {
      throw new Error(e);
    }
  }

  public static void writeField(final Class targetClass, final Object targetInstance, final String fieldName, final Object value) {
    try {
      Field field = targetClass.getDeclaredField(fieldName);
      field.setAccessible(true);
      field.set(targetInstance, value);
    } catch (IllegalAccessException | NoSuchFieldException e) {
      throw new Error(e);
    }
  }
}

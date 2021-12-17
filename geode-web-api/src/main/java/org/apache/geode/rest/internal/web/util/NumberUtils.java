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

package org.apache.geode.rest.internal.web.util;

import org.springframework.util.StringUtils;

import org.apache.geode.internal.lang.utils.ClassUtils;

/**
 * The NumberUtils class is a utility class for working with numbers.
 * <p/>
 *
 * @see java.lang.Number
 * @since GemFire 8.0
 */
@SuppressWarnings("unused")
public abstract class NumberUtils {

  public static boolean isNumeric(String value) {
    value = String.valueOf(value).trim(); // guard against null

    for (char chr : value.toCharArray()) {
      if (!Character.isDigit(chr)) {
        return false;
      }
    }

    return true;
  }

  public static Long longValue(final Object value) {
    if (value instanceof Number) {
      return Long.valueOf(((Number) value).longValue());
    }
    return parseLong(String.valueOf(value));
  }

  public static Long parseLong(final String value) {
    try {
      return Long.parseLong(StringUtils.trimAllWhitespace(value));
    } catch (NumberFormatException ignore) {
      return null;
    }
  }

  public static boolean isPrimitiveOrWrapper(Class<?> klass) {
    return Byte.class.equals(klass) || byte.class.equals(klass) || Short.class.equals(klass)
        || short.class.equals(klass) || Integer.class.equals(klass) || int.class.equals(klass)
        || Long.class.equals(klass) || long.class.equals(klass) || Float.class.equals(klass)
        || float.class.equals(klass) || Double.class.equals(klass) || double.class.equals(klass)
        || Boolean.class.equals(klass) || boolean.class.equals(klass) || String.class.equals(klass);
  }

  public static boolean isPrimitiveOrObject(final String type) {
    return "byte".equalsIgnoreCase(type) || "short".equalsIgnoreCase(type)
        || "int".equalsIgnoreCase(type) || "long".equalsIgnoreCase(type)
        || "float".equalsIgnoreCase(type) || "double".equalsIgnoreCase(type)
        || "boolean".equalsIgnoreCase(type) || "string".equalsIgnoreCase(type);
  }

  public static boolean isValuePrimitiveType(final Object value) {
    return Byte.class.equals(ClassUtils.getClass(value))
        || Short.class.equals(ClassUtils.getClass(value))
        || Integer.class.equals(ClassUtils.getClass(value))
        || Long.class.equals(ClassUtils.getClass(value))
        || Float.class.equals(ClassUtils.getClass(value))
        || Double.class.equals(ClassUtils.getClass(value))
        || Boolean.class.equals(ClassUtils.getClass(value))
        || String.class.equals(ClassUtils.getClass(value));
  }
}

/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.rest.internal.web.util;

import org.springframework.util.StringUtils;

import org.springframework.util.StringUtils;

import com.gemstone.gemfire.internal.lang.ClassUtils;

/**
 * The NumberUtils class is a utility class for working with numbers.
 * <p/>
 * 
 * @author John Blum, Nilkanth Patel.
 * @see java.lang.Number
 * @since 8.0
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
    return Byte.class.equals(klass)
        || byte.class.equals(klass)
        || Short.class.equals(klass)
        || short.class.equals(klass)
        || Integer.class.equals(klass)
        || int.class.equals(klass)
        || Long.class.equals(klass)
        || long.class.equals(klass)
        || Float.class.equals(klass)
        || float.class.equals(klass)
        || Double.class.equals(klass)
        || double.class.equals(klass)
        || Boolean.class.equals(klass)
        || boolean.class.equals(klass)
        || String.class.equals(klass);
  }        
  
  public static boolean isPrimitiveOrObject(final String type) {
    return "byte".equalsIgnoreCase(type)
        || "short".equalsIgnoreCase(type)
        || "int".equalsIgnoreCase(type)
        || "long".equalsIgnoreCase(type)
        || "float".equalsIgnoreCase(type)
        || "double".equalsIgnoreCase(type)
        || "boolean".equalsIgnoreCase(type)
        || "string".equalsIgnoreCase(type);
  }
  
  public static boolean isValuePrimitiveType(final Object value){
    return Byte.class.equals(ClassUtils.getClass(value))
        || Short.class.equals(ClassUtils.getClass(value))
        || Integer.class.equals(ClassUtils.getClass(value))
        || Long.class.equals(ClassUtils.getClass(value))
        || Float.class.equals(ClassUtils.getClass(value))
        || Double.class.equals(ClassUtils.getClass(value))
        || Boolean.class.equals(ClassUtils.getClass(value))
        || String.class.equals(ClassUtils.getClass(value));
  }
  
  @SuppressWarnings({ "rawtypes", "unchecked" })
  public static Object convertToActualType(String stringValue, String type)
      throws IllegalArgumentException {
    if (stringValue != null && type == null) {
      return stringValue;
    }else {
      Object o = null;

      if ("string".equalsIgnoreCase(type))
        return stringValue;

      try {
        if ("byte".equalsIgnoreCase(type)) {
          o = Byte.parseByte(stringValue);
          return o;
        } else if ("short".equalsIgnoreCase(type)) {
          o = Short.parseShort(stringValue);
          return o;
        } else if ("int".equalsIgnoreCase(type)) {
          o = Integer.parseInt(stringValue);
          return o;
        } else if ("long".equalsIgnoreCase(type)) {
          o = Long.parseLong(stringValue);
          return o;
        } else if ("double".equalsIgnoreCase(type)) {
          o = Double.parseDouble(stringValue);
          return o;
        } else if ("boolean".equalsIgnoreCase(type)) {
          o = Boolean.parseBoolean(stringValue);
          return o;
        } else if ("float".equalsIgnoreCase(type)) {
          o = Float.parseFloat(stringValue);
          return o;
        }
        return o;
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("Failed to convert input key to "
            + type + " Msg : " + e.getMessage());
      }
      
    }
  }
}

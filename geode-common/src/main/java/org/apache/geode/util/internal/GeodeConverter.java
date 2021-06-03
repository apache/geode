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

package org.apache.geode.util.internal;

public class GeodeConverter {
  public static Object convertToActualType(String stringValue, String type)
      throws IllegalArgumentException {
    if (stringValue != null && type == null) {
      return stringValue;
    } else {
      Object o = null;
      if (type.startsWith("java.lang.")) {
        type = type.substring(10);
      }

      if ("string".equalsIgnoreCase(type)) {
        return stringValue;
      }

      if ("char".equalsIgnoreCase(type) || "Character".equals(type)) {
        return stringValue.charAt(0);
      }

      try {
        if ("byte".equalsIgnoreCase(type)) {
          o = Byte.parseByte(stringValue);
          return o;
        } else if ("short".equalsIgnoreCase(type)) {
          o = Short.parseShort(stringValue);
          return o;
        } else if ("int".equalsIgnoreCase(type) || "Integer".equals(type)) {
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
        throw new IllegalArgumentException(
            "Failed to convert input key to " + type + " Msg : " + e.getMessage());
      }
    }
  }
}

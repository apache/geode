/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.management.internal.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * This class contains utility methods for JSON (http://www.json.org/) which is used by classes used
 * for the Command Line Interface (CLI).
 *
 * @since GemFire 7.0
 */
public class JsonUtil {

  public static boolean isPrimitiveOrWrapper(Class<?> klass) {
    return klass.isAssignableFrom(Byte.class) || klass.isAssignableFrom(byte.class)
        || klass.isAssignableFrom(Short.class) || klass.isAssignableFrom(short.class)
        || klass.isAssignableFrom(Integer.class) || klass.isAssignableFrom(int.class)
        || klass.isAssignableFrom(Long.class) || klass.isAssignableFrom(long.class)
        || klass.isAssignableFrom(Float.class) || klass.isAssignableFrom(float.class)
        || klass.isAssignableFrom(Double.class) || klass.isAssignableFrom(double.class)
        || klass.isAssignableFrom(Boolean.class) || klass.isAssignableFrom(boolean.class)
        || klass.isAssignableFrom(Character.class) || klass.isAssignableFrom(char.class)
        || klass.isAssignableFrom(String.class);
  }

  public static List<String> toStringList(JsonNode jsonArray) {
    if (!jsonArray.isArray()) {
      return Collections.emptyList();
    }

    List<String> result = new ArrayList<>();
    for (JsonNode node : jsonArray) {
      result.add(node.asText());
    }

    return result;
  }
}

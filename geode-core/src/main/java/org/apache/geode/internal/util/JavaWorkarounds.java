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
package org.apache.geode.internal.util;

import java.util.Map;
import java.util.function.Function;

public class JavaWorkarounds {

  public static final boolean postJava8 = calcIfJDKIsPost8();

  private static boolean calcIfJDKIsPost8() {
    String version = System.getProperty("java.version");
    return !version.matches("1\\.[0-8]\\.");
  }

  // This is a workaround for computeIfAbsent which unnecessarily takes out a write lock in the case
  // where the entry for the key exists already.  This only affects pre Java 9 jdks
  // https://bugs.openjdk.java.net/browse/JDK-8161372
  public static <K, V> V computeIfAbsent(Map<K, V> map, K key,
      Function<? super K, ? extends V> mappingFunction) {
    if (postJava8) {
      return map.computeIfAbsent(key, mappingFunction);
    } else {
      V existingValue = map.get(key);
      if (existingValue == null) {
        return map.computeIfAbsent(key, mappingFunction);
      }
      return existingValue;
    }
  }
}

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

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * The ArrayUtils class is an abstract utility class for working with Object arrays.
 * <p/>
 *
 * @see java.util.Arrays
 * @since GemFire 8.0
 */
public abstract class ArrayUtils {

  public static boolean isEmpty(final Object[] array) {
    return (array == null || array.length == 0);
  }

  public static boolean isNotEmpty(final Object[] array) {
    return !isEmpty(array);
  }

  public static int length(final Object[] array) {
    return (array == null ? 0 : array.length);
  }

  public static String toString(final Object... array) {
    final StringBuilder buffer = new StringBuilder("[");
    int count = 0;

    if (array != null) {
      for (Object element : array) {
        buffer.append(count++ > 0 ? ", " : "").append(element);
      }
    }

    buffer.append("]");

    return buffer.toString();
  }

  public static String toString(final String... array) {
    return toString((Object[]) array);
  }

  public static Set<String> asSet(String[] filter) {
    LinkedHashSet<String> linkedHashSet = new LinkedHashSet<>(filter.length);
    Collections.addAll(linkedHashSet, filter);
    return linkedHashSet;
  }
}

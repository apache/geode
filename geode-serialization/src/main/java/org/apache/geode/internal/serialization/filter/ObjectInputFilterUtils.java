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
package org.apache.geode.internal.serialization.filter;

import java.util.function.Function;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.internal.lang.utils.ClassUtils;

/**
 * Utility methods for {@code ObjectInputFilter}.
 */
public class ObjectInputFilterUtils {

  private static final String UNSUPPORTED_MESSAGE = "ObjectInputFilter is not available.";

  /**
   * Returns true if this JVM supports {@code ObjectInputFilter}.
   */
  public static boolean supportsObjectInputFilter() {
    return supportsObjectInputFilter(ClassUtils::isClassAvailable);
  }

  static void throwUnsupportedOperationException() {
    throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE, null);
  }

  static void throwUnsupportedOperationException(Throwable cause) {
    throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE, cause);
  }

  static void throwUnsupportedOperationException(String message) {
    throw new UnsupportedOperationException(message, null);
  }

  static void throwUnsupportedOperationException(String message, Throwable cause) {
    throw new UnsupportedOperationException(message, cause);
  }

  @VisibleForTesting
  static boolean supportsObjectInputFilter(Function<String, Boolean> isClassAvailable) {
    return isClassAvailable.apply("sun.misc.ObjectInputFilter") ||
        isClassAvailable.apply("java.io.ObjectInputFilter");
  }

  private ObjectInputFilterUtils() {
    // do not instantiate
  }
}

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
package org.apache.geode.internal.cache.tier;

import org.jetbrains.annotations.NotNull;

/**
 * Pre-defined interest list types supported by the system.
 * Partial duplicate of {@link org.apache.geode.cache.operations.InterestType}.
 */
public enum InterestType {

  /** For registering interest in a specific key or list of keys */
  KEY,

  /** For registering interest in all keys matching a regular expression */
  REGULAR_EXPRESSION,

  /** For registering interest in all key/value pairs that satisfy a provided filtering class */
  FILTER_CLASS,

  /** For registering interest in all key/value pairs that satisfy an OQL query */
  OQL_QUERY,

  /** For registering CQs */
  CQ;

  /**
   * Convert an interest type to a printable string
   *
   * @param kind the type to convert
   * @return a printable string
   */
  public static String getString(final @NotNull InterestType kind) {
    switch (kind) {
      case KEY:
        return "KEY";
      case REGULAR_EXPRESSION:
        return "REGEX";
      case FILTER_CLASS:
        return "FILTER";
      case OQL_QUERY:
        return "OQL_QUERY";
      case CQ:
        return "CQ";
      default:
        return "Invalid(" + kind + ")";
    }
  }

  public static @NotNull InterestType valueOf(final int ordinal) {
    return values()[ordinal];
  }
}

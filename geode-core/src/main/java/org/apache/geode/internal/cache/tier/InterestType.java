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
package org.apache.geode.internal.cache.tier;

/**
 * Pre-defined interest list types supported by the system
 *
 * @version $Revision: 13166 $
 * @since GemFire 2.0.2
 */
public class InterestType {

  /** For registering interest in a specific key or list of keys */
  public static final int KEY = 0;

  /** For registering interest in all keys matching a regular expression */
  public static final int REGULAR_EXPRESSION = 1;

  /** For registering interest in all key/value pairs that satisfy a provided filtering class */
  public static final int FILTER_CLASS = 2;

  /** For registering interest in all key/value pairs that satisfy an OQL query */
  public static final int OQL_QUERY = 3;

  /** For registering CQs */
  public static final int CQ = 4;

  /**
   * Convert an interest type to a printable string
   * @param kind the type to convert
   * @return a printable string
   */
  static public String getString(int kind) {
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
      return "Invalid(" + Integer.toString(kind) + ")";
    }
  }
}

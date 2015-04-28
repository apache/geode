/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.tier;

/**
 * Pre-defined interest list types supported by the system
 *
 * @author Sudhir Menon
 * @version $Revision: 13166 $
 * @since 2.0.2
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

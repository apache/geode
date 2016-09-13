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

package com.gemstone.gemfire.cache.operations;

/**
 * Enumeration for various interest types supported by GemFire.
 * 
 * @since GemFire 5.5
 */
public final class InterestType {

  public static final byte TP_KEY = 0;

  public static final byte TP_REGEX = 1;

  //public static final byte TP_FILTER_CLASS = 2;

  //public static final byte TP_OQL = 3;

  public static final byte TP_LIST = 4;

  private static byte nextOrdinal = 0;

  private static final InterestType[] VALUES = new InterestType[10];

  /**
   * For registering interest in a specific key.
   */
  public static final InterestType KEY = new InterestType("KEY", TP_KEY);

  /**
   * For registering interest in a list of keys.
   */
  public static final InterestType LIST = new InterestType("LIST", TP_LIST);

  /**
   * For registering interest in all keys matching a regular expression.
   */
  public static final InterestType REGULAR_EXPRESSION = new InterestType(
      "REGULAR_EXPRESSION", TP_REGEX);

  /**
   * For registering interest in all key/value pairs that satisfy a provided
   * filtering class.
   */
  //public static final InterestType FILTER_CLASS = new InterestType(
  //    "FILTER_CLASS", TP_FILTER_CLASS);

  /**
   * For registering interest in all key/value pairs that satisfy an OQL query.
   */
  //public static final InterestType OQL_QUERY = new InterestType("OQL_QUERY",
  //    TP_OQL);

  /** The name of this interest type. */
  private final String name;

  /** byte used as ordinal to represent this interest type */
  private final byte ordinal;

  /**
   * One of the following: TP_KEY, TP_LIST, TP_REGEX, TP_FILTER_CLASS, TP_OQL.
   * TP_FILTER_CLASS and TP_OQL are currently unimplemented.
   */
  private final byte interestType;

  /** Creates a new instance of <code>InterestType</code>. */
  private InterestType(String name, byte interestType) {
    this.name = name;
    this.interestType = interestType;
    this.ordinal = nextOrdinal++;
    VALUES[this.ordinal] = this;
  }

  /**
   * Returns true if this is a key interest type.
   */
  public boolean isKey() {
    return (this.interestType == TP_KEY);
  }

  /**
   * Returns true if this is a key list interest type.
   */
  public boolean isList() {
    return (this.interestType == TP_LIST);
  }

  /**
   * Returns true if this is a regular expression interest type.
   */
  public boolean isRegularExpression() {
    return (this.interestType == TP_REGEX);
  }

  /**
   * Returns true if this is a filter class interest type.
   */
  //public boolean isFilterClass() {
  //  return (this.interestType == TP_FILTER_CLASS);
  //}

  /**
   * Returns true if this is an OQL query interest type.
   */
  //public boolean isOQLQuery() {
  //  return (this.interestType == TP_OQL);
  //}

  /**
   * Returns the <code>InterestType</code> represented by specified ordinal.
   */
  public static InterestType fromOrdinal(byte ordinal) {
    return VALUES[ordinal];
  }

  /**
   * Returns the ordinal for this interest type.
   * 
   * @return the ordinal of this interest type.
   */
  public byte toOrdinal() {
    return this.ordinal;
  }

  /**
   * Returns a string representation for this interest type.
   * 
   * @return the name of this interest type.
   */
  @Override // GemStoneAddition
  public String toString() {
    return this.name;
  }

}

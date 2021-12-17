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

package org.apache.geode.cache.operations;

import org.apache.geode.annotations.Immutable;

/**
 * Enumeration for various interest types.
 *
 * @since GemFire 5.5
 */
@Immutable
public final class InterestType {

  public static final byte TP_KEY = 0;

  public static final byte TP_REGEX = 1;

  public static final byte TP_LIST = 4;

  @Immutable
  private static final InterestType[] VALUES = new InterestType[10];

  /**
   * For registering interest in a specific key.
   */
  @Immutable
  public static final InterestType KEY = new InterestType("KEY", TP_KEY, 0);

  /**
   * For registering interest in a list of keys.
   */
  @Immutable
  public static final InterestType LIST = new InterestType("LIST", TP_LIST, 1);

  /**
   * For registering interest in all keys matching a regular expression.
   */
  @Immutable
  public static final InterestType REGULAR_EXPRESSION =
      new InterestType("REGULAR_EXPRESSION", TP_REGEX, 3);

  /** The name of this interest type. */
  private final String name;

  /** byte used as ordinal to represent this interest type */
  private final byte ordinal;

  /**
   * One of the following: TP_KEY, TP_LIST, TP_REGEX, TP_FILTER_CLASS, TP_OQL. TP_FILTER_CLASS and
   * TP_OQL are currently unimplemented.
   */
  private final byte interestType;

  /** Creates a new instance of <code>InterestType</code>. */
  private InterestType(String name, byte interestType, int ordinal) {
    this.name = name;
    this.interestType = interestType;
    this.ordinal = (byte) ordinal;
    VALUES[this.ordinal] = this;
  }

  /**
   * Returns true if this is a key interest type.
   */
  public boolean isKey() {
    return (interestType == TP_KEY);
  }

  /**
   * Returns true if this is a key list interest type.
   */
  public boolean isList() {
    return (interestType == TP_LIST);
  }

  /**
   * Returns true if this is a regular expression interest type.
   */
  public boolean isRegularExpression() {
    return (interestType == TP_REGEX);
  }

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
    return ordinal;
  }

  /**
   * Returns a string representation for this interest type.
   *
   * @return the name of this interest type.
   */
  @Override
  public String toString() {
    return name;
  }

}

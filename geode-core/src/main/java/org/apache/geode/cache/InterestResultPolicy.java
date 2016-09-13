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


package com.gemstone.gemfire.cache;

import com.gemstone.gemfire.internal.cache.tier.sockets.InterestResultPolicyImpl;
import java.io.ObjectStreamException;
import java.io.Serializable;

/**
 * Class <code>InterestResultPolicy</code> is an enumerated type for a
 * register interest result. The result of a call to Region.registerInterest
 * can be the keys and current values, just the keys or nothing.
 *
 *
 * @see com.gemstone.gemfire.cache.Region#registerInterest(Object)
 * @see com.gemstone.gemfire.cache.Region#registerInterestRegex(String)
 *
 * @since GemFire 4.2.3
 */
public class InterestResultPolicy implements Serializable {
  private static final long serialVersionUID = -4993765891973030160L;

  private static byte nextOrdinal = 0;

  private static final InterestResultPolicy[] VALUES = new InterestResultPolicy[3];

  public static final InterestResultPolicy NONE = new InterestResultPolicyImpl("NONE");
  public static final InterestResultPolicy KEYS = new InterestResultPolicyImpl("KEYS");
  public static final InterestResultPolicy KEYS_VALUES = new InterestResultPolicyImpl("KEYS_VALUES");

  /**
   * The <code>InterestResultPolicy</code> used by default; it is {@link #KEYS_VALUES}.
   */
  public static final InterestResultPolicy DEFAULT = KEYS_VALUES;


  /** The name of this <code>InterestResultPolicy</code>. */
  private final transient String name;

  /** The ordinal representing this <code>InterestResultPolicy</code>. */
  public final byte ordinal;

  protected Object readResolve() throws ObjectStreamException {
    return VALUES[ordinal];  // Canonicalize
  }


  /** should only be called from InterestResultPolicyImpl */
  protected InterestResultPolicy(String name) {
    this.name = name;
    this.ordinal = nextOrdinal++;
    VALUES[this.ordinal] = this;
  }

  /** Returns the <code>InterestResultPolicy</code> represented by specified ordinal */
  public static InterestResultPolicy fromOrdinal(byte ordinal) {
    return VALUES[ordinal];
  }
  /** Returns the ordinal value.
   * @since GemFire 5.0
   */
  public byte getOrdinal() {
    return this.ordinal;
  }

  /**
   * Returns true if this <code>InterestResultPolicy</code> is {@link #NONE}.
   * @return true if this <code>InterestResultPolicy</code> is {@link #NONE}.
   */
  public boolean isNone() {
    return this == NONE;
  }

  /**
   * Returns true if this <code>InterestResultPolicy</code> is {@link #KEYS}.
   * @return true if this <code>InterestResultPolicy</code> is {@link #KEYS}.
   */
  public boolean isKeys() {
    return this == KEYS;
  }

  /**
   * Returns true if this <code>InterestResultPolicy</code> is {@link #KEYS_VALUES}.
   * @return true if this <code>InterestResultPolicy</code> is {@link #KEYS_VALUES}.
   */
  public boolean isKeysValues() {
    return this == KEYS_VALUES;
  }

  /**
   * Returns true if this <code>InterestResultPolicy</code> is the default.
   * @return true if this <code>InterestResultPolicy</code> is the default.
   */
  public boolean isDefault() {
    return this == DEFAULT;
  }

  /** Returns a string representation for this <code>InterestResultPolicy</code>.
   * @return the name of this data policy.
   */
  @Override // GemStoneAddition
  public String toString() {
    return this.name;
  }
}

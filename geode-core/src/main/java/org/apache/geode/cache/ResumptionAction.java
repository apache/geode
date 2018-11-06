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
package org.apache.geode.cache;

import java.io.ObjectStreamException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;


/**
 * Specifies how the region is affected by resumption of reliability when one or more missing
 * required roles return to the distributed membership. The <code>ResumptionAction</code> is
 * specified when configuring a region's {@link org.apache.geode.cache.MembershipAttributes}.
 *
 * @deprecated this API is scheduled to be removed
 */
public class ResumptionAction implements java.io.Serializable {
  private static final long serialVersionUID = 6632254151314915610L;

  /** No special action takes place when reliability resumes. */
  public static final ResumptionAction NONE = new ResumptionAction("NONE");

  /**
   * Resumption of reliability causes the region to be cleared of all data and
   * {@link DataPolicy#withReplication replicated} regions will do a new getInitialImage operation
   * to repopulate the region. Any existing references to this region become unusable in that any
   * subsequent methods invoked on those references will throw a
   * {@link RegionReinitializedException}.
   */
  public static final ResumptionAction REINITIALIZE = new ResumptionAction("REINITIALIZE");

  /** The name of this mirror type. */
  private final transient String name;

  // The 4 declarations below are necessary for serialization
  /** byte used as ordinal to represent this Scope */
  public final byte ordinal = nextOrdinal++;

  private static byte nextOrdinal = 0;

  private static final ResumptionAction[] PRIVATE_VALUES = {NONE, REINITIALIZE};

  /** List of all ResumptionAction values */
  public static final List VALUES = Collections.unmodifiableList(Arrays.asList(PRIVATE_VALUES));

  private Object readResolve() throws ObjectStreamException {
    return PRIVATE_VALUES[ordinal]; // Canonicalize
  }

  /** Creates a new instance of ResumptionAction. */
  private ResumptionAction(String name) {
    this.name = name;
  }

  /** Return the ResumptionAction represented by specified ordinal */
  public static ResumptionAction fromOrdinal(byte ordinal) {
    return PRIVATE_VALUES[ordinal];
  }

  /** Return the ResumptionAction specified by name */
  public static ResumptionAction fromName(String name) {
    if (name == null || name.length() == 0) {
      throw new IllegalArgumentException(
          String.format("Invalid ResumptionAction name: %s",
              name));
    }
    for (int i = 0; i < PRIVATE_VALUES.length; i++) {
      if (name.equals(PRIVATE_VALUES[i].name)) {
        return PRIVATE_VALUES[i];
      }
    }
    throw new IllegalArgumentException(
        String.format("Invalid ResumptionAction name: %s", name));
  }

  /** Returns true if this is <code>NONE</code>. */
  public boolean isNone() {
    return this == NONE;
  }

  /** Returns true if this is <code>REINITIALIZE</code>. */
  public boolean isReinitialize() {
    return this == REINITIALIZE;
  }

  /**
   * Returns a string representation for this resumption action.
   *
   * @return the name of this resumption action
   */
  @Override
  public String toString() {
    return this.name;
  }

}

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
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;


/**
 * Specifies how access to the region is affected when one or more required roles are lost. A role
 * is lost when it is are offline and no longer present in the system membership. The
 * <code>LossAction</code> is specified when configuring a region's
 * {@link org.apache.geode.cache.MembershipAttributes}.
 *
 * @deprecated this feature is scheduled to be removed
 */
public class LossAction implements Serializable {
  private static final long serialVersionUID = -832035480397447797L;

  // NOTE: to change/add these files are impacted:
  // LossAction, package.html

  /**
   * The region is unavailable when required roles are missing. All operations including read and
   * write access are denied. All read and write operations on the region will result in
   * {@link RegionAccessException} while any required roles are absent. Basic administration of the
   * region is allowed, including {@linkplain Region#close close} and
   * {@linkplain Region#localDestroyRegion() localDestroyRegion}.
   */
  public static final LossAction NO_ACCESS = new LossAction("NO_ACCESS");

  /**
   * Only local access to the region is allowed when required roles are missing. All distributed
   * write operations on the region will throw {@link RegionAccessException} while any required
   * roles are absent. Reads which result in a netSearch behave normally, while any attempt to
   * invoke a netLoad is not allowed.
   */
  public static final LossAction LIMITED_ACCESS = new LossAction("LIMITED_ACCESS");

  /**
   * Access to the region is unaffected when required roles are missing.
   */
  public static final LossAction FULL_ACCESS = new LossAction("FULL_ACCESS");

  /**
   * Loss of required roles causes the entire cache to be closed. In addition, this process will
   * disconnect from the DistributedSystem and then reconnect. Attempting to use any existing
   * references to the regions or cache will throw a {@link CacheClosedException}.
   */
  public static final LossAction RECONNECT = new LossAction("RECONNECT");

  /** The name of this mirror type. */
  private final transient String name;

  // The 4 declarations below are necessary for serialization
  /** byte used as ordinal to represent this Scope */
  public final byte ordinal = nextOrdinal++;

  private static byte nextOrdinal = 0;

  private static final LossAction[] PRIVATE_VALUES =
      {NO_ACCESS, LIMITED_ACCESS, FULL_ACCESS, RECONNECT};

  /** List of all LossAction values */
  public static final List VALUES = Collections.unmodifiableList(Arrays.asList(PRIVATE_VALUES));

  private Object readResolve() throws ObjectStreamException {
    return PRIVATE_VALUES[ordinal]; // Canonicalize
  }

  /** Creates a new instance of LossAction. */
  private LossAction(String name) {
    this.name = name;
  }

  /** Return the LossAction represented by specified ordinal */
  public static LossAction fromOrdinal(byte ordinal) {
    return PRIVATE_VALUES[ordinal];
  }

  /** Return the LossAction specified by name */
  public static LossAction fromName(String name) {
    if (name == null || name.length() == 0) {
      throw new IllegalArgumentException(
          String.format("Invalid LossAction name: %s", name));
    }
    for (int i = 0; i < PRIVATE_VALUES.length; i++) {
      if (name.equals(PRIVATE_VALUES[i].name)) {
        return PRIVATE_VALUES[i];
      }
    }
    throw new IllegalArgumentException(
        String.format("Invalid LossAction name: %s", name));
  }

  /** Returns true if this is <code>NO_ACCESS</code>. */
  public boolean isNoAccess() {
    return this == NO_ACCESS;
  }

  /** Returns true if this is <code>LIMITED_ACCESS</code>. */
  public boolean isLimitedAccess() {
    return this == LIMITED_ACCESS;
  }

  /** Returns true if this is <code>FULL_ACCESS</code>. */
  public boolean isAllAccess() {
    return this == FULL_ACCESS;
  }

  /** Returns true if this is <code>RECONNECT</code>. */
  public boolean isReconnect() {
    return this == RECONNECT;
  }

  /**
   * Returns a string representation for this loss action.
   *
   * @return the name of this loss action
   */
  @Override
  public String toString() {
    return this.name;
  }

}

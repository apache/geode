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
package org.apache.geode.admin;


import org.apache.geode.annotations.Immutable;

/**
 * Type-safe definition for system members.
 *
 * @since GemFire 3.5
 *
 * @deprecated as of 7.0 use the <code><a href=
 *             "{@docRoot}/org/apache/geode/management/package-summary.html">management</a></code>
 *             package instead
 */
@Immutable
public class SystemMemberType implements java.io.Serializable {
  private static final long serialVersionUID = 3284366994485749302L;

  /** GemFire shared-memory manager connected to the distributed system */
  @Immutable
  public static final SystemMemberType MANAGER = new SystemMemberType("GemFireManager", 0);

  /** Application connected to the distributed system */
  @Immutable
  public static final SystemMemberType APPLICATION = new SystemMemberType("Application", 1);

  /** GemFire Cache VM connected to the distributed system */
  @Immutable
  public static final SystemMemberType CACHE_VM = new SystemMemberType("CacheVm", 2);

  /**
   * GemFire Cache Server connected to the distributed system
   *
   * @deprecated as of 5.7 use {@link #CACHE_VM} instead.
   */
  @Deprecated
  @Immutable
  public static final SystemMemberType CACHE_SERVER = CACHE_VM;


  /** The display-friendly name of this system member type. */
  private final transient String name;

  // The 4 declarations below are necessary for serialization
  /** int used as ordinal to represent this Scope */
  public final int ordinal;

  @Immutable
  private static final SystemMemberType[] VALUES = {MANAGER, APPLICATION, CACHE_VM};

  private Object readResolve() throws java.io.ObjectStreamException {
    return VALUES[ordinal]; // Canonicalize
  }

  /** Creates a new instance of SystemMemberType. */
  private SystemMemberType(String name, int ordinal) {
    this.name = name;
    this.ordinal = ordinal;
  }

  /** Return the SystemMemberType represented by specified ordinal */
  public static SystemMemberType fromOrdinal(int ordinal) {
    return VALUES[ordinal];
  }

  public String getName() {
    return name;
  }

  /** Return whether this is <code>MANAGER</code>. */
  public boolean isManager() {
    return equals(MANAGER);
  }

  /** Return whether this is <code>APPLICATION</code>. */
  public boolean isApplication() {
    return equals(APPLICATION);
  }

  /**
   * Return whether this is <code>CACHE_SERVER</code>.
   *
   * @deprecated as of 5.7 use {@link #isCacheVm} instead.
   */
  @Deprecated
  public boolean isCacheServer() {
    return isCacheVm();
  }

  /**
   * Return whether this is <code>CACHE_VM</code>.
   */
  public boolean isCacheVm() {
    return equals(CACHE_VM);
  }

  /**
   * Returns a string representation for this system member type.
   *
   * @return the name of this system member type
   */
  @Override
  public String toString() {
    return name;
  }

  /**
   * Indicates whether some other object is "equal to" this one.
   *
   * @param other the reference object with which to compare.
   * @return true if this object is the same as the obj argument; false otherwise.
   */
  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }
    if (other == null) {
      return false;
    }
    if (!(other instanceof SystemMemberType)) {
      return false;
    }
    final SystemMemberType that = (SystemMemberType) other;
    return ordinal == that.ordinal;
  }

  /**
   * Returns a hash code for the object. This method is supported for the benefit of hashtables such
   * as those provided by java.util.Hashtable.
   *
   * @return the integer 0 if description is null; otherwise a unique integer.
   */
  @Override
  public int hashCode() {
    int result = 17;
    final int mult = 37;
    result = mult * result + ordinal;
    return result;
  }

}

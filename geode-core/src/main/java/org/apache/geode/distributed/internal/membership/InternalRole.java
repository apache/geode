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
package org.apache.geode.distributed.internal.membership;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import org.apache.geode.distributed.Role;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;

/**
 * <p>
 * Members of the distributed system can fill one or more user defined roles. A role is metadata
 * that describes how the member relates to other members or what purpose it fills.
 * </p>
 *
 * <p>
 * This class should not be Serializable or DataSerializable. It has a private constructor and it
 * maintains a static canonical map of instances. Any serializable object which has instances of
 * InternalRole should flag those variables as transient. Objects that implement DataSerializable
 * should convert the roles to String names. For an example, please see
 * {@link org.apache.geode.cache.MembershipAttributes}.
 * </p>
 *
 * <p>
 * Serializable classes which hold references to Roles should customize serialization to transfer
 * string names for Roles. See {@link org.apache.geode.cache.RegionAccessException
 * RegionAccessException} and {@link org.apache.geode.cache.RegionDistributionException
 * RegionDistributionException} for examples on how to do this.
 * </p>
 *
 * @deprecated this feature is scheduled to be removed
 */
public class InternalRole implements Role {

  /** The name of this role */
  private final String name;

  /** Static canonical instances of roles. key=name, value=InternalRole */
  private static final Map roles = new HashMap(); // could use ConcurrentHashMap

  /** Contructs a new InternalRole instance for the specified role name */
  InternalRole(String name) {
    this.name = name;
  }

  public String getName() {
    return this.name;
  }

  /**
   * implements the java.lang.Comparable interface
   *
   * @see java.lang.Comparable
   * @param o the Object to be compared
   * @return a negative integer, zero, or a positive integer as this object is less than, equal to,
   *         or greater than the specified object.
   * @exception java.lang.ClassCastException if the specified object's type prevents it from being
   *            compared to this Object.
   */
  public int compareTo(Role o) {
    if ((o == null) || !(o instanceof InternalRole)) {
      throw new ClassCastException(
          "InternalRole.compareTo(): comparison between different classes");
    }
    InternalRole other = (InternalRole) o;
    return this.name.compareTo(other.name);
  }

  /**
   * Indicates whether some other object is "equal to" this one.
   *
   * @param other the reference object with which to compare.
   * @return true if this object is the same as the obj argument; false otherwise.
   */
  @Override
  public boolean equals(Object other) {
    if (other == this)
      return true;
    if (other == null)
      return false;
    if (!(other instanceof InternalRole))
      return false;
    final InternalRole that = (InternalRole) other;

    if (!StringUtils.equals(this.name, that.name))
      return false;

    return true;
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

    result = mult * result + (this.name == null ? 0 : this.name.hashCode());

    return result;
  }

  /**
   * Factory method to allow canonicalization of instances. Returns existing instance or creates a
   * new instance.
   */
  public static InternalRole getRole(String name) {
    synchronized (roles) {
      InternalRole role = (InternalRole) roles.get(name);
      if (role == null) {
        role = new InternalRole(name);
        roles.put(name, role);
      }
      return role;
    }
  }

  public boolean isPresent() {
    InternalDistributedSystem sys = InternalDistributedSystem.getAnyInstance();
    if (sys == null) {
      throw new IllegalStateException(
          "isPresent requires a connection to the distributed system.");
    }
    DistributionManager dm = sys.getDistributionManager();
    return dm.isRolePresent(this);
  }

  public int getCount() {
    InternalDistributedSystem sys = InternalDistributedSystem.getAnyInstance();
    if (sys == null) {
      throw new IllegalStateException(
          "getCount requires a connection to the distributed system.");
    }
    DistributionManager dm = sys.getDistributionManager();
    return dm.getRoleCount(this);
  }

  /**
   * Returns a string representation of the object.
   *
   * @return a string representation of the object
   */
  @Override
  public String toString() {
    return this.name;
  }

}

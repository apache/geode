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
package org.apache.geode.admin.jmx.internal;

import org.apache.commons.lang3.StringUtils;

import org.apache.geode.annotations.Immutable;

/**
 * Type-safe definition for ModelMBean managed resources. The class type ({@link #getClassTypeName})
 * must match the fully qualified class name listed in the type descriptor in
 * mbeans-descriptors.xml.
 *
 * @since GemFire 3.5
 *
 */
@Immutable
public class ManagedResourceType implements java.io.Serializable {
  private static final long serialVersionUID = 3752874768667480449L;

  /** Agent managed resource type */
  @Immutable
  public static final ManagedResourceType AGENT =
      new ManagedResourceType("Agent", org.apache.geode.admin.jmx.Agent.class, 0);

  /** DistributedSystem managed resource type */
  @Immutable
  public static final ManagedResourceType DISTRIBUTED_SYSTEM = new ManagedResourceType(
      "AdminDistributedSystem", org.apache.geode.admin.AdminDistributedSystem.class, 1);

  /** SystemMember managed resource type */
  @Immutable
  public static final ManagedResourceType SYSTEM_MEMBER =
      new ManagedResourceType("SystemMember", org.apache.geode.admin.SystemMember.class, 2);

  /** SystemMemberCache managed resource type */
  @Immutable
  public static final ManagedResourceType SYSTEM_MEMBER_CACHE =
      new ManagedResourceType("SystemMemberCache", org.apache.geode.admin.SystemMemberCache.class,
          3);

  /** SystemMemberCache managed resource type */
  @Immutable
  public static final ManagedResourceType SYSTEM_MEMBER_REGION = new ManagedResourceType(
      "SystemMemberRegion", org.apache.geode.admin.SystemMemberRegion.class, 4);

  /** SystemMemberCacheServer managed resource type */
  @Immutable
  public static final ManagedResourceType SYSTEM_MEMBER_CACHE_SERVER = new ManagedResourceType(
      "SystemMemberCacheServer", org.apache.geode.admin.SystemMemberCacheServer.class, 5);

  /** CacheVm managed resource type */
  @Immutable
  public static final ManagedResourceType CACHE_VM =
      new ManagedResourceType("CacheVm", org.apache.geode.admin.CacheVm.class, 6);

  /** StatisticResource managed resource type */
  @Immutable
  public static final ManagedResourceType STATISTIC_RESOURCE =
      new ManagedResourceType("StatisticResource", org.apache.geode.admin.StatisticResource.class,
          7);

  @Immutable
  public static final ManagedResourceType GEMFIRE_HEALTH =
      new ManagedResourceType("GemFireHealth", org.apache.geode.admin.GemFireHealth.class, 8);

  @Immutable
  public static final ManagedResourceType DISTRIBUTED_SYSTEM_HEALTH_CONFIG =
      new ManagedResourceType("DistributedSystemHealthConfig",
          org.apache.geode.admin.DistributedSystemHealthConfig.class, 9);

  @Immutable
  public static final ManagedResourceType GEMFIRE_HEALTH_CONFIG = new ManagedResourceType(
      "GemFireHealthConfig", org.apache.geode.admin.GemFireHealthConfig.class, 10);

  @Immutable
  public static final ManagedResourceType DISTRIBUTION_LOCATOR = new ManagedResourceType(
      "DistributionLocator", org.apache.geode.admin.DistributionLocator.class, 11);

  //////////////////// Instance Fields ////////////////////

  /** The display-friendly name of this managed resource type. */
  private final transient String name;

  /**
   * The interface/class used to externally represent this type. Note: this must match the mbean
   * type descriptor in mbeans-descriptors.xml.
   */
  private final transient Class clazz;

  public final int ordinal;

  @Immutable
  private static final ManagedResourceType[] VALUES =
      {AGENT, DISTRIBUTED_SYSTEM, SYSTEM_MEMBER, SYSTEM_MEMBER_CACHE, SYSTEM_MEMBER_REGION,
          SYSTEM_MEMBER_CACHE_SERVER, CACHE_VM, STATISTIC_RESOURCE, GEMFIRE_HEALTH,
          DISTRIBUTED_SYSTEM_HEALTH_CONFIG, GEMFIRE_HEALTH_CONFIG, DISTRIBUTION_LOCATOR};

  private Object readResolve() throws java.io.ObjectStreamException {
    return VALUES[ordinal]; // Canonicalize
  }

  /** Creates a new instance of ManagedResourceType. */
  private ManagedResourceType(String name, Class clazz, int ordinal) {
    this.name = name;
    this.clazz = clazz;
    this.ordinal = ordinal;
  }

  /** Returns the ManagedResourceType represented by specified ordinal */
  public static ManagedResourceType fromOrdinal(int ordinal) {
    return VALUES[ordinal];
  }

  /** Returns the display-friendly name of this managed resource type */
  public String getName() {
    return name;
  }

  /** Returns the interface/class used to externally represent this type */
  public Class getClassType() {
    return clazz;
  }

  /**
   * Returns the fully qualified name of the interface/class used to externally represent this type
   */
  public String getClassTypeName() {
    return clazz.getName();
  }

  /** Returns true if this is <code>AGENT</code>. */
  public boolean isAgent() {
    return equals(AGENT);
  }

  /** Returns true if this is <code>DISTRIBUTED_SYSTEM</code>. */
  public boolean isDistributedSystem() {
    return equals(DISTRIBUTED_SYSTEM);
  }

  /** Returns true if this is <code>SYSTEM_MEMBER</code>. */
  public boolean isSystemMember() {
    return equals(SYSTEM_MEMBER);
  }

  /** Returns whether this is <code>STATISTIC_RESOURCE</code>. */
  public boolean isStatisticResource() {
    return equals(STATISTIC_RESOURCE);
  }

  /** Return whether this is <code>GEMFIRE_HEALTH</code>. */
  public boolean isGemFireHealth() {
    return equals(GEMFIRE_HEALTH);
  }

  /**
   * Returns a string representation for this type.
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
    if (!(other instanceof ManagedResourceType)) {
      return false;
    }
    final ManagedResourceType that = (ManagedResourceType) other;

    if (!StringUtils.equals(name, that.name)) {
      return false;
    }
    return clazz == that.clazz || clazz != null && clazz.equals(that.clazz);
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

    result = mult * result + (name == null ? 0 : name.hashCode());
    result = mult * result + (clazz == null ? 0 : clazz.hashCode());

    return result;
  }

}

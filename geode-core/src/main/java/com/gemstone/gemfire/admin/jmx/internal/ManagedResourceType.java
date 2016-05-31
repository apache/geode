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
package com.gemstone.gemfire.admin.jmx.internal;

/**
 * Type-safe definition for ModelMBean managed resources.  The class type 
 * ({@link #getClassTypeName}) must match the fully qualified class name listed
 * in the type descriptor in mbeans-descriptors.xml.
 *
 * @since GemFire     3.5
 *
 */
public class ManagedResourceType implements java.io.Serializable {
  private static final long serialVersionUID = 3752874768667480449L;
  
  /** Agent managed resource type */
  public static final ManagedResourceType AGENT = 
      new ManagedResourceType("Agent", com.gemstone.gemfire.admin.jmx.Agent.class);

  /** DistributedSystem managed resource type */
  public static final ManagedResourceType DISTRIBUTED_SYSTEM = 
      new ManagedResourceType("AdminDistributedSystem", com.gemstone.gemfire.admin.AdminDistributedSystem.class);

  /** SystemMember managed resource type */
  public static final ManagedResourceType SYSTEM_MEMBER = 
      new ManagedResourceType("SystemMember", com.gemstone.gemfire.admin.SystemMember.class);

  /** SystemMemberCache managed resource type */
  public static final ManagedResourceType SYSTEM_MEMBER_CACHE = 
      new ManagedResourceType("SystemMemberCache", com.gemstone.gemfire.admin.SystemMemberCache.class);

  /** SystemMemberCache managed resource type */
  public static final ManagedResourceType SYSTEM_MEMBER_REGION = 
      new ManagedResourceType("SystemMemberRegion", com.gemstone.gemfire.admin.SystemMemberRegion.class);

  /** SystemMemberCacheServer managed resource type */
  public static final ManagedResourceType SYSTEM_MEMBER_CACHE_SERVER = 
      new ManagedResourceType("SystemMemberCacheServer", com.gemstone.gemfire.admin.SystemMemberCacheServer.class);

  /** CacheVm managed resource type */
  public static final ManagedResourceType CACHE_VM = 
      new ManagedResourceType("CacheVm", com.gemstone.gemfire.admin.CacheVm.class);

  /** StatisticResource managed resource type */
  public static final ManagedResourceType STATISTIC_RESOURCE = 
      new ManagedResourceType("StatisticResource", com.gemstone.gemfire.admin.StatisticResource.class);

  public static final ManagedResourceType GEMFIRE_HEALTH = 
      new ManagedResourceType("GemFireHealth", com.gemstone.gemfire.admin.GemFireHealth.class);

  public static final ManagedResourceType DISTRIBUTED_SYSTEM_HEALTH_CONFIG = 
      new ManagedResourceType("DistributedSystemHealthConfig", com.gemstone.gemfire.admin.DistributedSystemHealthConfig.class);

  public static final ManagedResourceType GEMFIRE_HEALTH_CONFIG = 
      new ManagedResourceType("GemFireHealthConfig", com.gemstone.gemfire.admin.GemFireHealthConfig.class);

  public static final ManagedResourceType DISTRIBUTION_LOCATOR = 
      new ManagedResourceType("DistributionLocator", com.gemstone.gemfire.admin.DistributionLocator.class);

  ////////////////////  Instance Fields  ////////////////////

  /** The display-friendly name of this managed resource type. */
  private final transient String name;
  
  /** 
   * The interface/class used to externally represent this type. Note: this must 
   * match the mbean type descriptor in mbeans-descriptors.xml.
   */
  private final transient Class clazz;
  
  // The 4 declarations below are necessary for serialization
  /** int used as ordinal to represent this Scope */
  public final int ordinal = nextOrdinal++;

  private static int nextOrdinal = 0;
  
  private static final ManagedResourceType[] VALUES =
    { AGENT, DISTRIBUTED_SYSTEM, SYSTEM_MEMBER,
      SYSTEM_MEMBER_CACHE, SYSTEM_MEMBER_REGION,
      SYSTEM_MEMBER_CACHE_SERVER, CACHE_VM,
      STATISTIC_RESOURCE, GEMFIRE_HEALTH, DISTRIBUTED_SYSTEM_HEALTH_CONFIG, 
      GEMFIRE_HEALTH_CONFIG, DISTRIBUTION_LOCATOR 
    };

  private Object readResolve() throws java.io.ObjectStreamException {
    return VALUES[ordinal];  // Canonicalize
  }
  
  /** Creates a new instance of ManagedResourceType. */
  private ManagedResourceType(String name, Class clazz) {
    this.name = name;
    this.clazz = clazz;
  }
    
  /** Returns the ManagedResourceType represented by specified ordinal */
  public static ManagedResourceType fromOrdinal(int ordinal) {
    return VALUES[ordinal];
  }

  /** Returns the display-friendly name of this managed resource type */
  public String getName() {
    return this.name;
  }
  
  /** Returns the interface/class used to externally represent this type */
  public Class getClassType() {
    return this.clazz;
  }
  
  /** 
   * Returns the fully qualified name of the interface/class used to externally 
   * represent this type 
   */
  public String getClassTypeName() {
    return this.clazz.getName();
  }
  
  /** Returns true if this is <code>AGENT</code>. */
  public boolean isAgent() {
    return this.equals(AGENT);
  }
    
  /** Returns true if this is <code>DISTRIBUTED_SYSTEM</code>. */
  public boolean isDistributedSystem() {
    return this.equals(DISTRIBUTED_SYSTEM);
  }
    
  /** Returns true if this is <code>SYSTEM_MEMBER</code>. */
  public boolean isSystemMember() {
    return this.equals(SYSTEM_MEMBER);
  }
    
  /** Returns whether this is <code>STATISTIC_RESOURCE</code>. */
  public boolean isStatisticResource() {
    return this.equals(STATISTIC_RESOURCE);
  }
    
  /** Return whether this is <code>GEMFIRE_HEALTH</code>. */
  public boolean isGemFireHealth() {
    return this.equals(GEMFIRE_HEALTH);
  }

  /** 
   * Returns a string representation for this type.
   */
  @Override
  public String toString() {
      return this.name;
  }
  
	/**
	 * Indicates whether some other object is "equal to" this one.
	 *
	 * @param  other  the reference object with which to compare.
	 * @return true if this object is the same as the obj argument;
	 *         false otherwise.
	 */
  @Override
	public boolean equals(Object other) {
		if (other == this) return true;
		if (other == null) return false;
		if (!(other instanceof ManagedResourceType)) return  false;
		final ManagedResourceType that = (ManagedResourceType) other;

		if (this.name != that.name &&
	  		!(this.name != null &&
	  		this.name.equals(that.name))) return false;
		if (this.clazz != that.clazz &&
	  		!(this.clazz != null &&
	  		this.clazz.equals(that.clazz))) return false;

		return true;
	}

	/**
	 * Returns a hash code for the object. This method is supported for the
	 * benefit of hashtables such as those provided by java.util.Hashtable.
	 *
	 * @return the integer 0 if description is null; otherwise a unique integer.
	 */
  @Override
	public int hashCode() {
		int result = 17;
		final int mult = 37;

		result = mult * result + 
			(this.name == null ? 0 : this.name.hashCode());
		result = mult * result + 
			(this.clazz == null ? 0 : this.clazz.hashCode());

		return result;
	}

}


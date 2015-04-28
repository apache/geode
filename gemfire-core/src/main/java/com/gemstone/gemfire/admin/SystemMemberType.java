/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.admin;

//import java.io.*;

/**
 * Type-safe definition for system members.
 *
 * @author    Kirk Lund
 * @since     3.5
 *
 * @deprecated as of 7.0 use the {@link com.gemstone.gemfire.management} package instead
 */
public class SystemMemberType implements java.io.Serializable {
  private static final long serialVersionUID = 3284366994485749302L;
    
  /** GemFire shared-memory manager connected to the distributed system */
  public static final SystemMemberType MANAGER = 
      new SystemMemberType("GemFireManager");

  /** Application connected to the distributed system */
  public static final SystemMemberType APPLICATION = 
      new SystemMemberType("Application");

  /** GemFire Cache VM connected to the distributed system */
  public static final SystemMemberType CACHE_VM =
    new SystemMemberType("CacheVm");

  /** GemFire Cache Server connected to the distributed system
   * @deprecated as of 5.7 use {@link #CACHE_VM} instead.
   */
  @Deprecated
  public static final SystemMemberType CACHE_SERVER = CACHE_VM;


  /** The display-friendly name of this system member type. */
  private final transient String name;
  
  // The 4 declarations below are necessary for serialization
  /** int used as ordinal to represent this Scope */
  public final int ordinal = nextOrdinal++;

  private static int nextOrdinal = 0;
  
  private static final SystemMemberType[] VALUES =
    { MANAGER, APPLICATION, CACHE_VM };

  private Object readResolve() throws java.io.ObjectStreamException {
    return VALUES[ordinal];  // Canonicalize
  }
  
  /** Creates a new instance of SystemMemberType. */
  private SystemMemberType(String name) {
    this.name = name;
  }
    
  /** Return the SystemMemberType represented by specified ordinal */
  public static SystemMemberType fromOrdinal(int ordinal) {
    return VALUES[ordinal];
  }

  public String getName() {
    return this.name;
  }
  
  /** Return whether this is <code>MANAGER</code>. */
  public boolean isManager() {
    return this.equals(MANAGER);
  }
    
  /** Return whether this is <code>APPLICATION</code>. */
  public boolean isApplication() {
    return this.equals(APPLICATION);
  }

  /** Return whether this is <code>CACHE_SERVER</code>.
   * @deprecated as of 5.7 use {@link #isCacheVm} instead.
   */
  @Deprecated
  public boolean isCacheServer() {
    return isCacheVm();
  }
  /** Return whether this is <code>CACHE_VM</code>.
   */
  public boolean isCacheVm() {
    return this.equals(CACHE_VM);
  }
    
  /** 
   * Returns a string representation for this system member type.
   *
   * @return the name of this system member type
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
		if (!(other instanceof SystemMemberType)) return  false;
		final SystemMemberType that = (SystemMemberType) other;
		if (this.ordinal != that.ordinal) return false;
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
		result = mult * result + this.ordinal;
		return result;
	}
  
}


/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.admin.jmx.internal;

/**
 * Type-safe definition for refresh notifications.
 *
 * @author    Kirk Lund
 * @since     3.5
 *
 */
public class RefreshNotificationType implements java.io.Serializable {
  private static final long serialVersionUID = 4376763592395613794L;
    
  /** Notify StatisticResource to refresh statistics */
  public static final RefreshNotificationType STATISTIC_RESOURCE_STATISTICS = 
      new RefreshNotificationType(
          "GemFire.Timer.StatisticResource.statistics.refresh", 
          "refresh");

  /** Notify SystemMember to refresh config */
  public static final RefreshNotificationType SYSTEM_MEMBER_CONFIG = 
      new RefreshNotificationType(
          "GemFire.Timer.SystemMember.config.refresh", 
          "refresh");

  /** Notification type for the javax.management.Notification */
  private final transient String type;
  
  /** Notification msg for the javax.management.Notification */
  private final transient String msg;
  
  // The 4 declarations below are necessary for serialization
  /** int used as ordinal to represent this Scope */
  public final int ordinal = nextOrdinal++;

  private static int nextOrdinal = 0;
  
  private static final RefreshNotificationType[] VALUES =
    { STATISTIC_RESOURCE_STATISTICS, SYSTEM_MEMBER_CONFIG };

  private Object readResolve() throws java.io.ObjectStreamException {
    return VALUES[ordinal];  // Canonicalize
  }
  
  /** Creates a new instance of RefreshNotificationType. */
  private RefreshNotificationType(String type, String msg) {
    this.type = type;
    this.msg = msg;
  }
    
  /** Return the RefreshNotificationType represented by specified ordinal */
  public static RefreshNotificationType fromOrdinal(int ordinal) {
    return VALUES[ordinal];
  }

	public String getType() {
		return this.type;
	}

	public String getMessage() {
		return this.msg;
	}
  
  /** 
   * Returns a string representation for this notification type.
   *
   * @return the type string for this Notification
   */
  @Override
  public String toString() {
      return this.type;
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
		if (!(other instanceof RefreshNotificationType)) return  false;
		final RefreshNotificationType that = (RefreshNotificationType) other;

		if (this.type != that.type &&
	  		!(this.type != null &&
	  		this.type.equals(that.type))) return false;
		if (this.msg != that.msg &&
	  		!(this.msg != null &&
	  		this.msg.equals(that.msg))) return false;

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
			(this.type == null ? 0 : this.type.hashCode());
		result = mult * result + 
			(this.msg == null ? 0 : this.msg.hashCode());

		return result;
	}
  
}


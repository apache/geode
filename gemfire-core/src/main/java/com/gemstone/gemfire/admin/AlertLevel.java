/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.admin;

import com.gemstone.gemfire.internal.admin.Alert;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

/**
 * Type-safe enumeration for {@link com.gemstone.gemfire.admin.Alert
 * Alert} level.
 *
 * @author    Kirk Lund
 * @since     3.5
 * @deprecated as of 7.0 use the {@link com.gemstone.gemfire.management} package instead
 */
public class AlertLevel implements java.io.Serializable {
  private static final long serialVersionUID = -4752438966587392126L;
    
  public static final AlertLevel WARNING =
    new AlertLevel(Alert.WARNING, "WARNING");
  public static final AlertLevel ERROR = 
    new AlertLevel(Alert.ERROR, "ERROR");
  public static final AlertLevel SEVERE =
    new AlertLevel(Alert.SEVERE, "SEVERE");
  
  public static final AlertLevel OFF =
    new AlertLevel(Alert.OFF, "OFF");

  /** The severity level of this AlertLevel. Greater is more severe. */
  private final transient int severity;
  
  /** The name of this AlertLevel. */
  private final transient String name;
  
  // The 4 declarations below are necessary for serialization
  /** int used as ordinal to represent this AlertLevel */
  public final int ordinal = nextOrdinal++;

  private static int nextOrdinal = 0;
  
  private static final AlertLevel[] VALUES =
    { WARNING, ERROR, SEVERE, OFF };

  private Object readResolve() throws java.io.ObjectStreamException {
    return VALUES[ordinal];  // Canonicalize
  }
  
  /** Creates a new instance of AlertLevel. */
  private AlertLevel(int severity, String name) {
    this.severity = severity;
    this.name = name;
  }
    
  /** Return the AlertLevel represented by specified ordinal */
  public static AlertLevel fromOrdinal(int ordinal) {
    return VALUES[ordinal];
  }

  /**
   * Returns the <code>AlertLevel</code> for the given severity
   *
   * @throws IllegalArgumentException
   *         If there is no alert level with the given
   *         <code>severity</code> 
   */
  public static AlertLevel forSeverity(int severity) {
    switch (severity) {
    case Alert.WARNING:
      return AlertLevel.WARNING;
    case Alert.ERROR:
      return AlertLevel.ERROR;
    case Alert.SEVERE:
      return AlertLevel.SEVERE;
    case Alert.OFF:
      return AlertLevel.OFF;
    default:
      throw new IllegalArgumentException(LocalizedStrings.AlertLevel_UNKNOWN_ALERT_SEVERITY_0.toLocalizedString(Integer.valueOf(severity)));
    }
  }

  /**
   * Returns the <code>AlertLevel</code> with the given name
   *
   * @throws IllegalArgumentException
   *         If there is no alert level named <code>name</code>
   */
  public static AlertLevel forName(String name) {
    for (int i = 0; i < VALUES.length; i++) {
      AlertLevel level = VALUES[i];
      if (level.getName().equalsIgnoreCase(name)) {
        return level;
      }
    }

    throw new IllegalArgumentException(LocalizedStrings.AlertLevel_THERE_IS_NO_ALERT_LEVEL_0.toLocalizedString(name));
  }

  public int getSeverity() {
    return this.severity;
  }
  
  public String getName() {
    return this.name;
  }

  public static AlertLevel[] values() {
    return VALUES;
  }
  
  /** 
   * Returns a string representation for this alert level.
   *
   * @return the name of this alert level
   */
  @Override
  public String toString() {
    return this.name /* + "=" + this.severity */;
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
		if (!(other instanceof AlertLevel)) return  false;
		final AlertLevel that = (AlertLevel) other;

		if (this.severity != that.severity) return false;
		if (this.name != that.name &&
	  		!(this.name != null &&
	  		this.name.equals(that.name))) return false;

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

		result = mult * result + this.severity;
		result = mult * result + 
			(this.name == null ? 0 : this.name.hashCode());

		return result;
	}

}

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

import org.apache.geode.internal.admin.Alert;

/**
 * Type-safe enumeration for {@link org.apache.geode.admin.Alert Alert} level.
 *
 * @since GemFire 3.5
 * @deprecated as of 7.0 use the <code><a href=
 *             "{@docRoot}/org/apache/geode/management/package-summary.html">management</a></code>
 *             package instead
 */
public class AlertLevel implements java.io.Serializable {
  private static final long serialVersionUID = -4752438966587392126L;

  public static final AlertLevel WARNING = new AlertLevel(Alert.WARNING, "WARNING");
  public static final AlertLevel ERROR = new AlertLevel(Alert.ERROR, "ERROR");
  public static final AlertLevel SEVERE = new AlertLevel(Alert.SEVERE, "SEVERE");

  public static final AlertLevel OFF = new AlertLevel(Alert.OFF, "OFF");

  /** The severity level of this AlertLevel. Greater is more severe. */
  private final transient int severity;

  /** The name of this AlertLevel. */
  private final transient String name;

  // The 4 declarations below are necessary for serialization
  /** int used as ordinal to represent this AlertLevel */
  public final int ordinal = nextOrdinal++;

  private static int nextOrdinal = 0;

  private static final AlertLevel[] VALUES = {WARNING, ERROR, SEVERE, OFF};

  private Object readResolve() throws java.io.ObjectStreamException {
    return VALUES[ordinal]; // Canonicalize
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
   * @throws IllegalArgumentException If there is no alert level with the given
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
        throw new IllegalArgumentException(String.format("Unknown alert severity: %s",
            Integer.valueOf(severity)));
    }
  }

  /**
   * Returns the <code>AlertLevel</code> with the given name
   *
   * @throws IllegalArgumentException If there is no alert level named <code>name</code>
   */
  public static AlertLevel forName(String name) {
    for (int i = 0; i < VALUES.length; i++) {
      AlertLevel level = VALUES[i];
      if (level.getName().equalsIgnoreCase(name)) {
        return level;
      }
    }

    throw new IllegalArgumentException(
        String.format("There is no alert level %s", name));
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
   * @param other the reference object with which to compare.
   * @return true if this object is the same as the obj argument; false otherwise.
   */
  @Override
  public boolean equals(Object other) {
    if (other == this)
      return true;
    if (other == null)
      return false;
    if (!(other instanceof AlertLevel))
      return false;
    final AlertLevel that = (AlertLevel) other;

    if (this.severity != that.severity)
      return false;
    if (this.name != null && !this.name.equals(that.name))
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

    result = mult * result + this.severity;
    result = mult * result + (this.name == null ? 0 : this.name.hashCode());

    return result;
  }

}

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
 * Type-safe definition for refresh notifications.
 *
 * @since GemFire 3.5
 *
 */
@Immutable
public class RefreshNotificationType implements java.io.Serializable {
  private static final long serialVersionUID = 4376763592395613794L;

  private static final String REFRESH = "refresh";
  /** Notify StatisticResource to refresh statistics */
  @Immutable
  public static final RefreshNotificationType STATISTIC_RESOURCE_STATISTICS =
      new RefreshNotificationType("GemFire.Timer.StatisticResource.statistics.refresh", REFRESH,
          0);

  /** Notify SystemMember to refresh config */
  @Immutable
  public static final RefreshNotificationType SYSTEM_MEMBER_CONFIG =
      new RefreshNotificationType("GemFire.Timer.SystemMember.config.refresh", REFRESH, 1);

  /** Notification type for the javax.management.Notification */
  private final transient String type;

  /** Notification msg for the javax.management.Notification */
  private final transient String msg;

  public final int ordinal;

  @Immutable
  private static final RefreshNotificationType[] VALUES =
      {STATISTIC_RESOURCE_STATISTICS, SYSTEM_MEMBER_CONFIG};

  private Object readResolve() throws java.io.ObjectStreamException {
    return VALUES[ordinal]; // Canonicalize
  }

  /** Creates a new instance of RefreshNotificationType. */
  private RefreshNotificationType(String type, String msg, int ordinal) {
    this.type = type;
    this.msg = msg;
    this.ordinal = ordinal;
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
    if (!(other instanceof RefreshNotificationType)) {
      return false;
    }
    final RefreshNotificationType that = (RefreshNotificationType) other;

    if (!StringUtils.equals(this.type, that.type)) {
      return false;
    }
    if (!StringUtils.equals(this.msg, that.msg)) {
      return false;
    }

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

    result = mult * result + (this.type == null ? 0 : this.type.hashCode());
    result = mult * result + (this.msg == null ? 0 : this.msg.hashCode());

    return result;
  }

}

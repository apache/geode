/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.parser;

import java.lang.reflect.Method;

import com.gemstone.gemfire.management.cli.CliMetaData;
import com.gemstone.gemfire.management.cli.CliMetaData.AvailabilityMetadata;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;

/**
 * Used for checking availability of a command
 *
 * @author Nikhil Jadhav
 * @since 7.0
 */
public class AvailabilityTarget {

  private final Object target;
  private final Method method;
  private final String availabilityDescription;

  public AvailabilityTarget(Object target, Method method) {
    this.target = target;
    this.method = method;
    AvailabilityMetadata availabilityMetadata = this.method.getAnnotation(CliMetaData.AvailabilityMetadata.class);
    String specifiedAvailabilityDesc = CliStrings.AVAILABILITYTARGET_MSG_DEFAULT_UNAVAILABILITY_DESCRIPTION;
    if (availabilityMetadata != null) {
      specifiedAvailabilityDesc = availabilityMetadata.availabilityDescription();
    }
    this.availabilityDescription = specifiedAvailabilityDesc;
  }

  public Object getTarget() {
    return target;
  }

  public Method getMethod() {
    return method;
  }

  public String getAvailabilityDescription() {
    return availabilityDescription;
  }

  @Override
  public int hashCode() {
    final int prime = 17;
    int result = 8;
    result = prime * result + ((target == null) ? 0 : target.hashCode());
    result = prime * result + ((method == null) ? 0 : method.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    AvailabilityTarget availabilityTarget = (AvailabilityTarget) obj;
    if (target == null) {
      if (availabilityTarget.getTarget() != null) {
        return false;
      }
    } else if (!target.equals(availabilityTarget.getTarget())) {
      return false;
    }
    if (method == null) {
      if (availabilityTarget.getMethod() != null) {
        return false;
      }
    } else if (!method.equals(availabilityTarget.getMethod())) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append(AvailabilityTarget.class.getSimpleName());
    builder.append("[" + "target=" + target);
    builder.append(",method=" + method);
    builder.append("]");
    return builder.toString();
  }
}

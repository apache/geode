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
package org.apache.geode.management.internal.cli.parser;

import java.lang.reflect.Method;

import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.CliMetaData.AvailabilityMetadata;
import org.apache.geode.management.internal.cli.i18n.CliStrings;

/**
 * Used for checking availability of a command
 *
 * @since GemFire 7.0
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

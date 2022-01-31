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
package org.apache.geode.internal.serialization.filter;

import static java.util.Collections.unmodifiableCollection;
import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.exception.ExceptionUtils.getRootCause;

import java.lang.reflect.InvocationTargetException;
import java.util.Collection;

/**
 * Implementation of {@code GlobalSerialFilter} that delegates to {@code ObjectInputFilterApi} to
 * maintain independence from the JRE version.
 */
class ReflectiveFacadeGlobalSerialFilter implements GlobalSerialFilter {

  private final ObjectInputFilterApi api;
  private final String pattern;
  private final Collection<String> sanctionedClasses;

  /**
   * Constructs instance with the specified collaborators.
   */
  ReflectiveFacadeGlobalSerialFilter(ObjectInputFilterApi api, String pattern,
      Collection<String> sanctionedClasses) {
    this.api = requireNonNull(api, "ObjectInputFilterApi is required");
    this.pattern = pattern;
    this.sanctionedClasses = unmodifiableCollection(sanctionedClasses);
  }

  /**
   * Invokes interface-defined operation to set this as the process-wide filter.
   */
  @Override
  public void setFilter() throws UnableToSetSerialFilterException {
    try {
      // create the ObjectInputFilter to set as the global serial filter
      Object objectInputFilter = api.createObjectInputFilterProxy(pattern, sanctionedClasses);

      // set the global serial filter
      api.setSerialFilter(objectInputFilter);

    } catch (IllegalAccessException | InvocationTargetException e) {
      handleExceptionThrownByApi(e);
    }
  }

  @Override
  public String toString() {
    return new StringBuilder(getClass().getSimpleName())
        .append('{')
        .append("api=").append(api)
        .append(", pattern='").append(pattern).append('\'')
        .append('}')
        .toString();
  }

  private void handleExceptionThrownByApi(ReflectiveOperationException e)
      throws UnableToSetSerialFilterException {
    String causeClassName = e.getCause() == null ? getClassName(e) : getClassName(e.getCause());
    switch (causeClassName) {
      case "java.lang.IllegalAccessException":
        throw new UnableToSetSerialFilterException(
            "Unable to configure a global serialization filter using reflection.",
            e);
      case "java.lang.reflect.InvocationTargetException":
        if (getRootCause(e).getClass().getName().equals("java.lang.IllegalStateException")) {
          // ObjectInputFilter throws IllegalStateException
          // if the filter has already been set non-null
          throw new FilterAlreadyConfiguredException(
              "Unable to configure a global serialization filter because filter has already been set non-null",
              e);
        }
        throw new UnableToSetSerialFilterException(
            "Unable to configure a global serialization filter because invocation target threw "
                + causeClassName,
            e);
      default:
        throw new UnableToSetSerialFilterException(
            "Unable to configure a global serialization filter.",
            e);
    }
  }

  private static String getClassName(Throwable throwable) {
    return throwable.getClass().getName();
  }
}

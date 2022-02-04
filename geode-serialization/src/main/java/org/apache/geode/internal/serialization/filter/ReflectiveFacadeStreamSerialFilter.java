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

import java.io.ObjectInputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;

import org.jetbrains.annotations.TestOnly;

/**
 * Implementation of {@code ObjectInputFilter} that delegates to {@code ObjectInputFilterApi} to
 * maintain independence from the JRE version.
 */
class ReflectiveFacadeStreamSerialFilter implements StreamSerialFilter {

  private final ObjectInputFilterApi api;
  private final String pattern;
  private final Collection<String> sanctionedClasses;

  /**
   * Constructs instance with the specified collaborators.
   */
  ReflectiveFacadeStreamSerialFilter(ObjectInputFilterApi api, String pattern,
      Collection<String> sanctionedClasses) {
    this.api = requireNonNull(api, "ObjectInputFilterApi is required");
    this.pattern = pattern;
    this.sanctionedClasses = unmodifiableCollection(sanctionedClasses);
  }

  /**
   * Invokes interface-defined operation to set this serialization filter on the specified target
   * {@code ObjectInputStream}.
   */
  @Override
  public void setFilterOn(ObjectInputStream objectInputStream)
      throws UnableToSetSerialFilterException {
    try {
      // create the ObjectInputFilter to set as the global serial filter
      Object objectInputFilter = api.createObjectInputFilterProxy(pattern, sanctionedClasses);

      // set the global serial filter
      api.setObjectInputFilter(objectInputStream, objectInputFilter);

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

  @TestOnly
  ObjectInputFilterApi getObjectInputFilterApi() {
    return api;
  }

  private void handleExceptionThrownByApi(ReflectiveOperationException e)
      throws UnableToSetSerialFilterException {
    String className = getClassName(e);
    switch (className) {
      case "java.lang.IllegalAccessException":
        throw new UnableToSetSerialFilterException(
            "Unable to configure an input stream serialization filter using reflection.",
            e);
      case "java.lang.reflect.InvocationTargetException":
        if (getRootCause(e) instanceof IllegalStateException) {
          // ObjectInputFilter throws IllegalStateException
          // if the filter has already been set non-null
          throw new FilterAlreadyConfiguredException(
              "Unable to configure an input stream serialization filter because a non-null filter has already been set.",
              e);
        }
        String causeClassName = e.getCause() == null ? getClassName(e) : getClassName(e.getCause());
        throw new UnableToSetSerialFilterException(
            "Unable to configure an input stream serialization filter because invocation target threw "
                + causeClassName + ".",
            e);
      default:
        throw new UnableToSetSerialFilterException(
            "Unable to configure an input stream serialization filter.",
            e);
    }
  }

  private static String getClassName(Throwable throwable) {
    return throwable.getClass().getName();
  }
}

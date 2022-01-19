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
package org.apache.geode.examples.security;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Method;
import java.util.Set;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.query.security.MethodInvocationAuthorizer;
import org.apache.geode.cache.query.security.RestrictedMethodAuthorizer;

@SuppressWarnings("unused")
public class ExampleAnnotationBasedMethodInvocationAuthorizer
    implements MethodInvocationAuthorizer {
  private RestrictedMethodAuthorizer defaultAuthorizer;

  /**
   * @param cache the {@link Cache} to which the MethodInvocationAuthorizer will belong
   * @param parameters a {@link Set} of {@link java.lang.String} that will be used to configure the
   */
  @Override
  public void initialize(Cache cache, Set<String> parameters) {
    // Register the default authorizer.
    defaultAuthorizer = new RestrictedMethodAuthorizer(cache);
  }

  /**
   * @param method the {@link Method} that should be authorized.
   * @param target the {@link Object} on which the {@link Method} will be executed.
   * @return {@code true} if the method is annotated with
   *         {@link ExampleAnnotationBasedMethodInvocationAuthorizer.Authorized} and not permanently
   *         forbidden, {@code false} otherwise
   */
  @Override
  public boolean authorize(Method method, Object target) {
    // Check if forbidden by default.
    if (defaultAuthorizer.isPermanentlyForbiddenMethod(method, target)) {
      return false;
    }

    // Check if annotation is present
    return method.isAnnotationPresent(Authorized.class);
  }

  @Target(ElementType.METHOD)
  @Retention(RetentionPolicy.RUNTIME)
  @interface Authorized {
  }
}

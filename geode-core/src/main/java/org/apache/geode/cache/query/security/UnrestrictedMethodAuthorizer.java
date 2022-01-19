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
package org.apache.geode.cache.query.security;

import java.lang.reflect.Method;
import java.util.Objects;
import java.util.Properties;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.Region;

/**
 * An immutable and thread-safe {@link MethodInvocationAuthorizer} that allows any method execution
 * as long as the target object does not belong to a Geode package, or does belong but it's marked
 * as safe (see {@link RestrictedMethodAuthorizer#isAllowedGeodeMethod(Method, Object)}).
 * <p/>
 *
 * Some known dangerous methods, like {@link Object#getClass()}, are also rejected by this
 * authorizer implementation, no matter whether the target object belongs to Geode or not
 * (see {@link RestrictedMethodAuthorizer#isPermanentlyForbiddenMethod(Method, Object)}).
 * <p/>
 *
 * This authorizer implementation addresses only three of the four known security risks:
 * {@code Java Reflection}, {@code Cache Modification} and {@code Region Modification}.
 * <p/>
 *
 * The {@code Region Entry Modification} security risk still exists: users with the
 * {@code DATA:READ:RegionName} privilege will be able to execute ANY method (even mutators) on the
 * objects stored within the region and on instances used as bind parameters of the OQL, so this
 * authorizer implementation must be used with extreme care.
 * <p/>
 *
 * Usage of this authorizer implementation is only recommended for secured clusters on which only
 * trusted users and applications have access to the OQL engine. It might also be used on clusters
 * on which the entries stored are immutable.
 * <p/>
 *
 * @see org.apache.geode.cache.Cache
 * @see org.apache.geode.cache.query.security.MethodInvocationAuthorizer
 * @see org.apache.geode.cache.query.security.RestrictedMethodAuthorizer
 */
public final class UnrestrictedMethodAuthorizer implements MethodInvocationAuthorizer {
  static final String NULL_CACHE_MESSAGE = "Cache should be provided to configure the authorizer.";
  static final String NULL_AUTHORIZER_MESSAGE =
      "RestrictedMethodAuthorizer should be provided to create this authorizer.";
  private static final String GEODE_BASE_PACKAGE = "org.apache.geode";
  private final RestrictedMethodAuthorizer restrictedMethodAuthorizer;

  /**
   * Creates a {@code UnrestrictedMethodAuthorizer} object and initializes it so it can be safely
   * used in a multi-threaded environment.
   * <p/>
   *
   * Applications can use this constructor as part of the initialization for custom authorizers
   * (see {@link Declarable#initialize(Cache, Properties)}), when using a declarative approach.
   *
   * @param cache the {@code Cache} instance that owns this authorizer, required in order to
   *        configure the default {@link RestrictedMethodAuthorizer}.
   */
  public UnrestrictedMethodAuthorizer(Cache cache) {
    Objects.requireNonNull(cache, NULL_CACHE_MESSAGE);
    restrictedMethodAuthorizer = new RestrictedMethodAuthorizer(cache);
  }

  /**
   * Creates a {@code UnrestrictedMethodAuthorizer} object and initializes it so it can be safely
   * used in a multi-threaded environment.
   * <p/>
   *
   * @param restrictedMethodAuthorizer the default {@code RestrictedMethodAuthorizer} to use.
   */
  public UnrestrictedMethodAuthorizer(RestrictedMethodAuthorizer restrictedMethodAuthorizer) {
    Objects.requireNonNull(restrictedMethodAuthorizer, NULL_AUTHORIZER_MESSAGE);
    this.restrictedMethodAuthorizer = restrictedMethodAuthorizer;
  }

  /**
   * Executes the authorization logic to determine whether the {@code method} is allowed to be
   * executed on the {@code target} object instance.
   * If the {@code target} object is an instance of {@link Region}, this methods also ensures that
   * the user has the {@code DATA:READ} permission granted for the target {@link Region}.
   * <p/>
   *
   * @param method the {@link Method} that should be authorized.
   * @param target the {@link Object} on which the {@link Method} will be executed.
   * @return {@code true} if the {@code method} can be executed on on the {@code target} instance,
   *         {@code false} otherwise.
   *
   * @see org.apache.geode.cache.query.security.MethodInvocationAuthorizer
   */
  @Override
  public boolean authorize(Method method, Object target) {
    // Return false for known dangerous methods.
    if (restrictedMethodAuthorizer.isPermanentlyForbiddenMethod(method, target)) {
      return false;
    }

    // Return true for non Geode classes.
    String packageName = target.getClass().getPackage().getName().toLowerCase();
    if (!packageName.startsWith(GEODE_BASE_PACKAGE)) {
      return true;
    }

    // Delegate to the default authorizer.
    return restrictedMethodAuthorizer.isAllowedGeodeMethod(method, target);
  }
}

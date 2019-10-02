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
import java.util.Collections;
import java.util.Iterator;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Declarable;

/**
 * An immutable and thread-safe {@link MethodInvocationAuthorizer} that allows any method execution
 * that follows the design patterns for accessor methods described in the JavaBean specification
 * 1.01, that is, any method whose name begins with "get" or "is". For additional security, only
 * methods belonging to classes in user-specified packages will be allowed. If a method does not
 * match the user-specified parameters, then the {@link RestrictedMethodAuthorizer} will make the
 * final decision
 *
 * <p/>
 *
 * Some known dangerous methods, like {@link Object#getClass()}, are also rejected by this
 * authorizer implementation (see
 * {@link RestrictedMethodAuthorizer#isKnownDangerousMethod(Method, Object)}).
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

public class JavaBeanAccessorMethodAuthorizer implements MethodInvocationAuthorizer {
  static final String NULL_PACKAGE_MESSAGE =
      "A set of allowed packages should be provided to configure the authorizer.";
  static final String NULL_AUTHORIZER_MESSAGE =
      "RestrictedMethodAuthorizer should be provided to create this authorizer.";
  static final String NULL_CACHE_MESSAGE = "Cache should be provided to configure the authorizer.";
  private final RestrictedMethodAuthorizer restrictedMethodAuthorizer;
  private final Set<String> allowedPackages;

  /**
   * Creates a {@code JavaBeanAccessorMethodAuthorizer} object and initializes it so it can be
   * safely used in a multi-threaded environment.
   * <p/>
   *
   * Applications can use this constructor as part of the initialization for custom authorizers
   * (see {@link Declarable#initialize(Cache, Properties)}), when using a declarative approach.
   *
   * @param cache the {@code Cache} instance that owns this authorizer, required in order to
   *        configure the default {@link RestrictedMethodAuthorizer}.
   * @param allowedPackages the packages containing classes for which 'is' and 'get' methods will
   *        be authorized.
   */
  public JavaBeanAccessorMethodAuthorizer(Cache cache, Set<String> allowedPackages) {
    Objects.requireNonNull(cache, NULL_CACHE_MESSAGE);
    Objects.requireNonNull(allowedPackages, NULL_PACKAGE_MESSAGE);
    this.restrictedMethodAuthorizer = new RestrictedMethodAuthorizer(cache);
    this.allowedPackages = Collections.unmodifiableSet(allowedPackages);
  }

  /**
   * Creates a {@code JavaBeanAccessorMethodAuthorizer} object and initializes it so it can be
   * safely used in a multi-threaded environment.
   * <p/>
   *
   * @param restrictedMethodAuthorizer the default {@code RestrictedMethodAuthorizer} to use.
   * @param allowedPackages the packages containing classes for which 'is' and 'get' methods will
   *        be authorized.
   */
  public JavaBeanAccessorMethodAuthorizer(RestrictedMethodAuthorizer restrictedMethodAuthorizer,
      Set<String> allowedPackages) {
    Objects.requireNonNull(restrictedMethodAuthorizer, NULL_AUTHORIZER_MESSAGE);
    Objects.requireNonNull(allowedPackages, NULL_PACKAGE_MESSAGE);
    this.restrictedMethodAuthorizer = restrictedMethodAuthorizer;
    this.allowedPackages = Collections.unmodifiableSet(allowedPackages);
  }

  /**
   * Executes the authorization logic to determine whether the {@code method} is allowed to be
   * executed on the {@code target} object instance.
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
    String methodName = method.getName().toLowerCase();
    String packageName = target.getClass().getPackage().getName().toLowerCase();

    // Return false for known dangerous methods.
    if (restrictedMethodAuthorizer.isKnownDangerousMethod(method, target)) {
      return false;
    }

    boolean matches = false;

    if ((methodName.startsWith("get") || methodName.startsWith("is"))) {
      Iterator<String> iterator = this.allowedPackages.iterator();
      while (iterator.hasNext() && !matches) {
        matches = iterator.next().startsWith(packageName);
      }
    }
    // Return true if we found a match, otherwise, delegate to the default authorizer.
    return matches || restrictedMethodAuthorizer.authorize(method, target);
  }

  /**
   * Returns an unmodifiable view of the allowed packages for this authorizer.
   * This method can be used to get "read-only" access to the set containing the packages
   * specified as allowed on construction of this authorizer.
   *
   * @return an unmodifiable view of the allowed packages for this authorizer.
   */
  Set<String> getAllowedPackages() {
    return allowedPackages;
  }

}

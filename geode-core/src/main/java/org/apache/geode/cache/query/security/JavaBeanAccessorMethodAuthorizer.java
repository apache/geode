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
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.Region;

/**
 * An immutable and thread-safe {@link MethodInvocationAuthorizer} that allows any method execution
 * that follows the design patterns for accessor methods described in the JavaBean specification
 * 1.01; that is, any method whose name begins with 'get' or 'is'. For additional security, only
 * methods belonging to classes in user-specified packages will be allowed. If a method does not
 * match the user-specified parameters, or belongs to the 'org.apache.geode' package, then the
 * decision of whether to authorize or not will be delegated to the default
 * {@link RestrictedMethodAuthorizer}.
 * <p/>
 *
 * Some known dangerous methods, like {@link Object#getClass()}, are also rejected by this
 * authorizer implementation (see
 * {@link RestrictedMethodAuthorizer#isPermanentlyForbiddenMethod(Method, Object)}).
 * <p/>
 *
 * When used as intended, with all region entries and OQL bind parameters following the JavaBean
 * specification 1.01, this authorizer implementation addresses all four of the known security
 * risks: {@code Java Reflection}, {@code Cache Modification}, {@code Region Modification} and
 * {@code Region Entry Modification}.
 * <p/>
 *
 * It should be noted that the {@code Region Entry Modification} security risk still potentially
 * exists: users with the {@code DATA:READ:RegionName} privilege will be able to execute any
 * method whose name starts with 'is' or 'get' on the objects stored within the region and on
 * instances used as bind parameters of the OQL, providing they are in the specified packages.
 * If those methods do not fully follow the JavaBean 1.01 specification that accessors do not
 * modify the instance's state then entry modifications are possible.
 * <p/>
 *
 * Usage of this authorizer implementation is only recommended for secured clusters on which the
 * Operator has full confidence that all objects stored in regions and used as OQL bind parameters
 * follow JavaBean specification 1.01. It might also be used on clusters on which the entries
 * stored are immutable.
 * <p/>
 *
 * @see org.apache.geode.cache.Cache
 * @see org.apache.geode.cache.query.security.MethodInvocationAuthorizer
 * @see org.apache.geode.cache.query.security.RestrictedMethodAuthorizer
 */

public final class JavaBeanAccessorMethodAuthorizer implements MethodInvocationAuthorizer {
  static final String NULL_PACKAGE_MESSAGE =
      "A set of allowed packages should be provided to configure the authorizer.";
  static final String NULL_AUTHORIZER_MESSAGE =
      "RestrictedMethodAuthorizer should be provided to create this authorizer.";
  static final String NULL_CACHE_MESSAGE = "Cache should be provided to configure the authorizer.";
  static final String GEODE_BASE_PACKAGE = "org.apache.geode";

  private static final Pattern pattern = Pattern.compile("^(get|is)($|[A-Z])+.*");

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
    restrictedMethodAuthorizer = new RestrictedMethodAuthorizer(cache);
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
   * If the {@code target} object is an instance of {@link Region}, this methods also ensures that
   * the user has the {@code DATA:READ} permission granted for the target {@link Region}.
   * </p>
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

    String packageName = target.getClass().getPackage().getName();

    // If the target object belongs to the 'org.apache.geode' package, delegate to the default
    // authorizer.
    if (packageName.startsWith(GEODE_BASE_PACKAGE)) {
      return restrictedMethodAuthorizer.isAllowedGeodeMethod(method, target);
    }

    String methodName = method.getName();

    // Return true if the the object belongs to an allowed package and the method starts with
    // exactly 'get' or 'is' followed by a non-lowercase character (to prevent matching with
    // methods that might have names like 'getawayDate' or 'islandName' etc.)
    if (pattern.matcher(methodName).matches()) {
      if (allowedPackages.stream().anyMatch(packageName::startsWith)) {
        return true;
      }
    }

    // Delegate to the default authorizer if none of the above criteria are met.
    return restrictedMethodAuthorizer.authorize(method, target);
  }

  /**
   * Returns an unmodifiable view of the allowed packages for this authorizer.
   * This method can be used to get "read-only" access to the set containing the packages
   * specified as allowed on construction of this authorizer.
   *
   * @return an unmodifiable view of the allowed packages for this authorizer.
   */
  public Set<String> getAllowedPackages() {
    return allowedPackages;
  }
}

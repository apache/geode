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
import java.util.HashSet;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.Region;

/**
 * An immutable and thread-safe {@link MethodInvocationAuthorizer} that only allows the execution of
 * those methods matching the configured regular expression.
 * <p/>
 *
 * Some known dangerous methods, like {@link Object#getClass()}, are also rejected by this
 * authorizer implementation, no matter whether the method matches the configured regular
 * expressions
 * or not (see {@link RestrictedMethodAuthorizer#isPermanentlyForbiddenMethod(Method, Object)}).
 * <p/>
 *
 * When correctly configured, this authorizer implementation addresses the four known security
 * risks: {@code Java Reflection}, {@code Cache Modification}, {@code Region Modification} and
 * {@code Region Entry Modification}.
 * <p/>
 *
 * For the above statement to remain true, however, the regular expressions used must be
 * exhaustively studied and configured so no mutator methods match. If the regular expressions are
 * not restrictive enough, the {@code Region Entry Modification} security risk still exists: users
 * with the {@code DATA:READ:RegionName} privileges will be able to execute methods (even those
 * modifying the entry) on the objects stored within the region and on instances used as bind
 * parameters of the query, so this authorizer must be used with extreme care.
 * <p/>
 *
 * Usage of this authorizer implementation is only recommended for scenarios on which the user or
 * operator knows exactly what code is deployed to the cluster, how and when; allowing a correct
 * configuration of the regular expressions. It might also be used on clusters on which the entries
 * stored are immutable.
 * <p/>
 *
 * @see org.apache.geode.cache.Cache
 * @see org.apache.geode.cache.query.security.MethodInvocationAuthorizer
 * @see org.apache.geode.cache.query.security.RestrictedMethodAuthorizer
 */
public final class RegExMethodAuthorizer implements MethodInvocationAuthorizer {
  private static final String GEODE_BASE_PACKAGE = "org.apache.geode";
  static final String NULL_CACHE_MESSAGE = "Cache should be provided to configure this authorizer.";
  static final String NULL_AUTHORIZER_MESSAGE =
      "RestrictedMethodAuthorizer should be provided to create this authorizer.";
  static final String NULL_REGULAR_EXPRESSIONS_MESSAGE =
      "A set of regular expression should be provided to configure this authorizer.";
  private final Set<String> allowedPatterns;
  private final Set<Pattern> compiledPatterns;
  private final RestrictedMethodAuthorizer restrictedMethodAuthorizer;

  private Set<Pattern> compilePatterns() {
    Set<Pattern> patterns = new HashSet<>();
    allowedPatterns.forEach(regEx -> patterns.add(Pattern.compile(regEx)));

    return patterns;
  }

  /**
   * Returns an unmodifiable view of the regular expressions used to configure this authorizer.
   * This method can be used to get "read-only" access to the set containing the regular expressions
   * that will be used to determine whether a method is allowed or not.
   *
   * @return an unmodifiable view of the regular expressions used to configure this authorizer.
   */
  public Set<String> getAllowedPatterns() {
    return allowedPatterns;
  }

  /**
   * Creates a {@code RegExMethodAuthorizer} object and initializes it so it can be safely used
   * in a multi-threaded environment.
   * <p/>
   *
   * Applications can use this constructor as part of the initialization for custom authorizers
   * (see {@link Declarable#initialize(Cache, Properties)}, when using a declarative approach.
   *
   * @param cache the {@code Cache} instance that owns this authorizer, required in order to
   *        configure the default {@link RestrictedMethodAuthorizer}.
   * @param allowedPatterns the regular expressions that will be used to determine whether a method
   *        is authorized or not.
   */
  public RegExMethodAuthorizer(Cache cache, Set<String> allowedPatterns) {
    Objects.requireNonNull(cache, NULL_CACHE_MESSAGE);
    Objects.requireNonNull(allowedPatterns, NULL_REGULAR_EXPRESSIONS_MESSAGE);

    this.allowedPatterns = Collections.unmodifiableSet(allowedPatterns);
    compiledPatterns = Collections.unmodifiableSet(compilePatterns());
    restrictedMethodAuthorizer = new RestrictedMethodAuthorizer(cache);
  }

  /**
   * Creates a {@code RegExMethodAuthorizer} object and initializes it so it can be safely used
   * in a multi-threaded environment.
   * <p/>
   *
   * @param restrictedMethodAuthorizer the default {@code RestrictedMethodAuthorizer} to use.
   * @param allowedPatterns the regular expressions that will be used to determine whether a method
   *        is authorized or not.
   */
  public RegExMethodAuthorizer(RestrictedMethodAuthorizer restrictedMethodAuthorizer,
      Set<String> allowedPatterns) {
    Objects.requireNonNull(allowedPatterns, NULL_REGULAR_EXPRESSIONS_MESSAGE);
    Objects.requireNonNull(restrictedMethodAuthorizer, NULL_AUTHORIZER_MESSAGE);

    this.allowedPatterns = Collections.unmodifiableSet(allowedPatterns);
    compiledPatterns = Collections.unmodifiableSet(compilePatterns());
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

    // If the target object belongs to Geode, make sure the method is considered safe.
    if (target.getClass().getPackage().getName().startsWith(GEODE_BASE_PACKAGE)) {
      return restrictedMethodAuthorizer.isAllowedGeodeMethod(method, target);
    }

    // Compare fully qualified method name against compiled expressions.
    String fullyQualifiedMethodName = target.getClass().getName() + "." + method.getName();
    if (compiledPatterns.stream()
        .anyMatch(pattern -> pattern.matcher(fullyQualifiedMethodName).matches())) {
      return true;
    }

    // Delegate to the default authorizer.
    return restrictedMethodAuthorizer.authorize(method, target);
  }
}

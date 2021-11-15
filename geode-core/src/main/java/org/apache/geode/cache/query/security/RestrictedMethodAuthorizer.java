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
import java.sql.Timestamp;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.internal.QRegion;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.security.DefaultSecurityServiceFactory;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.internal.security.SecurityServiceFactory;
import org.apache.geode.security.NotAuthorizedException;
import org.apache.geode.security.ResourcePermission;

/**
 * The default, immutable and thread-safe {@link MethodInvocationAuthorizer} used by Geode to
 * determine whether a {@link java.lang.reflect.Method} is allowed to be executed on a specific
 * {@link java.lang.Object} instance.
 * <p/>
 *
 * This authorizer addresses the four known security risks: {@code Java Reflection},
 * {@code Cache Modification}, {@code Region Modification} and {@code Region Entry Modification}.
 * <p/>
 *
 * Custom applications can delegate to this class and use it as the starting point for providing
 * use case specific authorizers.
 *
 * @see org.apache.geode.cache.Cache
 * @see org.apache.geode.cache.query.security.MethodInvocationAuthorizer
 */
public final class RestrictedMethodAuthorizer implements MethodInvocationAuthorizer {
  public static final String UNAUTHORIZED_STRING = "Unauthorized access to method: ";
  @Immutable
  static final Set<String> FORBIDDEN_METHODS =
      Collections.unmodifiableSet(createForbiddenList());
  @Immutable
  static final Map<String, Set<Class>> GEODE_ALLOWED_METHODS =
      Collections.unmodifiableMap(createGeodeAcceptanceList());
  @Immutable
  static final Map<String, Set<Class>> DEFAULT_ALLOWED_METHODS =
      Collections.unmodifiableMap(createDefaultAcceptanceList());
  final SecurityService securityService;
  private final Set<String> forbiddenMethods;
  private final Map<String, Set<Class>> allowedMethodsPerClass;
  private final Map<String, Set<Class>> allowedGeodeMethodsPerClass;

  private static Set<String> createForbiddenList() {
    Set<String> forbiddenList = new HashSet<>();

    // Reflection Calls
    forbiddenList.add("getClass");

    // Serialization Calls
    forbiddenList.add("readObject");
    forbiddenList.add("readResolve");
    forbiddenList.add("readObjectNoData");
    forbiddenList.add("writeObject");
    forbiddenList.add("writeReplace");

    return forbiddenList;
  }

  private static Map<String, Set<Class>> createGeodeAcceptanceList() {
    Map<String, Set<Class>> acceptanceListMap = new HashMap<>();

    Set<Class> objectCallers = new HashSet<>();
    objectCallers.add(Object.class);
    objectCallers = Collections.unmodifiableSet(objectCallers);
    acceptanceListMap.put("equals", objectCallers);
    acceptanceListMap.put("toString", objectCallers);

    Set<Class> entryCallers = new HashSet<>();
    entryCallers.add(Region.Entry.class);
    entryCallers = Collections.unmodifiableSet(entryCallers);
    acceptanceListMap.put("getKey", entryCallers);
    acceptanceListMap.put("getValue", entryCallers);

    Set<Class> regionCallers = new HashSet<>();
    regionCallers.add(Region.class);
    regionCallers.add(QRegion.class);
    regionCallers = Collections.unmodifiableSet(regionCallers);
    acceptanceListMap.put("containsKey", regionCallers);
    acceptanceListMap.put("entrySet", regionCallers);
    acceptanceListMap.put("get", regionCallers);
    acceptanceListMap.put("keySet", regionCallers);
    acceptanceListMap.put("values", regionCallers);
    acceptanceListMap.put("getEntries", regionCallers);
    acceptanceListMap.put("getValues", regionCallers);

    return acceptanceListMap;
  }

  private static Map<String, Set<Class>> createDefaultAcceptanceList() {
    Map<String, Set<Class>> acceptanceListMap = new HashMap<>();

    Set<Class> objectCallers = new HashSet<>();
    objectCallers.add(Object.class);
    objectCallers = Collections.unmodifiableSet(objectCallers);
    acceptanceListMap.put("compareTo", objectCallers);
    acceptanceListMap.put("equals", objectCallers);
    acceptanceListMap.put("toString", objectCallers);

    Set<Class> booleanCallers = new HashSet<>();
    booleanCallers.add(Boolean.class);
    booleanCallers = Collections.unmodifiableSet(booleanCallers);
    acceptanceListMap.put("booleanValue", booleanCallers);

    Set<Class> numericCallers = new HashSet<>();
    numericCallers.add(Number.class);
    numericCallers = Collections.unmodifiableSet(numericCallers);
    acceptanceListMap.put("byteValue", numericCallers);
    acceptanceListMap.put("doubleValue", numericCallers);
    acceptanceListMap.put("floatValue", numericCallers);
    acceptanceListMap.put("intValue", numericCallers);
    acceptanceListMap.put("longValue", numericCallers);
    acceptanceListMap.put("shortValue", numericCallers);

    Set<Class> dateCallers = new HashSet<>();
    dateCallers.add(Date.class);
    dateCallers = Collections.unmodifiableSet(dateCallers);
    acceptanceListMap.put("after", dateCallers);
    acceptanceListMap.put("before", dateCallers);
    acceptanceListMap.put("getTime", dateCallers);

    Set<Class> timestampCallers = new HashSet<>();
    timestampCallers.add(Timestamp.class);
    timestampCallers = Collections.unmodifiableSet(timestampCallers);
    acceptanceListMap.put("getNanos", timestampCallers);

    Set<Class> stringCallers = new HashSet<>();
    stringCallers.add(String.class);
    stringCallers = Collections.unmodifiableSet(stringCallers);
    acceptanceListMap.put("charAt", stringCallers);
    acceptanceListMap.put("codePointAt", stringCallers);
    acceptanceListMap.put("codePointBefore", stringCallers);
    acceptanceListMap.put("codePointCount", stringCallers);
    acceptanceListMap.put("compareToIgnoreCase", stringCallers);
    acceptanceListMap.put("concat", stringCallers);
    acceptanceListMap.put("contains", stringCallers);
    acceptanceListMap.put("contentEquals", stringCallers);
    acceptanceListMap.put("endsWith", stringCallers);
    acceptanceListMap.put("equalsIgnoreCase", stringCallers);
    acceptanceListMap.put("getBytes", stringCallers);
    acceptanceListMap.put("hashCode", stringCallers);
    acceptanceListMap.put("indexOf", stringCallers);
    acceptanceListMap.put("intern", stringCallers);
    acceptanceListMap.put("isEmpty", stringCallers);
    acceptanceListMap.put("lastIndexOf", stringCallers);
    acceptanceListMap.put("length", stringCallers);
    acceptanceListMap.put("matches", stringCallers);
    acceptanceListMap.put("offsetByCodePoints", stringCallers);
    acceptanceListMap.put("regionMatches", stringCallers);
    acceptanceListMap.put("replace", stringCallers);
    acceptanceListMap.put("replaceAll", stringCallers);
    acceptanceListMap.put("replaceFirst", stringCallers);
    acceptanceListMap.put("split", stringCallers);
    acceptanceListMap.put("startsWith", stringCallers);
    acceptanceListMap.put("substring", stringCallers);
    acceptanceListMap.put("toCharArray", stringCallers);
    acceptanceListMap.put("toLowerCase", stringCallers);
    acceptanceListMap.put("toUpperCase", stringCallers);
    acceptanceListMap.put("trim", stringCallers);

    Set<Class> mapEntryCallers = new HashSet<>();
    mapEntryCallers.add(Map.Entry.class);
    mapEntryCallers = Collections.unmodifiableSet(mapEntryCallers);
    acceptanceListMap.put("getKey", mapEntryCallers);
    acceptanceListMap.put("getValue", mapEntryCallers);

    Set<Class> regionCallers = new HashSet<>();
    regionCallers.add(Map.class);
    regionCallers.add(QRegion.class);
    regionCallers = Collections.unmodifiableSet(regionCallers);
    acceptanceListMap.put("containsKey", regionCallers);
    acceptanceListMap.put("entrySet", regionCallers);
    acceptanceListMap.put("get", regionCallers);
    acceptanceListMap.put("keySet", regionCallers);
    acceptanceListMap.put("values", regionCallers);
    acceptanceListMap.put("getEntries", regionCallers);
    acceptanceListMap.put("getValues", regionCallers);

    return acceptanceListMap;
  }

  /**
   * Returns an unmodifiable view of the methods disallowed by default.
   * This method can be used to get "read-only" access to the set containing the methods that
   * are considered non safe by default.
   *
   * @return an unmodifiable view of the default disallowed methods.
   */
  Set<String> getForbiddenMethods() {
    return forbiddenMethods;
  }

  /**
   * Returns an unmodifiable view of the default allowed methods.
   * This method can be used to get "read-only" access to the map containing the default set
   * of allowed methods per class.
   *
   * @return an unmodifiable view of the default allowed methods per class map.
   */
  Map<String, Set<Class>> getAllowedMethodsPerClass() {
    return allowedMethodsPerClass;
  }

  /**
   * Returns an unmodifiable view of the default Geode allowed methods.
   * This method can be used to get "read-only" access to the map containing the default set
   * of allowed geode methods per class.
   *
   * @return an unmodifiable view of the default allowed geode methods per class map.
   */
  Map<String, Set<Class>> getAllowedGeodeMethodsPerClass() {
    return allowedGeodeMethodsPerClass;
  }

  /**
   * Creates a {@code RestrictedMethodAuthorizer} object and initializes it so it can be safely
   * used in a multi-threaded environment.
   * <p/>
   *
   * If the {@link Cache} instance passed as parameter was previously created by Geode, the
   * authorizer will use the security service already configured in order to determine whether a
   * specific user has read privileges upon a particular region.
   * If the {@link Cache} instance passed as parameter is a wrapper created by external frameworks,
   * the authorizer will create a new instance of the security service using the configuration
   * properties used to initialize the cache.
   * <p/>
   *
   * Applications can also use this constructor as part of the initialization for custom authorizers
   * (see {@link Declarable#initialize(Cache, Properties)}), when using a declarative approach.
   *
   * @param cache the {@code Cache} instance that owns this authorizer, required in order to
   *        configure the security rules used.
   */
  public RestrictedMethodAuthorizer(Cache cache) {
    Objects.requireNonNull(cache, "Cache should be provided to configure the authorizer.");

    // Set the correct SecurityService.
    if (cache instanceof InternalCache) {
      // Use the already created SecurityService.
      this.securityService = ((InternalCache) cache).getSecurityService();
    } else {
      // Create the SecurityService using the distributed system properties.
      Objects.requireNonNull(cache.getDistributedSystem(),
          "Distributed system properties should be provided to configure the authorizer.");
      SecurityServiceFactory securityServiceFactory = new DefaultSecurityServiceFactory();
      this.securityService =
          securityServiceFactory.create(cache.getDistributedSystem().getSecurityProperties());
    }

    this.forbiddenMethods = FORBIDDEN_METHODS;
    this.allowedMethodsPerClass = DEFAULT_ALLOWED_METHODS;
    this.allowedGeodeMethodsPerClass = GEODE_ALLOWED_METHODS;
  }

  private boolean isAllowedByDefault(Method method, Object target) {
    String methodName = method.getName();
    Set<Class> allowedClasses = allowedMethodsPerClass.get(methodName);

    if (allowedClasses == null) {
      return false;
    }

    for (Class<?> clazz : allowedClasses) {
      if (clazz.isAssignableFrom(target.getClass())) {
        return true;
      }
    }

    return false;
  }

  private void authorizeRegionAccess(SecurityService securityService, Object target) {
    if (target instanceof Region) {
      String regionName = ((Region) target).getName();
      securityService.authorize(ResourcePermission.Resource.DATA, ResourcePermission.Operation.READ,
          regionName);
    }
  }

  /**
   * Executes the verification logic to determine whether the {@code target} object instance belongs
   * to Geode and whether the {@code method} on the {@code target} object instance is considered
   * to be safe according to Geode security rules.
   * If the {@code target} object is an instance of {@link Region}, this methods also ensures that
   * the user has the {@code DATA:READ} permission granted for the target {@link Region}.
   * <p/>
   *
   * @param method the {@link Method} that should be verified.
   * @param target the {@link Object} on which the {@link Method} will be executed.
   * @return {@code true} if and only if the {@code target} object instance belongs to Geode and
   *         the {@code method} is considered safe to be executed on the {@code target} object
   *         instance according to the Geode security rules, {@code false} otherwise.
   */
  public boolean isAllowedGeodeMethod(Method method, Object target) {
    String methodName = method.getName();
    Set<Class> allowedGeodeClassesForMethod = allowedGeodeMethodsPerClass.get(methodName);

    if (allowedGeodeClassesForMethod == null) {
      return false;
    }

    for (Class<?> clazz : allowedGeodeClassesForMethod) {
      if (clazz.isAssignableFrom(target.getClass())) {
        try {
          authorizeRegionAccess(securityService, target);
          return true;
        } catch (NotAuthorizedException noAuthorizedException) {
          return false;
        }
      }
    }

    return false;
  }

  /**
   * Executes the verification logic to determine whether the {@code method} on the {@code target}
   * object instance is considered to be non safe according to Geode security rules.
   * <p/>
   *
   * The following methods are currently considered non safe, no matter what the {@code target}
   * object is:
   * <p>
   * <ul>
   * <li>{@code getClass}
   * <li>{@code readObject}
   * <li>{@code readResolve}
   * <li>{@code readObjectNoData}
   * <li>{@code writeObject}
   * <li>{@code writeReplace}
   * </ul>
   * <p>
   *
   * @param method the {@link Method} that should be verified.
   * @param target the {@link Object} on which the {@link Method} will be executed.
   * @return {@code true} if the {@code method} is considered non safe to be executed on the
   *         {@code target} instance according to the Geode security rules, {@code false} otherwise.
   */
  public boolean isPermanentlyForbiddenMethod(Method method,
      @SuppressWarnings("unused") Object target) {
    return forbiddenMethods.contains(method.getName());
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
    if (!isAllowedByDefault(method, target)) {
      return false;
    }

    try {
      authorizeRegionAccess(securityService, target);
    } catch (NotAuthorizedException noAuthorizedException) {
      return false;
    }

    return true;
  }
}

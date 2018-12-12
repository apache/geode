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
package org.apache.geode.cache.query.internal;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.geode.cache.Region;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.security.NotAuthorizedException;
import org.apache.geode.security.ResourcePermission;

public class RestrictedMethodInvocationAuthorizer implements MethodInvocationAuthorizer {

  public static final String UNAUTHORIZED_STRING = "Unauthorized access to method: ";

  protected static final HashMap<String, Set> DEFAULT_ACCEPTLIST = createAcceptList();

  private SecurityService securityService;

  // List of methods that can be invoked by
  private final HashMap<String, Set> acceptListedMethodsToClass;


  public RestrictedMethodInvocationAuthorizer(SecurityService securityService) {
    this.securityService = securityService;
    acceptListedMethodsToClass = DEFAULT_ACCEPTLIST;
  }

  private static HashMap<String, Set> createAcceptList() {
    HashMap<String, Set> acceptListMap = new HashMap();
    Set<Class> objectCallers = new HashSet();
    objectCallers.add(Object.class);
    acceptListMap.put("toString", objectCallers);
    acceptListMap.put("equals", objectCallers);
    acceptListMap.put("compareTo", objectCallers);

    Set<Class> booleanCallers = new HashSet();
    booleanCallers.add(Boolean.class);
    acceptListMap.put("booleanValue", booleanCallers);

    Set<Class> numericCallers = new HashSet();
    numericCallers.add(Number.class);
    acceptListMap.put("byteValue", numericCallers);
    acceptListMap.put("intValue", numericCallers);
    acceptListMap.put("doubleValue", numericCallers);
    acceptListMap.put("floatValue", numericCallers);
    acceptListMap.put("longValue", numericCallers);
    acceptListMap.put("shortValue", numericCallers);

    Set<Class> mapCallers = new HashSet();
    mapCallers.add(Collection.class);
    mapCallers.add(Map.class);
    acceptListMap.put("get", mapCallers);
    acceptListMap.put("entrySet", mapCallers);
    acceptListMap.put("keySet", mapCallers);
    acceptListMap.put("values", mapCallers);
    acceptListMap.put("getEntries", mapCallers);
    acceptListMap.put("getValues", mapCallers);
    acceptListMap.put("containsKey", mapCallers);

    Set<Class> mapEntryCallers = new HashSet();
    mapEntryCallers.add(Map.Entry.class);
    acceptListMap.put("getKey", mapEntryCallers);
    acceptListMap.put("getValue", mapEntryCallers);

    Set<Class> dateCallers = new HashSet<>();
    dateCallers.add(Date.class);
    acceptListMap.put("after", dateCallers);
    acceptListMap.put("before", dateCallers);
    acceptListMap.put("getNanos", dateCallers);
    acceptListMap.put("getTime", dateCallers);

    Set<Class> stringCallers = new HashSet<>();
    stringCallers.add(String.class);
    acceptListMap.put("charAt", stringCallers);
    acceptListMap.put("codePointAt", stringCallers);
    acceptListMap.put("codePointBefore", stringCallers);
    acceptListMap.put("codePointCount", stringCallers);
    acceptListMap.put("compareToIgnoreCase", stringCallers);
    acceptListMap.put("concat", stringCallers);
    acceptListMap.put("contains", stringCallers);
    acceptListMap.put("contentEquals", stringCallers);
    acceptListMap.put("endsWith", stringCallers);
    acceptListMap.put("equalsIgnoreCase", stringCallers);
    acceptListMap.put("getBytes", stringCallers);
    acceptListMap.put("hashCode", stringCallers);
    acceptListMap.put("indexOf", stringCallers);
    acceptListMap.put("intern", stringCallers);
    acceptListMap.put("isEmpty", stringCallers);
    acceptListMap.put("lastIndexOf", stringCallers);
    acceptListMap.put("length", stringCallers);
    acceptListMap.put("matches", stringCallers);
    acceptListMap.put("offsetByCodePoints", stringCallers);
    acceptListMap.put("regionMatches", stringCallers);
    acceptListMap.put("replace", stringCallers);
    acceptListMap.put("replaceAll", stringCallers);
    acceptListMap.put("replaceFirst", stringCallers);
    acceptListMap.put("split", stringCallers);
    acceptListMap.put("startsWith", stringCallers);
    acceptListMap.put("substring", stringCallers);
    acceptListMap.put("toCharArray", stringCallers);
    acceptListMap.put("toLowerCase", stringCallers);
    acceptListMap.put("toUpperCase", stringCallers);
    acceptListMap.put("trim", stringCallers);

    return acceptListMap;
  }

  protected HashMap<String, Set> getAcceptList() {
    return acceptListedMethodsToClass;
  }

  boolean isAcceptlisted(Method method) {
    String methodName = method.getName();

    Set<Class> allowedClasses = acceptListedMethodsToClass.get(methodName);
    if (allowedClasses == null) {
      return false;
    }
    for (Class clazz : allowedClasses) {
      if (clazz.isAssignableFrom(method.getDeclaringClass())) {
        return true;
      }
    }
    return false;
  }

  @Override
  public void authorizeMethodInvocation(Method method, Object target) {
    if (!isAcceptlisted(method)) {
      throw new NotAuthorizedException(UNAUTHORIZED_STRING + method.getName());
    }
    authorizeRegionAccess(securityService, target);
  }

  private void authorizeRegionAccess(SecurityService securityService, Object target) {
    if (target instanceof Region) {
      String regionName = ((Region) target).getName();
      securityService.authorize(ResourcePermission.Resource.DATA, ResourcePermission.Operation.READ,
          regionName);
    }
  }
}

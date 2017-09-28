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

import java.lang.reflect.Member;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.internal.index.DummyQRegion;
import org.apache.geode.internal.cache.EntrySnapshot;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.security.NotAuthorizedException;
import org.apache.geode.security.ResourcePermission;

public class RestrictedMethodInvocationAuthorizer implements MethodInvocationAuthorizer {

  public static final String UNAUTHORIZED_STRING = "Unauthorized access to method: ";

  private SecurityService securityService;

  // List of methods that can be invoked by
  private final HashMap<String, Set> whiteListedMethodsToClass;


  public RestrictedMethodInvocationAuthorizer(SecurityService securityService) {
    this.securityService = securityService;
    whiteListedMethodsToClass = createWhiteList();
  }

  private HashMap<String, Set> createWhiteList() {
    HashMap<String, Set> whiteListMap = new HashMap();
    Set<Class> objectCallers = new HashSet();
    objectCallers.add(Object.class);
    whiteListMap.put("toString", objectCallers);
    whiteListMap.put("equals", objectCallers);
    whiteListMap.put("compareTo", objectCallers);

    Set<Class> booleanCallers = new HashSet();
    booleanCallers.add(Boolean.class);
    whiteListMap.put("booleanValue", booleanCallers);

    Set<Class> numericCallers = new HashSet();
    numericCallers.add(Number.class);
    whiteListMap.put("byteValue", numericCallers);
    whiteListMap.put("intValue", numericCallers);
    whiteListMap.put("doubleValue", numericCallers);
    whiteListMap.put("floatValue", numericCallers);
    whiteListMap.put("longValue", numericCallers);
    whiteListMap.put("shortValue", numericCallers);

    Set<Class> mapCallers = new HashSet();
    mapCallers.add(Collection.class);
    mapCallers.add(Map.class);
    whiteListMap.put("entrySet", mapCallers);
    whiteListMap.put("keySet", mapCallers);
    whiteListMap.put("values", mapCallers);
    whiteListMap.put("getEntries", mapCallers);
    whiteListMap.put("getValues", mapCallers);
    whiteListMap.put("containsKey", mapCallers);

    Set<Class> mapEntryCallers = new HashSet();
    mapEntryCallers.add(Map.Entry.class);
    whiteListMap.put("getKey", mapEntryCallers);
    whiteListMap.put("getValue", mapEntryCallers);

    Set<Class> dateCallers = new HashSet<>();
    dateCallers.add(Date.class);
    whiteListMap.put("after", dateCallers);
    whiteListMap.put("before", dateCallers);
    whiteListMap.put("getNanos", dateCallers);
    whiteListMap.put("getTime", dateCallers);

    Set<Class> stringCallers = new HashSet<>();
    stringCallers.add(String.class);
    whiteListMap.put("toLowerCase", stringCallers);
    whiteListMap.put("toUpperCase", stringCallers);
    return whiteListMap;
  }

  boolean isWhitelisted(Method method) {
    String methodName = method.getName();

    Set<Class> allowedClasses = whiteListedMethodsToClass.get(methodName);
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
    if (!isWhitelisted(method)) {
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

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
import java.math.BigDecimal;
import java.math.BigInteger;
import java.security.Security;
import java.sql.Timestamp;
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
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.security.NotAuthorizedException;
import org.apache.geode.security.ResourcePermission;

public class MethodInvocationAuthorizer {

  public static final String UNAUTHORIZED_STRING = "Unauthorized access to method: ";
  // List of methods that can be invoked by
  public static final Set<String> whiteListedMethods = new HashSet<>();
  public static final HashMap<String, Set> whiteListedClassMethods = new HashMap<>();
  static {
    whiteListedMethods.add("toString");
    whiteListedMethods.add("equals");
    whiteListedMethods.add("compareTo");

    Set<String> booleanMethods = new HashSet<>();
    booleanMethods.add("booleanValue");
    whiteListedClassMethods.put(Boolean.class.getName(), booleanMethods);

    Set<String> numberMethods = new HashSet<>();
    numberMethods.add("byteValue");
    numberMethods.add("doubleValue");
    numberMethods.add("intValue");
    numberMethods.add("floatValue");
    numberMethods.add("longValue");
    numberMethods.add("shortValue");
    whiteListedClassMethods.put(Byte.class.getName(), numberMethods);
    whiteListedClassMethods.put(Double.class.getName(), numberMethods);
    whiteListedClassMethods.put(Float.class.getName(), numberMethods);
    whiteListedClassMethods.put(Integer.class.getName(), numberMethods);
    whiteListedClassMethods.put(Short.class.getName(), numberMethods);
    whiteListedClassMethods.put(Number.class.getName(), numberMethods);
    whiteListedClassMethods.put(BigDecimal.class.getName(), numberMethods);
    whiteListedClassMethods.put(BigInteger.class.getName(), numberMethods);
    whiteListedClassMethods.put(AtomicInteger.class.getName(), numberMethods);
    whiteListedClassMethods.put(AtomicLong.class.getName(), numberMethods);

    Set<String> dateMethods = new HashSet<>();
    dateMethods.add("after");
    dateMethods.add("before");
    whiteListedClassMethods.put(Date.class.getName(), dateMethods);

    Set<String> timestampMethods = new HashSet<>();
    timestampMethods.add("after");
    timestampMethods.add("before");
    timestampMethods.add("getNanos");
    timestampMethods.add("getTime");
    whiteListedClassMethods.put(Timestamp.class.getName(), timestampMethods);

    Set<String> mapMethods = new HashSet<>();
    mapMethods.add("entrySet");
    mapMethods.add("keySet");
    mapMethods.add("values");
    mapMethods.add("getEntries");
    mapMethods.add("getValues");
    mapMethods.add("containsKey");
    whiteListedClassMethods.put(QRegion.class.getName(), mapMethods);
    whiteListedClassMethods.put(DummyQRegion.class.getName(), mapMethods);
    whiteListedClassMethods.put(PartitionedRegion.class.getName(), mapMethods);

    Set<String> mapEntryMethods = new HashSet<>();
    mapEntryMethods.add("getKey");
    mapEntryMethods.add("getValue");
    whiteListedClassMethods.put(Map.Entry.class.getName(), mapEntryMethods);
    whiteListedClassMethods.put(LocalRegion.NonTXEntry.class.getName(), mapEntryMethods);
    whiteListedClassMethods.put(EntrySnapshot.class.getName(), mapEntryMethods);

    Set<String> stringMethods = new HashSet<>();
    stringMethods.add("toString");
    stringMethods.add("toLowerCase");
    stringMethods.add("toUpperCase");
    whiteListedClassMethods.put(String.class.getName(), stringMethods);

    Set<String> localRegionMethods = new HashSet<>();
    localRegionMethods.add("values");
    whiteListedClassMethods.put(LocalRegion.class.getName(), localRegionMethods);
  }

  public MethodInvocationAuthorizer() {

  }

  public static boolean isWhitelisted(Member method) {
    return isWhitelisted(method.getDeclaringClass().getName(), method.getName());
  }

  public static void allowedRegionAccess(SecurityService securityService, Member method,
      Object target) {
    if (target instanceof Region) {
      String regionName = ((Region) target).getName();
      securityService.authorize(ResourcePermission.Resource.DATA, ResourcePermission.Operation.READ, regionName);
    }
  }

  public static boolean isWhitelisted(String className, String methodName) {
    if (DefaultQuery.ALLOW_UNTRUSTED_METHOD_INVOCATION || whiteListedMethods.contains((methodName))) {
      return true;
    }

    Set whiteListedMethods = whiteListedClassMethods.get(className);
    if (whiteListedMethods != null) {
      return whiteListedMethods.contains(methodName);
    }
    return false;
  }
  public static void authorizeFunctionInvocation(Member method) {
    if (!isWhitelisted(method) ) {
      throw new NotAuthorizedException(UNAUTHORIZED_STRING + method.getName());
    }
  }

  public static void authorizeFunctionInvocation(SecurityService securityService, Member method, Object target) {
    if (!isWhitelisted(method)) {
      throw new NotAuthorizedException(UNAUTHORIZED_STRING + method.getName());
    }
    allowedRegionAccess(securityService, method, target);
  }
}

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


import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.DoubleAccumulator;
import java.util.concurrent.atomic.DoubleAdder;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.concurrent.atomic.LongAdder;

import org.apache.shiro.SecurityUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.internal.QRegion;
import org.apache.geode.cache.query.internal.index.DummyQRegion;
import org.apache.geode.cache.wan.GatewayQueueEvent;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.examples.SimpleSecurityManager;
import org.apache.geode.internal.cache.EntrySnapshot;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.NonTXEntry;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionDataStore;
import org.apache.geode.internal.cache.ha.HAContainerMap;
import org.apache.geode.internal.security.IntegratedSecurityService;
import org.apache.geode.internal.security.LegacySecurityService;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.security.NotAuthorizedException;
import org.apache.geode.security.ResourcePermission;
import org.apache.geode.test.junit.categories.SecurityTest;

@Category(SecurityTest.class)
public class RestrictedMethodAuthorizerTest {
  private Cache mockCache;
  private SecurityService mockSecurityService;
  private RestrictedMethodAuthorizer methodAuthorizer;

  @Before
  public void setUp() {
    mockCache = mock(InternalCache.class);
    mockSecurityService = mock(SecurityService.class);
    when(((InternalCache) mockCache).getSecurityService()).thenReturn(mockSecurityService);

    methodAuthorizer = new RestrictedMethodAuthorizer(mockCache);

    // Make sure the shiro security-manager is null to avoid polluting tests.
    SecurityUtils.setSecurityManager(null);
  }

  @Test
  public void constructorShouldThrowExceptionWhenCacheIsNull() {
    assertThatThrownBy(() -> new RestrictedMethodAuthorizer(null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Cache should be provided to configure the authorizer.");
  }

  @Test
  public void constructorShouldSetTheConfiguredSecurityServiceForGeodeCreatedCacheInstances() {
    InternalCache cache = mock(InternalCache.class);
    SecurityService securityService = mock(SecurityService.class);
    when(cache.getSecurityService()).thenReturn(securityService);

    RestrictedMethodAuthorizer defaultAuthorizer = new RestrictedMethodAuthorizer(cache);
    verify(cache).getSecurityService();
    assertThat(defaultAuthorizer.securityService).isSameAs(securityService);
  }

  @Test
  public void constructorShouldThrowExceptionForNonGeodeCreatedCacheInstancesWhenDistributedSystemIsNull() {
    Cache cache = mock(Cache.class);

    assertThatThrownBy(() -> new RestrictedMethodAuthorizer(cache))
        .isInstanceOf(NullPointerException.class)
        .hasMessage(
            "Distributed system properties should be provided to configure the authorizer.");
  }

  @Test
  public void constructorShouldSetTheLegacySecurityServiceForNonGeodeCreatedCacheInstancesWithDefaultProperties() {
    Cache mockCache = mock(Cache.class);
    DistributedSystem mockDistributedSystem = mock(DistributedSystem.class);
    when(mockCache.getDistributedSystem()).thenReturn(mockDistributedSystem);

    RestrictedMethodAuthorizer authorizerWithLegacyService =
        new RestrictedMethodAuthorizer(mockCache);
    verify(mockDistributedSystem).getSecurityProperties();
    assertThat(authorizerWithLegacyService.securityService)
        .isInstanceOf(LegacySecurityService.class);
  }

  @Test
  public void constructorShouldSetTheIntegratedSecurityServiceForNonGeodeCreatedCacheInstancesWithNonDefaultProperties() {
    Cache mockCache = mock(Cache.class);
    Properties securityProperties = new Properties();
    securityProperties.setProperty(SECURITY_MANAGER, SimpleSecurityManager.class.getName());
    DistributedSystem mockDistributedSystem = mock(DistributedSystem.class);
    when(mockCache.getDistributedSystem()).thenReturn(mockDistributedSystem);
    when(mockDistributedSystem.getSecurityProperties()).thenReturn(securityProperties);

    RestrictedMethodAuthorizer authorizerWithIntegratedService =
        new RestrictedMethodAuthorizer(mockCache);
    verify(mockDistributedSystem).getSecurityProperties();
    assertThat(authorizerWithIntegratedService.securityService)
        .isInstanceOf(IntegratedSecurityService.class);
    assertThat(authorizerWithIntegratedService.securityService.getSecurityManager())
        .isInstanceOf(SimpleSecurityManager.class);
  }

  @Test
  public void internalStructuresShouldBeAccessibleBuNotModifiable() {
    assertThatThrownBy(() -> methodAuthorizer.getForbiddenMethods().remove("getClass"))
        .isInstanceOf(UnsupportedOperationException.class);

    assertThatThrownBy(() -> methodAuthorizer.getAllowedMethodsPerClass().remove("compareTo"))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> methodAuthorizer.getAllowedMethodsPerClass().get("compareTo")
        .remove(Object.class)).isInstanceOf(UnsupportedOperationException.class);

    assertThatThrownBy(() -> methodAuthorizer.getAllowedGeodeMethodsPerClass().remove("get"))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> methodAuthorizer.getAllowedGeodeMethodsPerClass().get("get")
        .remove(Region.class)).isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void verifyAuthorizersUsesDefaultForbiddenList() {
    RestrictedMethodAuthorizer authorizer1 = new RestrictedMethodAuthorizer(mockCache);
    RestrictedMethodAuthorizer authorizer2 = new RestrictedMethodAuthorizer(mockCache);

    assertThat(authorizer1.getForbiddenMethods()).isSameAs(authorizer2.getForbiddenMethods());
    assertThat(authorizer1.getForbiddenMethods())
        .isSameAs(RestrictedMethodAuthorizer.FORBIDDEN_METHODS);
    assertThat(authorizer2.getForbiddenMethods())
        .isSameAs(RestrictedMethodAuthorizer.FORBIDDEN_METHODS);
  }

  @Test
  public void verifyAuthorizersUsesDefaultAllowedList() {
    RestrictedMethodAuthorizer authorizer1 = new RestrictedMethodAuthorizer(mockCache);
    RestrictedMethodAuthorizer authorizer2 = new RestrictedMethodAuthorizer(mockCache);

    assertThat(authorizer1.getAllowedMethodsPerClass())
        .isSameAs(authorizer2.getAllowedMethodsPerClass());
    assertThat(authorizer1.getAllowedMethodsPerClass())
        .isSameAs(RestrictedMethodAuthorizer.DEFAULT_ALLOWED_METHODS);
    assertThat(authorizer2.getAllowedMethodsPerClass())
        .isSameAs(RestrictedMethodAuthorizer.DEFAULT_ALLOWED_METHODS);
  }

  @Test
  public void verifyAuthorizersUsesGeodeAllowedList() {
    RestrictedMethodAuthorizer authorizer1 = new RestrictedMethodAuthorizer(mockCache);
    RestrictedMethodAuthorizer authorizer2 = new RestrictedMethodAuthorizer(mockCache);

    assertThat(authorizer1.getAllowedGeodeMethodsPerClass())
        .isSameAs(authorizer2.getAllowedGeodeMethodsPerClass());
    assertThat(authorizer1.getAllowedGeodeMethodsPerClass())
        .isSameAs(RestrictedMethodAuthorizer.GEODE_ALLOWED_METHODS);
    assertThat(authorizer2.getAllowedGeodeMethodsPerClass())
        .isSameAs(RestrictedMethodAuthorizer.GEODE_ALLOWED_METHODS);
  }

  @SuppressWarnings("unchecked")
  private void verifyGeneralObjectMethods(Class type, Object object) {
    try {
      Method toStringMethod = type.getMethod("toString");
      Method equalsMethod = type.getMethod("equals", Object.class);
      Method compareToMethod = type.getMethod("compareTo", Object.class);

      assertThat(methodAuthorizer.authorize(toStringMethod, object)).isTrue();
      assertThat(methodAuthorizer.authorize(equalsMethod, object)).isTrue();
      assertThat(methodAuthorizer.authorize(compareToMethod, object)).isTrue();
    } catch (NoSuchMethodException noSuchMethodException) {
      throw new RuntimeException(noSuchMethodException);
    }
  }

  @Test
  public void authorizeShouldReturnFalseForNotAllowedMethods() throws Exception {
    Method method = Integer.class.getMethod("getClass");
    assertThat(methodAuthorizer.authorize(method, 0)).isFalse();
  }

  @Test
  public void authorizeShouldReturnTrueForAllowedMethodsOnAnyObjectInstance()
      throws NoSuchMethodException {
    Method toStringMethod = Object.class.getMethod("toString");
    Method equalsMethod = Object.class.getMethod("equals", Object.class);
    Method compareToMethod = Comparable.class.getMethod("compareTo", Object.class);

    assertThat(methodAuthorizer.authorize(toStringMethod, new Object())).isTrue();
    assertThat(methodAuthorizer.authorize(equalsMethod, new Object())).isTrue();
    assertThat(methodAuthorizer.authorize(compareToMethod, new Object())).isTrue();
  }

  @Test
  public void authorizeShouldReturnTrueForAllowedMethodsOnBooleanInstances() throws Exception {
    Method booleanValueMethod = Boolean.class.getMethod("booleanValue");
    assertThat(methodAuthorizer.authorize(booleanValueMethod, Boolean.TRUE)).isTrue();
    verifyGeneralObjectMethods(Boolean.class, Boolean.TRUE);
  }

  @Test
  public void authorizeShouldReturnTrueForAllowedMethodsOnNumberInstances() throws Exception {
    List<Number> instances = new ArrayList<>();
    instances.add(new AtomicInteger(0));
    instances.add(new AtomicLong(0));
    instances.add(new BigDecimal("0"));
    instances.add(new BigInteger("0"));
    instances.add(new Byte("0"));
    instances.add(new Double("0d"));
    instances.add(new DoubleAccumulator(Double::sum, 0L));
    instances.add(new DoubleAdder());
    instances.add(new Float("1f"));
    instances.add(new Integer("1"));
    instances.add(new Long("1"));
    instances.add(new LongAccumulator(Long::sum, 0L));
    instances.add(new LongAdder());
    instances.add(new Short("1"));

    Method byteValueMethod = Number.class.getMethod("byteValue");
    Method doubleValueMethod = Number.class.getMethod("doubleValue");
    Method intValueMethod = Number.class.getMethod("intValue");
    Method floatValueMethod = Number.class.getMethod("longValue");
    Method longValueMethod = Number.class.getMethod("floatValue");
    Method shortValueMethod = Number.class.getMethod("shortValue");

    instances.forEach((number) -> {
      assertThat(methodAuthorizer.authorize(byteValueMethod, number)).isTrue();
      assertThat(methodAuthorizer.authorize(doubleValueMethod, number)).isTrue();
      assertThat(methodAuthorizer.authorize(intValueMethod, number)).isTrue();
      assertThat(methodAuthorizer.authorize(floatValueMethod, number)).isTrue();
      assertThat(methodAuthorizer.authorize(longValueMethod, number)).isTrue();
      assertThat(methodAuthorizer.authorize(shortValueMethod, number)).isTrue();
    });
  }

  @Test
  public void authorizeShouldReturnTrueForAllowedMethodsOnDateInstances() throws Exception {
    List<Date> instances = new ArrayList<>();
    instances.add(new Date(0));
    instances.add(new java.sql.Date(0));
    instances.add(new Time(0));
    instances.add(new Timestamp(0));

    Method afterMethod = Date.class.getMethod("after", Date.class);
    Method beforeMethod = Date.class.getMethod("before", Date.class);
    Method getTimeMethod = Date.class.getMethod("getTime");

    instances.forEach((date) -> {
      assertThat(methodAuthorizer.authorize(afterMethod, date)).isTrue();
      assertThat(methodAuthorizer.authorize(beforeMethod, date)).isTrue();
      assertThat(methodAuthorizer.authorize(getTimeMethod, date)).isTrue();
      verifyGeneralObjectMethods(date.getClass(), date);
    });
  }

  @Test
  public void authorizeShouldReturnTrueForAllowedMethodsOnTimestampInstances() throws Exception {
    Timestamp timestamp = new Timestamp(0);
    Method afterMethod = Timestamp.class.getMethod("after", Date.class);
    Method beforeMethod = Timestamp.class.getMethod("before", Date.class);
    Method getNanosMethod = Timestamp.class.getMethod("getNanos");
    Method getTimeMethod = Timestamp.class.getMethod("getTime");

    assertThat(methodAuthorizer.authorize(afterMethod, timestamp)).isTrue();
    assertThat(methodAuthorizer.authorize(beforeMethod, timestamp)).isTrue();
    assertThat(methodAuthorizer.authorize(getNanosMethod, timestamp)).isTrue();
    assertThat(methodAuthorizer.authorize(getTimeMethod, timestamp)).isTrue();
    verifyGeneralObjectMethods(Timestamp.class, timestamp);
  }

  @Test
  public void authorizeShouldReturnTrueForAllowedMethodsOnStringInstances() throws Exception {
    String string = "";
    List<Method> stringMethods = new ArrayList<>();
    stringMethods.add(String.class.getMethod("charAt", int.class));
    stringMethods.add(String.class.getMethod("codePointAt", int.class));
    stringMethods.add(String.class.getMethod("codePointBefore", int.class));
    stringMethods.add(String.class.getMethod("codePointCount", int.class, int.class));
    stringMethods.add(String.class.getMethod("compareToIgnoreCase", String.class));
    stringMethods.add(String.class.getMethod("concat", String.class));
    stringMethods.add(String.class.getMethod("contains", CharSequence.class));
    stringMethods.add(String.class.getMethod("contentEquals", CharSequence.class));
    stringMethods.add(String.class.getMethod("contentEquals", StringBuffer.class));
    stringMethods.add(String.class.getMethod("endsWith", String.class));
    stringMethods.add(String.class.getMethod("equalsIgnoreCase", String.class));
    stringMethods.add(String.class.getMethod("getBytes"));
    stringMethods.add(String.class.getMethod("getBytes", Charset.class));
    stringMethods.add(String.class.getMethod("hashCode"));
    stringMethods.add(String.class.getMethod("indexOf", int.class));
    stringMethods.add(String.class.getMethod("indexOf", String.class));
    stringMethods.add(String.class.getMethod("indexOf", String.class, int.class));
    stringMethods.add(String.class.getMethod("intern"));
    stringMethods.add(String.class.getMethod("isEmpty"));
    stringMethods.add(String.class.getMethod("lastIndexOf", int.class));
    stringMethods.add(String.class.getMethod("lastIndexOf", int.class, int.class));
    stringMethods.add(String.class.getMethod("lastIndexOf", String.class));
    stringMethods.add(String.class.getMethod("lastIndexOf", String.class, int.class));
    stringMethods.add(String.class.getMethod("length"));
    stringMethods.add(String.class.getMethod("matches", String.class));
    stringMethods.add(String.class.getMethod("offsetByCodePoints", int.class, int.class));
    stringMethods.add(String.class.getMethod("regionMatches", boolean.class, int.class,
        String.class, int.class, int.class));
    stringMethods.add(
        String.class.getMethod("regionMatches", int.class, String.class, int.class, int.class));
    stringMethods.add(String.class.getMethod("replace", char.class, char.class));
    stringMethods.add(String.class.getMethod("replace", CharSequence.class, CharSequence.class));
    stringMethods.add(String.class.getMethod("replaceAll", String.class, String.class));
    stringMethods.add(String.class.getMethod("replaceFirst", String.class, String.class));
    stringMethods.add(String.class.getMethod("split", String.class));
    stringMethods.add(String.class.getMethod("split", String.class, int.class));
    stringMethods.add(String.class.getMethod("startsWith", String.class));
    stringMethods.add(String.class.getMethod("startsWith", String.class, int.class));
    stringMethods.add(String.class.getMethod("substring", int.class));
    stringMethods.add(String.class.getMethod("substring", int.class, int.class));
    stringMethods.add(String.class.getMethod("toCharArray"));
    stringMethods.add(String.class.getMethod("toLowerCase"));
    stringMethods.add(String.class.getMethod("toUpperCase"));
    stringMethods.add(String.class.getMethod("trim"));

    stringMethods
        .forEach(method -> assertThat(methodAuthorizer.authorize(method, string)).isTrue());
    verifyGeneralObjectMethods(String.class, string);
  }

  @Test
  public void authorizeShouldReturnTrueForAllowedMethodsOnMapEntryInstances() throws Exception {
    List<Map.Entry> instances = new ArrayList<>();
    instances.add(mock(Map.Entry.class));
    instances.add(mock(NonTXEntry.class));
    instances.add(mock(EntrySnapshot.class));
    instances.add(mock(Region.Entry.class));
    Method getKeyMethod = Map.Entry.class.getMethod("getKey");
    Method getValueMethod = Map.Entry.class.getMethod("getValue");

    instances.forEach((mapEntry) -> {
      assertThat(methodAuthorizer.authorize(getKeyMethod, mapEntry)).isTrue();
      assertThat(methodAuthorizer.authorize(getValueMethod, mapEntry)).isTrue();
    });
  }

  @Test
  public void authorizeShouldReturnTrueForAllowedMethodsOnMapInstances() throws Exception {
    Map mapInstance = mock(Map.class);
    List<Method> mapMethods = new ArrayList<>();
    mapMethods.add(Map.class.getMethod("containsKey", Object.class));
    mapMethods.add(Map.class.getMethod("entrySet"));
    mapMethods.add(Map.class.getMethod("get", Object.class));
    mapMethods.add(Map.class.getMethod("keySet"));
    mapMethods.add(Map.class.getMethod("values"));

    mapMethods
        .forEach(method -> assertThat(methodAuthorizer.authorize(method, mapInstance)).isTrue());
  }

  @Test
  public void authorizeShouldReturnTrueForAllowedMethodsOnQRegionInstances() throws Exception {
    QRegion qRegionInstance = mock(QRegion.class);
    DummyQRegion dummyQRegionInstance = mock(DummyQRegion.class);

    List<Method> methods = new ArrayList<>();
    methods.add(Map.class.getMethod("containsKey", Object.class));
    methods.add(Map.class.getMethod("entrySet"));
    methods.add(Map.class.getMethod("get", Object.class));
    methods.add(Map.class.getMethod("keySet"));
    methods.add(Map.class.getMethod("values"));
    methods.add(QRegion.class.getMethod("getEntries"));
    methods.add(QRegion.class.getMethod("getValues"));

    methods.forEach(
        method -> assertThat(methodAuthorizer.authorize(method, qRegionInstance)).isTrue());
    methods.forEach(
        method -> assertThat(methodAuthorizer.authorize(method, dummyQRegionInstance)).isTrue());
  }

  @Test
  public void authorizeShouldReturnTrueForMapMethodsOnRegionInstancesWheneverTheSecurityServiceAllowsOperationsOnTheRegion()
      throws Exception {
    Region region = mock(Region.class);
    Region localRegion = mock(LocalRegion.class);
    PartitionedRegion partitionedRegion = mock(PartitionedRegion.class);

    List<Method> methods = new ArrayList<>();
    methods.add(Map.class.getMethod("containsKey", Object.class));
    methods.add(Map.class.getMethod("entrySet"));
    methods.add(Map.class.getMethod("get", Object.class));
    methods.add(Map.class.getMethod("keySet"));
    methods.add(Map.class.getMethod("values"));

    methods.forEach(method -> assertThat(methodAuthorizer.authorize(method, region)).isTrue());
    methods.forEach(method -> assertThat(methodAuthorizer.authorize(method, localRegion)).isTrue());
    methods.forEach(
        method -> assertThat(methodAuthorizer.authorize(method, partitionedRegion)).isTrue());
  }

  @Test
  public void authorizeShouldReturnFalseWheneverTheSecurityServiceDoesNotAllowOperationsOnTheRegion()
      throws Exception {
    doThrow(new NotAuthorizedException("Mock Exception")).when(mockSecurityService).authorize(
        ResourcePermission.Resource.DATA, ResourcePermission.Operation.READ, "testRegion");
    Region region = mock(Region.class);
    when(region.getName()).thenReturn("testRegion");
    PartitionedRegion partitionedRegion = mock(PartitionedRegion.class);
    when(partitionedRegion.getName()).thenReturn("testRegion");

    List<Method> methods = new ArrayList<>();
    methods.add(Map.class.getMethod("containsKey", Object.class));
    methods.add(Map.class.getMethod("entrySet"));
    methods.add(Map.class.getMethod("get", Object.class));
    methods.add(Map.class.getMethod("keySet"));
    methods.add(Map.class.getMethod("values"));

    methods.forEach(method -> assertThat(methodAuthorizer.authorize(method, region)).isFalse());
    methods.forEach(
        method -> assertThat(methodAuthorizer.authorize(method, partitionedRegion)).isFalse());
  }

  @Test
  public void isAllowedGeodeMethodShouldReturnFalseForNonGeodeObjectInstances() throws Exception {
    Map mapInstance = mock(Map.class);
    List<Method> mapMethods = new ArrayList<>();
    mapMethods.add(Map.class.getMethod("containsKey", Object.class));
    mapMethods.add(Map.class.getMethod("entrySet"));
    mapMethods.add(Map.class.getMethod("get", Object.class));
    mapMethods.add(Map.class.getMethod("keySet"));
    mapMethods.add(Map.class.getMethod("values"));

    Map.Entry mapEntryInstance = mock(Map.Entry.class);
    List<Method> mapEntryMethods = new ArrayList<>();
    mapEntryMethods.add(Map.Entry.class.getMethod("getKey"));
    mapEntryMethods.add(Map.Entry.class.getMethod("getValue"));

    mapMethods.forEach(
        method -> assertThat(methodAuthorizer.isAllowedGeodeMethod(method, mapInstance)).isFalse());
    mapEntryMethods.forEach(
        method -> assertThat(methodAuthorizer.isAllowedGeodeMethod(method, mapEntryInstance))
            .isFalse());
  }

  @Test
  public void isAllowedGeodeMethodShouldReturnFalseForGeodeObjectsThatAreNotInstanceOfRegionAndRegionEntry()
      throws Exception {
    EntryEvent entryEvent = mock(EntryEvent.class);
    HAContainerMap haContainerMap = mock(HAContainerMap.class);
    GatewayQueueEvent gatewayQueueEvent = mock(GatewayQueueEvent.class);
    PartitionedRegionDataStore partitionedRegionDataStore = mock(PartitionedRegionDataStore.class);

    List<Method> queueRegionMethods = new ArrayList<>();
    queueRegionMethods.add(QRegion.class.getMethod("getEntries"));
    queueRegionMethods.add(QRegion.class.getMethod("getValues"));

    List<Method> regionEntryMethods = new ArrayList<>();
    regionEntryMethods.add(Region.Entry.class.getMethod("getKey"));
    regionEntryMethods.add(Region.Entry.class.getMethod("getValue"));

    List<Method> regionMethods = new ArrayList<>();
    regionMethods.add(Region.class.getMethod("containsKey", Object.class));
    regionMethods.add(Region.class.getMethod("entrySet"));
    regionMethods.add(Region.class.getMethod("get", Object.class));
    regionMethods.add(Region.class.getMethod("keySet"));
    regionMethods.add(Region.class.getMethod("values"));

    regionEntryMethods.forEach(
        method -> assertThat(methodAuthorizer.isAllowedGeodeMethod(method, entryEvent)).isFalse());
    regionEntryMethods.forEach(
        method -> assertThat(methodAuthorizer.isAllowedGeodeMethod(method, gatewayQueueEvent))
            .isFalse());
    regionMethods
        .forEach(method -> assertThat(methodAuthorizer.isAllowedGeodeMethod(method, haContainerMap))
            .isFalse());
    regionMethods.forEach(method -> assertThat(
        methodAuthorizer.isAllowedGeodeMethod(method, partitionedRegionDataStore)).isFalse());
    queueRegionMethods
        .forEach(method -> assertThat(methodAuthorizer.isAllowedGeodeMethod(method, haContainerMap))
            .isFalse());
    queueRegionMethods.forEach(method -> assertThat(
        methodAuthorizer.isAllowedGeodeMethod(method, partitionedRegionDataStore)).isFalse());
  }

  @Test
  public void isAllowedGeodeMethodShouldReturnTrueForRegionEntryMethods() throws Exception {
    List<Region.Entry> regionEntryInstances = new ArrayList<>();
    regionEntryInstances.add(mock(NonTXEntry.class));
    regionEntryInstances.add(mock(Region.Entry.class));
    regionEntryInstances.add(mock(EntrySnapshot.class));
    Method getKeyMethod = Region.Entry.class.getMethod("getKey");
    Method getValueMethod = Region.Entry.class.getMethod("getValue");

    regionEntryInstances.forEach((regionEntry) -> {
      assertThat(methodAuthorizer.isAllowedGeodeMethod(getKeyMethod, regionEntry)).isTrue();
      assertThat(methodAuthorizer.isAllowedGeodeMethod(getValueMethod, regionEntry)).isTrue();
    });
  }

  @Test
  public void isAllowedGeodeMethodShouldReturnTrueForRegionMethodsOnRegionInstancesWheneverTheSecurityServiceAllowsOperationsOnTheRegion()
      throws Exception {
    Region region = mock(Region.class);
    LocalRegion localRegion = mock(LocalRegion.class);
    PartitionedRegion partitionedRegion = mock(PartitionedRegion.class);

    List<Method> regionMethods = new ArrayList<>();
    regionMethods.add(Region.class.getMethod("containsKey", Object.class));
    regionMethods.add(Region.class.getMethod("entrySet"));
    regionMethods.add(Region.class.getMethod("get", Object.class));
    regionMethods.add(Region.class.getMethod("keySet"));
    regionMethods.add(Region.class.getMethod("values"));

    regionMethods.forEach(
        method -> assertThat(methodAuthorizer.isAllowedGeodeMethod(method, region)).isTrue());
    regionMethods.forEach(
        method -> assertThat(methodAuthorizer.isAllowedGeodeMethod(method, localRegion)).isTrue());
    regionMethods.forEach(
        method -> assertThat(methodAuthorizer.isAllowedGeodeMethod(method, partitionedRegion))
            .isTrue());
  }

  @Test
  public void isAllowedGeodeMethodShouldReturnFalseWheneverTheSecurityServiceDoesNotAllowOperationsOnTheRegion()
      throws Exception {
    doThrow(new NotAuthorizedException("Mock Exception")).when(mockSecurityService).authorize(
        ResourcePermission.Resource.DATA, ResourcePermission.Operation.READ, "testRegion");
    Region region = mock(Region.class);
    when(region.getName()).thenReturn("testRegion");
    LocalRegion localRegion = mock(LocalRegion.class);
    when(localRegion.getName()).thenReturn("testRegion");
    PartitionedRegion partitionedRegion = mock(PartitionedRegion.class);
    when(partitionedRegion.getName()).thenReturn("testRegion");

    List<Method> regionMethods = new ArrayList<>();
    regionMethods.add(Region.class.getMethod("containsKey", Object.class));
    regionMethods.add(Region.class.getMethod("entrySet"));
    regionMethods.add(Region.class.getMethod("get", Object.class));
    regionMethods.add(Region.class.getMethod("keySet"));
    regionMethods.add(Region.class.getMethod("values"));

    regionMethods.forEach(
        method -> assertThat(methodAuthorizer.isAllowedGeodeMethod(method, region)).isFalse());
    regionMethods.forEach(
        method -> assertThat(methodAuthorizer.isAllowedGeodeMethod(method, localRegion)).isFalse());
    regionMethods.forEach(
        method -> assertThat(methodAuthorizer.isAllowedGeodeMethod(method, partitionedRegion))
            .isFalse());
  }

  @Test
  public void isKnownDangerousMethodShouldReturnTrueForNonSafeMethods() throws Exception {
    TestBean testBean = new TestBean();
    List<Method> dangerousMethodds = new ArrayList<>();
    dangerousMethodds.add(Object.class.getMethod("getClass"));
    dangerousMethodds.add(TestBean.class.getMethod("readResolve"));
    dangerousMethodds.add(TestBean.class.getMethod("readObjectNoData"));
    dangerousMethodds.add(TestBean.class.getMethod("readObject", ObjectInputStream.class));
    dangerousMethodds.add(TestBean.class.getMethod("writeReplace"));
    dangerousMethodds.add(TestBean.class.getMethod("writeObject", ObjectOutputStream.class));

    dangerousMethodds.forEach(
        method -> assertThat(methodAuthorizer.isKnownDangerousMethod(method, testBean)).isTrue());
  }

  @SuppressWarnings("unused")
  private static class TestBean implements Serializable {
    public Object writeReplace() throws ObjectStreamException {
      return new TestBean();
    }

    public void writeObject(ObjectOutputStream stream) throws IOException {
      throw new IOException();
    }

    public Object readResolve() throws ObjectStreamException {
      return new TestBean();
    }

    public void readObjectNoData() throws ObjectStreamException {}

    public void readObject(ObjectInputStream stream) throws IOException, ClassNotFoundException {
      if (new Random().nextBoolean()) {
        throw new IOException();
      } else {
        throw new ClassNotFoundException();
      }
    }
  }
}

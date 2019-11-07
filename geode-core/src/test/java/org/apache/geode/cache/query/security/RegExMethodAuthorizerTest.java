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

import static org.apache.geode.cache.query.security.RegExMethodAuthorizer.NULL_AUTHORIZER_MESSAGE;
import static org.apache.geode.cache.query.security.RegExMethodAuthorizer.NULL_CACHE_MESSAGE;
import static org.apache.geode.cache.query.security.RegExMethodAuthorizer.NULL_REGULAR_EXPRESSIONS_MESSAGE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.internal.cache.BucketDump;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.ha.HAContainerMap;
import org.apache.geode.internal.cache.persistence.query.mock.Pair;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.internal.util.Valuable;
import org.apache.geode.security.NotAuthorizedException;
import org.apache.geode.security.ResourcePermission;
import org.apache.geode.test.junit.categories.SecurityTest;

@Category(SecurityTest.class)
public class RegExMethodAuthorizerTest {
  private InternalCache mockCache;
  private SecurityService mockSecurityService;
  private RegExMethodAuthorizer geodeRegExMatchingMethodAuthorizer;

  @Before
  public void setUp() {
    mockCache = mock(InternalCache.class);
    mockSecurityService = mock(SecurityService.class);
    when(mockCache.getSecurityService()).thenReturn(mockSecurityService);
    geodeRegExMatchingMethodAuthorizer =
        new RegExMethodAuthorizer(new RestrictedMethodAuthorizer(mockCache),
            Collections.singleton("^org\\.apache\\.geode\\..*"));
  }

  @Test
  public void constructorShouldThrowExceptionWhenCacheIsNull() {
    assertThatThrownBy(() -> new RegExMethodAuthorizer((Cache) null, Collections.emptySet()))
        .isInstanceOf(NullPointerException.class)
        .hasMessage(NULL_CACHE_MESSAGE);
  }

  @Test
  public void constructorShouldThrowExceptionWhenRestrictedMethodAuthorizerIsNull() {
    assertThatThrownBy(
        () -> new RegExMethodAuthorizer((RestrictedMethodAuthorizer) null, Collections.emptySet()))
            .isInstanceOf(NullPointerException.class)
            .hasMessage(NULL_AUTHORIZER_MESSAGE);
  }

  @Test
  public void constructorShouldThrowExceptionWhenAllowedPatternsIsNull() {
    assertThatThrownBy(() -> new RegExMethodAuthorizer(mockCache, null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage(NULL_REGULAR_EXPRESSIONS_MESSAGE);

    assertThatThrownBy(
        () -> new RegExMethodAuthorizer(new RestrictedMethodAuthorizer(mockCache), null))
            .isInstanceOf(NullPointerException.class)
            .hasMessage(NULL_REGULAR_EXPRESSIONS_MESSAGE);
  }

  @Test
  public void allowedPatternsShouldBeAccessibleBuNotModifiable() {
    assertThatThrownBy(() -> geodeRegExMatchingMethodAuthorizer.getParameters().add(".*"))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void authorizeShouldReturnFalseForKnownDangerousMethods() throws Exception {
    TestBean testBean = new TestBean();
    List<Method> dangerousMethods = new ArrayList<>();
    dangerousMethods.add(TestBean.class.getMethod("getClass"));
    dangerousMethods.add(TestBean.class.getMethod("readResolve"));
    dangerousMethods.add(TestBean.class.getMethod("readObjectNoData"));
    dangerousMethods.add(TestBean.class.getMethod("readObject", ObjectInputStream.class));
    dangerousMethods.add(TestBean.class.getMethod("writeReplace"));
    dangerousMethods.add(TestBean.class.getMethod("writeObject", ObjectOutputStream.class));

    dangerousMethods.forEach(method -> assertThat(
        geodeRegExMatchingMethodAuthorizer.authorize(method, testBean)).isFalse());
  }

  @Test
  public void authorizeShouldReturnFalseForGeodeMethodsThatAreNotConsideredSafeEvenIfTheyMatchTheUserRegEx()
      throws Exception {
    Set<Pair<Method, Object>> methods = new HashSet<>();
    methods.add(new Pair<>(TestBean.class.getMethod("accessor"), new TestBean()));
    methods.add(new Pair<>(TestBean.class.getMethod("mutator"), new TestBean()));
    methods.add(new Pair<>(BucketDump.class.getMethod("getValues"), mock(BucketDump.class)));
    methods.add(new Pair<>(RegionConfig.class.getMethod("getEntries"), mock(RegionConfig.class)));
    methods.add(new Pair<>(EntryEvent.class.getMethod("getKey"), mock(EntryEvent.class)));
    methods.add(new Pair<>(Valuable.class.getMethod("getValue"), mock(Valuable.class)));
    methods.add(new Pair<>(HAContainerMap.class.getMethod("containsKey", Object.class),
        mock(HAContainerMap.class)));
    methods.add(new Pair<>(HAContainerMap.class.getMethod("entrySet"), mock(HAContainerMap.class)));
    methods.add(new Pair<>(HAContainerMap.class.getMethod("get", Object.class),
        mock(HAContainerMap.class)));
    methods.add(new Pair<>(HAContainerMap.class.getMethod("keySet"), mock(HAContainerMap.class)));
    methods.add(new Pair<>(HAContainerMap.class.getMethod("values"), mock(HAContainerMap.class)));

    methods.forEach(
        pair -> assertThat(geodeRegExMatchingMethodAuthorizer.authorize(pair.getX(), pair.getY()))
            .as("For method: " + pair.getX().getName()).isFalse());
  }

  @Test
  public void authorizeShouldReturnFalseWheneverTheSecurityServiceDoesNotAllowOperationsOnTheRegion()
      throws Exception {
    Region region = mock(Region.class);
    Region localRegion = mock(LocalRegion.class);
    Region partitionedRegion = mock(PartitionedRegion.class);
    when(region.getName()).thenReturn("testRegion");
    when(localRegion.getName()).thenReturn("testRegion");
    when(partitionedRegion.getName()).thenReturn("testRegion");
    doThrow(new NotAuthorizedException("Mock Exception")).when(mockSecurityService).authorize(
        ResourcePermission.Resource.DATA, ResourcePermission.Operation.READ, "testRegion");

    List<Method> methods = new ArrayList<>();
    methods.add(Map.class.getMethod("containsKey", Object.class));
    methods.add(Map.class.getMethod("entrySet"));
    methods.add(Map.class.getMethod("get", Object.class));
    methods.add(Map.class.getMethod("keySet"));
    methods.add(Map.class.getMethod("values"));

    methods.forEach(method -> {
      assertThat(geodeRegExMatchingMethodAuthorizer.authorize(method, region)).isFalse();
      assertThat(geodeRegExMatchingMethodAuthorizer.authorize(method, localRegion)).isFalse();
      assertThat(geodeRegExMatchingMethodAuthorizer.authorize(method, partitionedRegion)).isFalse();
    });
  }

  @Test
  public void authorizeShouldReturnTrueForMapMethodsOnRegionInstancesWheneverTheSecurityServiceAllowsOperationsOnTheRegion()
      throws Exception {
    Region region = mock(Region.class);
    Region localRegion = mock(LocalRegion.class);
    Region partitionedRegion = mock(PartitionedRegion.class);

    List<Method> methods = new ArrayList<>();
    methods.add(Map.class.getMethod("containsKey", Object.class));
    methods.add(Map.class.getMethod("entrySet"));
    methods.add(Map.class.getMethod("get", Object.class));
    methods.add(Map.class.getMethod("keySet"));
    methods.add(Map.class.getMethod("values"));

    methods.forEach(method -> {
      assertThat(geodeRegExMatchingMethodAuthorizer.authorize(method, region)).isTrue();
      assertThat(geodeRegExMatchingMethodAuthorizer.authorize(method, localRegion)).isTrue();
      assertThat(geodeRegExMatchingMethodAuthorizer.authorize(method, partitionedRegion)).isTrue();
    });
  }

  @Test
  public void authorizeShouldReturnTrueForNonMatchingMethodsConsideredSafeByDefault()
      throws Exception {
    Set<Pair<Method, Object>> someSafeMethods = new HashSet<>();
    someSafeMethods.add(new Pair<>(Object.class.getMethod("toString"), new Object()));
    someSafeMethods.add(new Pair<>(Object.class.getMethod("equals", Object.class), new Object()));
    someSafeMethods.add(
        new Pair<>(Comparable.class.getMethod("compareTo", Object.class), mock(Comparable.class)));
    someSafeMethods.add(new Pair<>(Number.class.getMethod("byteValue"), new Integer("0")));
    someSafeMethods.add(new Pair<>(Date.class.getMethod("after", Date.class), new Date()));
    someSafeMethods.add(new Pair<>(String.class.getMethod("indexOf", int.class), ""));
    someSafeMethods.add(new Pair<>(Map.Entry.class.getMethod("getKey"), mock(Map.Entry.class)));
    someSafeMethods
        .add(new Pair<>(Map.class.getMethod("get", Object.class), Collections.emptyMap()));
    RegExMethodAuthorizer authorizer = new RegExMethodAuthorizer(
        new RestrictedMethodAuthorizer(mockCache), Collections.emptySet());

    someSafeMethods.forEach(pair -> assertThat(authorizer.authorize(pair.getX(), pair.getY()))
        .as("For method: " + pair.getX().getName()).isTrue());
  }

  @Test
  public void authorizeShouldReturnFalseForNonMatchingMethodsThatAreNotConsideredSafeByDefault()
      throws Exception {
    Set<Pair<Method, Object>> someNonSafeMethods = new HashSet<>();
    someNonSafeMethods.add(new Pair<>(Object.class.getMethod("wait"), new Object()));
    someNonSafeMethods.add(new Pair<>(Number.class.getMethod("notify"), new Integer("0")));
    someNonSafeMethods.add(new Pair<>(Date.class.getMethod("setYear", int.class), new Date()));
    someNonSafeMethods.add(new Pair<>(Set.class.getMethod("add", Object.class), new HashSet<>()));
    someNonSafeMethods.add(
        new Pair<>(Map.Entry.class.getMethod("setValue", Object.class), mock(Map.Entry.class)));
    someNonSafeMethods.add(
        new Pair<>(Map.class.getMethod("put", Object.class, Object.class), Collections.emptyMap()));
    RegExMethodAuthorizer authorizer = new RegExMethodAuthorizer(
        new RestrictedMethodAuthorizer(mockCache), Collections.emptySet());

    someNonSafeMethods.forEach(pair -> assertThat(authorizer.authorize(pair.getX(), pair.getY()))
        .as("For method: " + pair.getX().getName()).isFalse());
  }

  @Test
  public void authorizeShouldReturnTrueForMatchingMethods() throws Exception {
    RestrictedMethodAuthorizer defaultAuthorizer = new RestrictedMethodAuthorizer(mockCache);
    Set<String> regularExpressions =
        new HashSet<>(Arrays.asList("^java\\.lang\\..*\\.(?:wait|notify)",
            "^java\\.util\\..*\\.(?:set|put|add).*", ".*MockitoMock.*"));
    RegExMethodAuthorizer customAuthorizer =
        new RegExMethodAuthorizer(defaultAuthorizer, regularExpressions);

    Set<Pair<Method, Object>> someNonSafeMethods = new HashSet<>();
    someNonSafeMethods.add(new Pair<>(Object.class.getMethod("wait"), new Object()));
    someNonSafeMethods.add(new Pair<>(Number.class.getMethod("notify"), new Integer("0")));
    someNonSafeMethods.add(new Pair<>(Date.class.getMethod("setYear", int.class), new Date()));
    someNonSafeMethods.add(new Pair<>(Set.class.getMethod("add", Object.class), new HashSet<>()));
    someNonSafeMethods.add(
        new Pair<>(Map.Entry.class.getMethod("setValue", Object.class), mock(Map.Entry.class)));
    someNonSafeMethods
        .add(new Pair<>(Map.class.getMethod("put", Object.class, Object.class), new HashMap<>()));

    someNonSafeMethods
        .forEach(pair -> assertThat(customAuthorizer.authorize(pair.getX(), pair.getY()))
            .as("For method: " + pair.getX().getName()).isTrue());
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

    public void mutator() {}

    public void accessor() {}
  }
}

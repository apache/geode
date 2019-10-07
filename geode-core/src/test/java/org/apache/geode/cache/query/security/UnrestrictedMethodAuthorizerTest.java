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

import static org.apache.geode.cache.query.security.UnrestrictedMethodAuthorizer.NULL_AUTHORIZER_MESSAGE;
import static org.apache.geode.cache.query.security.UnrestrictedMethodAuthorizer.NULL_CACHE_MESSAGE;
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
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.internal.QRegion;
import org.apache.geode.cache.query.internal.index.DummyQRegion;
import org.apache.geode.cache.wan.GatewayQueueEvent;
import org.apache.geode.internal.cache.EntrySnapshot;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.NonTXEntry;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionDataStore;
import org.apache.geode.internal.cache.ha.HAContainerMap;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.security.NotAuthorizedException;
import org.apache.geode.security.ResourcePermission;
import org.apache.geode.test.junit.categories.SecurityTest;

@Category(SecurityTest.class)
public class UnrestrictedMethodAuthorizerTest {
  private SecurityService mockSecurityService;
  private UnrestrictedMethodAuthorizer unrestrictedMethodAuthorizer;

  @Before
  public void setUp() {
    InternalCache mockCache = mock(InternalCache.class);
    mockSecurityService = mock(SecurityService.class);
    when(mockCache.getSecurityService()).thenReturn(mockSecurityService);
    unrestrictedMethodAuthorizer =
        new UnrestrictedMethodAuthorizer(new RestrictedMethodAuthorizer(mockCache));
  }

  @Test
  public void constructorShouldThrowExceptionWhenCacheIsNull() {
    assertThatThrownBy(() -> new UnrestrictedMethodAuthorizer((Cache) null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage(NULL_CACHE_MESSAGE);
  }

  @Test
  public void constructorShouldThrowExceptionWhenRestrictedMethodAuthorizerIsNull() {
    assertThatThrownBy(() -> new UnrestrictedMethodAuthorizer((RestrictedMethodAuthorizer) null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage(NULL_AUTHORIZER_MESSAGE);
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

    dangerousMethods.forEach(
        method -> assertThat(unrestrictedMethodAuthorizer.authorize(method, testBean)).isFalse());
  }

  @Test
  public void authorizeShouldReturnTrueForNonGeodeMethodsThatAreNotFlaggedAsDangerous()
      throws Exception {
    Map<Method, Object> nonGeodeMutatorMethods = new HashMap<>();
    nonGeodeMutatorMethods.put(Date.class.getMethod("setYear", int.class), mock(Date.class));
    nonGeodeMutatorMethods.put(Map.class.getMethod("remove", Object.class), mock(Map.class));
    nonGeodeMutatorMethods.put(Map.class.getMethod("put", Object.class, Object.class),
        mock(Map.class));
    nonGeodeMutatorMethods.put(Map.Entry.class.getMethod("setValue", Object.class),
        mock(Map.Entry.class));

    Map<Method, Object> nonGeodeAccessorMethods = new HashMap<>();
    nonGeodeAccessorMethods.put(Date.class.getMethod("getDay"), mock(Date.class));
    nonGeodeAccessorMethods.put(Map.class.getMethod("entrySet"), mock(Map.class));
    nonGeodeAccessorMethods.put(Map.class.getMethod("get", Object.class), mock(Map.class));
    nonGeodeAccessorMethods.put(Map.Entry.class.getMethod("getValue"), mock(Map.Entry.class));

    nonGeodeMutatorMethods.forEach((method,
        object) -> assertThat(unrestrictedMethodAuthorizer.authorize(method, object)).isTrue());
    nonGeodeAccessorMethods.forEach((method,
        object) -> assertThat(unrestrictedMethodAuthorizer.authorize(method, object)).isTrue());
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
    methods.add(Object.class.getMethod("toString"));
    methods.add(Object.class.getMethod("equals", Object.class));

    methods.forEach(method -> {
      assertThat(unrestrictedMethodAuthorizer.authorize(method, qRegionInstance)).isTrue();
      assertThat(unrestrictedMethodAuthorizer.authorize(method, dummyQRegionInstance)).isTrue();
    });
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
    methods.add(Object.class.getMethod("toString"));
    methods.add(Object.class.getMethod("equals", Object.class));

    methods.forEach(method -> {
      assertThat(unrestrictedMethodAuthorizer.authorize(method, region)).isTrue();
      assertThat(unrestrictedMethodAuthorizer.authorize(method, localRegion)).isTrue();
      assertThat(unrestrictedMethodAuthorizer.authorize(method, partitionedRegion)).isTrue();
    });
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

    methods.forEach(method -> {
      assertThat(unrestrictedMethodAuthorizer.authorize(method, region)).isFalse();
      assertThat(unrestrictedMethodAuthorizer.authorize(method, partitionedRegion)).isFalse();
    });
  }

  @Test
  public void authorizeShouldReturnFalseForGeodeObjectsThatAreNotInstanceOfRegionAndRegionEntry()
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

    regionEntryMethods.forEach(method -> {
      assertThat(unrestrictedMethodAuthorizer.authorize(method, entryEvent)).isFalse();
      assertThat(unrestrictedMethodAuthorizer.authorize(method, gatewayQueueEvent)).isFalse();
    });

    regionMethods.forEach(method -> {
      assertThat(unrestrictedMethodAuthorizer.authorize(method, haContainerMap)).isFalse();
      assertThat(unrestrictedMethodAuthorizer.authorize(method, partitionedRegionDataStore))
          .isFalse();
    });

    queueRegionMethods.forEach(method -> {
      assertThat(unrestrictedMethodAuthorizer.authorize(method, haContainerMap)).isFalse();
      assertThat(unrestrictedMethodAuthorizer.authorize(method, partitionedRegionDataStore))
          .isFalse();
    });
  }

  @Test
  public void authorizeShouldReturnTrueForRegionEntryMethods() throws Exception {
    List<Region.Entry> regionEntryInstances = new ArrayList<>();
    regionEntryInstances.add(mock(NonTXEntry.class));
    regionEntryInstances.add(mock(Region.Entry.class));
    regionEntryInstances.add(mock(EntrySnapshot.class));
    Method getKeyMethod = Region.Entry.class.getMethod("getKey");
    Method getValueMethod = Region.Entry.class.getMethod("getValue");
    Method toStringMethod = Object.class.getMethod("toString");
    Method equalsMethod = Object.class.getMethod("equals", Object.class);

    regionEntryInstances.forEach((regionEntry) -> {
      assertThat(unrestrictedMethodAuthorizer.authorize(getKeyMethod, regionEntry)).isTrue();
      assertThat(unrestrictedMethodAuthorizer.authorize(getValueMethod, regionEntry)).isTrue();
      assertThat(unrestrictedMethodAuthorizer.authorize(equalsMethod, regionEntry)).isTrue();
      assertThat(unrestrictedMethodAuthorizer.authorize(toStringMethod, regionEntry)).isTrue();
    });
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

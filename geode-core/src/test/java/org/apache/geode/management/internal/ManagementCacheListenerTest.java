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
package org.apache.geode.management.internal;

import static org.apache.geode.internal.cache.util.UncheckedUtils.cast;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.same;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import javax.management.ObjectName;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;

import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.Region;
import org.apache.geode.distributed.DistributedMember;

/**
 * Unit tests for {@link ManagementCacheListener} (ie the SUT). These are characterization tests
 * that define behavior for an existing class. Test method names specify the SUT method, the result
 * of invoking that method, and whether or not there are any specific preconditions.
 */
public class ManagementCacheListenerTest {

  private static final String OBJECT_NAME_KEY =
      "GemFire:service=DiskStore,name=cluster_config,type=Member,member=locator1";

  private DistributedMember distributedMember;
  private MBeanProxyFactory mBeanProxyFactory;
  private EntryEvent<String, Object> monitoringEvent;
  private Object monitoringEventNewValue;
  private Object monitoringEventOldValue;
  private ObjectName objectName;
  private ProxyInfo proxyInfo;
  private ProxyInterface proxyInterface;
  private Region<String, Object> region;

  private ManagementCacheListener managementCacheListener;

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule().strictness(Strictness.STRICT_STUBS);

  @Before
  public void setUp() throws Exception {
    distributedMember = mock(DistributedMember.class);
    mBeanProxyFactory = mock(MBeanProxyFactory.class);
    monitoringEvent = cast(mock(EntryEvent.class));
    monitoringEventNewValue = new Object();
    monitoringEventOldValue = new Object();
    objectName = ObjectName.getInstance(OBJECT_NAME_KEY);
    proxyInfo = mock(ProxyInfo.class);
    proxyInterface = mock(ProxyInterface.class);
    region = cast(mock(Region.class));

    when(monitoringEvent.getKey())
        .thenReturn(OBJECT_NAME_KEY);

    managementCacheListener = new ManagementCacheListener(mBeanProxyFactory);
  }

  @Test
  public void afterCreate_createsProxy() {
    when(monitoringEvent.getDistributedMember())
        .thenReturn(distributedMember);
    when(monitoringEvent.getNewValue())
        .thenReturn(monitoringEventNewValue);
    when(monitoringEvent.getRegion())
        .thenReturn(region);

    managementCacheListener.afterCreate(monitoringEvent);

    verify(mBeanProxyFactory)
        .createProxy(
            same(distributedMember),
            eq(objectName),
            same(region),
            same(monitoringEventNewValue));
  }

  @Test
  public void afterDestroy_removesProxy() {
    when(monitoringEvent.getDistributedMember())
        .thenReturn(distributedMember);
    when(monitoringEvent.getOldValue())
        .thenReturn(monitoringEventOldValue);

    managementCacheListener.afterDestroy(monitoringEvent);

    verify(mBeanProxyFactory)
        .removeProxy(
            same(distributedMember),
            eq(objectName),
            same(monitoringEventOldValue));
  }

  @Test
  public void afterUpdate_updatesProxy_ifProxyExists() {
    when(mBeanProxyFactory.findProxyInfo(eq(objectName)))
        .thenReturn(proxyInfo);
    when(monitoringEvent.getNewValue())
        .thenReturn(monitoringEventNewValue);
    when(monitoringEvent.getOldValue())
        .thenReturn(monitoringEventOldValue);
    when(proxyInfo.getProxyInstance())
        .thenReturn(proxyInterface);

    managementCacheListener.afterUpdate(monitoringEvent);

    verify(mBeanProxyFactory)
        .updateProxy(
            eq(objectName),
            same(proxyInfo),
            same(monitoringEventNewValue),
            same(monitoringEventOldValue));
  }

  @Test
  public void afterUpdate_updatesLastRefreshedTime_onProxyInterface_ifProxyExists() {
    when(mBeanProxyFactory.findProxyInfo(eq(objectName)))
        .thenReturn(proxyInfo);
    when(proxyInfo.getProxyInstance())
        .thenReturn(proxyInterface);

    managementCacheListener.afterUpdate(monitoringEvent);

    verify(proxyInterface)
        .setLastRefreshedTime(anyLong());
  }

  @Test
  public void afterUpdate_doesNothing_ifProxyDoesNotExist() {
    managementCacheListener.afterUpdate(monitoringEvent);

    verify(mBeanProxyFactory, times(0))
        .updateProxy(any(), any(), any(), any());
  }
}

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

import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.lang.reflect.Method;

import javax.management.NotificationBroadcasterSupport;
import javax.management.ObjectName;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.Region;
import org.apache.geode.distributed.DistributedMember;

public class MBeanProxyInvocationHandlerTest {

  private DistributedMember member;
  private ObjectName objectName;
  private Region<String, Object> monitoringRegion;
  private boolean isMXBean;
  private NotificationBroadcasterSupport emitter;
  private ProxyInterface proxyImpl;

  @Before
  public void setUp() {
    member = mock(DistributedMember.class);
    objectName = mock(ObjectName.class);
    monitoringRegion = mock(Region.class);
    isMXBean = false;
    emitter = mock(NotificationBroadcasterSupport.class);
    proxyImpl = mock(ProxyInterface.class);
  }

  @Test
  public void invoke_methodName_getLastRefreshedTime_delegatesToProxyImpl() throws Exception {
    MBeanProxyInvocationHandler mBeanProxyInvocationHandler =
        new MBeanProxyInvocationHandler(member, objectName, monitoringRegion, isMXBean, emitter,
            proxyImpl);
    Method getLastRefreshedTimeMethod = proxyImpl.getClass()
        .getMethod("getLastRefreshedTime");

    mBeanProxyInvocationHandler.invoke(proxyImpl, getLastRefreshedTimeMethod, new Object[] {});

    verify(proxyImpl)
        .getLastRefreshedTime();
  }

  @Test
  public void invoke_methodName_setLastRefreshedTime_delegatesToProxyImpl() throws Exception {
    MBeanProxyInvocationHandler mBeanProxyInvocationHandler =
        new MBeanProxyInvocationHandler(member, objectName, monitoringRegion, isMXBean, emitter,
            proxyImpl);
    Method setLastRefreshedTimeMethod = proxyImpl.getClass()
        .getMethod("setLastRefreshedTime", long.class);
    long lastRefreshedTimeParameter = 24;

    mBeanProxyInvocationHandler.invoke(proxyImpl, setLastRefreshedTimeMethod,
        new Object[] {lastRefreshedTimeParameter});

    verify(proxyImpl)
        .setLastRefreshedTime(eq(lastRefreshedTimeParameter));
  }

  @Test
  public void invoke_methodName_sendNotification_delegatesToProxyImpl() throws Exception {
    ProxyWithNotificationBroadcasterSupport proxyWithNotificationBroadcasterSupport =
        mock(ProxyWithNotificationBroadcasterSupport.class);
    MBeanProxyInvocationHandler mBeanProxyInvocationHandler =
        new MBeanProxyInvocationHandler(member, objectName, monitoringRegion, isMXBean, emitter,
            proxyWithNotificationBroadcasterSupport);
    Method setLastRefreshedTimeMethod = proxyWithNotificationBroadcasterSupport.getClass()
        .getMethod("setLastRefreshedTime", long.class);
    long lastRefreshedTimeParameter = 24;

    mBeanProxyInvocationHandler.invoke(proxyWithNotificationBroadcasterSupport,
        setLastRefreshedTimeMethod, new Object[] {lastRefreshedTimeParameter});

    verify(proxyWithNotificationBroadcasterSupport)
        .setLastRefreshedTime(eq(lastRefreshedTimeParameter));
  }

  private abstract class ProxyWithNotificationBroadcasterSupport
      extends NotificationBroadcasterSupport
      implements ProxyInterface {
    // nothing
  }
}

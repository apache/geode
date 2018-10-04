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

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import java.rmi.RemoteException;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.ManagementService;
import org.apache.geode.test.dunit.internal.InternalBlackboard;
import org.apache.geode.test.dunit.internal.InternalBlackboardImpl;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.JMXTest;
import org.apache.geode.test.junit.rules.LocatorStarterRule;

@Category({JMXTest.class})
public class MBeanFederationErrorPathDUnitTest {
  private static final int SERVER_1_VM_INDEX = 1;
  private static final String REGION_NAME = "test-region-1";

  public MemberVM server1, server2, server3;

  @Rule
  public LocatorStarterRule locator1 = new LocatorStarterRule();

  @Rule
  public ClusterStartupRule lsRule = new ClusterStartupRule();


  private InternalBlackboard bb;

  @Before
  public void before() throws Exception {
    locator1.withJMXManager().startLocator();

    bb = InternalBlackboardImpl.getInstance();
  }

  @Test
  public void destroyMBeanBeforeFederationCompletes()
      throws MalformedObjectNameException, RemoteException {
    String bbKey = "sync1";

    String beanName = "GemFire:service=Region,name=\"/test-region-1\",type=Member,member=server-1";
    ObjectName objectName = new ObjectName(beanName);

    InternalCache cache = locator1.getCache();
    SystemManagementService service =
        (SystemManagementService) ManagementService.getManagementService(cache);
    FederatingManager federatingManager = service.getFederatingManager();
    MBeanProxyFactory mBeanProxyFactory = federatingManager.getProxyFactory();
    MBeanProxyFactory spy = spy(mBeanProxyFactory);
    service.getFederatingManager().setProxyFactory(spy);

    Answer answer1 = new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        server1.invoke(() -> {
          InternalCache serverCache = ClusterStartupRule.getCache();
          Region region = serverCache.getRegionByPath("/" + REGION_NAME);
          region.destroyRegion();
        });

        Region<String, Object> monitoringRegion = invocation.getArgument(2);
        monitoringRegion.destroy(objectName.toString());

        assertThat((monitoringRegion).get(objectName.toString())).isNull();

        try {
          invocation.callRealMethod();
        } catch (Exception e) {
          bb.setMailbox(bbKey, e);
          return null;
        }
        bb.setMailbox(bbKey, "this is fine");
        return null;
      }
    };

    doAnswer(answer1).when(spy).createProxy(any(), eq(objectName), any(), any());

    server1 = lsRule.startServerVM(SERVER_1_VM_INDEX, locator1.getPort());

    server1.invoke(() -> {
      InternalCache cache1 = ClusterStartupRule.getCache();
      cache1.createRegionFactory(RegionShortcut.REPLICATE).create(REGION_NAME);
    });

    await().until(() -> bb.getMailbox(bbKey) != null);
    Object e = bb.getMailbox("sync1");

    assertThat(e).isNotInstanceOf(NullPointerException.class);
    assertThat((String) e).contains("this is fine");
  }
}

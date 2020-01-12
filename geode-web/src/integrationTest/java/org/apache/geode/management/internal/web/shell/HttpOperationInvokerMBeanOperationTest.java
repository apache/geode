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

package org.apache.geode.management.internal.web.shell;

import static org.apache.geode.distributed.ConfigurationProperties.DISTRIBUTED_SYSTEM_ID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.Set;

import javax.management.ObjectName;
import javax.management.Query;
import javax.management.QueryExp;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.management.DistributedSystemMXBean;
import org.apache.geode.management.internal.ManagementConstants;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.LocatorStarterRule;

public class HttpOperationInvokerMBeanOperationTest {

  @ClassRule
  public static LocatorStarterRule locator =
      new LocatorStarterRule().withHttpService().withProperty(DISTRIBUTED_SYSTEM_ID, "100")
          .withAutoStart();
  private HttpOperationInvoker invoker;

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  @Before
  public void before() throws Exception {
    gfsh.connectAndVerify(locator.getHttpPort(), GfshCommandRule.PortType.http);
    invoker = (HttpOperationInvoker) gfsh.getGfsh().getOperationInvoker();
  }

  @Test
  public void getAttribute() throws Exception {
    Integer distributedSystemId =
        (Integer) invoker.getAttribute(ManagementConstants.OBJECTNAME__DISTRIBUTEDSYSTEM_MXBEAN,
            "DistributedSystemId");
    assertThat(distributedSystemId).isEqualTo(100);
  }

  @Test
  public void invoke() throws Exception {
    String[] gatewayReceivers =
        (String[]) invoker.invoke(ManagementConstants.OBJECTNAME__DISTRIBUTEDSYSTEM_MXBEAN,
            "listGatewayReceivers", new Object[0], new String[0]);

    assertThat(gatewayReceivers).isEmpty();
  }

  @Test
  public void queryName() throws Exception {
    ObjectName objectName = ObjectName.getInstance("GemFire:type=Member,*");
    QueryExp query = Query.eq(Query.attr("Name"), Query.value("mock"));

    Set<ObjectName> names = invoker.queryNames(objectName, query);
    assertTrue(names.isEmpty());
  }

  @Test
  public void getClusterId() throws Exception {
    assertThat(invoker.getClusterId()).isEqualTo(100);
  }

  @Test
  public void getDistributedSystemMbean() throws Exception {
    DistributedSystemMXBean bean = invoker.getDistributedSystemMXBean();
    assertThat(bean).isInstanceOf(DistributedSystemMXBean.class);
  }
}

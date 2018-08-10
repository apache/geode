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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Set;

import javax.management.ObjectName;
import javax.management.Query;
import javax.management.QueryExp;

import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.management.DistributedSystemMXBean;
import org.apache.geode.management.internal.ManagementConstants;
import org.apache.geode.management.internal.cli.CommandRequest;
import org.apache.geode.security.NotAuthorizedException;
import org.apache.geode.security.SimpleTestSecurityManager;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.LocatorStarterRule;

public class HttpOperationInvokerSecurityTest {

  @ClassRule
  public static LocatorStarterRule locator =
      new LocatorStarterRule().withHttpService()
          .withSecurityManager(SimpleTestSecurityManager.class).withAutoStart();

  @ClassRule
  public static GfshCommandRule gfsh = new GfshCommandRule();

  private static HttpOperationInvoker invoker;
  private static CommandRequest request;

  @Test
  public void performBeanOperationsHasAuthorizationCheck() throws Exception {
    gfsh.secureConnectAndVerify(locator.getHttpPort(), GfshCommandRule.PortType.http, "test",
        "test");
    invoker = (HttpOperationInvoker) gfsh.getGfsh().getOperationInvoker();

    Integer distributedSystemId =
        (Integer) invoker.getAttribute(ManagementConstants.OBJECTNAME__DISTRIBUTEDSYSTEM_MXBEAN,
            "DistributedSystemId");
    assertThat(distributedSystemId).isEqualTo(-1);

    assertThat(invoker.getClusterId()).isEqualTo(-1);

    DistributedSystemMXBean bean = invoker.getDistributedSystemMXBean();
    assertThat(bean).isInstanceOf(DistributedSystemMXBean.class);

    assertThatThrownBy(
        () -> invoker.invoke(ManagementConstants.OBJECTNAME__DISTRIBUTEDSYSTEM_MXBEAN,
            "listGatewayReceivers", new Object[0], new String[0]))
                .isInstanceOf(NotAuthorizedException.class);

    ObjectName objectName = ObjectName.getInstance("GemFire:type=Member,*");
    QueryExp query = Query.eq(Query.attr("Name"), Query.value("mock"));

    Set<ObjectName> names = invoker.queryNames(objectName, query);
    assertTrue(names.isEmpty());
    gfsh.disconnect();
  }

  @Test
  public void processCommandHasAuthorizationCheck() throws Exception {
    gfsh.secureConnectAndVerify(locator.getHttpPort(), GfshCommandRule.PortType.http, "test",
        "test");

    invoker = (HttpOperationInvoker) gfsh.getGfsh().getOperationInvoker();

    request = mock(CommandRequest.class);
    when(request.getUserInput()).thenReturn("list members");

    assertThatThrownBy(() -> invoker.processCommand(request))
        .isInstanceOf(NotAuthorizedException.class);
    gfsh.disconnect();
  }
}

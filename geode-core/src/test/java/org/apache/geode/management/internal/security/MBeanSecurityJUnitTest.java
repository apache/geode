/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.management.internal.security;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.util.Set;

import javax.management.DynamicMBean;
import javax.management.MBeanServer;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.ReflectionException;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.AvailablePort;
import org.apache.geode.management.ManagementException;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.MemberMXBean;
import org.apache.geode.management.internal.MBeanJMXAdapter;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.geode.test.junit.categories.SecurityTest;

@Category({ IntegrationTest.class, SecurityTest.class })
public class MBeanSecurityJUnitTest {

  private static int jmxManagerPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);

  @ClassRule
  public static JsonAuthorizationCacheStartRule serverRule = new JsonAuthorizationCacheStartRule(jmxManagerPort, "org/apache/geode/management/internal/security/cacheServer.json");

  @Rule
  public MBeanServerConnectionRule connectionRule = new MBeanServerConnectionRule(jmxManagerPort);

  /**
   * No user can call createBean or unregisterBean of GemFire Domain
   */
  @Test
  @JMXConnectionConfiguration(user = "super-user", password = "1234567")
  public void testNoAccessWithWhoever() throws Exception{
    MBeanServerConnection con = connectionRule.getMBeanServerConnection();
    assertThatThrownBy(
        () -> con.createMBean("FakeClassName", new ObjectName("GemFire", "name", "foo"))
    ).isInstanceOf(SecurityException.class);

    assertThatThrownBy(
        () -> con.unregisterMBean(new ObjectName("GemFire", "name", "foo"))
    ).isInstanceOf(SecurityException.class);

    // user is allowed to create beans of other domains
    assertThatThrownBy(
        () -> con.createMBean("FakeClassName", new ObjectName("OtherDomain", "name", "foo"))
    ).isInstanceOf(ReflectionException.class);
  }

  /**
   * looks like everyone can query for beans, but the AccessControlMXBean is filtered from the result
   */
  @Test
  @JMXConnectionConfiguration(user = "stranger", password = "1234567")
  public void testQueryBean() throws MalformedObjectNameException, IOException {
    MBeanServerConnection con = connectionRule.getMBeanServerConnection();
    Set<ObjectInstance> objects = con.queryMBeans(ObjectName.getInstance(ResourceConstants.OBJECT_NAME_ACCESSCONTROL), null);
    assertThat(objects.size()).isEqualTo(0); // no AccessControlMBean in the query result

    objects = con.queryMBeans(ObjectName.getInstance("GemFire:service=CacheServer,*"), null);
    assertThat(objects.size()).isEqualTo(1);
  }

  /**
   * These calls does not go through the MBeanServerWrapper authentication, therefore is not throwing the SecurityExceptions
   */
  @Test
  public void testLocalCalls() throws Exception{
    MBeanServer server = MBeanJMXAdapter.mbeanServer;
    assertThatThrownBy(
        () -> server.createMBean("FakeClassName", new ObjectName("GemFire", "name", "foo"))
    ).isInstanceOf(ReflectionException.class);

    MBeanJMXAdapter adapter = new MBeanJMXAdapter();
    assertThatThrownBy(
        () -> adapter.registerMBean(mock(DynamicMBean.class), new ObjectName("MockDomain", "name", "mock"), false)
    ).isInstanceOf(ManagementException.class);
  }

  @Test
  @JMXConnectionConfiguration(user = "stranger", password = "1234567")
  public void testServerSideCalls(){
    // calls through ManagementService is not going through authorization checks
    ManagementService service = ManagementService.getManagementService(serverRule.getCache());
    MemberMXBean bean = service.getMemberMXBean();
    bean.compactAllDiskStores();
  }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.management.internal.security;

import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.management.CacheServerMXBean;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import java.io.IOException;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class CacheServerMBeanSecurityJUnitTest {
  private static int jmxManagerPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);

  private CacheServerMXBean cacheServerMXBean;
  private MBeanServerConnection con;

  @ClassRule
  public static JsonAuthorizationMBeanServerStartRule serverRule = new JsonAuthorizationMBeanServerStartRule(jmxManagerPort, "cacheServer.json");

  @Rule
  public MBeanServerConnectionRule<CacheServerMXBean> connectionRule = new MBeanServerConnectionRule(jmxManagerPort);

  @Before
  public void setUp() throws Exception {
    //assertThat(cache.getCacheServers()).hasSize(1);
    cacheServerMXBean = connectionRule.getProxyMBean(CacheServerMXBean.class, "GemFire:service=CacheServer,*");
    con = connectionRule.getMBeanServerConnection();
  }

  /**
   * No user can call createBean or unregisterBean
   */
  @Test
  @JMXConnectionConfiguration(user = "superuser", password = "1234567")
  public void testNoAccessWithWhoever() throws Exception{
    assertThatThrownBy(
        () -> con.createMBean("FakeClassName", new ObjectName("GemFire", "name", "foo"))
    ).isInstanceOf(SecurityException.class);

    assertThatThrownBy(
        () -> con.unregisterMBean(new ObjectName("GemFire", "name", "foo"))
    ).isInstanceOf(SecurityException.class);
  }


  @Test
  @JMXConnectionConfiguration(user = "superuser", password = "1234567")
  public void testAllAccess() throws Exception {
    cacheServerMXBean.removeIndex("foo"); // "DESTROY_INDEX",
    cacheServerMXBean.executeContinuousQuery("bar"); //QUERY
    cacheServerMXBean.fetchLoadProbe(); //LIST_DS
    cacheServerMXBean.getActiveCQCount(); //LIST_DS
    cacheServerMXBean.stopContinuousQuery("bar"); //STOP_CONTINUOUS_QUERY
    cacheServerMXBean.closeAllContinuousQuery("bar"); //STOP_CONTINUOUS_QUERY
    cacheServerMXBean.isRunning(); //LIST_DS
    cacheServerMXBean.showClientQueueDetails("foo"); //LIST_DS
  }

  @Test
  @JMXConnectionConfiguration(user = "user", password = "1234567")
  public void testSomeAccess() throws Exception {
    assertThatThrownBy(() -> cacheServerMXBean.removeIndex("foo")).isInstanceOf(SecurityException.class);
    assertThatThrownBy(() -> cacheServerMXBean.executeContinuousQuery("bar")).isInstanceOf(SecurityException.class);
    cacheServerMXBean.fetchLoadProbe();
  }

  @Test
  @JMXConnectionConfiguration(user = "stranger", password = "1234567")
  public void testNoAccess() throws Exception {
    assertThatThrownBy(() -> cacheServerMXBean.removeIndex("foo")).isInstanceOf(SecurityException.class);
    assertThatThrownBy(() -> cacheServerMXBean.executeContinuousQuery("bar")).isInstanceOf(SecurityException.class);
    assertThatThrownBy(() -> cacheServerMXBean.fetchLoadProbe()).isInstanceOf(SecurityException.class);
    assertThatThrownBy(() -> cacheServerMXBean.getActiveCQCount()).isInstanceOf(SecurityException.class);
    assertThatThrownBy(() -> cacheServerMXBean.stopContinuousQuery("bar")).isInstanceOf(SecurityException.class);
    assertThatThrownBy(() -> cacheServerMXBean.closeAllContinuousQuery("bar")).isInstanceOf(SecurityException.class);
    assertThatThrownBy(() -> cacheServerMXBean.isRunning()).isInstanceOf(SecurityException.class);
    assertThatThrownBy(() -> cacheServerMXBean.showClientQueueDetails("bar")).isInstanceOf(SecurityException.class);
  }

  /*
   * looks like everyone can query for beans, but the AccessControlMXBean is filtered from the result
   */
  @Test
  @JMXConnectionConfiguration(user = "stranger", password = "1234567")
  public void testQueryBean() throws MalformedObjectNameException, IOException {
    Set<ObjectInstance> objects = con.queryMBeans(ObjectName.getInstance(ResourceConstants.OBJECT_NAME_ACCESSCONTROL), null);
    assertThat(objects.size()).isEqualTo(0); // no AccessControlMBean in the query result

    objects = con.queryMBeans(ObjectName.getInstance("GemFire:service=CacheServer,*"), null);
    assertThat(objects.size()).isEqualTo(1);
  }
}

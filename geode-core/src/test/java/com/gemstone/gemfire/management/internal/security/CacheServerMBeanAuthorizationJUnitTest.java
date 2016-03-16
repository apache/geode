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
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Category(IntegrationTest.class)
public class CacheServerMBeanAuthorizationJUnitTest {
  private static int jmxManagerPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);

  private CacheServerMXBean cacheServerMXBean;

  @ClassRule
  public static JsonAuthorizationCacheStartRule serverRule = new JsonAuthorizationCacheStartRule(
      jmxManagerPort, "cacheServer.json");

  @Rule
  public MBeanServerConnectionRule connectionRule = new MBeanServerConnectionRule(jmxManagerPort);

  @Before
  public void setUp() throws Exception {
    cacheServerMXBean = connectionRule.getProxyMBean(CacheServerMXBean.class);
  }

  @Test
  @JMXConnectionConfiguration(user = "superuser", password = "1234567")
  public void testAllAccess() throws Exception {
    cacheServerMXBean.removeIndex("foo");
    cacheServerMXBean.executeContinuousQuery("bar");
    cacheServerMXBean.fetchLoadProbe();
    cacheServerMXBean.getActiveCQCount();
    cacheServerMXBean.stopContinuousQuery("bar");
    cacheServerMXBean.closeAllContinuousQuery("bar");
    cacheServerMXBean.isRunning();
    cacheServerMXBean.showClientQueueDetails("foo");
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
    assertThatThrownBy(() -> cacheServerMXBean.removeIndex("foo")).isInstanceOf(SecurityException.class).hasMessageContaining("INDEX:DESTROY");
    assertThatThrownBy(() -> cacheServerMXBean.executeContinuousQuery("bar")).isInstanceOf(SecurityException.class).hasMessageContaining("CONTINUOUS_QUERY:EXECUTE");
    assertThatThrownBy(() -> cacheServerMXBean.fetchLoadProbe()).isInstanceOf(SecurityException.class).hasMessageContaining("JMX:GET");
    assertThatThrownBy(() -> cacheServerMXBean.getActiveCQCount()).isInstanceOf(SecurityException.class).hasMessageContaining("JMX:GET");
    assertThatThrownBy(() -> cacheServerMXBean.stopContinuousQuery("bar")).isInstanceOf(SecurityException.class).hasMessageContaining("ONTINUOUS_QUERY:STOP");
    assertThatThrownBy(() -> cacheServerMXBean.closeAllContinuousQuery("bar")).isInstanceOf(SecurityException.class).hasMessageContaining("ONTINUOUS_QUERY:STOP");
    assertThatThrownBy(() -> cacheServerMXBean.isRunning()).isInstanceOf(SecurityException.class).hasMessageContaining("JMX:GET");
    assertThatThrownBy(() -> cacheServerMXBean.showClientQueueDetails("bar")).isInstanceOf(SecurityException.class).hasMessageContaining("JMX:GET");
  }
}

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

@Category(IntegrationTest.class)
public class CacheServerMBeanAuthenticationJUnitTest {
  private static int jmxManagerPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);

  private CacheServerMXBean cacheServerMXBean;

  @ClassRule
  public static JsonAuthorizationCacheStartRule serverRule = new JsonAuthorizationCacheStartRule(
      jmxManagerPort, "cacheServer.json", false);

  @Rule
  public MBeanServerConnectionRule<CacheServerMXBean> connectionRule = new MBeanServerConnectionRule(jmxManagerPort);

  @Before
  public void setUp() throws Exception {
    cacheServerMXBean = connectionRule.getProxyMBean(CacheServerMXBean.class, "GemFire:service=CacheServer,*");
  }

  @Test
  @JMXConnectionConfiguration(user = "superuser", password = "1234567")
  public void testAllAccess() throws Exception {
    cacheServerMXBean.removeIndex("foo"); // "INDEX:DESTROY",
    cacheServerMXBean.executeContinuousQuery("bar"); // CONTNUOUS_QUERY:EXECUTE
    cacheServerMXBean.fetchLoadProbe(); // DISTRIBUTED_SYSTEM:LIST_DS
    cacheServerMXBean.getActiveCQCount(); // DISTRIBUTED_SYSTEM:LIST_DS
    cacheServerMXBean.stopContinuousQuery("bar"); // CONTINUOUS_QUERY:STOP
    cacheServerMXBean.closeAllContinuousQuery("bar"); // CONTINUOUS_QUERY:STOP
    cacheServerMXBean.isRunning(); // DISTRIBUTED_SYSTEM:LIST_DS
    cacheServerMXBean.showClientQueueDetails("foo"); // DISTRIBUTED_SYSTEM:LIST_DS
  }

  @Test
  @JMXConnectionConfiguration(user = "user", password = "1234567")
  public void testSomeAccess() throws Exception {
    cacheServerMXBean.removeIndex("foo");
    cacheServerMXBean.executeContinuousQuery("bar");
    cacheServerMXBean.fetchLoadProbe();
  }
}

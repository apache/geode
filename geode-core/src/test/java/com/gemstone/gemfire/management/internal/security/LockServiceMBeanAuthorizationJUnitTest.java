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

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.locks.DLockService;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.management.LockServiceMXBean;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Category(IntegrationTest.class)
public class LockServiceMBeanAuthorizationJUnitTest {
  private static int jmxManagerPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);

  private LockServiceMXBean lockServiceMBean;

  @ClassRule
  public static JsonAuthorizationCacheStartRule serverRule = new JsonAuthorizationCacheStartRule(
      jmxManagerPort, "cacheServer.json");

  @Rule
  public MBeanServerConnectionRule connectionRule = new MBeanServerConnectionRule(jmxManagerPort);

  @BeforeClass
  public static void beforeClassSetUp() {
    Cache cache = serverRule.getCache();
    DLockService.create("test-lock-service", (InternalDistributedSystem) cache.getDistributedSystem(), false, true, true);
  }

  @Before
  public void setUp() throws Exception {
    lockServiceMBean = connectionRule.getProxyMBean(LockServiceMXBean.class);
  }

  @AfterClass
  public static void afterClassTeardown() {
    DLockService.destroyAll();
  }

  @Test
  @JMXConnectionConfiguration(user = "superuser", password = "1234567")
  public void testAllAccess() throws Exception {
    lockServiceMBean.becomeLockGrantor();
    lockServiceMBean.fetchGrantorMember();
    lockServiceMBean.getMemberCount();
    lockServiceMBean.isDistributed();
    lockServiceMBean.listThreadsHoldingLock();
  }

  @Test
  @JMXConnectionConfiguration(user = "user", password = "1234567")
  public void testSomeAccess() throws Exception {
    assertThatThrownBy(() -> lockServiceMBean.becomeLockGrantor()).isInstanceOf(SecurityException.class);
    lockServiceMBean.getMemberCount();
  }

  @Test
  @JMXConnectionConfiguration(user = "stranger", password = "1234567")
  public void testNoAccess() throws Exception {
    assertThatThrownBy(() -> lockServiceMBean.becomeLockGrantor()).isInstanceOf(SecurityException.class).hasMessageContaining("LOCK_SERVICE:BECOME_LOCK_GRANTOR");
    assertThatThrownBy(() -> lockServiceMBean.fetchGrantorMember()).isInstanceOf(SecurityException.class).hasMessageContaining("JMX:GET");
    assertThatThrownBy(() -> lockServiceMBean.getMemberCount()).isInstanceOf(SecurityException.class).hasMessageContaining("JMX:GET");
    assertThatThrownBy(() -> lockServiceMBean.isDistributed()).isInstanceOf(SecurityException.class).hasMessageContaining("JMX:GET");
    assertThatThrownBy(() -> lockServiceMBean.listThreadsHoldingLock()).isInstanceOf(SecurityException.class).hasMessageContaining("JMX:GET");
  }
}

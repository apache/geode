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
package org.apache.geode.management.internal.security;

import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_MANAGER;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.geode.cache.Cache;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.locks.DLockService;
import org.apache.geode.management.LockServiceMXBean;
import org.apache.geode.security.TestSecurityManager;
import org.apache.geode.test.dunit.rules.ConnectionConfiguration;
import org.apache.geode.test.dunit.rules.LocalServerStarterRule;
import org.apache.geode.test.dunit.rules.MBeanServerConnectionRule;
import org.apache.geode.test.dunit.rules.ServerStarterBuilder;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({IntegrationTest.class, SecurityTest.class})
public class LockServiceMBeanAuthorizationJUnitTest {
  private LockServiceMXBean lockServiceMBean;

  @ClassRule
  public static LocalServerStarterRule server = new ServerStarterBuilder().withJMXManager()
      .withProperty(SECURITY_MANAGER, TestSecurityManager.class.getName())
      .withProperty(TestSecurityManager.SECURITY_JSON,
          "org/apache/geode/management/internal/security/cacheServer.json")
      .buildInThisVM();

  @Rule
  public MBeanServerConnectionRule connectionRule =
      new MBeanServerConnectionRule(server.getJmxPort());

  @BeforeClass
  public static void beforeClassSetUp() {
    Cache cache = server.getCache();
    DLockService.create("test-lock-service",
        (InternalDistributedSystem) cache.getDistributedSystem(), false, true, true);
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
  @ConnectionConfiguration(user = "data-admin", password = "1234567")
  public void testAllAccess() throws Exception {
    lockServiceMBean.becomeLockGrantor();
    lockServiceMBean.fetchGrantorMember();
    lockServiceMBean.getMemberCount();
    lockServiceMBean.isDistributed();
    lockServiceMBean.listThreadsHoldingLock();
  }

  @Test
  @ConnectionConfiguration(user = "cluster-admin", password = "1234567")
  public void testSomeAccess() throws Exception {
    assertThatThrownBy(() -> lockServiceMBean.becomeLockGrantor());
    lockServiceMBean.getMemberCount();
  }

  @Test
  @ConnectionConfiguration(user = "data-user", password = "1234567")
  public void testNoAccess() throws Exception {
    assertThatThrownBy(() -> lockServiceMBean.becomeLockGrantor())
        .hasMessageContaining(TestCommand.dataManage.toString());
    assertThatThrownBy(() -> lockServiceMBean.fetchGrantorMember())
        .hasMessageContaining(TestCommand.clusterRead.toString());
    assertThatThrownBy(() -> lockServiceMBean.getMemberCount())
        .hasMessageContaining(TestCommand.clusterRead.toString());
    assertThatThrownBy(() -> lockServiceMBean.isDistributed())
        .hasMessageContaining(TestCommand.clusterRead.toString());
    assertThatThrownBy(() -> lockServiceMBean.listThreadsHoldingLock())
        .hasMessageContaining(TestCommand.clusterRead.toString());
  }
}

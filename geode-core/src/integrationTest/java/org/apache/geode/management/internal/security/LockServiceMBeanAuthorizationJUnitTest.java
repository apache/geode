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

import org.assertj.core.api.SoftAssertions;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.locks.DLockService;
import org.apache.geode.examples.SimpleSecurityManager;
import org.apache.geode.management.LockServiceMXBean;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.rules.ConnectionConfiguration;
import org.apache.geode.test.junit.rules.MBeanServerConnectionRule;
import org.apache.geode.test.junit.rules.ServerStarterRule;

@Category({SecurityTest.class})
public class LockServiceMBeanAuthorizationJUnitTest {
  private LockServiceMXBean lockServiceMBean;

  @ClassRule
  public static ServerStarterRule server = new ServerStarterRule().withJMXManager()
      .withProperty(SECURITY_MANAGER, SimpleSecurityManager.class.getName()).withAutoStart();

  @Rule
  public MBeanServerConnectionRule connectionRule =
      new MBeanServerConnectionRule(server::getJmxPort);

  @BeforeClass
  public static void beforeClassSetUp() {
    Cache cache = server.getCache();
    DLockService.create("test-lock-service",
        (InternalDistributedSystem) cache.getDistributedSystem(), false, true);
  }

  @Before
  public void setUp() throws Exception {
    lockServiceMBean = connectionRule.getProxyMXBean(LockServiceMXBean.class);
  }

  @AfterClass
  public static void afterClassTeardown() {
    DLockService.destroyAll();
  }

  @Test
  @ConnectionConfiguration(user = "clusterRead,clusterManage",
      password = "clusterRead,clusterManage")
  public void testAllAccess() throws Exception {
    lockServiceMBean.becomeLockGrantor();
    lockServiceMBean.fetchGrantorMember();
    lockServiceMBean.getMemberCount();
    lockServiceMBean.isDistributed();
    lockServiceMBean.listThreadsHoldingLock();
  }

  @Test
  @ConnectionConfiguration(user = "clusterManage", password = "clusterManage")
  public void testClusterManage() throws Exception {
    SoftAssertions softly = new SoftAssertions();
    lockServiceMBean.becomeLockGrantor(); // c:m
    softly.assertThatThrownBy(() -> lockServiceMBean.fetchGrantorMember())
        .hasMessageContaining(ResourcePermissions.CLUSTER_READ.toString());
    softly.assertThatThrownBy(() -> lockServiceMBean.getMemberCount())
        .hasMessageContaining(ResourcePermissions.CLUSTER_READ.toString());
    softly.assertThatThrownBy(() -> lockServiceMBean.isDistributed())
        .hasMessageContaining(ResourcePermissions.CLUSTER_READ.toString());
    softly.assertThatThrownBy(() -> lockServiceMBean.listThreadsHoldingLock())
        .hasMessageContaining(ResourcePermissions.CLUSTER_READ.toString());
    softly.assertAll();
  }

  @Test
  @ConnectionConfiguration(user = "clusterRead", password = "clusterRead")
  public void testClusterRead() throws Exception {
    SoftAssertions softly = new SoftAssertions();
    softly.assertThatThrownBy(() -> lockServiceMBean.becomeLockGrantor())
        .hasMessageContaining(ResourcePermissions.CLUSTER_MANAGE.toString());
    lockServiceMBean.fetchGrantorMember();
    lockServiceMBean.getMemberCount();
    lockServiceMBean.isDistributed();
    lockServiceMBean.listThreadsHoldingLock();
    softly.assertAll();
  }

  @Test
  @ConnectionConfiguration(user = "user", password = "user")
  public void testNoAccess() throws Exception {
    SoftAssertions softly = new SoftAssertions();

    softly.assertThatThrownBy(() -> lockServiceMBean.becomeLockGrantor())
        .hasMessageContaining(ResourcePermissions.CLUSTER_MANAGE.toString());
    softly.assertThatThrownBy(() -> lockServiceMBean.fetchGrantorMember())
        .hasMessageContaining(ResourcePermissions.CLUSTER_READ.toString());
    softly.assertThatThrownBy(() -> lockServiceMBean.getMemberCount())
        .hasMessageContaining(ResourcePermissions.CLUSTER_READ.toString());
    softly.assertThatThrownBy(() -> lockServiceMBean.isDistributed())
        .hasMessageContaining(ResourcePermissions.CLUSTER_READ.toString());
    softly.assertThatThrownBy(() -> lockServiceMBean.listThreadsHoldingLock())
        .hasMessageContaining(ResourcePermissions.CLUSTER_READ.toString());

    softly.assertAll();
  }
}

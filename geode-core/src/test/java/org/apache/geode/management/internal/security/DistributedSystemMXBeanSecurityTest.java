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

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.management.DistributedSystemMXBean;
import org.apache.geode.security.NotAuthorizedException;
import org.apache.geode.security.SimpleTestSecurityManager;
import org.apache.geode.security.TestSecurityManager;
import org.apache.geode.test.dunit.rules.ConnectionConfiguration;
import org.apache.geode.test.dunit.rules.MBeanServerConnectionRule;
import org.apache.geode.test.dunit.rules.ServerStarterRule;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.SecurityTest;

@Category({DistributedTest.class, SecurityTest.class})
public class DistributedSystemMXBeanSecurityTest {

  private DistributedSystemMXBean bean;

  @ClassRule
  public static ServerStarterRule server = new ServerStarterRule().withJMXManager()
      .withProperty(SECURITY_MANAGER, TestSecurityManager.class.getName())
      .withSecurityManager(SimpleTestSecurityManager.class).withAutoStart();

  @Rule
  public MBeanServerConnectionRule connectionRule =
      new MBeanServerConnectionRule(server::getJmxPort);

  @Before
  public void setUp() throws Exception {
    bean = connectionRule.getProxyMBean(DistributedSystemMXBean.class);
  }

  @Test
  @ConnectionConfiguration(user = "dataRead", password = "dataRead")
  public void testDataReadAccess() throws Exception {
    assertThatThrownBy(() -> bean.backupAllMembers(null, null))
        .isInstanceOf(NotAuthorizedException.class);
  }

  @Test
  @ConnectionConfiguration(user = "clusterManageDisk", password = "clusterManageDisk")
  public void testDiskManageAccess() throws Exception {
    assertThatThrownBy(() -> bean.backupAllMembers(null, null))
        .isInstanceOf(NotAuthorizedException.class);
  }

  @Test
  @ConnectionConfiguration(user = "dataRead,clusterWriteDisk",
      password = "dataRead,clusterWriteDisk")
  public void testBothAccess() throws Exception {
    assertThatThrownBy(() -> bean.backupAllMembers(null, null))
        .isNotInstanceOf(NotAuthorizedException.class);
  }
}

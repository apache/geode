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

import org.assertj.core.api.SoftAssertions;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.management.DiskStoreMXBean;
import org.apache.geode.security.SimpleTestSecurityManager;
import org.apache.geode.test.dunit.rules.ConnectionConfiguration;
import org.apache.geode.test.dunit.rules.MBeanServerConnectionRule;
import org.apache.geode.test.dunit.rules.ServerStarterRule;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.geode.test.junit.categories.SecurityTest;

@Category({IntegrationTest.class, SecurityTest.class})
public class DiskStoreMXBeanSecurityJUnitTest {
  private DiskStoreMXBean bean;

  @ClassRule
  public static ServerStarterRule server = new ServerStarterRule().withJMXManager()
      .withProperty(SECURITY_MANAGER, SimpleTestSecurityManager.class.getName()).withAutoStart();

  @BeforeClass
  public static void beforeClass() throws Exception {
    server.getCache().createDiskStoreFactory().create("diskstore");
  }

  @Rule
  public MBeanServerConnectionRule connectionRule =
      new MBeanServerConnectionRule(server::getJmxPort);

  @Before
  public void setUp() throws Exception {
    bean = connectionRule.getProxyMBean(DiskStoreMXBean.class);
  }

  @Test
  @ConnectionConfiguration(user = "clusterRead", password = "clusterRead")
  public void testClusterReadAccess() throws Exception {
    assertThatThrownBy(() -> bean.flush()).hasMessageContaining(TestCommand.diskManage.toString());
    assertThatThrownBy(() -> bean.forceCompaction())
        .hasMessageContaining(TestCommand.diskManage.toString());
    assertThatThrownBy(() -> bean.forceRoll())
        .hasMessageContaining(TestCommand.diskManage.toString());
    assertThatThrownBy(() -> bean.setDiskUsageCriticalPercentage(0.5f))
        .hasMessageContaining(TestCommand.diskManage.toString());
    assertThatThrownBy(() -> bean.setDiskUsageWarningPercentage(0.5f))
        .hasMessageContaining(TestCommand.diskManage.toString());

    bean.getCompactionThreshold();
    bean.getDiskDirectories();
    bean.getDiskReadsRate();
    bean.isAutoCompact();
    bean.isForceCompactionAllowed();
  }

  @Test
  @ConnectionConfiguration(user = "clusterManageDisk", password = "clusterManageDisk")
  public void testDiskManageAccess() throws Exception {
    assertThatThrownBy(() -> bean.getCompactionThreshold())
        .hasMessageContaining(TestCommand.clusterRead.toString());
    assertThatThrownBy(() -> bean.getDiskDirectories())
        .hasMessageContaining(TestCommand.clusterRead.toString());
    assertThatThrownBy(() -> bean.getDiskReadsRate())
        .hasMessageContaining(TestCommand.clusterRead.toString());
    assertThatThrownBy(() -> bean.isAutoCompact())
        .hasMessageContaining(TestCommand.clusterRead.toString());
    assertThatThrownBy(() -> bean.isForceCompactionAllowed())
        .hasMessageContaining(TestCommand.clusterRead.toString());

    bean.flush();
    bean.forceCompaction();
    bean.forceRoll();
    bean.setDiskUsageCriticalPercentage(0.5f);
    bean.setDiskUsageWarningPercentage(0.5f);
  }

  @Test
  @ConnectionConfiguration(user = "data,cluster", password = "data,cluster")
  public void testAllAccess() throws Exception {
    bean.flush();
    bean.forceCompaction();
    bean.forceRoll();
    bean.getCompactionThreshold();
    bean.getDiskDirectories();
    bean.getDiskReadsRate();
    bean.isAutoCompact();
    bean.isForceCompactionAllowed();
    bean.setDiskUsageCriticalPercentage(0.5f);
    bean.setDiskUsageWarningPercentage(0.5f);
  }

  @Test
  @ConnectionConfiguration(user = "noAccess", password = "noAccess")
  public void testNoAccess() throws Exception {
    SoftAssertions softly = new SoftAssertions();

    softly.assertThatThrownBy(() -> bean.flush())
        .hasMessageContaining(TestCommand.clusterManageDisk.toString());
    softly.assertThatThrownBy(() -> bean.forceCompaction())
        .hasMessageContaining(TestCommand.clusterManageDisk.toString());
    softly.assertThatThrownBy(() -> bean.forceRoll())
        .hasMessageContaining(TestCommand.clusterManageDisk.toString());
    softly.assertThatThrownBy(() -> bean.getCompactionThreshold())
        .hasMessageContaining(TestCommand.clusterRead.toString());
    softly.assertThatThrownBy(() -> bean.getDiskDirectories())
        .hasMessageContaining(TestCommand.clusterRead.toString());
    softly.assertThatThrownBy(() -> bean.getDiskReadsRate())
        .hasMessageContaining(TestCommand.clusterRead.toString());
    softly.assertThatThrownBy(() -> bean.isAutoCompact())
        .hasMessageContaining(TestCommand.clusterRead.toString());
    softly.assertThatThrownBy(() -> bean.isForceCompactionAllowed())
        .hasMessageContaining(TestCommand.clusterRead.toString());
    softly.assertThatThrownBy(() -> bean.setDiskUsageCriticalPercentage(0.5f))
        .hasMessageContaining(TestCommand.clusterManageDisk.toString());
    softly.assertThatThrownBy(() -> bean.setDiskUsageWarningPercentage(0.5f))
        .hasMessageContaining(TestCommand.clusterManageDisk.toString());
    softly.assertAll();
  }
}

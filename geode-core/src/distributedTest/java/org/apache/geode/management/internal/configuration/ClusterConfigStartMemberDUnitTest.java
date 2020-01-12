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
 *
 */
package org.apache.geode.management.internal.configuration;

import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_CONFIGURATION_DIR;
import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_CLUSTER_CONFIGURATION;
import static org.apache.geode.distributed.ConfigurationProperties.GROUPS;
import static org.apache.geode.distributed.ConfigurationProperties.LOAD_CLUSTER_CONFIGURATION_FROM_DIR;

import java.io.File;
import java.util.Properties;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.management.internal.configuration.utils.ZipUtils;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;


public class ClusterConfigStartMemberDUnitTest extends ClusterConfigTestBase {

  private MemberVM locator;

  @Rule
  public ClusterStartupRule lsRule = new ClusterStartupRule();

  @Before
  public void before() throws Exception {
    locator = startLocatorWithLoadCCFromDir();
  }

  @Test
  public void testStartLocator() throws Exception {
    MemberVM secondLocator = lsRule.startLocatorVM(1, locator.getPort());
    REPLICATED_CONFIG_FROM_ZIP.verify(secondLocator);
  }

  @Test
  public void testStartServerWithSingleGroup() throws Exception {
    ClusterConfig expectedNoGroupConfig = new ClusterConfig(CLUSTER);
    ClusterConfig expectedGroup1Config = new ClusterConfig(CLUSTER, GROUP1);
    ClusterConfig expectedGroup2Config = new ClusterConfig(CLUSTER, GROUP2);

    MemberVM serverWithNoGroup = lsRule.startServerVM(1, serverProps, locator.getPort());
    expectedNoGroupConfig.verify(serverWithNoGroup);

    serverProps.setProperty(GROUPS, "group1");
    MemberVM serverForGroup1 = lsRule.startServerVM(2, serverProps, locator.getPort());
    expectedGroup1Config.verify(serverForGroup1);

    serverProps.setProperty(GROUPS, "group2");
    MemberVM serverForGroup2 = lsRule.startServerVM(3, serverProps, locator.getPort());
    expectedGroup2Config.verify(serverForGroup2);
  }

  @Test
  public void testStartServerWithMultipleGroup() throws Exception {
    ClusterConfig expectedGroup1And2Config = new ClusterConfig(CLUSTER, GROUP1, GROUP2);

    serverProps.setProperty(GROUPS, "group1,group2");
    MemberVM server = lsRule.startServerVM(1, serverProps, locator.getPort());

    expectedGroup1And2Config.verify(server);
  }

  private MemberVM startLocatorWithLoadCCFromDir() throws Exception {
    File locatorDir = new File(lsRule.getWorkingDirRoot(), "vm0");
    File configDir = new File(locatorDir, "cluster_config");

    // The unzip should yield a cluster config directory structure like:
    // tempFolder/locator-0/cluster_config/cluster/cluster.xml
    // tempFolder/locator-0/cluster_config/cluster/cluster.properties
    // tempFolder/locator-0/cluster_config/cluster/cluster.jar
    // tempFolder/locator-0/cluster_config/group1/ {group1.xml, group1.properties, group1.jar}
    // tempFolder/locator-0/cluster_config/group2/ ...
    ZipUtils.unzip(clusterConfigZipPath, configDir.getCanonicalPath());

    Properties properties = new Properties();
    properties.setProperty(ENABLE_CLUSTER_CONFIGURATION, "true");
    properties.setProperty(LOAD_CLUSTER_CONFIGURATION_FROM_DIR, "true");
    properties.setProperty(CLUSTER_CONFIGURATION_DIR, locatorDir.getCanonicalPath());

    MemberVM locator = lsRule.startLocatorVM(0, properties);

    return locator;
  }
}

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
import static org.junit.Assert.assertTrue;

import java.io.File;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.management.MemberMXBean;
import org.apache.geode.security.NotAuthorizedException;
import org.apache.geode.security.SimpleTestSecurityManager;
import org.apache.geode.test.dunit.rules.ConnectionConfiguration;
import org.apache.geode.test.dunit.rules.MBeanServerConnectionRule;
import org.apache.geode.test.dunit.rules.ServerStarterRule;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.geode.test.junit.categories.SecurityTest;

@Category({IntegrationTest.class, SecurityTest.class})
public class DeployCommandsSecurityTest {

  private MemberMXBean bean;

  @ClassRule
  public static ServerStarterRule server = new ServerStarterRule()
      .withProperty(SECURITY_MANAGER, SimpleTestSecurityManager.class.getName()).withJMXManager()
      .withAutoStart();

  @ClassRule
  public static TemporaryFolder temporaryFolder = new TemporaryFolder();
  private static String deployCommand = null;
  private static String zipFileName = "functions.jar";

  @BeforeClass
  public static void beforeClass() throws Exception {
    File zipFile = temporaryFolder.newFile(zipFileName);
    deployCommand = "deploy --jar=" + zipFile.getAbsolutePath();
  }

  @Rule
  public MBeanServerConnectionRule connectionRule =
      new MBeanServerConnectionRule(server::getJmxPort);

  @Before
  public void setUp() throws Exception {
    bean = connectionRule.getProxyMBean(MemberMXBean.class);
  }


  @Test // regular user can't deploy
  @ConnectionConfiguration(user = "user", password = "user")
  public void testNoAccess1() {
    assertThatThrownBy(() -> bean.processCommand(deployCommand))
        .isInstanceOf(NotAuthorizedException.class);
  }

  @Test // only data access right is not enough to deploy
  @ConnectionConfiguration(user = "data", password = "data")
  public void testNoAccess2() {
    assertThatThrownBy(() -> bean.processCommand(deployCommand))
        .isInstanceOf(NotAuthorizedException.class);
  }

  @Test // not sufficient privilege
  @ConnectionConfiguration(user = "clusterRead,clusterWrite,dataRead,dataWrite",
      password = "clusterRead,clusterWrite,dataRead,dataWrite")
  public void testNoAccess4() {
    assertThatThrownBy(() -> bean.processCommand(deployCommand))
        .isInstanceOf(NotAuthorizedException.class);
  }

  @Test // only power user can deploy
  @ConnectionConfiguration(user = "cluster,data", password = "cluster,data")
  public void testPowerAccess1() {
    String result = bean.processCommand(deployCommand);
    assertTrue(result.contains("File does not contain valid JAR content: functions.jar"));
  }

  @Test // only power user can deploy
  @ConnectionConfiguration(user = "clusterManage,clusterWrite,dataManage,dataWrite",
      password = "clusterManage,clusterWrite,dataManage,dataWrite")
  public void testPowerAccess2() {
    String result = bean.processCommand(deployCommand);
    assertTrue(result.contains("File does not contain valid JAR content: functions.jar"));
  }



}

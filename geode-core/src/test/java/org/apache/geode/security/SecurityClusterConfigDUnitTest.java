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

package org.apache.geode.security;

import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_START;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_MANAGER;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_POST_PROCESSOR;
import static org.assertj.core.api.Java6Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.geode.GemFireConfigException;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.dunit.rules.LocatorServerStartupRule;
import org.apache.geode.test.dunit.rules.ServerStarterRule;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.FlakyTest;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Properties;

@Category({DistributedTest.class, SecurityTest.class})
public class SecurityClusterConfigDUnitTest extends JUnit4DistributedTestCase {

  @Rule
  public LocatorServerStartupRule lsRule = new LocatorServerStartupRule();

  @Before
  public void before() throws Exception {
    IgnoredException
        .addIgnoredException(LocalizedStrings.GEMFIRE_CACHE_SECURITY_MISCONFIGURATION.toString());
    IgnoredException
        .addIgnoredException(LocalizedStrings.GEMFIRE_CACHE_SECURITY_MISCONFIGURATION_2.toString());
    Properties props = new Properties();
    props.setProperty(SECURITY_MANAGER, SimpleTestSecurityManager.class.getName());
    props.setProperty(JMX_MANAGER, "false");
    props.setProperty(JMX_MANAGER_START, "false");
    props.setProperty(JMX_MANAGER_PORT, 0 + "");
    props.setProperty(SECURITY_POST_PROCESSOR, PDXPostProcessor.class.getName());
    lsRule.startLocatorVM(0, props);
  }

  @Category(FlakyTest.class) // GEODE-1977
  @Test
  public void testStartServerWithClusterConfig() throws Exception {
    Properties props = new Properties();
    // the following are needed for peer-to-peer authentication
    props.setProperty("security-username", "cluster");
    props.setProperty("security-password", "cluster");
    props.setProperty("use-cluster-configuration", "true");

    // initial security properties should only contain initial set of values
    ServerStarterRule serverStarter = new ServerStarterRule(props);
    serverStarter.startServer(lsRule.getMember(0).getPort());
    DistributedSystem ds = serverStarter.cache.getDistributedSystem();

    // after cache is created, we got the security props passed in by cluster config
    Properties secProps = ds.getSecurityProperties();
    assertEquals(4, secProps.size());
    assertTrue(secProps.containsKey("security-manager"));
    assertTrue(secProps.containsKey("security-post-processor"));
  }

  @Test
  public void testStartServerWithSameSecurityManager() throws Exception {
    Properties props = new Properties();

    // the following are needed for peer-to-peer authentication
    props.setProperty("security-username", "cluster");
    props.setProperty("security-password", "cluster");
    props.setProperty("use-cluster-configuration", "true");
    props.setProperty(SECURITY_MANAGER, SimpleTestSecurityManager.class.getName());

    // initial security properties should only contain initial set of values
    ServerStarterRule serverStarter = new ServerStarterRule(props);
    serverStarter.startServer(lsRule.getMember(0).getPort());
    DistributedSystem ds = serverStarter.cache.getDistributedSystem();

    // after cache is created, we got the security props passed in by cluster config
    Properties secProps = ds.getSecurityProperties();
    assertTrue(secProps.containsKey("security-manager"));
    assertTrue(secProps.containsKey("security-post-processor"));
  }

  @Category(FlakyTest.class) // GEODE-1975
  @Test
  public void serverWithDifferentSecurityManagerShouldThrowException() {
    Properties props = new Properties();

    // the following are needed for peer-to-peer authentication
    props.setProperty("security-username", "cluster");
    props.setProperty("security-password", "cluster");
    props.setProperty("security-manager", "mySecurityManager");
    props.setProperty("use-cluster-configuration", "true");

    // initial security properties should only contain initial set of values
    ServerStarterRule serverStarter = new ServerStarterRule(props);

    assertThatThrownBy(() -> serverStarter.startServer(lsRule.getMember(0).getPort()))
        .isInstanceOf(GemFireConfigException.class)
        .hasMessage(LocalizedStrings.GEMFIRE_CACHE_SECURITY_MISCONFIGURATION.toLocalizedString());

  }

  @Test
  public void serverWithDifferentPostProcessorShouldThrowException() {
    Properties props = new Properties();

    // the following are needed for peer-to-peer authentication
    props.setProperty("security-username", "cluster");
    props.setProperty("security-password", "cluster");
    props.setProperty(SECURITY_POST_PROCESSOR, "this-is-not-ok");
    props.setProperty("use-cluster-configuration", "true");

    // initial security properties should only contain initial set of values
    ServerStarterRule serverStarter = new ServerStarterRule(props);

    assertThatThrownBy(() -> serverStarter.startServer(lsRule.getMember(0).getPort()))
        .isInstanceOf(GemFireConfigException.class)
        .hasMessage(LocalizedStrings.GEMFIRE_CACHE_SECURITY_MISCONFIGURATION.toLocalizedString());

  }

  @Category(FlakyTest.class) // GEODE-1974
  @Test
  public void serverConnectingToSecuredLocatorMustUseClusterConfig() {
    Properties props = new Properties();

    // the following are needed for peer-to-peer authentication
    props.setProperty("security-username", "cluster");
    props.setProperty("security-password", "cluster");
    props.setProperty("security-manager", "mySecurityManager");
    props.setProperty("use-cluster-configuration", "false");

    ServerStarterRule serverStarter = new ServerStarterRule(props);

    assertThatThrownBy(() -> serverStarter.startServer(lsRule.getMember(0).getPort()))
        .isInstanceOf(GemFireConfigException.class)
        .hasMessage(LocalizedStrings.GEMFIRE_CACHE_SECURITY_MISCONFIGURATION_2.toLocalizedString());

  }

}

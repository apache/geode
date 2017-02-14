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

import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.assertj.core.api.Java6Assertions.*;
import static org.junit.Assert.*;

import java.util.Properties;

import org.apache.geode.test.dunit.rules.LocatorServerStartupRule;
import org.apache.geode.test.dunit.rules.ServerStarterRule;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.GemFireConfigException;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.SecurityTest;

@Category({DistributedTest.class, SecurityTest.class})

public class ClusterConfigWithoutSecurityDUnitTest extends JUnit4DistributedTestCase {

  @Rule
  public LocatorServerStartupRule lsRule = new LocatorServerStartupRule();

  @Before
  public void before() throws Exception {
    IgnoredException
        .addIgnoredException(LocalizedStrings.GEMFIRE_CACHE_SECURITY_MISCONFIGURATION.toString());
    IgnoredException
        .addIgnoredException(LocalizedStrings.GEMFIRE_CACHE_SECURITY_MISCONFIGURATION_2.toString());
    lsRule.startLocatorVM(0, new Properties());
  }

  @After
  public void after() {
    IgnoredException.removeAllExpectedExceptions();
  }

  // when locator is not secured, a secured server should be allowed to start with its own security
  // manager
  // if use-cluster-config is false
  @Test
  public void serverShouldBeAllowedToStartWithSecurityIfNotUsingClusterConfig() throws Exception {
    Properties props = new Properties();
    props.setProperty(SECURITY_MANAGER, SimpleTestSecurityManager.class.getName());
    props.setProperty(SECURITY_POST_PROCESSOR, PDXPostProcessor.class.getName());

    props.setProperty("use-cluster-configuration", "false");

    // initial security properties should only contain initial set of values
    ServerStarterRule serverStarter = new ServerStarterRule(props);
    serverStarter.startServer(lsRule.getMember(0).getPort());
    DistributedSystem ds = serverStarter.cache.getDistributedSystem();

    // after cache is created, the configuration won't chagne
    Properties secProps = ds.getSecurityProperties();
    assertEquals(2, secProps.size());
    assertEquals(SimpleTestSecurityManager.class.getName(),
        secProps.getProperty("security-manager"));
    assertEquals(PDXPostProcessor.class.getName(), secProps.getProperty("security-post-processor"));
  }


  @Test
  // when locator is not secured, server should not be secured if use-cluster-config is true
  public void serverShouldNotBeAllowedToStartWithSecurityIfUsingClusterConfig() {
    Properties props = new Properties();

    props.setProperty("security-manager", "mySecurityManager");
    props.setProperty("use-cluster-configuration", "true");

    ServerStarterRule serverStarter = new ServerStarterRule(props);

    assertThatThrownBy(() -> serverStarter.startServer(lsRule.getMember(0).getPort()))
        .isInstanceOf(GemFireConfigException.class)
        .hasMessage(LocalizedStrings.GEMFIRE_CACHE_SECURITY_MISCONFIGURATION.toLocalizedString());
  }

}

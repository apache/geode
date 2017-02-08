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
import static org.junit.Assert.*;

import java.util.Properties;

import org.apache.geode.test.dunit.rules.LocatorServerStartupRule;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.dunit.rules.ServerStarterRule;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.SecurityTest;

@Category({DistributedTest.class, SecurityTest.class})

public class SecurityWithoutClusterConfigDUnitTest extends JUnit4DistributedTestCase {

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
    props.setProperty(SECURITY_POST_PROCESSOR, PDXPostProcessor.class.getName());
    props.setProperty(ENABLE_CLUSTER_CONFIGURATION, "false");
    lsRule.startLocatorVM(0, props);
  }

  @Test
  // if a secured locator is started without cluster config service, the server is free to use any
  // security manager
  // or no security manager at all. Currently this is valid, but not recommended.
  public void testStartServerWithClusterConfig() throws Exception {

    Properties props = new Properties();
    // the following are needed for peer-to-peer authentication
    props.setProperty("security-username", "cluster");
    props.setProperty("security-password", "cluster");
    props.setProperty("security-manager", TestSecurityManager.class.getName());
    props.setProperty("use-cluster-configuration", "true");

    // initial security properties should only contain initial set of values
    ServerStarterRule serverStarter = new ServerStarterRule(props);
    serverStarter.startServer(lsRule.getMember(0).getPort());
    DistributedSystem ds = serverStarter.cache.getDistributedSystem();
    assertEquals(3, ds.getSecurityProperties().size());

    // after cache is created, we got the security props passed in by cluster config
    Properties secProps = ds.getSecurityProperties();
    assertEquals(3, secProps.size());
    assertEquals(TestSecurityManager.class.getName(), secProps.getProperty("security-manager"));
    assertFalse(secProps.containsKey("security-post-processor"));
  }
}

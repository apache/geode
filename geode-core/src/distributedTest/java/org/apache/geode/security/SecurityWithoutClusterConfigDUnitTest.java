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

import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_CLUSTER_CONFIGURATION;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_MANAGER;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_POST_PROCESSOR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.Properties;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.rules.ServerStarterRule;

@Category({SecurityTest.class})

public class SecurityWithoutClusterConfigDUnitTest {

  @Rule
  public ClusterStartupRule lsRule = new ClusterStartupRule();

  @Rule
  public ServerStarterRule serverStarter = new ServerStarterRule();

  private MemberVM locator;

  @Before
  public void before() throws Exception {
    IgnoredException
        .addIgnoredException(
            "A server cannot specify its own security-manager or security-post-processor when using cluster configuration"
                .toString());
    IgnoredException
        .addIgnoredException(
            "A server must use cluster configuration when joining a secured cluster.".toString());
    Properties props = new Properties();
    props.setProperty(SECURITY_MANAGER, SimpleTestSecurityManager.class.getName());
    props.setProperty(SECURITY_POST_PROCESSOR, PDXPostProcessor.class.getName());
    props.setProperty(ENABLE_CLUSTER_CONFIGURATION, "false");
    locator = lsRule.startLocatorVM(0, props);
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
    serverStarter.withProperties(props).withConnectionToLocator(locator.getPort()).startServer();
    DistributedSystem ds = serverStarter.getCache().getDistributedSystem();
    assertEquals(3, ds.getSecurityProperties().size());

    // after cache is created, we got the security props passed in by cluster config
    Properties secProps = ds.getSecurityProperties();
    assertEquals(3, secProps.size());
    assertEquals(TestSecurityManager.class.getName(), secProps.getProperty("security-manager"));
    assertFalse(secProps.containsKey("security-post-processor"));
  }
}

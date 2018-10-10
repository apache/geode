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

import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_MANAGER;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_POST_PROCESSOR;
import static org.apache.geode.distributed.ConfigurationProperties.USE_CLUSTER_CONFIGURATION;
import static org.assertj.core.api.Java6Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.GemFireConfigException;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.rules.ServerStarterRule;

@Category({SecurityTest.class})
public class ClusterConfigWithoutSecurityDUnitTest {

  @Rule
  public ClusterStartupRule lsRule = new ClusterStartupRule();

  @Rule
  public ServerStarterRule serverStarter = new ServerStarterRule();

  @Before
  public void before() throws Exception {
    IgnoredException
        .addIgnoredException(
            "A server cannot specify its own security-manager or security-post-processor when using cluster configuration"
                .toString());
    IgnoredException
        .addIgnoredException(
            "A server must use cluster configuration when joining a secured cluster.".toString());
    this.lsRule.startLocatorVM(0, new Properties());
  }

  @After
  public void after() throws Exception {
    IgnoredException.removeAllExpectedExceptions();
  }

  /**
   * when locator is not secured, a secured server should be allowed to start with its own security
   * manager if use-cluster-config is false
   */
  @Test
  public void serverShouldBeAllowedToStartWithSecurityIfNotUsingClusterConfig() throws Exception {
    Properties props = new Properties();
    props.setProperty(SECURITY_MANAGER, SimpleTestSecurityManager.class.getName());
    props.setProperty(SECURITY_POST_PROCESSOR, PDXPostProcessor.class.getName());
    props.setProperty(USE_CLUSTER_CONFIGURATION, "false");

    // initial security properties should only contain initial set of values
    this.serverStarter.startServer(props, this.lsRule.getMember(0).getPort());
    DistributedSystem ds = this.serverStarter.getCache().getDistributedSystem();

    // after cache is created, the configuration won't chagne
    Properties secProps = ds.getSecurityProperties();
    assertEquals(2, secProps.size());
    assertEquals(SimpleTestSecurityManager.class.getName(),
        secProps.getProperty("security-manager"));
    assertEquals(PDXPostProcessor.class.getName(), secProps.getProperty("security-post-processor"));
  }

  /**
   * when locator is not secured, server should not be secured if use-cluster-config is true
   */
  @Test
  public void serverShouldNotBeAllowedToStartWithSecurityIfUsingClusterConfig() throws Exception {
    Properties props = new Properties();
    props.setProperty(SECURITY_MANAGER, SimpleTestSecurityManager.class.getName());
    props.setProperty(USE_CLUSTER_CONFIGURATION, "true");

    assertThatThrownBy(
        () -> this.serverStarter.startServer(props, this.lsRule.getMember(0).getPort()))
            .isInstanceOf(GemFireConfigException.class).hasMessage(
                "A server cannot specify its own security-manager or security-post-processor when using cluster configuration");
  }

  @Test
  public void nonExistentSecurityManagerThrowsGemFireSecurityException() throws Exception {
    Properties props = new Properties();
    props.setProperty(SECURITY_MANAGER, "mySecurityManager");
    props.setProperty(USE_CLUSTER_CONFIGURATION, "true");

    assertThatThrownBy(() -> this.serverStarter.startServer(props, this.lsRule.getMember(0)
        .getPort())).isInstanceOf(GemFireSecurityException.class).hasMessage(
            "Instance could not be obtained, java.lang.ClassNotFoundException: mySecurityManager");
  }

}

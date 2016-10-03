/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.geode.security;

import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.assertj.core.api.Java6Assertions.*;
import static org.junit.Assert.*;

import java.io.File;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.GemFireConfigException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.security.templates.SampleSecurityManager;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.SecurityTest;

@Category({ DistributedTest.class, SecurityTest.class })

public class SecurityClusterConfigDUnitTest extends JUnit4DistributedTestCase {

  private int locatorPort = 0;

  @Rule
  public transient TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void before() throws Exception {
    IgnoredException.addIgnoredException(LocalizedStrings.GEMFIRE_CACHE_SECURITY_MISCONFIGURATION.toString());
    IgnoredException.addIgnoredException(LocalizedStrings.GEMFIRE_CACHE_SECURITY_MISCONFIGURATION_2.toString());
    File locatorFile = temporaryFolder.newFile("locator.log");
    final Host host = Host.getHost(0);
    VM locator = host.getVM(0);
    // set up locator with security
    this.locatorPort = locator.invoke(() -> {
      Properties props = new Properties();
      props.setProperty(SampleSecurityManager.SECURITY_JSON, "org/apache/geode/management/internal/security/cacheServer.json");
      props.setProperty(SECURITY_MANAGER, SampleSecurityManager.class.getName());
      props.setProperty(MCAST_PORT, "0");
      props.put(JMX_MANAGER, "false");
      props.put(JMX_MANAGER_START, "false");
      props.put(JMX_MANAGER_PORT, 0);
      props.setProperty(SECURITY_POST_PROCESSOR, PDXPostProcessor.class.getName());
      Locator lc = Locator.startLocatorAndDS(locatorPort, locatorFile, props);
      return lc.getPort();
    });
  }

  @After
  public void after() {
    IgnoredException.removeAllExpectedExceptions();
  }

  @Test
  public void testStartServerWithClusterConfig() throws Exception {
    // set up server with security
    String locators = "localhost[" + locatorPort + "]";

    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, locators);

    // the following are needed for peer-to-peer authentication
    props.setProperty("security-username", "cluster-manager");
    props.setProperty("security-password", "1234567");
    props.setProperty("use-cluster-configuration", "true");

    // initial security properties should only contain initial set of values
    InternalDistributedSystem ds = getSystem(props);
    assertEquals(2, ds.getSecurityProperties().size());

    CacheFactory.create(ds);

    // after cache is created, we got the security props passed in by cluster config
    Properties secProps = ds.getSecurityProperties();
    assertEquals(4, secProps.size());
    assertTrue(secProps.containsKey("security-manager"));
    assertTrue(secProps.containsKey("security-post-processor"));
  }

  @Test
  public void testStartServerWithSameSecurityManager() throws Exception {
    // set up server with security
    String locators = "localhost[" + locatorPort + "]";

    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, locators);

    // the following are needed for peer-to-peer authentication
    props.setProperty("security-username", "cluster-manager");
    props.setProperty("security-password", "1234567");
    props.setProperty("use-cluster-configuration", "true");
    props.setProperty(SampleSecurityManager.SECURITY_JSON, "org/apache/geode/management/internal/security/cacheServer.json");
    props.setProperty(SECURITY_MANAGER, SampleSecurityManager.class.getName());

    // initial security properties should only contain initial set of values
    InternalDistributedSystem ds = getSystem(props);

    CacheFactory.create(ds);

    // after cache is created, we got the security props passed in by cluster config
    Properties secProps = ds.getSecurityProperties();
    assertTrue(secProps.containsKey("security-manager"));
    assertTrue(secProps.containsKey("security-post-processor"));
  }

  @Test
  public void serverWithDifferentSecurityManagerShouldThrowException() {
    // set up server with security
    String locators = "localhost[" + locatorPort + "]";

    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, locators);

    // the following are needed for peer-to-peer authentication
    props.setProperty("security-username", "cluster-manager");
    props.setProperty("security-password", "1234567");
    props.setProperty("security-manager", "mySecurityManager");
    props.setProperty("use-cluster-configuration", "true");

    // initial security properties should only contain initial set of values
    InternalDistributedSystem ds = getSystem(props);

    assertThatThrownBy(() -> CacheFactory.create(ds)).isInstanceOf(GemFireConfigException.class)
                                                     .hasMessage(LocalizedStrings.GEMFIRE_CACHE_SECURITY_MISCONFIGURATION
                                                       .toLocalizedString());

  }

  @Test
  public void serverWithDifferentPostProcessorShouldThrowException() {
    // set up server with security
    String locators = "localhost[" + locatorPort + "]";

    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, locators);

    // the following are needed for peer-to-peer authentication
    props.setProperty("security-username", "cluster-manager");
    props.setProperty("security-password", "1234567");
    props.setProperty(SECURITY_POST_PROCESSOR, "this-is-not-ok");
    props.setProperty("use-cluster-configuration", "true");


    // initial security properties should only contain initial set of values
    InternalDistributedSystem ds = getSystem(props);

    assertThatThrownBy(() -> CacheFactory.create(ds)).isInstanceOf(GemFireConfigException.class)
                                                     .hasMessage(LocalizedStrings.GEMFIRE_CACHE_SECURITY_MISCONFIGURATION
                                                       .toLocalizedString());

  }


  @Test
  public void serverConnectingToSecuredLocatorMustUseClusterConfig() {
    // set up server with security
    String locators = "localhost[" + locatorPort + "]";

    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, locators);

    // the following are needed for peer-to-peer authentication
    props.setProperty("security-username", "cluster-manager");
    props.setProperty("security-password", "1234567");
    props.setProperty("security-manager", "mySecurityManager");
    props.setProperty("use-cluster-configuration", "false");

    // initial security properties should only contain initial set of values
    InternalDistributedSystem ds = getSystem(props);

    assertThatThrownBy(() -> CacheFactory.create(ds)).isInstanceOf(GemFireConfigException.class)
                                                     .hasMessage(LocalizedStrings.GEMFIRE_CACHE_SECURITY_MISCONFIGURATION_2
                                                       .toLocalizedString());

  }

}

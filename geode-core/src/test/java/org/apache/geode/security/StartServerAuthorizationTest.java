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
import static org.assertj.core.api.Assertions.*;

import java.io.File;
import java.util.Properties;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.distributed.Locator;
import org.apache.geode.security.templates.SampleSecurityManager;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.SecurityTest;

@Category({ DistributedTest.class, SecurityTest.class })
public class StartServerAuthorizationTest extends JUnit4DistributedTestCase {
  private int locatorPort = 0;

  @Before
  public void before() throws Exception {
    final Host host = Host.getHost(0);
    VM locator = host.getVM(0);
    // set up locator with security
    this.locatorPort = locator.invoke(()->{
      Properties props = new Properties();
      props.setProperty(SampleSecurityManager.SECURITY_JSON, "org/apache/geode/management/internal/security/cacheServer.json");
      props.setProperty(SECURITY_MANAGER, SampleSecurityManager.class.getName());
      props.setProperty(MCAST_PORT, "0");
      props.put(JMX_MANAGER, "true");
      props.put(JMX_MANAGER_START, "true");
      props.put(JMX_MANAGER_PORT, 0);
      props.setProperty(SECURITY_POST_PROCESSOR, PDXPostProcessor.class.getName());
      Locator lc = Locator.startLocatorAndDS(locatorPort, new File("locator.log"), props);
      return lc.getPort();
    });
  }

  @Test
  public void testStartServerWithInvalidCredential() throws Exception{
    // set up server with security
    String locators = "localhost[" + locatorPort + "]";

    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, locators);

    // the following are needed for peer-to-peer authentication
    props.setProperty("security-username", "stranger");
    props.setProperty("security-password", "wrongPswd");

    assertThatThrownBy(()->getSystem(props)).isInstanceOf(GemFireSecurityException.class).hasMessageContaining("Authentication error. Please check your credentials.");
  }

  @Test
  public void testStartServerWithInsufficientPrevilage() throws Exception{
    // set up server with security
    String locators = "localhost[" + locatorPort + "]";

    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, locators);

    // the following are needed for peer-to-peer authentication
    props.setProperty("security-username", "stranger");
    props.setProperty("security-password", "1234567");


    assertThatThrownBy(()->getSystem(props)).isInstanceOf(GemFireSecurityException.class).hasMessageContaining("stranger not authorized for CLUSTER:MANAGE");
  }

  @Test
  public void testStartServerWithSufficientPrevilage() throws Exception{
    // set up server with security
    String locators = "localhost[" + locatorPort + "]";

    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, locators);

    // the following are needed for peer-to-peer authentication
    props.setProperty("security-username", "cluster-manager");
    props.setProperty("security-password", "1234567");

    // No exception should be thrown
    getSystem(props);
  }

}

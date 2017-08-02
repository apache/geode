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

package org.apache.geode.tools.pulse;

import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_BIND_ADDRESS;
import static org.assertj.core.api.Assertions.assertThat;

import java.net.InetAddress;
import java.util.Properties;

import org.apache.geode.test.dunit.rules.EmbeddedPulseRule;
import org.apache.geode.test.dunit.rules.LocatorStarterRule;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.geode.tools.pulse.internal.data.Cluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

@RunWith(Enclosed.class)
@Category(IntegrationTest.class)
public class PulseConnectivityTest {
  @ClassRule
  public static LocatorStarterRule locator = new LocatorStarterRule().withJMXManager();

  // Test Connectivity for Default Configuration
  @Category(IntegrationTest.class)
  public static class DefaultBindAddressTest {
    @Rule
    public EmbeddedPulseRule pulse = new EmbeddedPulseRule();

    @BeforeClass
    public static void beforeClass() throws Exception {
      locator.startLocator();
    }

    @Test
    public void testConnectToJmx() throws Exception {
      pulse.useJmxPort(locator.getJmxPort());
      Cluster cluster = pulse.getRepository().getCluster("admin", null);
      assertThat(cluster.isConnectedFlag()).isTrue();
      assertThat(cluster.getServerCount()).isEqualTo(0);
    }

    @Test
    public void testConnectToLocator() throws Exception {
      pulse.useLocatorPort(locator.getPort());
      Cluster cluster = pulse.getRepository().getCluster("admin", null);
      assertThat(cluster.isConnectedFlag()).isTrue();
      assertThat(cluster.getServerCount()).isEqualTo(0);
    }

    @AfterClass
    public static void afterClass() throws Exception {
      locator.after();
    }
  }

  // GEODE-3292: Test Connectivity for Non Default Configuration
  @Category(IntegrationTest.class)
  public static class NonDefaultBindAddressTest {
    public static String jmxBindAddress;

    @Rule
    public EmbeddedPulseRule pulse = new EmbeddedPulseRule();

    @BeforeClass
    public static void beforeClass() throws Exception {
      // Make sure we use something different than "localhost" for this test.
      jmxBindAddress = InetAddress.getLocalHost().getHostName();
      if ("localhost".equals(jmxBindAddress)) {
        jmxBindAddress = InetAddress.getLocalHost().getHostAddress();
      }

      Properties locatorProperties = new Properties();
      locatorProperties.setProperty(JMX_MANAGER_BIND_ADDRESS, jmxBindAddress);
      locator.withProperties(locatorProperties).startLocator();
    }

    @Test
    public void testConnectToJmx() throws Exception {
      pulse.useJmxManager(jmxBindAddress, locator.getJmxPort());
      Cluster cluster = pulse.getRepository().getCluster("admin", null);
      assertThat(cluster.isConnectedFlag()).isTrue();
      assertThat(cluster.getServerCount()).isEqualTo(0);
    }

    @AfterClass
    public static void afterClass() throws Exception {
      locator.after();
    }
  }
}

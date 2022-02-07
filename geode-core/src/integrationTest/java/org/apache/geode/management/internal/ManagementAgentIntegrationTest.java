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

package org.apache.geode.management.internal;

import static org.apache.geode.management.internal.ManagementAgent.JAVA_RMI_SERVER_HOSTNAME;
import static org.apache.geode.management.internal.ManagementAgent.setRmiServerHostname;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import javax.management.MBeanServer;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXServiceURL;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junitpioneer.jupiter.ClearSystemProperty;

import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.internal.serialization.filter.FilterConfiguration;

class ManagementAgentIntegrationTest {

  private static final String A_HOST_NAME = "a.host.name";

  @TempDir
  Path temporaryFolder;

  @Test
  void testSetMBeanServer() throws IOException {
    final DistributionConfig distributionConfig = mock(DistributionConfig.class);
    final InternalCache internalCache = mock(InternalCache.class);
    final FilterConfiguration filterConfiguration = mock(FilterConfiguration.class);
    final SecurityService securityService = mock(SecurityService.class);
    when(internalCache.getSecurityService()).thenReturn(securityService);
    when(securityService.isIntegratedSecurity()).thenReturn(false);
    final Path tempFile = Files.createFile(temporaryFolder.resolve("testFile")).toAbsolutePath();
    when(distributionConfig.getJmxManagerAccessFile()).thenReturn(tempFile.toString());
    final ManagementAgent managementAgent =
        new ManagementAgent(distributionConfig, internalCache, filterConfiguration);
    final MBeanServer mBeanServer = mock(MBeanServer.class);

    managementAgent.setJmxConnectorServer(new JmxConnectorServerWithMBeanServer(mBeanServer));
    assertThatCode(() -> managementAgent
        .setMBeanServerForwarder(ManagementFactory.getPlatformMBeanServer(), new HashMap<>()))
            .doesNotThrowAnyException();

    managementAgent.setMBeanServerForwarder(ManagementFactory.getPlatformMBeanServer(),
        new HashMap<>());

    managementAgent.setJmxConnectorServer(new JmxConnectorServerNullMBeanServer());
    assertThatCode(() -> managementAgent
        .setMBeanServerForwarder(ManagementFactory.getPlatformMBeanServer(), new HashMap<>()))
            .doesNotThrowAnyException();
  }

  @Test
  @ClearSystemProperty(key = JAVA_RMI_SERVER_HOSTNAME)
  void setRmiServerHostnameSetsSystemPropertyWhenNotBlank() {
    setRmiServerHostname(A_HOST_NAME);
    assertThat(System.getProperty(JAVA_RMI_SERVER_HOSTNAME)).isEqualTo(A_HOST_NAME);
  }

  @Test
  @ClearSystemProperty(key = JAVA_RMI_SERVER_HOSTNAME)
  void setRmiServerHostnameDoesNotSetSystemPropertyWhenNull() {
    setRmiServerHostname(null);
    assertThat(System.getProperty(JAVA_RMI_SERVER_HOSTNAME)).isNull();
  }

  @Test
  @ClearSystemProperty(key = JAVA_RMI_SERVER_HOSTNAME)
  void setRmiServerHostnameDoesNotSetSystemPropertyWhenEmpty() {
    setRmiServerHostname("");
    assertThat(System.getProperty(JAVA_RMI_SERVER_HOSTNAME)).isNull();
  }

  private static class JmxConnectorServerWithMBeanServer extends JMXConnectorServer {
    public JmxConnectorServerWithMBeanServer(final MBeanServer mBeanServer) {
      super(mBeanServer);
    }

    @Override
    public void start() {}

    @Override
    public void stop() {}

    @Override
    public boolean isActive() {
      return false;
    }

    @Override
    public JMXServiceURL getAddress() {
      return null;
    }

    @Override
    public Map<String, ?> getAttributes() {
      return null;
    }
  }

  private static class JmxConnectorServerNullMBeanServer extends JMXConnectorServer {
    @Override
    public void start() {}

    @Override
    public void stop() {}

    @Override
    public boolean isActive() {
      return false;
    }

    @Override
    public JMXServiceURL getAddress() {
      return null;
    }

    @Override
    public Map<String, ?> getAttributes() {
      return null;
    }
  }
}

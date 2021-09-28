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

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Map;

import javax.management.MBeanServer;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXServiceURL;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.internal.serialization.filter.FilterConfiguration;

public class ManagementAgentIntegrationTest {

  // private ManagementAgent managementAgent = spy(ManagementAgent.class);
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testSetMBeanServer() throws IOException {
    DistributionConfig distributionConfig = mock(DistributionConfig.class);
    InternalCache internalCache = mock(InternalCache.class);
    FilterConfiguration filter = mock(FilterConfiguration.class);
    SecurityService securityService = mock(SecurityService.class);
    when(internalCache.getSecurityService()).thenReturn(securityService);
    when(securityService.isIntegratedSecurity()).thenReturn(false);
    File tempFile = temporaryFolder.newFile("testFile");
    when(distributionConfig.getJmxManagerAccessFile()).thenReturn(tempFile.getCanonicalPath());
    ManagementAgent managementAgent =
        new ManagementAgent(distributionConfig, internalCache, filter);
    MBeanServer mBeanServer = mock(MBeanServer.class);
    JMXConnectorServer jmxConnectorServerWithMBeanServer = new JMXConnectorServer(mBeanServer) {
      @Override
      public void start() throws IOException {

      }

      @Override
      public void stop() throws IOException {

      }

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
    };
    managementAgent.setJmxConnectorServer(jmxConnectorServerWithMBeanServer);
    assertThatCode(() -> managementAgent
        .setMBeanServerForwarder(ManagementFactory.getPlatformMBeanServer(), new HashMap<>()))
            .doesNotThrowAnyException();
    managementAgent.setMBeanServerForwarder(ManagementFactory.getPlatformMBeanServer(),
        new HashMap<>());

    JMXConnectorServer jmxConnectorServerNullMBeanServer = new JMXConnectorServer() {
      @Override
      public void start() throws IOException {

      }

      @Override
      public void stop() throws IOException {

      }

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
    };

    managementAgent.setJmxConnectorServer(jmxConnectorServerNullMBeanServer);
    assertThatCode(() -> managementAgent
        .setMBeanServerForwarder(ManagementFactory.getPlatformMBeanServer(), new HashMap<>()))
            .doesNotThrowAnyException();
  }
}

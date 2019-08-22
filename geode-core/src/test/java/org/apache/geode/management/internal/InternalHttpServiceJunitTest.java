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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;

import org.eclipse.jetty.server.ServerConnector;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionConfigImpl;
import org.apache.geode.internal.cache.InternalHttpService;
import org.apache.geode.internal.net.SSLConfigurationFactory;
import org.apache.geode.internal.security.SecurableCommunicationChannel;

/**
 * The InternalHttpServiceJunitTest class is a test suite of test cases testing the contract and
 * functionality of the InternalHttpService class. It does not start Jetty.
 */
public class InternalHttpServiceJunitTest {
  private DistributionConfig distributionConfig;

  @Before
  public void setup() {
    distributionConfig = new DistributionConfigImpl(new Properties());
    SSLConfigurationFactory.setDistributionConfig(distributionConfig);
  }

  @After
  public void teardown() {
    SSLConfigurationFactory.close();
  }

  @Test
  public void testSetPortNoBindAddress() {
    final InternalHttpService jetty = new InternalHttpService(null, 8090, SSLConfigurationFactory
        .getSSLConfigForComponent(distributionConfig, SecurableCommunicationChannel.WEB));
    assertThat(jetty).isNotNull();
    assertThat(jetty.getHttpServer().getConnectors()[0]).isNotNull();
    assertThat(((ServerConnector) jetty.getHttpServer().getConnectors()[0]).getPort())
        .isEqualTo(8090);
  }

  @Test
  public void testSetPortWithBindAddress() {
    final InternalHttpService jetty =
        new InternalHttpService("10.123.50.1", 10480, SSLConfigurationFactory
            .getSSLConfigForComponent(distributionConfig, SecurableCommunicationChannel.WEB));
    assertThat(jetty).isNotNull();
    assertThat(jetty.getHttpServer().getConnectors()[0]).isNotNull();
    assertThat(((ServerConnector) jetty.getHttpServer().getConnectors()[0]).getPort())
        .isEqualTo(10480);
  }
}

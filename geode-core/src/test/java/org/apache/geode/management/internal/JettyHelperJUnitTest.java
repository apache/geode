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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.util.Properties;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.GemFireConfigException;
import org.apache.geode.distributed.internal.DistributionConfigImpl;
import org.apache.geode.internal.net.SSLConfigurationFactory;
import org.apache.geode.internal.security.SecurableCommunicationChannel;
import org.apache.geode.test.junit.categories.UnitTest;

/**
 * The JettyHelperJUnitTest class is a test suite of test cases testing the contract and
 * functionality of the JettyHelper class. Does not start Jetty.
 *
 * @see org.apache.geode.management.internal.JettyHelper
 * @see org.jmock.Mockery
 * @see org.junit.Assert
 * @see org.junit.Test
 */
public class JettyHelperJUnitTest {

  @Before
  public void setup() {
    SSLConfigurationFactory.setDistributionConfig(new DistributionConfigImpl(new Properties()));
  }

  @After
  public void teardown() {
    SSLConfigurationFactory.close();
  }

  @Test
  public void testSetPortNoBindAddress() throws Exception {

    final Server jetty;
    try {
      jetty = JettyHelper.initJetty(null, 8090,
          SSLConfigurationFactory.getSSLConfigForComponent(SecurableCommunicationChannel.WEB));
      assertNotNull(jetty);
      assertNotNull(jetty.getConnectors()[0]);
      assertEquals(8090, ((ServerConnector) jetty.getConnectors()[0]).getPort());
    } catch (GemFireConfigException e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testSetPortWithBindAddress() throws Exception {
    try {
      final Server jetty = JettyHelper.initJetty("10.123.50.1", 10480,
          SSLConfigurationFactory.getSSLConfigForComponent(SecurableCommunicationChannel.WEB));

      assertNotNull(jetty);
      assertNotNull(jetty.getConnectors()[0]);
      assertEquals(10480, ((ServerConnector) jetty.getConnectors()[0]).getPort());
    } catch (GemFireConfigException e) {
      fail(e.getMessage());
    }
  }
}

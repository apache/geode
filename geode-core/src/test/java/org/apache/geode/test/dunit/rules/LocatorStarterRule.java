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

package org.apache.geode.test.dunit.rules;

import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_START;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.assertTrue;

import org.awaitility.Awaitility;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.InternalLocator;
import org.junit.rules.ExternalResource;

import java.io.Serializable;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * This is a rule to start up a locator in your current VM. It's useful for your Integration Tests.
 *
 * You can create this rule with and without a Property. If the rule is created with a property, the
 * locator will started automatically for you. If not, you can start the locator by using one of the
 * startLocator function. Either way, the rule will handle shutting down the locator properly for
 * you.
 *
 * If you need a rule to start a server/locator in different VMs for Distributed tests, You should
 * use {@link LocatorServerStartupRule}.
 */

public class LocatorStarterRule extends ExternalResource implements Serializable {

  public InternalLocator locator;

  private Properties properties;

  public LocatorStarterRule() {}

  public LocatorStarterRule(Properties properties) {
    this.properties = properties;
  }

  public void startLocator() throws Exception {
    if (properties == null)
      properties = new Properties();
    startLocator(properties);
  }

  public void startLocator(Properties properties) throws Exception {
    if (!properties.containsKey(MCAST_PORT)) {
      properties.setProperty(MCAST_PORT, "0");
    }
    if (properties.containsKey(JMX_MANAGER_PORT)) {
      int jmxPort = Integer.parseInt(properties.getProperty(JMX_MANAGER_PORT));
      if (jmxPort > 0) {
        if (!properties.containsKey(JMX_MANAGER)) {
          properties.put(JMX_MANAGER, "true");
        }
        if (!properties.containsKey(JMX_MANAGER_START)) {
          properties.put(JMX_MANAGER_START, "true");
        }
      }
    }
    locator = (InternalLocator) Locator.startLocatorAndDS(0, null, properties);
    int locatorPort = locator.getPort();
    locator.resetInternalLocatorFileNamesWithCorrectPortNumber(locatorPort);

    if (locator.getConfig().getEnableClusterConfiguration()) {
      Awaitility.await().atMost(65, TimeUnit.SECONDS)
          .until(() -> assertTrue(locator.isSharedConfigurationRunning()));
    }
  }

  @Override
  protected void before() throws Throwable {
    if (properties != null)
      startLocator(properties);
  }

  @Override
  protected void after() {
    if (locator != null) {
      locator.stop();
    }
  }
}

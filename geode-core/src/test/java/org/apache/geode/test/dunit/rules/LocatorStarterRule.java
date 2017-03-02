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
import static org.apache.geode.distributed.ConfigurationProperties.LOG_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.NAME;
import static org.apache.geode.distributed.Locator.startLocatorAndDS;
import static org.junit.Assert.assertTrue;

import org.apache.geode.distributed.internal.InternalLocator;
import org.awaitility.Awaitility;

import java.io.File;
import java.io.IOException;
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

public class LocatorStarterRule extends MemberStarterRule implements Locator {

  private transient InternalLocator locator;

  public LocatorStarterRule() {}

  public LocatorStarterRule(File workingDir) {
    this.workingDir = workingDir.getAbsoluteFile();
  }

  public InternalLocator getLocator() {
    return locator;
  }

  @Override
  protected void stopMember() {
    if (locator != null) {
      locator.stop();
    }
  }

  public LocatorStarterRule startLocator() {
    return startLocator(new Properties());
  }

  public LocatorStarterRule startLocator(Properties properties) {
    if (properties == null)
      properties = new Properties();
    if (!properties.containsKey(NAME)) {
      properties.setProperty(NAME, "locator");
    }

    name = properties.getProperty(NAME);
    if (!properties.containsKey(LOG_FILE)) {
      properties.setProperty(LOG_FILE, new File(name + ".log").getAbsolutePath());
    }

    if (!properties.containsKey(MCAST_PORT)) {
      properties.setProperty(MCAST_PORT, "0");
    }
    if (properties.containsKey(JMX_MANAGER_PORT)) {
      jmxPort = Integer.parseInt(properties.getProperty(JMX_MANAGER_PORT));
      if (jmxPort > 0) {
        if (!properties.containsKey(JMX_MANAGER)) {
          properties.put(JMX_MANAGER, "true");
        }
        if (!properties.containsKey(JMX_MANAGER_START)) {
          properties.put(JMX_MANAGER_START, "true");
        }
      }
    }
    try {
      locator = (InternalLocator) startLocatorAndDS(0, null, properties);
    } catch (IOException e) {
      throw new RuntimeException("unable to start up locator.", e);
    }
    memberPort = locator.getPort();
    locator.resetInternalLocatorFileNamesWithCorrectPortNumber(memberPort);

    if (locator.getConfig().getEnableClusterConfiguration()) {
      Awaitility.await().atMost(65, TimeUnit.SECONDS)
          .until(() -> assertTrue(locator.isSharedConfigurationRunning()));
    }
    return this;
  }
}

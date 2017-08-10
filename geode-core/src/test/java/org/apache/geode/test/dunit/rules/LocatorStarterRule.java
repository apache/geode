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

import static org.apache.geode.distributed.Locator.startLocatorAndDS;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;

import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalLocator;

/**
 * This is a rule to start up a locator in your current VM. It's useful for your Integration Tests.
 *
 * This rules allows you to create/start a locator using any @ConfigurationProperties, you can chain
 * the configuration of the rule like this: LocatorStarterRule locator = new LocatorStarterRule()
 * .withProperty(key, value) .withName(name) .withProperties(properties) .withSecurityManager(class)
 * .withJmxManager() etc, etc. If your rule calls withAutoStart(), the locator will be started
 * before your test code.
 *
 * In your test code, you can use the rule to access the locator's attributes, like the port
 * information, working dir, name, and the InternalLocator it creates.
 *
 * If you need a rule to start a server/locator in different VMs for Distributed tests, You should
 * use {@link LocatorServerStartupRule}.
 */

public class LocatorStarterRule extends MemberStarterRule<LocatorStarterRule> implements Locator {

  private transient InternalLocator locator;

  public InternalLocator getLocator() {
    return locator;
  }

  @Override
  protected void stopMember() {
    if (locator != null) {
      locator.stop();
    }
  }

  @Override
  public void before() {
    normalizeProperties();
    // always use a random jmxPort/httpPort when using the rule to start the locator
    if (jmxPort < 0) {
      withJMXManager(false);
    }
    if (autoStart) {
      startLocator();
    }
  }

  public void startLocator() {
    try {
      // this will start a jmx manager and admin rest service by default
      locator = (InternalLocator) startLocatorAndDS(0, null, properties);
    } catch (IOException e) {
      throw new RuntimeException("unable to start up locator.", e);
    }
    memberPort = locator.getPort();
    DistributionConfig config = locator.getConfig();
    jmxPort = config.getJmxManagerPort();
    httpPort = config.getHttpServicePort();
    locator.resetInternalLocatorFileNamesWithCorrectPortNumber(memberPort);

    if (config.getEnableClusterConfiguration()) {
      Awaitility.await().atMost(65, TimeUnit.SECONDS)
          .until(() -> assertTrue(locator.isSharedConfigurationRunning()));
    }
  }
}

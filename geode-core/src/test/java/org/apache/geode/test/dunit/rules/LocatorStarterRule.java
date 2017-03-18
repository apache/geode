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

import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalLocator;
import org.awaitility.Awaitility;

import java.io.File;
import java.io.IOException;
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

public class LocatorStarterRule extends MemberStarterRule<LocatorStarterRule> implements Locator {

  private transient InternalLocator locator;

  public LocatorStarterRule() {}

  public LocatorStarterRule(File workingDir) {
    super(workingDir);
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
    normalizeProperties();
    // start locator will start a jmx manager by default, if withJmxManager is not called explicitly
    // the tests will use random ports by default.
    if (jmxPort < 0) {
      withJMXManager();
    }

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
    return this;
  }
}

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
package org.apache.geode.test.junit.rules;

import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_CLUSTER_CONFIGURATION;
import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_MANAGEMENT_REST_SERVICE;
import static org.apache.geode.distributed.Locator.startLocatorAndDS;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.UncheckedIOException;

import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;

/**
 * This is a rule to start up a locator in your current VM. It's useful for your Integration Tests.
 *
 * <p>
 * This rules allows you to create/start a locator using any @ConfigurationProperties, you can chain
 * the configuration of the rule like this: LocatorStarterRule locator = new LocatorStarterRule()
 * .withProperty(key, value) .withName(name) .withProperties(properties) .withSecurityManager(class)
 * .withJmxManager() etc, etc. If your rule calls withAutoStart(), the locator will be started
 * before your test code.
 *
 * <p>
 * In your test code, you can use the rule to access the locator's attributes, like the port
 * information, working dir, name, and the InternalLocator it creates.
 *
 * <p>
 * by default the rule starts a locator with jmx and cluster configuration service
 * you can turn off cluster configuration service to have your test
 * run faster if your test does not need them.
 *
 * http service can be started when needed in your test.
 * </p>
 *
 * <p>
 * If you need a rule to start a server/locator in different VMs for Distributed tests, You should
 * use {@code ClusterStartupRule}.
 */
public class LocatorStarterRule extends MemberStarterRule<LocatorStarterRule> implements Locator {
  private transient InternalLocator locator;

  public LocatorStarterRule() {
    // in test environment, enable management request/response logging
    withSystemProperty("geode.management.request.logging", "true");
  }

  @Override
  public void before() {
    super.before();
    // always use a random jmxPort/httpPort when using the rule to start the locator
    if (jmxPort < 0) {
      withJMXManager(false);
    }
    if (autoStart) {
      startLocator();
    }
  }

  @Override
  public InternalLocator getLocator() {
    return locator;
  }

  @Override
  protected void stopMember() {
    if (locator != null) {
      locator.stop();
    }
  }

  public void startLocator() {
    try {
      // this will start a jmx manager and admin rest service by default
      locator = (InternalLocator) startLocatorAndDS(memberPort, null, properties);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    // memberPort is by default zero, which translates to "randomly select an available port,"
    // which is why it is updated here after being specified above.
    memberPort = locator.getPort();

    DistributionConfig config = locator.getConfig();
    jmxPort = config.getJmxManagerPort();
    httpPort = config.getHttpServicePort();

    if (config.getEnableClusterConfiguration()) {
      await()
          .untilAsserted(() -> assertTrue(locator.isSharedConfigurationRunning()));
    }
  }

  public LocatorStarterRule withoutClusterConfigurationService() {
    properties.put(ENABLE_CLUSTER_CONFIGURATION, "false");
    return this;
  }

  public LocatorStarterRule withoutManagementRestService() {
    properties.put(ENABLE_MANAGEMENT_REST_SERVICE, "false");
    return this;
  }

  @Override
  public InternalCache getCache() {
    return locator.getCache();
  }

  @Override
  public void waitTilFullyReconnected() {
    try {
      await().until(() -> {
        InternalLocator intLocator = ClusterStartupRule.getLocator();
        InternalCache cache = ClusterStartupRule.getCache();
        return intLocator != null && cache != null && intLocator.getDistributedSystem()
            .isConnected() && intLocator.isReconnected();
      });
    } catch (Exception e) {
      // provide more information when condition is not satisfied after awaitility timeout
      InternalLocator intLocator = ClusterStartupRule.getLocator();
      InternalCache cache = ClusterStartupRule.getCache();
      DistributedSystem ds = intLocator.getDistributedSystem();
      System.out.println("locator is: " + (intLocator != null ? "not null" : "null"));
      System.out.println("cache is: " + (cache != null ? "not null" : "null"));
      if (ds != null) {
        System.out.println(
            "distributed system is: " + (ds.isConnected() ? "connected" : "not connected"));
      } else {
        System.out.println("distributed system is: null");
      }
      System.out.println("locator is reconnected: " + (intLocator.isReconnected()));
      throw e;
    }
  }
}

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
package org.apache.geode.internal.cache.rollingupgrade;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.Assert.fail;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.Properties;

import org.junit.Test;

import org.apache.geode.cache.GemFireCache;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionInvocationTargetException;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.version.VersionManager;

public class RollingUpgradeNonHAFunction extends RollingUpgrade2DUnitTestBase {

  @Test
  public void functionExceptionsThrownFromDifferentVersionServerShouldCorrectlyWrapFunctionExceptionCauses()
      throws Exception {
    final Host host = Host.getHost(0);
    VM currentServer1 = host.getVM(VersionManager.CURRENT_VERSION, 0);
    VM oldServer = host.getVM(oldVersion, 1);
    VM currentServer2 = host.getVM(VersionManager.CURRENT_VERSION, 2);
    VM oldServerAndLocator = host.getVM(oldVersion, 3);

    String regionName = "cqs";

    RegionShortcut shortcut = RegionShortcut.PARTITION;

    String serverHostName = NetworkUtils.getServerHostName();
    int port = AvailablePortHelper.getRandomAvailableTCPPort();
    try {
      Properties props = getSystemProperties();
      props.put(ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER,
          "org.apache.geode.internal.cache.rollingupgrade.**");
      props.remove(DistributionConfig.LOCATORS_NAME);
      invokeRunnableInVMs(invokeStartLocatorAndServer(serverHostName, port, props),
          oldServerAndLocator);

      // Locators before 1.4 handled configuration asynchronously.
      // We must wait for configuration configuration to be ready, or confirm that it is disabled.
      oldServerAndLocator.invoke(
          () -> await()
              .untilAsserted(() -> assertTrue(
                  !InternalLocator.getLocator().getConfig().getEnableClusterConfiguration()
                      || InternalLocator.getLocator().isSharedConfigurationRunning())));

      props.put(DistributionConfig.LOCATORS_NAME, serverHostName + "[" + port + "]");
      invokeRunnableInVMs(invokeCreateCache(props), currentServer1, currentServer2, oldServer);

      currentServer1
          .invoke(invokeAssertVersion(VersionManager.getInstance().getCurrentVersionOrdinal()));
      currentServer2
          .invoke(invokeAssertVersion(VersionManager.getInstance().getCurrentVersionOrdinal()));

      // create region
      invokeRunnableInVMs(invokeCreateRegion(regionName, shortcut), currentServer1, currentServer2,
          oldServer, oldServerAndLocator);

      // Locators before 1.4 handled configuration asynchronously.
      // We must wait for configuration configuration to be ready, or confirm that it is disabled.
      oldServerAndLocator.invoke(
          () -> await()
              .untilAsserted(() -> assertTrue(
                  !InternalLocator.getLocator().getConfig().getEnableClusterConfiguration()
                      || InternalLocator.getLocator().isSharedConfigurationRunning())));

      putDataSerializableAndVerify(currentServer1, regionName, 0, 100, currentServer2, oldServer,
          oldServerAndLocator);
      runFunction(SEPARATOR + regionName, currentServer1,
          currentServer2, oldServer, oldServerAndLocator);

    } finally {
      invokeRunnableInVMs(invokeCloseCache(), currentServer1, currentServer2, oldServer,
          oldServerAndLocator);
    }
  }

  protected void runFunction(String queryString, VM... vms) {
    for (VM vm : vms) {
      vm.invoke(invokeAssertFunctionResults(queryString));
    }
  }

  private CacheSerializableRunnable invokeAssertFunctionResults(final String queryString) {
    return new CacheSerializableRunnable("execute: assertQueryResults") {
      @Override
      public void run2() {
        try {
          invokeAssertFunctionResult(RollingUpgrade2DUnitTestBase.cache, queryString);
        } catch (Exception e) {
          fail("Error asserting query results", e);
        }
      }
    };
  }

  private static void invokeAssertFunctionResult(GemFireCache cache, String regionName) {
    ResultCollector rc = null;
    try {
      rc = FunctionService.onRegion(cache.getRegion(regionName)).execute(new ExceptionalFunction());
      rc.getResult();
      fail("Function executed was expected to throw an exception");
    } catch (Exception e) {
      assertNotNull(e.getCause());
      assertSame(FunctionInvocationTargetException.class, e.getCause().getClass());
    }
  }

  private static class ExceptionalFunction implements Function {

    @Override
    public void execute(FunctionContext context) {
      throw new FunctionInvocationTargetException("This is an explicitly thrown test exception");
    }

    @Override
    public boolean isHA() {
      return false;
    }
  }

}

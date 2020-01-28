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

import static org.apache.geode.test.dunit.rules.ClusterStartupRule.getCache;
import static org.apache.geode.test.dunit.rules.ClusterStartupRule.getClientCache;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

import java.util.Collection;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.ServerOperationException;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;
import org.apache.geode.test.version.VersionManager;

/**
 * This test class exists to test the ServerOperationException A ServerOperationException is thrown
 * when a cluster has a server that is previous to version 1.14.0 which doesn't support the
 * Partitioned Region Clear feature.
 * <p>
 * When the exception is thrown it is expected to contain the members that have the bad version, the
 * version number necessary, and the feature that is not supported.
 */


@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class RollingUpgradePartitionRegionClearServerVersionMismatch {

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();

  @Parameterized.Parameter
  public String oldVersion;

  private MemberVM serverOld;
  private ClientVM clientVM;

  @Parameterized.Parameters(name = "from_v{0}")
  public static Collection<String> data() {
    List<String> result = VersionManager.getInstance().getVersionsWithoutCurrent();
    if (result.size() < 1) {
      throw new RuntimeException("No older versions of Geode were found to test against");
    } else {
      System.out.println("running against these versions: " + result);
    }
    return result;
  }

  // This is the message that we are expected to be in the exception in both tests below.
  private static final String expectedMessage =
      "A server's [server-2] version was too old (< GEODE 1.14.0) for : Partitioned Region Clear";

  private MemberVM locator;
  private MemberVM serverNew;

  @Before
  public void before() {
    locator = cluster.startLocatorVM(0,
        l -> l.withSystemProperty("gemfire.allow_old_members_to_join_for_testing", "true")
            .withProperty(DistributionConfig.ENABLE_CLUSTER_CONFIGURATION_NAME, "false"));
    int locatorPort = locator.getPort();

    serverNew = cluster.startServerVM(1, locatorPort);
    serverOld = cluster.startServerVM(2, oldVersion, s -> s.withConnectionToLocator(locatorPort));

    MemberVM.invokeInEveryMember(() -> {
      Cache cache = getCache();
      assertThat(cache).isNotNull();
      getCache().createRegionFactory(RegionShortcut.PARTITION).create("regionA");
    }, serverNew, serverOld);

    // Put in some boiler plate data for region clear
    serverNew.invoke(() -> {
      Cache cache = getCache();
      assertThat(cache).isNotNull();

      Region<String, String> region = cache.getRegion("regionA");
      region.put("A", "ValueA");
      region.put("B", "ValueB");
    });
  }

  @After
  public void after() {
    locator.stop();
    serverNew.stop();
    serverOld.stop();

  }

  /**
   * testClient_UnsupportedOperationExceptionCurrentServerVersion - validates that when a client
   * invokes a partitioned region clear on a cluster where one server is running an
   * unsupported version for this feature we return a UnsupportedOperationException
   */
  @Test
  public void testClient_UnsupportedOperationExceptionCurrentServerVersion() throws Exception {
    IgnoredException.addIgnoredException(ServerOperationException.class);

    // Get a client VM
    int serverPort = serverNew.getPort();
    clientVM = cluster.startClientVM(3, oldVersion, c -> c.withServerConnection(serverPort));

    clientVM.invoke(() -> {
      // Validate we have a cache and region
      ClientCache clientCache = getClientCache();
      assertThat(clientCache).isNotNull();

      ClientRegionFactory<String, String> clientRegionFactory =
          clientCache.createClientRegionFactory(ClientRegionShortcut.PROXY);
      Region<String, String> region = clientRegionFactory.create("regionA");
      assertThat(region).isNotNull();

      // Validate that we get a UnsupportedOperationException wrapped in a ServerOperationException
      Throwable thrown = catchThrowable(region::clear);
      assertThat(thrown).isNotNull();
      assertThat(thrown).isInstanceOf(ServerOperationException.class);
      Throwable cause = thrown.getCause();
      assertThat(cause).isInstanceOf(UnsupportedOperationException.class);
      assertThat(cause.getMessage()).contains(expectedMessage);
    });
  }

  @Test
  public void testClient_UnsupportedOperationExceptionOldServerVersion() throws Exception {
    IgnoredException.addIgnoredException(ServerOperationException.class);

    // Get a client VM
    int serverPort = serverOld.getPort();
    clientVM = cluster.startClientVM(3, oldVersion, c -> c.withServerConnection(serverPort));

    clientVM.invoke(() -> {
      // Validate we have a cache and region
      ClientCache clientCache = getClientCache();
      assertThat(clientCache).isNotNull();
      ClientRegionFactory<String, String> clientRegionFactory =
          clientCache.createClientRegionFactory(ClientRegionShortcut.PROXY);
      Region<String, String> region = clientRegionFactory.create("regionA");
      assertThat(region).isNotNull();

      // Validate that we get a UnsupportedOperationException wrapped in a ServerOperationException
      Throwable thrown = catchThrowable(region::clear);
      assertThat(thrown).isNotNull();
      assertThat(thrown).isInstanceOf(ServerOperationException.class);
      assertThat(thrown.getMessage()).contains("While performing a remote clear region");
      assertThat(thrown.getCause()).isNotNull();
      Throwable cause = thrown.getCause();
      assertThat(cause).isInstanceOf(UnsupportedOperationException.class);
      assertThat(cause.getMessage()).isNull();
    });
  }


  /**
   * testServer_UnsupportedOperationException - validates that when a partitioned region clear is
   * invoked on a cluster where one server is running an unsupported version for this feature we
   * return a UnsupportedOperationException
   */
  @Test
  public void testServer_UnsupportedOperationException() {
    IgnoredException.addIgnoredException(UnsupportedOperationException.class);

    serverNew.invoke(() -> {
      // Validate we have a cache and region
      Cache cache = getCache();
      assertThat(cache).isNotNull();

      Region<String, String> region = cache.getRegion("regionA");
      assertThat(region).isNotNull();

      // Validate that the message is exactly as we expect it.
      assertThatThrownBy(region::clear).isInstanceOf(UnsupportedOperationException.class)
          .hasMessageContaining(expectedMessage);

      assertThat(region.get("A")).isEqualTo("ValueA");
      assertThat(region.get("B")).isEqualTo("ValueB");
    });
  }
}

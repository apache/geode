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
package org.apache.geode.modules.util;

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;

import javax.servlet.http.HttpSession;

import org.apache.juli.logging.Log;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.NoAvailableServersException;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.MembershipListener;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.execute.metrics.FunctionStats;
import org.apache.geode.internal.cache.execute.metrics.FunctionStatsManager;
import org.apache.geode.modules.session.catalina.ClientServerSessionCache;
import org.apache.geode.modules.session.catalina.SessionManager;
import org.apache.geode.test.dunit.DistributedTestUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.ClientCacheRule;
import org.apache.geode.test.dunit.rules.DistributedRule;

public class ClientServerSessionCacheDUnitTest implements Serializable {
  private static final String SESSION_REGION_NAME = RegionHelper.NAME + "_sessions";
  private CacheRule cacheRule = new CacheRule();
  private DistributedRule distributedRule = new DistributedRule();
  private ClientCacheRule clientCacheRule = new ClientCacheRule();

  @Rule
  public transient RuleChain ruleChain = RuleChain.outerRule(distributedRule)
      .around(cacheRule)
      .around(clientCacheRule);


  @Test
  public void multipleGeodeServersCreateSessionRegion() {
    final VM server0 = VM.getVM(0);
    final VM server1 = VM.getVM(1);
    final VM client = VM.getVM(2);

    server0.invoke(this::startCacheServer);
    server1.invoke(this::startCacheServer);

    client.invoke(this::startClientSessionCache);

    server0.invoke(this::validateServer);
    server1.invoke(this::validateServer);
  }

  @Test
  public void addServerToExistingClusterCreatesSessionRegion() throws InterruptedException {
    final VM server0 = VM.getVM(0);
    final VM server1 = VM.getVM(1);
    final VM client = VM.getVM(2);

    server0.invoke(this::startCacheServer);

    client.invoke(this::startClientSessionCache);
    Thread.sleep(1000);

    server0.invoke(this::validateServer);

    server1.invoke(this::startCacheServer);

    // Session region may be created asynchronously on the second server
    server1.invoke(() -> await().untilAsserted(this::validateServer));
  }

  @Test
  public void startingAClientWithoutServersFails() {
    final VM client = VM.getVM(2);

    assertThatThrownBy(() -> client.invoke(this::startClientSessionCache))
        .hasCauseInstanceOf(NoAvailableServersException.class);
  }

  @Test
  public void canPreCreateSessionRegionBeforeStartingClient() {
    final VM server0 = VM.getVM(0);
    final VM server1 = VM.getVM(1);
    final VM client = VM.getVM(2);

    server0.invoke(this::startCacheServer);
    server1.invoke(this::startCacheServer);

    server0.invoke(this::createSessionRegion);
    server1.invoke(this::createSessionRegion);

    client.invoke(this::startClientSessionCache);

    server0.invoke(this::validateServer);
    server1.invoke(this::validateServer);
  }

  @Test
  public void preCreatedRegionIsNotCopiedToNewlyStartedServers() throws InterruptedException {
    final VM server0 = VM.getVM(0);
    final VM server1 = VM.getVM(1);
    final VM client = VM.getVM(2);

    server0.invoke(this::startCacheServer);
    Thread.sleep(1000);

    server0.invoke(this::createSessionRegion);

    client.invoke(this::startClientSessionCache);
    server1.invoke(this::startCacheServer);

    server1.invoke(() -> await().untilAsserted(this::validateBootstrapped));

    // server1 should not have created the session region
    // If the user precreated the region, they must manually
    // create it on all servers
    server1.invoke(() -> {
      Region<Object, Object> region = cacheRule.getCache().getRegion(SESSION_REGION_NAME);
      assertThat(region).isNull();
    });

  }

  @Test
  public void cantPreCreateMismatchedSessionRegionBeforeStartingClient() {
    final VM server0 = VM.getVM(0);
    final VM server1 = VM.getVM(1);
    final VM client = VM.getVM(2);

    server0.invoke(this::startCacheServer);
    server1.invoke(this::startCacheServer);

    server0.invoke(this::createMismatchedSessionRegion);
    server1.invoke(this::createMismatchedSessionRegion);

    assertThatThrownBy(() -> client.invoke(this::startClientSessionCache))
        .hasCauseInstanceOf(IllegalStateException.class);
  }

  @Test
  public void sessionCacheSizeShouldNotInvokeFunctionsOnTheCluster() {
    final VM server1 = VM.getVM(0);
    final VM server2 = VM.getVM(1);
    final VM client1 = VM.getVM(3);

    server1.invoke(this::startCacheServer);
    server2.invoke(this::startCacheServer);
    server1.invoke(this::createSessionRegion);
    server2.invoke(this::createSessionRegion);

    client1.invoke(() -> {
      final SessionManager sessionManager = mock(SessionManager.class);
      final Log logger = mock(Log.class);
      when(sessionManager.getLogger()).thenReturn(logger);
      when(sessionManager.getRegionName()).thenReturn(RegionHelper.NAME + "_sessions");
      when(sessionManager.getRegionAttributesId())
          .thenReturn(RegionShortcut.PARTITION_REDUNDANT.toString());

      final ClientCacheFactory clientCacheFactory = new ClientCacheFactory();
      clientCacheFactory.addPoolLocator("localhost", DistributedTestUtils.getLocatorPort());
      clientCacheFactory.setPoolSubscriptionEnabled(true);
      clientCacheRule.createClientCache(clientCacheFactory);

      final ClientCache clientCache = clientCacheRule.getClientCache();
      ClientServerSessionCache clientServerSessionCache =
          new ClientServerSessionCache(sessionManager, clientCache);
      clientServerSessionCache.initialize();

      assertThat(clientServerSessionCache.size()).isEqualTo(0);
    });

    // Verify defaults
    server1.invoke(this::validateServer);
    server2.invoke(this::validateServer);

    // Verify that RegionSizeFunction was never executed .
    server1.invoke(this::validateRegionSizeFunctionCalls);
    server2.invoke(this::validateRegionSizeFunctionCalls);
  }

  private void createSessionRegion() {
    Cache cache = cacheRule.getCache();
    cache.<String, HttpSession>createRegionFactory(RegionShortcut.PARTITION_REDUNDANT)
        .setCustomEntryIdleTimeout(new SessionCustomExpiry())
        .create(SESSION_REGION_NAME);
  }

  private void createMismatchedSessionRegion() {
    Cache cache = cacheRule.getCache();
    cache.<String, HttpSession>createRegionFactory(RegionShortcut.PARTITION)
        .setCustomEntryIdleTimeout(new SessionCustomExpiry())
        .create(SESSION_REGION_NAME);
  }

  private void validateSessionRegion() {
    final InternalCache cache = cacheRule.getCache();

    final Region<String, HttpSession> region = cache.getRegion(SESSION_REGION_NAME);
    assertThat(region).isNotNull();

    final RegionAttributes<Object, Object> expectedAttributes =
        cache.getRegionAttributes(RegionShortcut.PARTITION_REDUNDANT.toString());

    final RegionAttributes attributes = region.getAttributes();
    assertThat(attributes.getScope()).isEqualTo(expectedAttributes.getScope());
    assertThat(attributes.getDataPolicy()).isEqualTo(expectedAttributes.getDataPolicy());
    assertThat(attributes.getPartitionAttributes())
        .isEqualTo(expectedAttributes.getPartitionAttributes());
    assertThat(attributes.getCustomEntryIdleTimeout()).isInstanceOf(SessionCustomExpiry.class);
  }

  private void validateServer() {
    validateBootstrapped();
    validateSessionRegion();
  }

  private void validateBootstrapped() {
    final InternalCache cache = cacheRule.getCache();

    final DistributionManager distributionManager =
        cache.getInternalDistributedSystem().getDistributionManager();
    final Collection<MembershipListener> listeners =
        distributionManager.getMembershipListeners();
    assertThat(listeners)
        .filteredOn(listener -> listener instanceof BootstrappingFunction)
        .hasSize(1);
    assertThat(FunctionService.getFunction(CreateRegionFunction.ID))
        .isInstanceOf(CreateRegionFunction.class);
    assertThat(FunctionService.getFunction(TouchPartitionedRegionEntriesFunction.ID))
        .isInstanceOf(TouchPartitionedRegionEntriesFunction.class);
    assertThat(FunctionService.getFunction(TouchReplicatedRegionEntriesFunction.ID))
        .isInstanceOf(TouchReplicatedRegionEntriesFunction.class);
    assertThat(FunctionService.getFunction(RegionSizeFunction.ID))
        .isInstanceOf(RegionSizeFunction.class);

    final Region<String, RegionConfiguration> region =
        cache.getRegion(CreateRegionFunction.REGION_CONFIGURATION_METADATA_REGION);
    assertThat(region).isNotNull();

    final RegionAttributes<String, RegionConfiguration> attributes = region.getAttributes();
    assertThat(attributes.getDataPolicy()).isEqualTo(DataPolicy.REPLICATE);
    assertThat(attributes.getScope()).isEqualTo(Scope.DISTRIBUTED_ACK);
    assertThat(attributes.getDataPolicy()).isEqualTo(DataPolicy.REPLICATE);
    assertThat(attributes.getCacheListeners())
        .filteredOn(listener -> listener instanceof RegionConfigurationCacheListener)
        .hasSize(1);
  }

  private void validateRegionSizeFunctionCalls() {
    FunctionStats functionStats = FunctionStatsManager.getFunctionStats(RegionSizeFunction.ID);
    assertThat(functionStats.getFunctionExecutionCalls())
        .as("No function should be invoked to get the region size.")
        .isEqualTo(0);
  }

  private void startClientSessionCache() {
    final SessionManager sessionManager = mock(SessionManager.class);
    final Log logger = mock(Log.class);
    when(sessionManager.getLogger()).thenReturn(logger);
    when(sessionManager.getRegionName()).thenReturn(RegionHelper.NAME + "_sessions");
    when(sessionManager.getRegionAttributesId())
        .thenReturn(RegionShortcut.PARTITION_REDUNDANT.toString());

    final ClientCacheFactory clientCacheFactory = new ClientCacheFactory();
    clientCacheFactory.addPoolLocator("localhost", DistributedTestUtils.getLocatorPort());
    clientCacheFactory.setPoolSubscriptionEnabled(true);
    clientCacheRule.createClientCache(clientCacheFactory);

    final ClientCache clientCache = clientCacheRule.getClientCache();
    new ClientServerSessionCache(sessionManager, clientCache).initialize();
  }

  private void startCacheServer() throws IOException {
    final Cache cache = cacheRule.getOrCreateCache();
    final CacheServer cacheServer = cache.addCacheServer();
    cacheServer.setPort(0);
    cacheServer.start();
    await().until(cacheServer::isRunning);
  }
}

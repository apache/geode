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
package org.apache.geode.internal.metrics;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.geode.distributed.ConfigurationProperties.DISABLE_AUTO_RECONNECT;
import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_CLUSTER_CONFIGURATION;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MAX_WAIT_TIME_RECONNECT;
import static org.apache.geode.distributed.ConfigurationProperties.MEMBER_TIMEOUT;
import static org.apache.geode.distributed.ConfigurationProperties.NAME;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.awaitility.GeodeAwaitility.getTimeout;
import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import java.io.File;
import java.io.Serializable;
import java.util.Properties;
import java.util.function.Consumer;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.ForcedDisconnectException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.distributed.LocatorLauncher;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.api.MemberDisconnectedException;
import org.apache.geode.distributed.internal.membership.api.MembershipManagerHelper;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;

public class MeterSubregistryReconnectDistributedTest implements Serializable {

  private static final long TIMEOUT = getTimeout().toMillis();

  private static LocatorLauncher locatorLauncher;

  private static InternalDistributedSystem system;

  private VM locatorVM;
  private VM otherServer;

  private static final String LOCATOR_NAME = "locator";
  private static final String OTHER_SERVER_NAME = "other-server";
  private static final String NAME_OF_SERVER_TO_RECONNECT = "server-to-reconnect";

  private File locatorDir;
  private int locatorPort;

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder();

  @Before
  public void setUp() throws Exception {
    locatorVM = getVM(0);
    otherServer = getVM(1);

    locatorDir = temporaryFolder.newFolder(LOCATOR_NAME);
    locatorPort = locatorVM.invoke(this::createLocator);
    otherServer.invoke(() -> createServer(OTHER_SERVER_NAME));

    addIgnoredException(ForcedDisconnectException.class);
    addIgnoredException(MemberDisconnectedException.class);
    addIgnoredException("Possible loss of quorum");
  }

  @After
  public void tearDown() {
    locatorVM.invoke(() -> {
      locatorLauncher.stop();
      locatorLauncher = null;
      system = null;
    });

    otherServer.invoke(() -> {
      system.disconnect();
      system = null;
    });

    system.disconnect();
  }

  @Test
  public void reconnect_restoresOnlyUserRegistriesFromCacheFactory() throws InterruptedException {
    MeterRegistry userRegistryFromCacheFactory = meterRegistryFrom("CacheFactory");
    MeterRegistry publishingRegistryFromPublishingService =
        meterRegistryFrom("MetricsPublishingService");

    Consumer<CompositeMeterRegistry> addPublishingRegistryViaPublishingService =
        compositeRegistry -> compositeRegistry.add(publishingRegistryFromPublishingService);

    Consumer<CacheFactory> addUserRegistryViaCacheFactory =
        cacheFactory -> cacheFactory.addMeterSubregistry(userRegistryFromCacheFactory);

    createServer(NAME_OF_SERVER_TO_RECONNECT,
        addUserRegistryViaCacheFactory,
        addPublishingRegistryViaPublishingService);

    reconnect();

    assertThat(cacheMeterRegistry().getRegistries())
        .contains(userRegistryFromCacheFactory)
        .doesNotContain(publishingRegistryFromPublishingService);
  }

  private CompositeMeterRegistry cacheMeterRegistry() {
    InternalCache cache = system.getCache();
    return (CompositeMeterRegistry) cache.getMeterRegistry();
  }

  private void reconnect() throws InterruptedException {
    MembershipManagerHelper.crashDistributedSystem(system);
    await().until(() -> system.isReconnecting());
    system.waitUntilReconnected(TIMEOUT, MILLISECONDS);

    system = (InternalDistributedSystem) system.getReconnectedSystem();
  }

  private int createLocator() {
    LocatorLauncher.Builder builder = new LocatorLauncher.Builder();
    builder.setMemberName(LOCATOR_NAME);
    builder.setWorkingDirectory(locatorDir.getAbsolutePath());
    builder.setPort(0);
    builder.set(DISABLE_AUTO_RECONNECT, "false");
    builder.set(ENABLE_CLUSTER_CONFIGURATION, "false");
    builder.set(MAX_WAIT_TIME_RECONNECT, "1000");
    builder.set(MEMBER_TIMEOUT, "2000");

    locatorLauncher = builder.build();
    locatorLauncher.start();

    system = (InternalDistributedSystem) locatorLauncher.getCache().getDistributedSystem();

    return locatorLauncher.getPort();
  }

  private void createServer(String serverName) {
    createServer(serverName, cacheFactory -> {
    }, compositeMeterRegistry -> {
    });
  }

  private void createServer(String serverName, Consumer<CacheFactory> cacheInitializer,
      Consumer<CompositeMeterRegistry> registryInitializer) {
    Properties configProperties = new Properties();
    configProperties.setProperty(LOCATORS, "localHost[" + locatorPort + "]");
    configProperties.setProperty(DISABLE_AUTO_RECONNECT, "false");
    configProperties.setProperty(ENABLE_CLUSTER_CONFIGURATION, "false");
    configProperties.setProperty(MAX_WAIT_TIME_RECONNECT, "1000");
    configProperties.setProperty(MEMBER_TIMEOUT, "2000");
    configProperties.setProperty(NAME, serverName);

    CacheFactory cacheFactory = new CacheFactory(configProperties);
    cacheInitializer.accept(cacheFactory);

    InternalCache cache = (InternalCache) cacheFactory.create();

    CompositeMeterRegistry compositeMeterRegistry =
        (CompositeMeterRegistry) cache.getMeterRegistry();

    registryInitializer.accept(compositeMeterRegistry);

    system = cache.getInternalDistributedSystem();
  }

  private static MeterRegistry meterRegistryFrom(String registrySource) {
    SimpleMeterRegistry registry = spy(SimpleMeterRegistry.class);
    doReturn("Subregistry added by " + registrySource).when(registry).toString();
    return registry;
  }
}

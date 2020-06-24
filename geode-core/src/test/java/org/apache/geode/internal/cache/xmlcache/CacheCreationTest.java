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
package org.apache.geode.internal.cache.xmlcache;

import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;

import org.apache.geode.cache.CacheXmlException;
import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache.wan.GatewayReceiver;
import org.apache.geode.cache.wan.GatewayReceiverFactory;
import org.apache.geode.distributed.ServerLauncherParameters;
import org.apache.geode.internal.cache.CacheServerImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.services.module.impl.ServiceLoaderModuleService;

public class CacheCreationTest {

  @Mock
  private InternalCache cache;

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule().strictness(Strictness.STRICT_STUBS);

  @Before
  public void setUp() {
    ServerLauncherParameters.INSTANCE.clear();
  }

  @After
  public void tearDown() {
    ServerLauncherParameters.INSTANCE.clear();
  }

  @Test
  public void verifyRunInitializerWithInitializerAndNullPropsCallsInitAndInitialize() {
    CacheCreation cacheCreation = new CacheCreation(new ServiceLoaderModuleService(
        LogService.getLogger()));
    Declarable initializer = mock(Declarable.class);
    Properties props = null;
    cacheCreation.setInitializer(initializer, props);

    cacheCreation.runInitializer(cache);

    verify(initializer, times(1)).init(eq(props));
    verify(initializer, times(1)).initialize(eq(cache), eq(props));
  }

  @Test
  public void verifyRunInitializerWithInitializerAndPropsCallsInitAndInitialize() {
    CacheCreation cacheCreation = new CacheCreation(new ServiceLoaderModuleService(
        LogService.getLogger()));
    Declarable initializer = mock(Declarable.class);
    Properties props = new Properties();
    props.setProperty("key", "value");
    cacheCreation.setInitializer(initializer, props);

    cacheCreation.runInitializer(cache);

    verify(initializer, times(1)).init(eq(props));
    verify(initializer, times(1)).initialize(eq(cache), eq(props));
  }

  @Test
  public void verifyInitializeDeclarablesMapWithNoDeclarablesPassesEmptyMap() {
    CacheCreation cacheCreation = new CacheCreation(new ServiceLoaderModuleService(
        LogService.getLogger()));

    cacheCreation.initializeDeclarablesMap(cache);

    verify(cache, times(1)).addDeclarableProperties(eq(emptyMap()));
  }

  @Test
  public void verifyInitializeDeclarablesMapWithDeclarablesPassesExpectedMap() {
    CacheCreation cacheCreation = new CacheCreation(new ServiceLoaderModuleService(
        LogService.getLogger()));
    Map<Declarable, Properties> expected = new HashMap<>();
    Declarable declarable1 = mock(Declarable.class);
    cacheCreation.addDeclarableProperties(declarable1, null);
    expected.put(declarable1, null);
    Declarable declarable2 = mock(Declarable.class);
    Properties properties = new Properties();
    properties.setProperty("k2", "v2");
    cacheCreation.addDeclarableProperties(declarable2, properties);
    expected.put(declarable2, properties);

    cacheCreation.initializeDeclarablesMap(cache);

    verify(cache, times(1)).addDeclarableProperties(eq(expected));
  }

  @Test
  public void verifyInitializeDeclarablesMapWithDeclarableCallInitAndInitialize() {
    CacheCreation cacheCreation = new CacheCreation(new ServiceLoaderModuleService(
        LogService.getLogger()));
    Declarable declarable = mock(Declarable.class);
    Properties properties = new Properties();
    properties.setProperty("k2", "v2");
    cacheCreation.addDeclarableProperties(declarable, properties);

    cacheCreation.initializeDeclarablesMap(cache);

    verify(declarable, times(1)).init(eq(properties));
    verify(declarable, times(1)).initialize(eq(cache), eq(properties));
  }

  @Test
  public void verifyInitializeDeclarablesMapWithDeclarableThatThrowsWillThrowCacheXmlException() {
    CacheCreation cacheCreation = new CacheCreation(new ServiceLoaderModuleService(
        LogService.getLogger()));
    Declarable declarable = mock(Declarable.class);
    Properties properties = null;
    cacheCreation.addDeclarableProperties(declarable, properties);
    Throwable cause = new RuntimeException("expected");
    doThrow(cause).when(declarable).initialize(cache, null);

    Throwable thrown = catchThrowable(() -> cacheCreation.initializeDeclarablesMap(cache));

    assertThat(thrown).isExactlyInstanceOf(CacheXmlException.class)
        .hasMessageStartingWith("Exception while initializing an instance of").hasCause(cause);
  }

  @Test
  public void declarativeRegionIsCreated() {
    CacheCreation cacheCreation = new CacheCreation(new ServiceLoaderModuleService(
        LogService.getLogger()));
    RegionCreation declarativeRegion = mock(RegionCreation.class);
    Map<String, Region<?, ?>> declarativeRegions = new HashMap<>();
    declarativeRegions.put("testRegion", declarativeRegion);

    cacheCreation.initializeRegions(declarativeRegions, cache);

    verify(declarativeRegion, times(1)).createRoot(cache);
  }

  @Test
  public void defaultCacheServerIsNotCreatedWithDefaultPortWhenNoDeclarativeServerIsConfigured() {
    CacheCreation cacheCreation = new CacheCreation(new ServiceLoaderModuleService(
        LogService.getLogger()));

    cacheCreation.startCacheServers(cacheCreation.getCacheServers(), cache,
        ServerLauncherParameters.INSTANCE.withDisableDefaultServer(false));

    verify(cache, never()).addCacheServer();
  }

  @Test
  public void defaultCacheServerIsNotCreatedWhenDisableDefaultCacheServerIsTrue() {
    CacheCreation cacheCreation = new CacheCreation(new ServiceLoaderModuleService(
        LogService.getLogger()));

    cacheCreation.startCacheServers(cacheCreation.getCacheServers(), cache,
        ServerLauncherParameters.INSTANCE.withDisableDefaultServer(false));

    verify(cache, never()).addCacheServer();
  }

  @Test
  public void defaultCacheServerIsCreatedWithConfiguredPortWhenNoDeclarativeServerIsConfigured() {
    CacheCreation cacheCreation = new CacheCreation(new ServiceLoaderModuleService(
        LogService.getLogger()));
    CacheServerImpl mockServer = mock(CacheServerImpl.class);
    when(cache.addCacheServer()).thenReturn(mockServer);
    List<CacheServer> cacheServers = new ArrayList<>();
    when(cache.getCacheServers()).thenReturn(cacheServers);
    boolean disableDefaultCacheServer = false;
    int configuredServerPort = 9999;

    cacheCreation.startCacheServers(cacheCreation.getCacheServers(), cache,
        ServerLauncherParameters.INSTANCE.withPort(configuredServerPort)
            .withDisableDefaultServer(disableDefaultCacheServer));

    verify(cache, times(1)).addCacheServer();
    verify(mockServer).setPort(9999);
  }

  @Test
  public void declarativeCacheServerIsCreatedWithConfiguredServerPort() {
    CacheCreation cacheCreation = new CacheCreation(new ServiceLoaderModuleService(
        LogService.getLogger()));
    CacheServer cacheServer = new CacheServerCreation(cacheCreation, false);
    cacheServer.setPort(8888);
    cacheCreation.getCacheServers().add(cacheServer);
    CacheServerImpl mockServer = mock(CacheServerImpl.class);
    when(cache.addCacheServer()).thenReturn(mockServer);
    int configuredServerPort = 9999;
    boolean disableDefaultCacheServer = false;

    cacheCreation.startCacheServers(cacheCreation.getCacheServers(), cache,
        ServerLauncherParameters.INSTANCE.withPort(configuredServerPort)
            .withDisableDefaultServer(disableDefaultCacheServer));

    verify(cache, times(1)).addCacheServer();
    verify(mockServer).setPort(configuredServerPort);
  }

  @Test
  public void cacheServerCreationIsSkippedWhenAServerExistsForAGivenPort() {
    CacheCreation cacheCreation = new CacheCreation(new ServiceLoaderModuleService(
        LogService.getLogger()));
    CacheServer cacheServer = new CacheServerCreation(cacheCreation, false);
    cacheServer.setPort(40406);
    cacheCreation.getCacheServers().add(cacheServer);
    CacheServerImpl mockServer = mock(CacheServerImpl.class);
    when(mockServer.getPort()).thenReturn(40406);
    List<CacheServer> cacheServers = new ArrayList<>();
    cacheServers.add(mockServer);
    when(cache.getCacheServers()).thenReturn(cacheServers);

    cacheCreation.startCacheServers(cacheCreation.getCacheServers(), cache,
        ServerLauncherParameters.INSTANCE.withDisableDefaultServer(false));

    verify(cache, never()).addCacheServer();
  }

  @Test
  public void userCanCreateMultipleCacheServersDeclaratively() {
    CacheCreation cacheCreation = new CacheCreation(new ServiceLoaderModuleService(
        LogService.getLogger()));
    CacheServer cacheServer1 = new CacheServerCreation(cacheCreation, false);
    cacheServer1.setPort(40406);
    CacheServer cacheServer2 = new CacheServerCreation(cacheCreation, false);
    cacheServer1.setPort(40407);
    cacheCreation.getCacheServers().add(cacheServer1);
    cacheCreation.getCacheServers().add(cacheServer2);
    CacheServerImpl mockServer = mock(CacheServerImpl.class);
    when(cache.addCacheServer()).thenReturn(mockServer);

    cacheCreation.startCacheServers(cacheCreation.getCacheServers(), cache,
        ServerLauncherParameters.INSTANCE.withDisableDefaultServer(false));

    verify(cache, times(2)).addCacheServer();
    verify(mockServer).configureFrom(cacheServer1);
    verify(mockServer).configureFrom(cacheServer2);
  }

  @Test
  public void shouldThrowExceptionWhenUserTriesToDeclareMultipleCacheServersWithPort() {
    CacheCreation cacheCreation = new CacheCreation(new ServiceLoaderModuleService(
        LogService.getLogger()));
    cacheCreation.getCacheServers().add(new CacheServerCreation(cacheCreation, false));
    cacheCreation.getCacheServers().add(new CacheServerCreation(cacheCreation, false));
    int configuredServerPort = 50505;
    String configuredServerBindAddress = "localhost[50505]";
    boolean disableDefaultCacheServer = false;

    Throwable thrown = catchThrowable(() -> {
      cacheCreation.startCacheServers(cacheCreation.getCacheServers(), cache,
          ServerLauncherParameters.INSTANCE.withPort(configuredServerPort)
              .withBindAddress(configuredServerBindAddress)
              .withDisableDefaultServer(disableDefaultCacheServer));
    });

    assertThat(thrown).isInstanceOf(RuntimeException.class);
  }

  @Test
  public void shouldCreateGatewaySenderAfterRegions() {
    CacheCreation cacheCreation = new CacheCreation(new ServiceLoaderModuleService(
        LogService.getLogger()));
    GatewayReceiver receiver = mock(GatewayReceiver.class);
    cacheCreation.addGatewayReceiver(receiver);
    cacheCreation.addRootRegion(new RegionCreation(cacheCreation, "region"));
    InternalCache internalCache = mock(InternalCache.class);
    GatewayReceiverFactory receiverFactory = mock(GatewayReceiverFactory.class);
    when(internalCache.createGatewayReceiverFactory()).thenReturn(receiverFactory);
    when(receiverFactory.create()).thenReturn(receiver);

    cacheCreation.create(internalCache);

    InOrder inOrder = inOrder(internalCache, receiverFactory);
    inOrder.verify(internalCache).createGatewayReceiverFactory();
    inOrder.verify(receiverFactory).create();
  }

  @Test
  public void serverLauncherParametersShouldOverrideDefaultSettings() {
    CacheCreation cacheCreation = new CacheCreation(new ServiceLoaderModuleService(
        LogService.getLogger()));
    CacheServer cacheServerCreation = new CacheServerCreation(cacheCreation, false);
    cacheCreation.getCacheServers().add(cacheServerCreation);
    CacheServerImpl mockServer = mock(CacheServerImpl.class);
    when(cache.addCacheServer()).thenReturn(mockServer);
    int serverPort = 4444;
    int maxThreads = 5000;
    int maxConnections = 300;
    int maxMessageCount = 100;
    int socketBufferSize = 1024;
    String serverBindAddress = null;
    int messageTimeToLive = 500;
    String hostnameForClients = "hostnameForClients";
    boolean disableDefaultServer = false;
    ServerLauncherParameters.INSTANCE
        .withPort(serverPort)
        .withMaxThreads(maxThreads)
        .withBindAddress(serverBindAddress)
        .withMaxConnections(maxConnections)
        .withMaxMessageCount(maxMessageCount)
        .withSocketBufferSize(socketBufferSize)
        .withMessageTimeToLive(messageTimeToLive)
        .withHostnameForClients(hostnameForClients)
        .withDisableDefaultServer(disableDefaultServer);

    cacheCreation.startCacheServers(cacheCreation.getCacheServers(), cache,
        ServerLauncherParameters.INSTANCE);

    verify(cache, times(1)).addCacheServer();
    verify(mockServer, times(1)).setPort(serverPort);
    verify(mockServer, times(1)).setMaxThreads(maxThreads);
    verify(mockServer, times(1)).setMaxConnections(maxConnections);
    verify(mockServer, times(1)).setMaximumMessageCount(maxMessageCount);
    verify(mockServer, times(1)).setSocketBufferSize(socketBufferSize);
    verify(mockServer, times(0)).setBindAddress(serverBindAddress);
    verify(mockServer, times(1)).setMessageTimeToLive(messageTimeToLive);
    verify(mockServer, times(1)).setHostnameForClients(hostnameForClients);
  }
}

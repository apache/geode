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

import static com.googlecode.catchexception.CatchException.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.apache.geode.cache.CacheXmlException;
import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache.wan.GatewayReceiver;
import org.apache.geode.cache.wan.GatewayReceiverFactory;
import org.apache.geode.internal.cache.CacheServerImpl;
import org.apache.geode.internal.cache.CacheServerLauncher;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class CacheCreationJUnitTest {

  @Mock
  private InternalCache cache;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    CacheServerLauncher.clearStatics();
  }

  @SuppressWarnings("deprecation")
  @Test
  public void verifyRunInitializerWithInitializerAndNullPropsCallsInitAndInitialize() {
    @SuppressWarnings("resource")
    CacheCreation cacheCreation = new CacheCreation();
    Declarable initializer = mock(Declarable.class);
    Properties props = null;
    cacheCreation.setInitializer(initializer, props);

    cacheCreation.runInitializer(this.cache);

    verify(initializer, times(1)).init(eq(props));
    verify(initializer, times(1)).initialize(eq(this.cache), eq(props));
  }

  @SuppressWarnings("deprecation")
  @Test
  public void verifyRunInitializerWithInitializerAndPropsCallsInitAndInitialize() {
    @SuppressWarnings("resource")
    CacheCreation cacheCreation = new CacheCreation();
    Declarable initializer = mock(Declarable.class);
    Properties props = new Properties();
    props.setProperty("key", "value");
    cacheCreation.setInitializer(initializer, props);

    cacheCreation.runInitializer(this.cache);

    verify(initializer, times(1)).init(eq(props));
    verify(initializer, times(1)).initialize(eq(this.cache), eq(props));
  }

  @Test
  public void verifyInitializeDeclarablesMapWithNoDeclarablesPassesEmptyMap() {
    @SuppressWarnings("resource")
    CacheCreation cacheCreation = new CacheCreation();
    Map<Declarable, Properties> expected = Collections.emptyMap();

    cacheCreation.initializeDeclarablesMap(this.cache);

    verify(this.cache, times(1)).addDeclarableProperties(eq(expected));
  }

  @Test
  public void verifyInitializeDeclarablesMapWithDeclarablesPassesExpectedMap() {
    @SuppressWarnings("resource")
    CacheCreation cacheCreation = new CacheCreation();
    Map<Declarable, Properties> expected = new HashMap<>();
    Declarable d1 = mock(Declarable.class);
    cacheCreation.addDeclarableProperties(d1, null);
    expected.put(d1, null);
    Declarable d2 = mock(Declarable.class);
    Properties p2 = new Properties();
    p2.setProperty("k2", "v2");
    cacheCreation.addDeclarableProperties(d2, p2);
    expected.put(d2, p2);

    cacheCreation.initializeDeclarablesMap(this.cache);

    verify(this.cache, times(1)).addDeclarableProperties(eq(expected));
  }

  @Test
  public void verifyInitializeDeclarablesMapWithDeclarableCallInitAndInitialize() {
    @SuppressWarnings("resource")
    CacheCreation cacheCreation = new CacheCreation();
    Declarable d2 = mock(Declarable.class);
    Properties p2 = new Properties();
    p2.setProperty("k2", "v2");
    cacheCreation.addDeclarableProperties(d2, p2);

    cacheCreation.initializeDeclarablesMap(this.cache);

    verify(d2, times(1)).init(eq(p2));
    verify(d2, times(1)).initialize(eq(this.cache), eq(p2));
  }

  @Test
  public void verifyInitializeDeclarablesMapWithDeclarableThatThrowsWillThrowCacheXmlException() {
    @SuppressWarnings("resource")
    CacheCreation cacheCreation = new CacheCreation();
    Declarable d2 = mock(Declarable.class);
    Properties p2 = null;
    cacheCreation.addDeclarableProperties(d2, p2);
    Throwable cause = new RuntimeException("expected");
    doThrow(cause).when(d2).initialize(this.cache, null);

    catchException(cacheCreation).initializeDeclarablesMap(this.cache);

    assertThat((Exception) caughtException()).isExactlyInstanceOf(CacheXmlException.class)
        .hasMessageStartingWith("Exception while initializing an instance of").hasCause(cause);
  }

  @Test
  public void declarativeRegionIsCreated() {
    CacheCreation cacheCreation = new CacheCreation();

    RegionCreation declarativeRegion = mock(RegionCreation.class);
    when(declarativeRegion.getName()).thenReturn("testRegion");

    Map<String, Region<?, ?>> declarativeRegions = new HashMap<>();
    declarativeRegions.put("testRegion", declarativeRegion);

    when(this.cache.getRegion("testRegion")).thenReturn(null);

    cacheCreation.initializeRegions(declarativeRegions, this.cache);

    verify(declarativeRegion, times(1)).createRoot(this.cache);
  }

  @Test
  public void defaultCacheServerIsNotCreatedWithDefaultPortWhenNoDeclarativeServerIsConfigured() {
    CacheCreation cacheCreation = new CacheCreation();

    CacheServerImpl mockServer = mock(CacheServerImpl.class);
    when(this.cache.addCacheServer()).thenReturn(mockServer);

    List<CacheServer> cacheServers = new ArrayList<>();
    when(this.cache.getCacheServers()).thenReturn(cacheServers);

    Boolean disableDefaultCacheServer = false;
    Integer configuredServerPort = null;
    String configuredServerBindAddress = null;

    cacheCreation.startCacheServers(cacheCreation.getCacheServers(), this.cache,
        configuredServerPort, configuredServerBindAddress, disableDefaultCacheServer);

    verify(this.cache, never()).addCacheServer();
  }

  @Test
  public void defaultCacheServerIsNotCreatedWhenDisableDefaultCacheServerIsTrue() {
    CacheCreation cacheCreation = new CacheCreation();

    CacheServerImpl mockServer = mock(CacheServerImpl.class);
    when(this.cache.addCacheServer()).thenReturn(mockServer);

    List<CacheServer> cacheServers = new ArrayList<>();
    when(this.cache.getCacheServers()).thenReturn(cacheServers);

    Boolean disableDefaultCacheServer = true;
    Integer configuredServerPort = null;
    String configuredServerBindAddress = null;

    cacheCreation.startCacheServers(cacheCreation.getCacheServers(), this.cache,
        configuredServerPort, configuredServerBindAddress, disableDefaultCacheServer);

    verify(this.cache, never()).addCacheServer();
  }

  @Test
  public void defaultCacheServerIsCreatedWithConfiguredPortWhenNoDeclarativeServerIsConfigured() {
    CacheCreation cacheCreation = new CacheCreation();

    CacheServerImpl mockServer = mock(CacheServerImpl.class);
    when(this.cache.addCacheServer()).thenReturn(mockServer);

    List<CacheServer> cacheServers = new ArrayList<>();
    when(this.cache.getCacheServers()).thenReturn(cacheServers);

    Boolean disableDefaultCacheServer = false;
    Integer configuredServerPort = 9999;
    String configuredServerBindAddress = null;

    cacheCreation.startCacheServers(cacheCreation.getCacheServers(), this.cache,
        configuredServerPort, configuredServerBindAddress, disableDefaultCacheServer);

    verify(this.cache, times(1)).addCacheServer();
    verify(mockServer).setPort(9999);
  }

  @Test
  public void declarativeCacheServerIsCreatedWithConfiguredServerPort() {
    CacheCreation cacheCreation = new CacheCreation();
    CacheServerCreation br1 = new CacheServerCreation(cacheCreation, false);
    br1.setPort(8888);
    cacheCreation.getCacheServers().add(br1);

    CacheServerImpl mockServer = mock(CacheServerImpl.class);
    when(this.cache.addCacheServer()).thenReturn(mockServer);

    Integer configuredServerPort = 9999;
    String configuredServerBindAddress = null;
    Boolean disableDefaultCacheServer = false;

    cacheCreation.startCacheServers(cacheCreation.getCacheServers(), this.cache,
        configuredServerPort, configuredServerBindAddress, disableDefaultCacheServer);

    verify(this.cache, times(1)).addCacheServer();
    verify(mockServer).setPort(configuredServerPort);
  }

  @Test
  public void cacheServerCreationIsSkippedWhenAServerExistsForAGivenPort() {
    CacheCreation cacheCreation = new CacheCreation();
    CacheServerCreation br1 = new CacheServerCreation(cacheCreation, false);
    br1.setPort(40406);
    cacheCreation.getCacheServers().add(br1);

    CacheServerImpl mockServer = mock(CacheServerImpl.class);
    when(this.cache.addCacheServer()).thenReturn(mockServer);
    when(mockServer.getPort()).thenReturn(40406);

    List<CacheServer> cacheServers = new ArrayList<>();
    cacheServers.add(mockServer);

    when(this.cache.getCacheServers()).thenReturn(cacheServers);

    Integer configuredServerPort = null;
    String configuredServerBindAddress = null;
    Boolean disableDefaultCacheServer = false;

    cacheCreation.startCacheServers(cacheCreation.getCacheServers(), this.cache,
        configuredServerPort, configuredServerBindAddress, disableDefaultCacheServer);

    verify(this.cache, never()).addCacheServer();
  }

  @Test
  public void userCanCreateMultipleCacheServersDeclaratively() {
    CacheCreation cacheCreation = new CacheCreation();
    CacheServerCreation br1 = new CacheServerCreation(cacheCreation, false);
    br1.setPort(40406);
    CacheServerCreation br2 = new CacheServerCreation(cacheCreation, false);
    br1.setPort(40407);
    cacheCreation.getCacheServers().add(br1);
    cacheCreation.getCacheServers().add(br2);

    CacheServerImpl mockServer = mock(CacheServerImpl.class);
    when(this.cache.addCacheServer()).thenReturn(mockServer);

    Integer configuredServerPort = null;
    String configuredServerBindAddress = null;
    Boolean disableDefaultCacheServer = false;

    cacheCreation.startCacheServers(cacheCreation.getCacheServers(), this.cache,
        configuredServerPort, configuredServerBindAddress, disableDefaultCacheServer);

    verify(this.cache, times(2)).addCacheServer();
    verify(mockServer).configureFrom(br1);
    verify(mockServer).configureFrom(br2);
  }

  @Test(expected = RuntimeException.class)
  public void shouldThrowExceptionWhenUserTriesToDeclareMultipleCacheServersWithPort() {
    CacheCreation cacheCreation = new CacheCreation();
    cacheCreation.getCacheServers().add(new CacheServerCreation(cacheCreation, false));
    cacheCreation.getCacheServers().add(new CacheServerCreation(cacheCreation, false));

    Integer configuredServerPort = 50505;
    String configuredServerBindAddress = "localhost[50505]";
    Boolean disableDefaultCacheServer = false;

    cacheCreation.startCacheServers(cacheCreation.getCacheServers(), this.cache,
        configuredServerPort, configuredServerBindAddress, disableDefaultCacheServer);
  }

  @Test
  public void shouldCreateGatewaySenderAfterRegions() {
    CacheCreation cacheCreation = new CacheCreation();
    GatewayReceiver receiver = mock(GatewayReceiver.class);
    cacheCreation.addGatewayReceiver(receiver);
    cacheCreation.addRootRegion(new RegionCreation(cacheCreation, "region"));
    InternalCache internalCache = mock(InternalCache.class);
    GatewayReceiverFactory receiverFactory = mock(GatewayReceiverFactory.class);
    when(internalCache.createGatewayReceiverFactory()).thenReturn(receiverFactory);
    when(receiverFactory.create()).thenReturn(receiver);

    InOrder inOrder = inOrder(internalCache, receiverFactory);
    cacheCreation.create(internalCache);

    // inOrder.verify(cache).basicCreateRegion(eq("region"), any());
    inOrder.verify(internalCache).createGatewayReceiverFactory();
    inOrder.verify(receiverFactory).create();
  }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.internal.cache.xmlcache;

import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.gemstone.gemfire.cache.GemFireCache;
import com.gemstone.gemfire.cache.wan.GatewayReceiver;
import com.gemstone.gemfire.cache.wan.GatewayReceiverFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.internal.cache.CacheServerImpl;
import com.gemstone.gemfire.internal.cache.CacheServerLauncher;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class CacheCreationJUnitTest {

  @Mock
  private GemFireCacheImpl cache;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    CacheServerLauncher.clearStatics();
  }

  @Test
  public void declarativeRegionIsCreated() {
    CacheCreation cacheCreation = new CacheCreation();

    RegionCreation declarativeRegion = mock(RegionCreation.class);
    when(declarativeRegion.getName()).thenReturn("testRegion");

    Map declarativeRegions = new HashMap();
    declarativeRegions.put("testRegion", declarativeRegion);

    when(cache.getRegion("testRegion")).thenReturn(null);

    cacheCreation.initializeRegions(declarativeRegions, cache);

    verify(declarativeRegion, times(1)).createRoot(cache);
  }

  @Test
  public void defaultCacheServerIsNotCreatedWithDefaultPortWhenNoDeclarativeServerIsConfigured() {
    Boolean disableDefaultCacheServer = false;
    Integer configuredServerPort = null;
    String configuredServerBindAddress = null;

    CacheCreation cacheCreation = new CacheCreation();

    CacheServerImpl mockServer = mock(CacheServerImpl.class);
    when(cache.addCacheServer()).thenReturn(mockServer);

    List<CacheServer> cacheServers = new ArrayList<>();
    when(cache.getCacheServers()).thenReturn(cacheServers);

    cacheCreation.startCacheServers(cacheCreation.getCacheServers(), cache, configuredServerPort, configuredServerBindAddress, disableDefaultCacheServer);

    verify(cache, never()).addCacheServer();
  }

  @Test
  public void defaultCacheServerIsNotCreatedWhenDisableDefaultCacheServerIsTrue() {
    Boolean disableDefaultCacheServer = true;
    Integer configuredServerPort = null;
    String configuredServerBindAddress = null;

    CacheCreation cacheCreation = new CacheCreation();

    CacheServerImpl mockServer = mock(CacheServerImpl.class);
    when(cache.addCacheServer()).thenReturn(mockServer);

    List<CacheServer> cacheServers = new ArrayList<>();
    when(cache.getCacheServers()).thenReturn(cacheServers);

    cacheCreation.startCacheServers(cacheCreation.getCacheServers(), cache, configuredServerPort, configuredServerBindAddress, disableDefaultCacheServer);

    verify(cache, never()).addCacheServer();
  }

  @Test
  public void defaultCacheServerIsCreatedWithConfiguredPortWhenNoDeclarativeServerIsConfigured() {
    Boolean disableDefaultCacheServer = false;
    Integer configuredServerPort = 9999;
    String configuredServerBindAddress = null;

    CacheCreation cacheCreation = new CacheCreation();

    CacheServerImpl mockServer = mock(CacheServerImpl.class);
    when(cache.addCacheServer()).thenReturn(mockServer);

    List<CacheServer> cacheServers = new ArrayList<>();
    when(cache.getCacheServers()).thenReturn(cacheServers);

    cacheCreation.startCacheServers(cacheCreation.getCacheServers(), cache, configuredServerPort, configuredServerBindAddress, disableDefaultCacheServer);

    verify(cache, times(1)).addCacheServer();
    verify(mockServer).setPort(9999);
  }

  @Test
  public void declarativeCacheServerIsCreatedWithConfiguredServerPort() {
    Integer configuredServerPort = 9999;
    String configuredServerBindAddress = null;
    Boolean disableDefaultCacheServer = false;

    CacheCreation cacheCreation = new CacheCreation();
    CacheServerCreation br1 = new CacheServerCreation(cacheCreation, false);
    br1.setPort(8888);
    cacheCreation.getCacheServers().add(br1);

    CacheServerImpl mockServer = mock(CacheServerImpl.class);
    when(cache.addCacheServer()).thenReturn(mockServer);

    cacheCreation.startCacheServers(cacheCreation.getCacheServers(), cache, configuredServerPort, configuredServerBindAddress, disableDefaultCacheServer);

    verify(cache, times(1)).addCacheServer();
    verify(mockServer).setPort(configuredServerPort);
  }

  @Test
  public void cacheServerCreationIsSkippedWhenAServerExistsForAGivenPort() {
    Integer configuredServerPort = null;
    String configuredServerBindAddress = null;
    Boolean disableDefaultCacheServer = false;

    CacheCreation cacheCreation = new CacheCreation();
    CacheServerCreation br1 = new CacheServerCreation(cacheCreation, false);
    br1.setPort(40406);
    cacheCreation.getCacheServers().add(br1);

    CacheServerImpl mockServer = mock(CacheServerImpl.class);
    when(cache.addCacheServer()).thenReturn(mockServer);
    when(mockServer.getPort()).thenReturn(40406);

    List<CacheServer> cacheServers = new ArrayList<>();
    cacheServers.add(mockServer);

    when(cache.getCacheServers()).thenReturn(cacheServers);

    cacheCreation.startCacheServers(cacheCreation.getCacheServers(), cache, configuredServerPort, configuredServerBindAddress, disableDefaultCacheServer);

    verify(cache, never()).addCacheServer();
  }

  @Test
  public void userCanCreateMultipleCacheServersDeclaratively() {
    Integer configuredServerPort = null;
    String configuredServerBindAddress = null;
    Boolean disableDefaultCacheServer = false;

    CacheCreation cacheCreation = new CacheCreation();
    CacheServerCreation br1 = new CacheServerCreation(cacheCreation, false);
    br1.setPort(40406);
    CacheServerCreation br2 = new CacheServerCreation(cacheCreation, false);
    br1.setPort(40407);
    cacheCreation.getCacheServers().add(br1);
    cacheCreation.getCacheServers().add(br2);

    CacheServerImpl mockServer = mock(CacheServerImpl.class);
    when(cache.addCacheServer()).thenReturn(mockServer);

    cacheCreation.startCacheServers(cacheCreation.getCacheServers(), cache, configuredServerPort, configuredServerBindAddress, disableDefaultCacheServer);

    verify(cache, times(2)).addCacheServer();
    verify(mockServer).configureFrom(br1);
    verify(mockServer).configureFrom(br2);
  }

  @Test(expected = RuntimeException.class)
  public void shouldThrowExceptionWhenUserTriesToDeclareMultipleCacheServersWithPort() {
    Integer configuredServerPort = 50505;
    String configuredServerBindAddress = "localhost[50505]";
    Boolean disableDefaultCacheServer = false;

    CacheCreation cacheCreation = new CacheCreation();
    cacheCreation.getCacheServers().add(new CacheServerCreation(cacheCreation, false));
    cacheCreation.getCacheServers().add(new CacheServerCreation(cacheCreation, false));

    cacheCreation.startCacheServers(cacheCreation.getCacheServers(), cache, configuredServerPort, configuredServerBindAddress, disableDefaultCacheServer);
  }

  @Test
  public void shouldCreateGatewaySenderAfterRegions() {
    CacheCreation cacheCreation = new CacheCreation();
    GatewayReceiver receiver = mock(GatewayReceiver.class);
    cacheCreation.addGatewayReceiver(receiver);
    cacheCreation.addRootRegion(new RegionCreation(cacheCreation, "region"));
    GemFireCacheImpl cache = mock(GemFireCacheImpl.class);
    GatewayReceiverFactory receiverFactory = mock(GatewayReceiverFactory.class);
    when(cache.createGatewayReceiverFactory()).thenReturn(receiverFactory);
    when(receiverFactory.create()).thenReturn(receiver);

    InOrder inOrder = inOrder(cache, receiverFactory);
    cacheCreation.create(cache);

    inOrder.verify(cache).basicCreateRegion(eq("region"),any());
    inOrder.verify(cache).createGatewayReceiverFactory();
    inOrder.verify(receiverFactory).create();
  }
}

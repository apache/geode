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
package org.apache.geode.cache;

import static org.apache.geode.cache.EvictionAttributes.createLRUEntryAttributes;
import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_CLUSTER_CONFIGURATION;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.internal.util.ArrayUtils.asList;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.apache.geode.test.dunit.rules.DistributedRule.getLocatorPort;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.Serializable;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.distributed.ServerLauncher;
import org.apache.geode.internal.cache.CachePerfStats;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.DistributedReference;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;

@SuppressWarnings("serial")
public class ReplicateRegionNetsearchDistributedTest implements Serializable {

  private static final String REPLICATE_1_NAME = "replicate1";
  private static final String REPLICATE_2_NAME = "replicate2";
  private static final String PROXY_NAME = "proxy";
  private static final String REGION_NAME = "region";

  private VM replicate1;
  private VM replicate2;
  private VM proxy;
  private VM client;

  private File replicate1Dir;
  private File replicate2Dir;
  private File proxyDir;

  @Rule
  public DistributedRule distributedRule = new DistributedRule();
  @Rule
  public DistributedReference<ServerLauncher> serverLauncher = new DistributedReference<>();
  @Rule
  public DistributedReference<ClientCache> clientCache = new DistributedReference<>();
  @Rule
  public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder();

  @Before
  public void setUp() throws Exception {
    replicate1 = getVM(0);
    replicate2 = getVM(1);
    proxy = getVM(2);
    client = getVM(3);

    replicate1Dir = temporaryFolder.newFolder(REPLICATE_1_NAME);
    replicate2Dir = temporaryFolder.newFolder(REPLICATE_2_NAME);
    proxyDir = temporaryFolder.newFolder(PROXY_NAME);

    int locatorPort = getLocatorPort();

    replicate1.invoke(() -> {
      serverLauncher.set(startServer(REPLICATE_1_NAME, replicate1Dir, locatorPort));
    });
    replicate2.invoke(() -> {
      serverLauncher.set(startServer(REPLICATE_2_NAME, replicate2Dir, locatorPort));
    });
    proxy.invoke(() -> {
      serverLauncher.set(startServer(PROXY_NAME, proxyDir, locatorPort));
    });
  }

  @Test
  public void proxyReplicateDoesNetsearchFromFullReplicate() {
    replicate1.invoke(() -> {
      Region<String, String> region = serverLauncher.get().getCache()
          .<String, String>createRegionFactory(RegionShortcut.REPLICATE)
          .create(REGION_NAME);

      region.put("key-1", "value-1");
    });

    proxy.invoke(() -> {
      serverLauncher.get().getCache()
          .<String, String>createRegionFactory(RegionShortcut.REPLICATE_PROXY)
          .create(REGION_NAME);
    });

    proxy.invoke(() -> {
      Region<String, String> region = serverLauncher.get().getCache().getRegion(REGION_NAME);
      CachePerfStats regionPerfStats = getRegionPerfStats(region);

      assertThat(regionPerfStats.getNetsearchesCompleted()).isZero();

      String value = region.get("key-1");

      assertThat(value).isEqualTo("value-1");
      assertThat(regionPerfStats.getNetsearchesCompleted()).isOne();
      assertThat(region.getAttributes().getPartitionAttributes()).isNull();
    });
  }

  @Test
  public void fullReplicateDoesNotPerformNetsearch() {
    replicate1.invoke(() -> {
      Region<String, String> region = serverLauncher.get().getCache()
          .<String, String>createRegionFactory(RegionShortcut.REPLICATE)
          .create(REGION_NAME);

      region.put("key-1", "value-1");
    });

    proxy.invoke(() -> {
      serverLauncher.get().getCache()
          .<String, String>createRegionFactory(RegionShortcut.REPLICATE_PROXY)
          .create(REGION_NAME);
    });

    replicate1.invoke(() -> {
      Region<String, String> region = serverLauncher.get().getCache().getRegion(REGION_NAME);
      CachePerfStats regionPerfStats = getRegionPerfStats(region);

      assertThat(regionPerfStats.getNetsearchesCompleted()).isZero();

      String value = region.get("key-1");

      assertThat(value).isEqualTo("value-1");
      assertThat(regionPerfStats.getNetsearchesCompleted()).isZero();
    });
  }

  @Test
  public void replicateWithExpirationDoesNetsearchOnMiss() {
    replicate1.invoke(() -> {
      Region<String, String> region = serverLauncher.get().getCache()
          .<String, String>createRegionFactory(RegionShortcut.REPLICATE)
          .create(REGION_NAME);

      region.put("key-1", "value-1");
      region.put("key-2", "value-2");
    });

    replicate2.invoke(() -> {
      Region<String, String> region = serverLauncher.get().getCache()
          .<String, String>createRegionFactory(RegionShortcut.REPLICATE)
          .setEvictionAttributes(createLRUEntryAttributes(1))
          .create(REGION_NAME);

      assertThat(region).hasSize(1);

      CachePerfStats regionPerfStats = getRegionPerfStats(region);

      assertThat(regionPerfStats.getGets()).isZero();
      assertThat(regionPerfStats.getGetInitialImagesCompleted()).isOne();
      assertThat(regionPerfStats.getMisses()).isZero();
      assertThat(regionPerfStats.getNetsearchesCompleted()).isZero();
      assertThat(regionPerfStats.getHandlingNetsearchesCompleted()).isZero();
      assertThat(regionPerfStats.getHandlingNetsearchesFailed()).isZero();
    });

    replicate1.invoke(() -> {
      Region<String, String> region = serverLauncher.get().getCache().getRegion(REGION_NAME);
      CachePerfStats regionPerfStats = getRegionPerfStats(region);

      assertThat(regionPerfStats.getGets()).isZero();
      assertThat(regionPerfStats.getGetInitialImagesCompleted()).isZero();
      assertThat(regionPerfStats.getMisses()).isZero();
      assertThat(regionPerfStats.getNetsearchesCompleted()).isZero();
      assertThat(regionPerfStats.getHandlingNetsearchesCompleted()).isZero();
      assertThat(regionPerfStats.getHandlingNetsearchesFailed()).isZero();
    });

    replicate2.invoke(() -> {
      Region<String, String> region = serverLauncher.get().getCache().getRegion(REGION_NAME);

      assertThat(region).hasSize(1);

      CachePerfStats regionPerfStats = getRegionPerfStats(region);

      assertThat(regionPerfStats.getGets()).isZero();
      assertThat(regionPerfStats.getGetInitialImagesCompleted()).isOne();
      assertThat(regionPerfStats.getMisses()).isZero();
      assertThat(regionPerfStats.getNetsearchesCompleted()).isZero();
      assertThat(regionPerfStats.getHandlingNetsearchesCompleted()).isZero();
      assertThat(regionPerfStats.getHandlingNetsearchesFailed()).isZero();

      String value = region.get("key-1");

      assertThat(value).isEqualTo("value-1");

      assertThat(region).hasSize(1);

      assertThat(regionPerfStats.getGets()).isOne();
      assertThat(regionPerfStats.getGetInitialImagesCompleted()).isOne();
      assertThat(regionPerfStats.getMisses()).isOne();
      assertThat(regionPerfStats.getNetsearchesCompleted()).isOne();
      assertThat(regionPerfStats.getHandlingNetsearchesCompleted()).isZero();
      assertThat(regionPerfStats.getHandlingNetsearchesFailed()).isZero();
    });

    replicate1.invoke(() -> {
      Region<String, String> region = serverLauncher.get().getCache().getRegion(REGION_NAME);
      CachePerfStats regionPerfStats = getRegionPerfStats(region);

      assertThat(regionPerfStats.getGets()).isZero();
      assertThat(regionPerfStats.getGetInitialImagesCompleted()).isZero();
      assertThat(regionPerfStats.getMisses()).isZero();
      assertThat(regionPerfStats.getNetsearchesCompleted()).isZero();
      assertThat(regionPerfStats.getHandlingNetsearchesCompleted()).isOne();
      assertThat(regionPerfStats.getHandlingNetsearchesFailed()).isZero();
    });

    replicate2.invoke(() -> {
      Region<String, String> region = serverLauncher.get().getCache().getRegion(REGION_NAME);

      assertThat(region).hasSize(1);

      CachePerfStats regionPerfStats = getRegionPerfStats(region);

      assertThat(regionPerfStats.getGets()).isOne();
      assertThat(regionPerfStats.getGetInitialImagesCompleted()).isOne();
      assertThat(regionPerfStats.getMisses()).isOne();
      assertThat(regionPerfStats.getNetsearchesCompleted()).isOne();
      assertThat(regionPerfStats.getHandlingNetsearchesCompleted()).isZero();
      assertThat(regionPerfStats.getHandlingNetsearchesFailed()).isZero();

      String value = region.get("key-1");

      assertThat(value).isEqualTo("value-1");

      assertThat(region).hasSize(1);

      assertThat(regionPerfStats.getGets()).isEqualTo(2);
      assertThat(regionPerfStats.getGetInitialImagesCompleted()).isOne();
      assertThat(regionPerfStats.getMisses()).isOne();
      assertThat(regionPerfStats.getNetsearchesCompleted()).isOne();
      assertThat(regionPerfStats.getHandlingNetsearchesCompleted()).isZero();
      assertThat(regionPerfStats.getHandlingNetsearchesFailed()).isZero();
    });

    replicate2.invoke(() -> {
      Region<String, String> region = serverLauncher.get().getCache().getRegion(REGION_NAME);

      assertThat(region).hasSize(1);

      CachePerfStats regionPerfStats = getRegionPerfStats(region);

      assertThat(regionPerfStats.getGets()).isEqualTo(2);
      assertThat(regionPerfStats.getGetInitialImagesCompleted()).isOne();
      assertThat(regionPerfStats.getMisses()).isOne();
      assertThat(regionPerfStats.getNetsearchesCompleted()).isOne();
      assertThat(regionPerfStats.getHandlingNetsearchesCompleted()).isZero();
      assertThat(regionPerfStats.getHandlingNetsearchesFailed()).isZero();

      String value = region.get("key-2");

      assertThat(value).isEqualTo("value-2");

      assertThat(region).hasSize(1);

      assertThat(regionPerfStats.getGets()).isEqualTo(3);
      assertThat(regionPerfStats.getGetInitialImagesCompleted()).isOne();
      assertThat(regionPerfStats.getMisses()).isEqualTo(2);
      assertThat(regionPerfStats.getNetsearchesCompleted()).isEqualTo(2);
      assertThat(regionPerfStats.getHandlingNetsearchesCompleted()).isZero();
      assertThat(regionPerfStats.getHandlingNetsearchesFailed()).isZero();
    });

    replicate1.invoke(() -> {
      Region<String, String> region = serverLauncher.get().getCache().getRegion(REGION_NAME);
      CachePerfStats regionPerfStats = getRegionPerfStats(region);

      assertThat(regionPerfStats.getGets()).isZero();
      assertThat(regionPerfStats.getGetInitialImagesCompleted()).isZero();
      assertThat(regionPerfStats.getMisses()).isZero();
      assertThat(regionPerfStats.getNetsearchesCompleted()).isZero();
      assertThat(regionPerfStats.getHandlingNetsearchesCompleted()).isEqualTo(2);
      assertThat(regionPerfStats.getHandlingNetsearchesFailed()).isZero();
    });
  }

  @Test
  public void proxyReplicateDoesNetsearchFromOnlyOneFullReplicate() {
    replicate1.invoke(() -> {
      Region<String, String> region = serverLauncher.get().getCache()
          .<String, String>createRegionFactory(RegionShortcut.REPLICATE)
          .create(REGION_NAME);

      region.put("key-1", "value-1");
      region.put("key-2", "value-2");

      CachePerfStats regionPerfStats = getRegionPerfStats(region);

      assertThat(regionPerfStats.getGets()).isZero();
      assertThat(regionPerfStats.getGetInitialImagesCompleted()).isZero();
      assertThat(regionPerfStats.getMisses()).isZero();
      assertThat(regionPerfStats.getNetsearchesCompleted()).isZero();
      assertThat(regionPerfStats.getHandlingNetsearchesCompleted()).isZero();
      assertThat(regionPerfStats.getHandlingNetsearchesFailed()).isZero();
    });

    replicate2.invoke(() -> {
      Region<String, String> region = serverLauncher.get().getCache()
          .<String, String>createRegionFactory(RegionShortcut.REPLICATE)
          .create(REGION_NAME);

      CachePerfStats regionPerfStats = getRegionPerfStats(region);

      assertThat(regionPerfStats.getGets()).isZero();
      assertThat(regionPerfStats.getGetInitialImagesCompleted()).isOne();
      assertThat(regionPerfStats.getMisses()).isZero();
      assertThat(regionPerfStats.getNetsearchesCompleted()).isZero();
      assertThat(regionPerfStats.getHandlingNetsearchesCompleted()).isZero();
      assertThat(regionPerfStats.getHandlingNetsearchesFailed()).isZero();
    });

    proxy.invoke(() -> {
      Region<String, String> region = serverLauncher.get().getCache()
          .<String, String>createRegionFactory(RegionShortcut.REPLICATE_PROXY)
          .create(REGION_NAME);
      CachePerfStats regionPerfStats = getRegionPerfStats(region);

      assertThat(regionPerfStats.getGets()).isZero();
      assertThat(regionPerfStats.getGetInitialImagesCompleted()).isZero();
      assertThat(regionPerfStats.getMisses()).isZero();
      assertThat(regionPerfStats.getNetsearchesCompleted()).isZero();
      assertThat(regionPerfStats.getHandlingNetsearchesCompleted()).isZero();
      assertThat(regionPerfStats.getHandlingNetsearchesFailed()).isZero();
    });

    for (VM vm : asList(replicate1, replicate2)) {
      vm.invoke(() -> {
        Region<String, String> region = serverLauncher.get().getCache().getRegion(REGION_NAME);
        CachePerfStats regionPerfStats = getRegionPerfStats(region);

        assertThat(regionPerfStats.getGets()).isZero();
        assertThat(regionPerfStats.getMisses()).isZero();
        assertThat(regionPerfStats.getNetsearchesCompleted()).isZero();
        assertThat(regionPerfStats.getHandlingNetsearchesFailed()).isZero();
      });
    }

    proxy.invoke(() -> {
      Region<String, String> region = serverLauncher.get().getCache().getRegion(REGION_NAME);

      assertThat(region.get("key-1")).isEqualTo("value-1");

      CachePerfStats regionPerfStats = getRegionPerfStats(region);

      assertThat(regionPerfStats.getGets()).isOne();
      assertThat(regionPerfStats.getMisses()).isOne();
      assertThat(regionPerfStats.getNetsearchesCompleted()).isOne();
      assertThat(regionPerfStats.getHandlingNetsearchesCompleted()).isZero();
      assertThat(regionPerfStats.getHandlingNetsearchesFailed()).isZero();
    });

    long handlingNetsearchesCompletedInReplicate1 = replicate1.invoke(() -> {
      Region<String, String> region = serverLauncher.get().getCache().getRegion(REGION_NAME);
      CachePerfStats regionPerfStats = getRegionPerfStats(region);
      return regionPerfStats.getHandlingNetsearchesCompleted();
    });

    long handlingNetsearchesCompletedInReplicate2 = replicate2.invoke(() -> {
      Region<String, String> region = serverLauncher.get().getCache().getRegion(REGION_NAME);
      CachePerfStats regionPerfStats = getRegionPerfStats(region);
      return regionPerfStats.getHandlingNetsearchesCompleted();
    });

    // only one replicate should have been used to handle the netsearch
    assertThat(handlingNetsearchesCompletedInReplicate1 + handlingNetsearchesCompletedInReplicate2)
        .isOne();

    proxy.invoke(() -> {
      Region<String, String> region = serverLauncher.get().getCache().getRegion(REGION_NAME);
      CachePerfStats regionPerfStats = getRegionPerfStats(region);

      assertThat(regionPerfStats.getGets()).isOne();
      assertThat(regionPerfStats.getMisses()).isOne();
      assertThat(regionPerfStats.getNetsearchesCompleted()).isOne();
      assertThat(regionPerfStats.getHandlingNetsearchesCompleted()).isZero();
      assertThat(regionPerfStats.getHandlingNetsearchesFailed()).isZero();
    });
  }

  @Test
  public void clientGetFromProxyReplicateDoesNetsearchFromFullReplicate() {
    replicate1.invoke(() -> {
      Region<String, String> region = serverLauncher.get().getCache()
          .<String, String>createRegionFactory(RegionShortcut.REPLICATE)
          .create(REGION_NAME);

      region.put("key-1", "value-1");
    });

    replicate2.invoke(() -> {
      Region<String, String> region = serverLauncher.get().getCache()
          .<String, String>createRegionFactory(RegionShortcut.REPLICATE)
          .create(REGION_NAME);
      CachePerfStats regionPerfStats = getRegionPerfStats(region);

      assertThat(regionPerfStats.getGets()).isZero();
      assertThat(regionPerfStats.getGetInitialImagesCompleted()).isOne();
      assertThat(regionPerfStats.getMisses()).isZero();
      assertThat(regionPerfStats.getNetsearchesCompleted()).isZero();
      assertThat(regionPerfStats.getHandlingNetsearchesCompleted()).isZero();
      assertThat(regionPerfStats.getHandlingNetsearchesFailed()).isZero();
    });

    int proxyServerPort = proxy.invoke(() -> {
      Region<String, String> region = serverLauncher.get().getCache()
          .<String, String>createRegionFactory(RegionShortcut.REPLICATE_PROXY)
          .create(REGION_NAME);
      CachePerfStats regionPerfStats = getRegionPerfStats(region);

      assertThat(regionPerfStats.getGets()).isZero();
      assertThat(regionPerfStats.getGetInitialImagesCompleted()).isZero();
      assertThat(regionPerfStats.getMisses()).isZero();
      assertThat(regionPerfStats.getNetsearchesCompleted()).isZero();
      assertThat(regionPerfStats.getHandlingNetsearchesCompleted()).isZero();
      assertThat(regionPerfStats.getHandlingNetsearchesFailed()).isZero();

      return serverLauncher.get().getCache().getCacheServers().get(0).getPort();
    });

    client.invoke(() -> {
      clientCache.set(new ClientCacheFactory()
          .addPoolServer("localhost", proxyServerPort)
          .create());
      Region<String, String> region = clientCache.get()
          .<String, String>createClientRegionFactory(ClientRegionShortcut.PROXY)
          .create(REGION_NAME);
      CachePerfStats regionPerfStats = getRegionPerfStats(region);

      assertThat(regionPerfStats.getGets()).isZero();
      assertThat(regionPerfStats.getGetInitialImagesCompleted()).isZero();
      assertThat(regionPerfStats.getMisses()).isZero();
      assertThat(regionPerfStats.getNetsearchesCompleted()).isZero();
      assertThat(regionPerfStats.getHandlingNetsearchesCompleted()).isZero();
      assertThat(regionPerfStats.getHandlingNetsearchesFailed()).isZero();
    });

    for (VM vm : asList(replicate1, replicate2, proxy)) {
      vm.invoke(() -> {
        Region<String, String> region = serverLauncher.get().getCache().getRegion(REGION_NAME);
        CachePerfStats regionPerfStats = getRegionPerfStats(region);

        assertThat(regionPerfStats.getGets()).isZero();
        assertThat(regionPerfStats.getMisses()).isZero();
        assertThat(regionPerfStats.getNetsearchesCompleted()).isZero();
        assertThat(regionPerfStats.getHandlingNetsearchesCompleted()).isZero();
        assertThat(regionPerfStats.getHandlingNetsearchesFailed()).isZero();
      });
    }

    client.invoke(() -> {
      Region<String, String> region = clientCache.get().getRegion(REGION_NAME);
      CachePerfStats regionPerfStats = getRegionPerfStats(region);

      assertThat(region.get("key-1")).isEqualTo("value-1");

      assertThat(regionPerfStats.getGets()).isOne();
      assertThat(regionPerfStats.getMisses()).isOne();
      assertThat(regionPerfStats.getNetsearchesCompleted()).isZero();
      assertThat(regionPerfStats.getHandlingNetsearchesCompleted()).isZero();
      assertThat(regionPerfStats.getHandlingNetsearchesFailed()).isZero();
    });

    for (VM vm : asList(replicate1, replicate2)) {
      vm.invoke(() -> {
        Region<String, String> region = serverLauncher.get().getCache().getRegion(REGION_NAME);
        CachePerfStats regionPerfStats = getRegionPerfStats(region);

        assertThat(regionPerfStats.getGets()).isZero();
        assertThat(regionPerfStats.getMisses()).isZero();
        assertThat(regionPerfStats.getNetsearchesCompleted()).isZero();
        assertThat(regionPerfStats.getHandlingNetsearchesFailed()).isZero();
      });
    }

    long handlingNetsearchesCompletedInReplicate1 = replicate1.invoke(() -> {
      Region<String, String> region = serverLauncher.get().getCache().getRegion(REGION_NAME);
      CachePerfStats regionPerfStats = getRegionPerfStats(region);
      return regionPerfStats.getHandlingNetsearchesCompleted();
    });

    long handlingNetsearchesCompletedInReplicate2 = replicate2.invoke(() -> {
      Region<String, String> region = serverLauncher.get().getCache().getRegion(REGION_NAME);
      CachePerfStats regionPerfStats = getRegionPerfStats(region);
      return regionPerfStats.getHandlingNetsearchesCompleted();
    });

    // only one replicate should have been used to handle the netsearch
    assertThat(handlingNetsearchesCompletedInReplicate1 + handlingNetsearchesCompletedInReplicate2)
        .isOne();

    proxy.invoke(() -> {
      Region<String, String> region = serverLauncher.get().getCache().getRegion(REGION_NAME);
      CachePerfStats regionPerfStats = getRegionPerfStats(region);

      assertThat(regionPerfStats.getGets()).isOne();
      assertThat(regionPerfStats.getMisses()).isOne();
      assertThat(regionPerfStats.getNetsearchesCompleted()).isOne();
      assertThat(regionPerfStats.getHandlingNetsearchesCompleted()).isZero();
      assertThat(regionPerfStats.getHandlingNetsearchesFailed()).isZero();
    });
  }

  private ServerLauncher startServer(String serverName, File serverDir, int locatorPort) {
    ServerLauncher serverLauncher = new ServerLauncher.Builder()
        .setMemberName(serverName)
        .setWorkingDirectory(serverDir.getAbsolutePath())
        .setServerPort(0)
        .set(LOCATORS, "localHost[" + locatorPort + "]")
        .set(ENABLE_CLUSTER_CONFIGURATION, "false")
        .build();

    serverLauncher.start();

    return serverLauncher;
  }

  private CachePerfStats getRegionPerfStats(Region<?, ?> region) {
    return ((InternalRegion) region).getRegionPerfStats();
  }
}

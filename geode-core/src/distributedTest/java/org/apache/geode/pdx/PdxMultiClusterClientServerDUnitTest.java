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
package org.apache.geode.pdx;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.junit.Assert.assertEquals;

import java.util.Properties;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.Locator;
import org.apache.geode.test.dunit.SerializableCallableIF;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.SerializationTest;

/**
 * Test of how PDX works when a single client is connected to multiple
 * clusters (that are not connected to each other through WAN).
 */
@Category({SerializationTest.class})
public class PdxMultiClusterClientServerDUnitTest extends JUnit4CacheTestCase {

  public static final String REGION_NAME = "testSimplePdx";
  public static final int SITE_A_DSID = 1;
  public static final int SITE_B_DSID = 2;
  public static final int SITE_C_DSID = 3;
  private VM locatorA;
  private VM locatorB;
  private VM locatorC;
  private VM serverA;
  private VM serverB;
  private VM serverC;

  @Before
  public void setupVMs() {
    locatorA = VM.getVM(0);
    locatorB = VM.getVM(1);
    serverA = VM.getVM(2);
    serverB = VM.getVM(3);
    locatorC = VM.getVM(4);
    serverC = VM.getVM(5);
  }

  @Test
  public void putFromOneClientToTwoClustersSendsTypeToBothClusters() {

    ClientCache client = createScenario();

    Region regionA = client.getRegion("regionA");
    Region regionB = client.getRegion("regionB");

    regionA.put("key", new SimpleClass(5, (byte) 6));
    regionB.put("key", new SimpleClass(5, (byte) 6));

    serverB.invoke(() -> assertEquals(new SimpleClass(5, (byte) 6),
        getCache().getRegion("regionB").get("key")));
    serverA.invoke(() -> assertEquals(new SimpleClass(5, (byte) 6),
        getCache().getRegion("regionA").get("key")));
  }

  /**
   * Test that if a value is copied from one cluster to another using the client,
   * the new cluster will be able to deserialize the value.
   */
  @Test
  @Ignore("This use case is not currently supported")
  public void copyingValueFromOneClusterToAnotherUsingClientCopiesType() {

    ClientCache client = createScenario();

    Region regionA = client.getRegion("regionA");
    Region regionB = client.getRegion("regionB");

    // Put into serverA ( which will create the pdx type in serverA)
    serverA.invoke(() -> {
      getCache().getRegion("regionA").put("key", new SimpleClass(5, (byte) 6));

    });

    // Copy the value from serverA to serverB
    regionB.put("key", regionA.get("key"));

    // Make sure serverB can deserialize the value
    serverB.invoke(() -> assertEquals(new SimpleClass(5, (byte) 6),
        getCache().getRegion("regionB").get("key")));
  }

  /**
   * See what happens if both clusters have independently defined a type, and then
   * a client does a put in both clusters.
   *
   * It will get the type from one of the clusters, so the question is if it can be deserialized
   * in both
   */
  @Test
  public void bothClustersDefineTypeAndClientPutsInBothClusters() {

    ClientCache client = createScenario();

    Region regionA = client.getRegion("regionA");
    Region regionB = client.getRegion("regionB");

    createType(serverA, "regionA");
    createType(serverB, "regionB");

    // Put from the client into both servers (this will fetch the type from one of them)
    regionA.put("key", new SimpleClass(5, (byte) 6));
    regionB.put("key", new SimpleClass(5, (byte) 6));

    // Make sure both servers can deserialize the type
    serverB.invoke(() -> assertEquals(new SimpleClass(5, (byte) 6),
        getCache().getRegion("regionB").get("key")));
    serverA.invoke(() -> assertEquals(new SimpleClass(5, (byte) 6),
        getCache().getRegion("regionA").get("key")));
  }

  /**
   * If a client has multiple pools, but one of the pools has no available servers, creating
   * a new type should still succeed.
   */
  @Test
  public void sendingTypeIgnoresClustersThatAreNotRunning() {

    ClientCache client = createScenario();
    int siteCLocatorPort = createLocator(locatorC, SITE_C_DSID);

    createServerRegion(serverC, siteCLocatorPort, "regionC", SITE_C_DSID);

    Pool poolC =
        PoolManager.createFactory().addLocator("localhost", siteCLocatorPort).create("poolC");

    client.createClientRegionFactory(ClientRegionShortcut.PROXY)
        .setPoolName("poolC")
        .create("regionC");

    Region regionA = client.getRegion("regionA");
    Region regionC = client.getRegion("regionC");

    createType(serverA, "regionA");

    serverB.invoke(JUnit4CacheTestCase::closeCache);

    // Put from the client serverA. This will try to send the type to serverB and serverC
    regionA.put("key", new SimpleClass(5, (byte) 6));
    regionC.put("key", new SimpleClass(5, (byte) 6));

    // Make sure both remaining servers can deserialize the type
    serverC.invoke(() -> assertEquals(new SimpleClass(5, (byte) 6),
        getCache().getRegion("regionC").get("key")));
    serverA.invoke(() -> assertEquals(new SimpleClass(5, (byte) 6),
        getCache().getRegion("regionA").get("key")));
  }

  private void createType(VM vm, String region) {
    vm.invoke(() -> {
      getCache().getRegion(region).put("createType", new SimpleClass(5, (byte) 6));

    });
  }

  /**
   * Create the two sites (A and B) the client.
   *
   * The client has two regions
   * - regionA = connected using poolA to serverA
   * - regionB = connected using poolB to serverB
   */
  private ClientCache createScenario() {
    int siteALocatorPort = createLocator(locatorA, SITE_A_DSID);
    int siteBLocatorPort = createLocator(locatorB, SITE_B_DSID);

    createServerRegion(serverA, siteALocatorPort, "regionA", SITE_A_DSID);
    createServerRegion(serverB, siteBLocatorPort, "regionB", SITE_B_DSID);

    ClientCache client = new ClientCacheFactory().create();

    Pool poolA =
        PoolManager.createFactory().addLocator("localhost", siteALocatorPort).create("poolA");
    Pool poolB =
        PoolManager.createFactory().addLocator("localhost", siteBLocatorPort).create("poolB");

    client.createClientRegionFactory(ClientRegionShortcut.PROXY)
        .setPoolName("poolA")
        .create("regionA");

    client.createClientRegionFactory(ClientRegionShortcut.PROXY)
        .setPoolName("poolB")
        .create("regionB");
    return client;
  }

  private int createLocator(VM vm, int dsid) {
    return vm.invoke(() -> {

      Properties properties = new Properties();
      properties.setProperty(ConfigurationProperties.DISTRIBUTED_SYSTEM_ID, Integer.toString(dsid));
      Locator locator = Locator.startLocatorAndDS(0, null, null);
      return locator.getPort();
    });
  }


  private int createServerRegion(VM vm, int locatorPort, String regionName, int dsid) {
    SerializableCallableIF<Integer> createRegion = () -> {
      Properties properties = new Properties();
      properties.setProperty(LOCATORS, "localhost[" + locatorPort + "]");
      properties.setProperty(ConfigurationProperties.DISTRIBUTED_SYSTEM_ID, Integer.toString(dsid));
      Cache cache = getCache(properties);
      cache.createRegionFactory(RegionShortcut.REPLICATE).create(regionName);
      CacheServer cacheServer = cache.addCacheServer();
      cacheServer.setPort(0);
      cacheServer.start();
      return cacheServer.getPort();
    };

    return vm.invoke(createRegion);
  }

}

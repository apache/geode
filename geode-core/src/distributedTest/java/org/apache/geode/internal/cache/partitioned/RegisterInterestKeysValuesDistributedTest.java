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
package org.apache.geode.internal.cache.partitioned;

import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.apache.geode.test.dunit.VM.getHostName;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.internal.InternalClientCache;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.ClientCacheRule;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.categories.RegionsTest;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

/**
 * Functional distributed tests for Register Interest with KEYS_VALUES policy.
 *
 * <p>
 * TRAC #43684: Register interest with policy KEYS_VALUES is inefficient
 */
@Category(RegionsTest.class)
@SuppressWarnings("serial")
public class RegisterInterestKeysValuesDistributedTest implements Serializable {

  private VM server1;
  private VM server2;
  private VM server3;
  private VM client1;

  private String regionName;
  private String hostName;
  private int bucketCount;
  private int regexCount;
  private int port1;

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public CacheRule cacheRule = new CacheRule();

  @Rule
  public ClientCacheRule clientCacheRule = new ClientCacheRule();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @Before
  public void setUp() throws Exception {
    server1 = getVM(0);
    server2 = getVM(1);
    server3 = getVM(2);
    client1 = getVM(3);

    regionName = getClass().getSimpleName() + "_" + testName.getMethodName();
    hostName = getHostName();
    bucketCount = 11;
    regexCount = 20;

    addIgnoredException("Connection refused: connect");
  }

  @Test
  public void testRIWithSingleKeyOnRR() {
    port1 = server1.invoke(() -> createServerCache(true, false));
    server2.invoke(() -> createServerCache(true, false));
    server3.invoke(() -> createServerCache(true, false));

    doRegisterInterest("KEY_1", null, bucketCount);
  }

  @Test
  public void testRIWithAllKeysOnRR() {
    port1 = server1.invoke(() -> createServerCache(true, false));
    server2.invoke(() -> createServerCache(true, false));
    server3.invoke(() -> createServerCache(true, false));

    doRegisterInterest(null, null, bucketCount);
  }

  @Test
  public void testRIWithKeyListOnRR() {
    port1 = server1.invoke(() -> createServerCache(true, false));
    server2.invoke(() -> createServerCache(true, false));
    server3.invoke(() -> createServerCache(true, false));

    List<String> riKeys = new ArrayList<>();
    riKeys.add("KEY_0");
    riKeys.add("KEY_1");
    riKeys.add("KEY_2");
    riKeys.add("KEY_5");
    riKeys.add("KEY_6");
    riKeys.add("KEY_7");
    riKeys.add("KEY_9");

    doRegisterInterest(riKeys, null, bucketCount);
  }

  @Test
  public void testRIWithRegularExpressionOnRR() {
    port1 = server1.invoke(() -> createServerCache(true, false));
    server2.invoke(() -> createServerCache(true, false));
    server3.invoke(() -> createServerCache(true, false));

    doRegisterInterest(null, "^[X][_].*", bucketCount);
  }

  @Test
  public void testRIWithSingleKeyOnPR() {
    port1 = server1.invoke(() -> createServerCache(false, false));
    server2.invoke(() -> createServerCache(false, false));
    server3.invoke(() -> createServerCache(false, false));

    doRegisterInterest("KEY_1", null, bucketCount);
  }

  @Test
  public void testRIWithAllKeysOnPR() {
    port1 = server1.invoke(() -> createServerCache(false, false));
    server2.invoke(() -> createServerCache(false, false));
    server3.invoke(() -> createServerCache(false, false));

    doRegisterInterest(null, null, bucketCount);
  }

  @Test
  public void testRIWithKeyListOnPR() {
    port1 = server1.invoke(() -> createServerCache(false, false));
    server2.invoke(() -> createServerCache(false, false));
    server3.invoke(() -> createServerCache(false, false));

    List<String> riKeys = new ArrayList<>();
    riKeys.add("KEY_0");
    riKeys.add("KEY_1");
    riKeys.add("KEY_2");
    riKeys.add("KEY_5");
    riKeys.add("KEY_6");
    riKeys.add("KEY_7");
    riKeys.add("KEY_9");

    doRegisterInterest(riKeys, null, bucketCount);
  }

  @Test
  public void testRIWithRegularExpressionOnPR() {
    port1 = server1.invoke(() -> createServerCache(false, false));
    server2.invoke(() -> createServerCache(false, false));
    server3.invoke(() -> createServerCache(false, false));

    doRegisterInterest(null, "^[X][_].*", bucketCount);
  }

  @Test
  public void testRIWithMoreEntriesOnPR() {
    port1 = server1.invoke(() -> createServerCache(false, false));
    server2.invoke(() -> createServerCache(false, false));
    server3.invoke(() -> createServerCache(false, false));

    doRegisterInterest(null, null, 5147);
  }

  @Test
  public void testRIWithSingleKeyOnEmptyPrimaryOnPR() {
    port1 = server1.invoke(() -> createServerCache(false, true));
    server2.invoke(() -> createServerCache(false, false));
    server3.invoke(() -> createServerCache(false, false));

    doRegisterInterest("KEY_1", null, bucketCount);
  }

  @Test
  public void testRIWithAllKeysOnEmptyPrimaryOnPR() {
    port1 = server1.invoke(() -> createServerCache(false, true));
    server2.invoke(() -> createServerCache(false, false));
    server3.invoke(() -> createServerCache(false, false));

    doRegisterInterest(null, null, bucketCount);
  }

  @Test
  public void testRIWithKeyListOnEmptyPrimaryOnPR() {
    port1 = server1.invoke(() -> createServerCache(false, true));
    server2.invoke(() -> createServerCache(false, false));
    server3.invoke(() -> createServerCache(false, false));

    List<String> riKeys = new ArrayList<>();
    riKeys.add("KEY_0");
    riKeys.add("KEY_1");
    riKeys.add("KEY_2");
    riKeys.add("KEY_5");
    riKeys.add("KEY_6");
    riKeys.add("KEY_7");
    riKeys.add("KEY_9");

    doRegisterInterest(riKeys, null, bucketCount);
  }

  @Test
  public void testRIWithRegularExpressionOnEmptyPrimaryOnPR() {
    port1 = server1.invoke(() -> createServerCache(false, true));
    server2.invoke(() -> createServerCache(false, false));
    server3.invoke(() -> createServerCache(false, false));

    doRegisterInterest(null, "^[X][_].*", bucketCount);
  }

  @Test
  public void testNativeClientIssueOnPR() {
    port1 = server1.invoke(() -> createServerCache(false, false));
    server2.invoke(() -> createServerCache(false, false));
    server3.invoke(() -> createServerCache(false, false));

    List<String> keys = new ArrayList<>();
    keys.add("OPKEY_0");
    keys.add("OPKEY_1");
    keys.add("OPKEY_2");
    keys.add("OPKEY_3");
    keys.add("OPKEY_4");
    keys.add("OPKEY_5");
    keys.add("OPKEY_6");
    keys.add("OPKEY_7");
    keys.add("OPKEY_8");
    keys.add("OPKEY_9");
    keys.add("OPKEY_10");
    keys.add("OPKEY_11");
    keys.add("OPKEY_12");
    keys.add("OPKEY_13");

    client1.invoke(() -> createClientCache(port1));
    createClientCache(port1);
    doOps();

    client1.invoke(() -> {
      Region<Object, Object> region = clientCache().getRegion(regionName);
      region.registerInterest(keys);
      region.registerInterest("UNKNOWN_KEY");
    });

    client1.invoke(() -> {
      InternalRegion region = (InternalRegion) clientCache().getRegion(regionName);
      for (int i = 0; i < 7; i++) {
        assertTrue(region.containsKey("OPKEY_" + i));
      }
      for (int i = 7; i < 14; i++) {
        assertFalse(region.containsKey("OPKEY_" + i));
        assertTrue(region.containsTombstone("OPKEY_" + i));
      }
    });
  }

  private void doRegisterInterest(Object keys, String regEx, int numOfPuts) {
    server1.invoke(() -> doPuts(numOfPuts, regEx, regexCount));

    client1.invoke(() -> createClientCache(port1));
    client1.invoke(() -> registerInterest(keys, regEx));

    server1.invoke(() -> cache().close());

    // int size = getExpectedSize(keys, regEx, numOfPuts);
    client1.invoke(() -> verifyResponse(getExpectedSize(keys, regEx, numOfPuts)));
  }

  private int getExpectedSize(Object keys, String regEx, int numOfPuts) {
    return keys != null ? keys instanceof List ? ((List) keys).size() : 1
        : regEx == null ? numOfPuts : regexCount;
  }

  private int createServerCache(boolean isReplicated, boolean isPrimaryEmpty) throws IOException {
    createCache();

    RegionFactory rf;
    if (isReplicated) {
      RegionShortcut rs =
          isPrimaryEmpty ? RegionShortcut.REPLICATE_PROXY : RegionShortcut.REPLICATE;
      rf = cache().createRegionFactory(rs);
    } else {
      RegionShortcut rs =
          isPrimaryEmpty ? RegionShortcut.PARTITION_PROXY : RegionShortcut.PARTITION;
      rf = cache().createRegionFactory(rs);
      rf.setPartitionAttributes(
          new PartitionAttributesFactory().setTotalNumBuckets(bucketCount).create());
    }
    rf.create(regionName);

    CacheServer server = cache().addCacheServer();
    server.setPort(0);
    server.start();
    return server.getPort();
  }

  private void createClientCache(int port) {
    ClientCacheFactory ccf = new ClientCacheFactory();
    ccf.addPoolServer(hostName, port).setPoolSubscriptionEnabled(true);

    createClientCache(ccf);

    ClientRegionFactory crf =
        clientCache().createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY);
    crf.create(regionName);
  }

  private void registerInterest(Object keys, String regEx) {
    Region<Object, Object> region = clientCache().getRegion(regionName);
    if (keys == null && regEx == null) {
      region.registerInterest("ALL_KEYS");
    } else if (keys != null) {
      region.registerInterest(keys);
      region.registerInterest("UNKNOWN_KEY");
    } else {
      region.registerInterestRegex(regEx);
    }
  }

  private void doPuts(int count, String regex, int regexNum) {
    Region<String, String> region = cache().getRegion(regionName);
    for (int i = 0; i < count; i++) {
      region.create("KEY_" + i, "VALUE__" + i);
    }
    if (regex != null) {
      for (int i = 0; i < regexNum; i++) {
        region.create("X_KEY_" + i, "X_VALUE__" + i);
      }
    }
  }

  private void doOps() {
    Region<String, String> region = clientCache().getRegion(regionName);
    for (int i = 0; i < 14; i++) {
      region.create("OPKEY_" + i, "OPVALUE__" + i);
    }

    for (int i = 7; i < 14; i++) {
      region.destroy("OPKEY_" + i);
    }
  }

  private void verifyResponse(int size) {
    Region region = clientCache().getRegion(regionName);
    assertEquals(size, region.size());
  }

  private InternalCache cache() {
    return cacheRule.getCache();
  }

  private InternalClientCache clientCache() {
    return clientCacheRule.getClientCache();
  }

  private void createCache() {
    cacheRule.createCache();
  }

  private void createClientCache(ClientCacheFactory clientCacheFactory) {
    clientCacheRule.createClientCache(clientCacheFactory);
  }
}

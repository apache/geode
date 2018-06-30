/**
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
package org.apache.geode.internal.cache;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.CacheTransactionManager;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EntryOperation;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.PartitionResolver;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolFactory;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.Version;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.dunit.standalone.VersionManager;
import org.apache.geode.test.junit.categories.BackwardCompatibilityTest;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;

/**
 * This test class tests the communication pattern of a transaction for replicate and partition
 * region. There are five VMs that two CacheServers, CacheServer with Pool and two clients. The
 * communicate of a transaction is the following three patterns. In addition, this test class also
 * tests transactions from older version clients to check backwards compatibility.<br>
 * <ol>
 * <li>CacheServer (transaction)-> CacheServer</li>
 * <li>Client (transaction)-> CacheServer</li>
 * <li>Old version client (transaction)-> CacheServer</li>
 * <li>CacheServer via Pool (transaction)-> CacheServer</li>
 * </ol>
 */
@Category({DistributedTest.class, BackwardCompatibilityTest.class})
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
@SuppressWarnings("serial")
public class TxCommitMessageBackwardCompatibilityDUnitTest extends JUnit4DistributedTestCase {
  private String testVersion;

  @Parameterized.Parameters
  public static Collection<String> data() {
    List<String> result = VersionManager.getInstance().getVersionsWithoutCurrent();
    if (result.size() < 1) {
      throw new RuntimeException("No older versions of Geode were found to test against");
    } else {
      System.out.println("running against these versions: " + result);
    }
    return result;
  }

  private static VM server1 = null;
  private static VM server2 = null;
  private static VM server3 = null;
  private static VM client = null;
  private static VM oldClient = null;

  private static GemFireCacheImpl cache;

  private static final String REPLICATE_REGION_NAME =
      TxCommitMessageBackwardCompatibilityDUnitTest.class.getSimpleName() + "_ReplicateRegion";
  private static final String PARTITION_REGION_NAME =
      TxCommitMessageBackwardCompatibilityDUnitTest.class.getSimpleName() + "_PartitionRegion";

  private static String KEY1 = "KEY1";
  private static String KEY2 = "KEY2";

  public TxCommitMessageBackwardCompatibilityDUnitTest(String version) {
    super();
    testVersion = version;
  }

  private TxCommitMessageBackwardCompatibilityDUnitTest() {}

  @Override
  public final void postSetUp() throws Exception {
    disconnectFromDS();
    Host host = Host.getHost(0);
    server1 = host.getVM(0); // server
    server2 = host.getVM(1); // server
    server3 = host.getVM(2); // server with pool
    client = host.getVM(testVersion, 3); // client
    oldClient = host.getVM(4); // client old version

    int port1 =
        server1.invoke(() -> TxCommitMessageBackwardCompatibilityDUnitTest.createServerCache());
    int port2 =
        server2.invoke(() -> TxCommitMessageBackwardCompatibilityDUnitTest.createServerCache());
    server3.invoke(() -> TxCommitMessageBackwardCompatibilityDUnitTest
        .createServerCacheWithPool(host.getHostName(), new Integer[] {port1, port2}));
    client.invoke(() -> TxCommitMessageBackwardCompatibilityDUnitTest
        .createClientCache(host.getHostName(), new Integer[] {port1, port2}));
    oldClient.invoke(() -> TxCommitMessageBackwardCompatibilityDUnitTest
        .createClientCache(host.getHostName(), new Integer[] {port1, port2}));
  }

  @Override
  public final void preTearDown() throws Exception {
    closeCache();

    server1.invoke(() -> TxCommitMessageBackwardCompatibilityDUnitTest.closeCache());
    server2.invoke(() -> TxCommitMessageBackwardCompatibilityDUnitTest.closeCache());
    server3.invoke(() -> TxCommitMessageBackwardCompatibilityDUnitTest.closeCache());
    client.invoke(() -> TxCommitMessageBackwardCompatibilityDUnitTest.closeCache());
    oldClient.invoke(() -> TxCommitMessageBackwardCompatibilityDUnitTest.closeCache());
  }

  public static void closeCache() throws Exception {
    if (cache != null) {
      cache.close();
    }
  }

  @SuppressWarnings("deprecation")
  public static int createServerCache() throws Exception {
    Properties props = new Properties();
    TxCommitMessageBackwardCompatibilityDUnitTest test =
        new TxCommitMessageBackwardCompatibilityDUnitTest();
    DistributedSystem ds = test.getSystem(props);
    ds.disconnect();
    cache = (GemFireCacheImpl) CacheFactory.create(test.getSystem());

    RegionFactory<String, String> rf1 = cache.createRegionFactory(RegionShortcut.REPLICATE);
    rf1.create(REPLICATE_REGION_NAME);

    PartitionAttributesFactory<String, Integer> paf2 = new PartitionAttributesFactory<>();
    PartitionAttributes<String, Integer> pa2 =
        paf2.setRedundantCopies(1).setPartitionResolver(new PartitionResolver<String, Integer>() {
          @Override
          public Object getRoutingObject(EntryOperation<String, Integer> opDetails) {
            return opDetails.getKey().substring(0, 3);
          }

          @Override
          public String getName() {
            return getClass().getSimpleName();
          }

          @Override
          public void close() {}
        }).create();
    RegionFactory<String, String> rf2 =
        cache.createRegionFactory(RegionShortcut.PARTITION_REDUNDANT);
    rf2.setPartitionAttributes(pa2);
    rf2.create(PARTITION_REGION_NAME);

    CacheServer server = cache.addCacheServer();
    server.setPort(AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET));
    server.start();
    return server.getPort();
  }

  @SuppressWarnings("deprecation")
  public static void createServerCacheWithPool(String hostName, Integer[] ports) throws Exception {
    Properties props = new Properties();
    TxCommitMessageBackwardCompatibilityDUnitTest test =
        new TxCommitMessageBackwardCompatibilityDUnitTest();
    DistributedSystem ds = test.getSystem(props);
    ds.disconnect();
    cache = (GemFireCacheImpl) CacheFactory.create(test.getSystem());

    String poolName = "ClientPool";
    PoolFactory pf = PoolManager.createFactory().setSubscriptionEnabled(true);
    for (int port : ports) {
      pf.addServer(hostName, port);
    }
    Pool pool = pf.create(poolName);

    RegionFactory<String, Integer> rf1 = cache.createRegionFactory(RegionShortcut.LOCAL);
    rf1.setDataPolicy(DataPolicy.EMPTY);
    rf1.setPoolName(pool.getName());
    Region<String, Integer> region1 = rf1.create(REPLICATE_REGION_NAME);
    region1.registerInterest("ALL_KEYS");

    RegionFactory<String, Integer> rf2 = cache.createRegionFactory(RegionShortcut.LOCAL);
    rf2.setDataPolicy(DataPolicy.EMPTY);
    rf2.setPoolName(pool.getName());
    Region<String, Integer> region2 = rf2.create(PARTITION_REGION_NAME);
    region2.registerInterest("ALL_KEYS");
  }

  @SuppressWarnings("deprecation")
  public static void createClientCache(String hostName, Integer[] ports) throws Exception {
    Properties props = new Properties();
    DistributedSystem ds = new TxCommitMessageBackwardCompatibilityDUnitTest().getSystem(props);
    ds.disconnect();
    ClientCacheFactory ccf = new ClientCacheFactory(props);
    ccf.setPoolSubscriptionEnabled(true);
    for (int port : ports) {
      ccf.addPoolServer(hostName, port);
    }
    cache = (GemFireCacheImpl) ccf.create();

    ClientRegionFactory<String, Integer> crf1 =
        cache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY);
    Region<String, Integer> region1 = crf1.create(REPLICATE_REGION_NAME);
    region1.registerInterest("ALL_KEYS");

    ClientRegionFactory<String, Integer> crf2 =
        cache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY);
    Region<String, Integer> region2 = crf2.create(PARTITION_REGION_NAME);
    region2.registerInterest("ALL_KEYS");
  }

  @SuppressWarnings("unchecked")
  public static void doTxPuts(String regionName) throws Exception {
    Region<String, Integer> region = cache.getRegion(regionName);

    CacheTransactionManager txMngr = cache.getCacheTransactionManager();
    txMngr.begin();
    Integer value1 = region.get(KEY1);
    if (value1 == null) {
      value1 = 1;
    } else {
      value1++;
    }
    region.put(KEY1, value1);

    Integer value2 = region.get(KEY2);
    if (value2 == null) {
      value2 = 1000;
    } else {
      value2++;
    }
    region.put(KEY2, value2);
    txMngr.commit();
  }

  @SuppressWarnings("unchecked")
  public static void doTxPutsBoth(String regionNameReplicate, String regionNamePartition)
      throws Exception {
    Region<String, Integer> regionReplicate = cache.getRegion(regionNameReplicate);
    Region<String, Integer> regionPartition = cache.getRegion(regionNamePartition);

    CacheTransactionManager txMngr = cache.getCacheTransactionManager();
    txMngr.begin();
    Integer valPart1 = regionPartition.get(KEY1);
    if (valPart1 == null) {
      valPart1 = 1500;
    } else {
      valPart1++;
    }
    regionPartition.put(KEY1, valPart1);

    Integer valPart2 = regionPartition.get(KEY2);
    if (valPart2 == null) {
      valPart2 = 2000;
    } else {
      valPart2++;
    }
    regionPartition.put(KEY2, valPart2);

    Integer valRepl1 = regionReplicate.get(KEY1);
    if (valRepl1 == null) {
      valRepl1 = 500;
    } else {
      valRepl1++;
    }
    regionReplicate.put(KEY1, valRepl1);

    Integer valRepl2 = regionReplicate.get(KEY2);
    if (valRepl2 == null) {
      valRepl2 = 1000;
    } else {
      valRepl2++;
    }
    regionReplicate.put(KEY2, valRepl2);
    txMngr.commit();
  }

  @SuppressWarnings("unchecked")
  public static List<Integer> doGets(String regionName) throws Exception {
    Region<String, Integer> region = cache.getRegion(regionName);

    Integer value1 = region.get(KEY1);
    Integer value2 = region.get(KEY2);

    return Arrays.asList(value1, value2);
  }

  private static void setVersion(String field, Version value) throws Exception {
    Field targetField = Version.class.getDeclaredField(field);
    Field modifiersField = Field.class.getDeclaredField("modifiers");
    modifiersField.setAccessible(true);
    modifiersField.set(targetField,
        targetField.getModifiers() & ~Modifier.PRIVATE & ~Modifier.FINAL);
    targetField.set(null, value);
  }

  @Test
  public void testServerToServerTxReplicate() throws Exception {
    String regionName = REPLICATE_REGION_NAME;

    List<Integer> beforeValues =
        server1.invoke(() -> TxCommitMessageBackwardCompatibilityDUnitTest.doGets(regionName));
    server1.invoke(() -> TxCommitMessageBackwardCompatibilityDUnitTest.doTxPuts(regionName));
    List<Integer> afterValues1 =
        server1.invoke(() -> TxCommitMessageBackwardCompatibilityDUnitTest.doGets(regionName));
    List<Integer> afterValues2 =
        server2.invoke(() -> TxCommitMessageBackwardCompatibilityDUnitTest.doGets(regionName));

    Integer expected1 = beforeValues.get(0) == null ? 1 : beforeValues.get(0) + 1;
    Integer expected2 = beforeValues.get(1) == null ? 1000 : beforeValues.get(1) + 1000;

    assertThat(afterValues1, contains(expected1, expected2));
    assertThat(afterValues2, contains(expected1, expected2));
  }

  @Test
  public void testClientToServerTxReplicate() throws Exception {
    String regionName = REPLICATE_REGION_NAME;

    List<Integer> beforeValues =
        client.invoke(() -> TxCommitMessageBackwardCompatibilityDUnitTest.doGets(regionName));
    client.invoke(() -> TxCommitMessageBackwardCompatibilityDUnitTest.doTxPuts(regionName));
    List<Integer> afterValues1 =
        client.invoke(() -> TxCommitMessageBackwardCompatibilityDUnitTest.doGets(regionName));
    List<Integer> afterValues2 =
        server1.invoke(() -> TxCommitMessageBackwardCompatibilityDUnitTest.doGets(regionName));
    List<Integer> afterValues3 =
        server2.invoke(() -> TxCommitMessageBackwardCompatibilityDUnitTest.doGets(regionName));

    Integer expected1 = beforeValues.get(0) == null ? 1 : beforeValues.get(0) + 1;
    Integer expected2 = beforeValues.get(1) == null ? 1000 : beforeValues.get(1) + 1000;

    assertThat(afterValues1, contains(expected1, expected2));
    assertThat(afterValues2, contains(expected1, expected2));
    assertThat(afterValues3, contains(expected1, expected2));
  }

  @Test
  public void testOldClientToServerTxReplicate() throws Exception {
    String regionName = REPLICATE_REGION_NAME;

    List<Integer> beforeValues =
        oldClient.invoke(() -> TxCommitMessageBackwardCompatibilityDUnitTest.doGets(regionName));
    oldClient.invoke(() -> TxCommitMessageBackwardCompatibilityDUnitTest.doTxPuts(regionName));
    List<Integer> afterValues1 =
        oldClient.invoke(() -> TxCommitMessageBackwardCompatibilityDUnitTest.doGets(regionName));
    List<Integer> afterValues2 =
        server1.invoke(() -> TxCommitMessageBackwardCompatibilityDUnitTest.doGets(regionName));
    List<Integer> afterValues3 =
        server2.invoke(() -> TxCommitMessageBackwardCompatibilityDUnitTest.doGets(regionName));

    Integer expected1 = beforeValues.get(0) == null ? 1 : beforeValues.get(0) + 1;
    Integer expected2 = beforeValues.get(1) == null ? 1000 : beforeValues.get(1) + 1000;

    assertThat(afterValues1, contains(expected1, expected2));
    assertThat(afterValues2, contains(expected1, expected2));
    assertThat(afterValues3, contains(expected1, expected2));
  }

  @Test
  public void testServerToServerViaPoolTxReplicate() throws Exception {
    String regionName = REPLICATE_REGION_NAME;

    List<Integer> beforeValues =
        server3.invoke(() -> TxCommitMessageBackwardCompatibilityDUnitTest.doGets(regionName));
    server3.invoke(() -> TxCommitMessageBackwardCompatibilityDUnitTest.doTxPuts(regionName));
    List<Integer> afterValues1 =
        server3.invoke(() -> TxCommitMessageBackwardCompatibilityDUnitTest.doGets(regionName));
    List<Integer> afterValues2 =
        server1.invoke(() -> TxCommitMessageBackwardCompatibilityDUnitTest.doGets(regionName));
    List<Integer> afterValues3 =
        server2.invoke(() -> TxCommitMessageBackwardCompatibilityDUnitTest.doGets(regionName));

    Integer expected1 = beforeValues.get(0) == null ? 1 : beforeValues.get(0) + 1;
    Integer expected2 = beforeValues.get(1) == null ? 1000 : beforeValues.get(1) + 1000;

    assertThat(afterValues1, contains(expected1, expected2));
    assertThat(afterValues2, contains(expected1, expected2));
    assertThat(afterValues3, contains(expected1, expected2));
  }

  @Test
  public void testServerToServerTxPartition() throws Exception {
    String regionName = PARTITION_REGION_NAME;

    List<Integer> beforeValues =
        server1.invoke(() -> TxCommitMessageBackwardCompatibilityDUnitTest.doGets(regionName));
    server1.invoke(() -> TxCommitMessageBackwardCompatibilityDUnitTest.doTxPuts(regionName));
    List<Integer> afterValues1 =
        server1.invoke(() -> TxCommitMessageBackwardCompatibilityDUnitTest.doGets(regionName));
    List<Integer> afterValues2 =
        server2.invoke(() -> TxCommitMessageBackwardCompatibilityDUnitTest.doGets(regionName));

    Integer expected1 = beforeValues.get(0) == null ? 1 : beforeValues.get(0) + 1;
    Integer expected2 = beforeValues.get(1) == null ? 1000 : beforeValues.get(1) + 1000;

    assertThat(afterValues1, contains(expected1, expected2));
    assertThat(afterValues2, contains(expected1, expected2));
  }

  @Test
  public void testClientToServerTxPartition() throws Exception {
    String regionName = PARTITION_REGION_NAME;

    List<Integer> beforeValues =
        client.invoke(() -> TxCommitMessageBackwardCompatibilityDUnitTest.doGets(regionName));
    client.invoke(() -> TxCommitMessageBackwardCompatibilityDUnitTest.doTxPuts(regionName));
    List<Integer> afterValues1 =
        client.invoke(() -> TxCommitMessageBackwardCompatibilityDUnitTest.doGets(regionName));
    List<Integer> afterValues2 =
        server1.invoke(() -> TxCommitMessageBackwardCompatibilityDUnitTest.doGets(regionName));
    List<Integer> afterValues3 =
        server2.invoke(() -> TxCommitMessageBackwardCompatibilityDUnitTest.doGets(regionName));

    Integer expected1 = beforeValues.get(0) == null ? 1 : beforeValues.get(0) + 1;
    Integer expected2 = beforeValues.get(1) == null ? 1000 : beforeValues.get(1) + 1000;

    assertThat(afterValues1, contains(expected1, expected2));
    assertThat(afterValues2, contains(expected1, expected2));
    assertThat(afterValues3, contains(expected1, expected2));
  }

  @Test
  public void testOldClientToServerTxPartition() throws Exception {
    String regionName = PARTITION_REGION_NAME;

    List<Integer> beforeValues =
        oldClient.invoke(() -> TxCommitMessageBackwardCompatibilityDUnitTest.doGets(regionName));
    oldClient.invoke(() -> TxCommitMessageBackwardCompatibilityDUnitTest.doTxPuts(regionName));
    List<Integer> afterValues1 =
        oldClient.invoke(() -> TxCommitMessageBackwardCompatibilityDUnitTest.doGets(regionName));
    List<Integer> afterValues2 =
        server1.invoke(() -> TxCommitMessageBackwardCompatibilityDUnitTest.doGets(regionName));
    List<Integer> afterValues3 =
        server2.invoke(() -> TxCommitMessageBackwardCompatibilityDUnitTest.doGets(regionName));

    Integer expected1 = beforeValues.get(0) == null ? 1 : beforeValues.get(0) + 1;
    Integer expected2 = beforeValues.get(1) == null ? 1000 : beforeValues.get(1) + 1000;

    assertThat(afterValues1, contains(expected1, expected2));
    assertThat(afterValues2, contains(expected1, expected2));
    assertThat(afterValues3, contains(expected1, expected2));
  }

  @Test
  public void testServerToServerViaPoolTxPartition() throws Exception {
    String regionName = PARTITION_REGION_NAME;

    List<Integer> beforeValues =
        server3.invoke(() -> TxCommitMessageBackwardCompatibilityDUnitTest.doGets(regionName));
    server3.invoke(() -> TxCommitMessageBackwardCompatibilityDUnitTest.doTxPuts(regionName));
    List<Integer> afterValues1 =
        server3.invoke(() -> TxCommitMessageBackwardCompatibilityDUnitTest.doGets(regionName));
    List<Integer> afterValues2 =
        server1.invoke(() -> TxCommitMessageBackwardCompatibilityDUnitTest.doGets(regionName));
    List<Integer> afterValues3 =
        server2.invoke(() -> TxCommitMessageBackwardCompatibilityDUnitTest.doGets(regionName));

    Integer expected1 = beforeValues.get(0) == null ? 1 : beforeValues.get(0) + 1;
    Integer expected2 = beforeValues.get(1) == null ? 1000 : beforeValues.get(1) + 1000;

    assertThat(afterValues1, contains(expected1, expected2));
    assertThat(afterValues2, contains(expected1, expected2));
    assertThat(afterValues3, contains(expected1, expected2));
  }

  @Test
  public void testServerToServerTxBoth() throws Exception {
    String regionNameRepl = REPLICATE_REGION_NAME;
    String regionNamePart = PARTITION_REGION_NAME;

    List<Integer> beforeValuesRepl =
        server1.invoke(() -> TxCommitMessageBackwardCompatibilityDUnitTest.doGets(regionNameRepl));
    List<Integer> beforeValuesPart =
        server1.invoke(() -> TxCommitMessageBackwardCompatibilityDUnitTest.doGets(regionNamePart));
    server1.invoke(() -> TxCommitMessageBackwardCompatibilityDUnitTest.doTxPutsBoth(regionNameRepl,
        regionNamePart));
    List<Integer> afterValuesRepl1 =
        server1.invoke(() -> TxCommitMessageBackwardCompatibilityDUnitTest.doGets(regionNameRepl));
    List<Integer> afterValuesRepl2 =
        server2.invoke(() -> TxCommitMessageBackwardCompatibilityDUnitTest.doGets(regionNameRepl));
    List<Integer> afterValuesPart1 =
        server1.invoke(() -> TxCommitMessageBackwardCompatibilityDUnitTest.doGets(regionNamePart));
    List<Integer> afterValuesPart2 =
        server2.invoke(() -> TxCommitMessageBackwardCompatibilityDUnitTest.doGets(regionNamePart));

    Integer expectedRepl1 = beforeValuesRepl.get(0) == null ? 500 : beforeValuesRepl.get(0) + 500;
    Integer expectedRepl2 = beforeValuesRepl.get(1) == null ? 1000 : beforeValuesRepl.get(1) + 1000;
    Integer expectedPart1 = beforeValuesPart.get(0) == null ? 1500 : beforeValuesPart.get(0) + 1500;
    Integer expectedPart2 = beforeValuesPart.get(1) == null ? 2000 : beforeValuesPart.get(1) + 2000;

    assertThat(afterValuesRepl1, contains(expectedRepl1, expectedRepl2));
    assertThat(afterValuesRepl2, contains(expectedRepl1, expectedRepl2));
    assertThat(afterValuesPart1, contains(expectedPart1, expectedPart2));
    assertThat(afterValuesPart2, contains(expectedPart1, expectedPart2));
  }

  @Test
  public void testClientToServerTxBoth() throws Exception {
    String regionNameRepl = REPLICATE_REGION_NAME;
    String regionNamePart = PARTITION_REGION_NAME;

    List<Integer> beforeValuesRepl =
        client.invoke(() -> TxCommitMessageBackwardCompatibilityDUnitTest.doGets(regionNameRepl));
    List<Integer> beforeValuesPart =
        client.invoke(() -> TxCommitMessageBackwardCompatibilityDUnitTest.doGets(regionNamePart));
    client.invoke(() -> TxCommitMessageBackwardCompatibilityDUnitTest.doTxPutsBoth(regionNameRepl,
        regionNamePart));
    List<Integer> afterValuesRepl1 =
        client.invoke(() -> TxCommitMessageBackwardCompatibilityDUnitTest.doGets(regionNameRepl));
    List<Integer> afterValuesPart1 =
        client.invoke(() -> TxCommitMessageBackwardCompatibilityDUnitTest.doGets(regionNamePart));
    List<Integer> afterValuesRepl2 =
        server1.invoke(() -> TxCommitMessageBackwardCompatibilityDUnitTest.doGets(regionNameRepl));
    List<Integer> afterValuesPart2 =
        server1.invoke(() -> TxCommitMessageBackwardCompatibilityDUnitTest.doGets(regionNamePart));
    List<Integer> afterValuesRepl3 =
        server2.invoke(() -> TxCommitMessageBackwardCompatibilityDUnitTest.doGets(regionNameRepl));
    List<Integer> afterValuesPart3 =
        server2.invoke(() -> TxCommitMessageBackwardCompatibilityDUnitTest.doGets(regionNamePart));

    Integer expectedRepl1 = beforeValuesRepl.get(0) == null ? 500 : beforeValuesRepl.get(0) + 500;
    Integer expectedRepl2 = beforeValuesRepl.get(1) == null ? 1000 : beforeValuesRepl.get(1) + 1000;
    Integer expectedPart1 = beforeValuesPart.get(0) == null ? 1500 : beforeValuesPart.get(0) + 1500;
    Integer expectedPart2 = beforeValuesPart.get(1) == null ? 2000 : beforeValuesPart.get(1) + 2000;

    assertThat(afterValuesRepl1, contains(expectedRepl1, expectedRepl2));
    assertThat(afterValuesPart1, contains(expectedPart1, expectedPart2));
    assertThat(afterValuesRepl2, contains(expectedRepl1, expectedRepl2));
    assertThat(afterValuesPart2, contains(expectedPart1, expectedPart2));
    assertThat(afterValuesRepl3, contains(expectedRepl1, expectedRepl2));
    assertThat(afterValuesPart3, contains(expectedPart1, expectedPart2));
  }

  @Test
  public void testOldClientToServerTxBoth() throws Exception {
    String regionNameRepl = REPLICATE_REGION_NAME;
    String regionNamePart = PARTITION_REGION_NAME;

    List<Integer> beforeValuesRepl = oldClient
        .invoke(() -> TxCommitMessageBackwardCompatibilityDUnitTest.doGets(regionNameRepl));
    List<Integer> beforeValuesPart = oldClient
        .invoke(() -> TxCommitMessageBackwardCompatibilityDUnitTest.doGets(regionNamePart));
    oldClient.invoke(() -> TxCommitMessageBackwardCompatibilityDUnitTest
        .doTxPutsBoth(regionNameRepl, regionNamePart));
    List<Integer> afterValuesRepl1 = oldClient
        .invoke(() -> TxCommitMessageBackwardCompatibilityDUnitTest.doGets(regionNameRepl));
    List<Integer> afterValuesPart1 = oldClient
        .invoke(() -> TxCommitMessageBackwardCompatibilityDUnitTest.doGets(regionNamePart));
    List<Integer> afterValuesRepl2 =
        server1.invoke(() -> TxCommitMessageBackwardCompatibilityDUnitTest.doGets(regionNameRepl));
    List<Integer> afterValuesPart2 =
        server1.invoke(() -> TxCommitMessageBackwardCompatibilityDUnitTest.doGets(regionNamePart));
    List<Integer> afterValuesRepl3 =
        server2.invoke(() -> TxCommitMessageBackwardCompatibilityDUnitTest.doGets(regionNameRepl));
    List<Integer> afterValuesPart3 =
        server2.invoke(() -> TxCommitMessageBackwardCompatibilityDUnitTest.doGets(regionNamePart));

    Integer expectedRepl1 = beforeValuesRepl.get(0) == null ? 500 : beforeValuesRepl.get(0) + 500;
    Integer expectedRepl2 = beforeValuesRepl.get(1) == null ? 1000 : beforeValuesRepl.get(1) + 1000;
    Integer expectedPart1 = beforeValuesPart.get(0) == null ? 1500 : beforeValuesPart.get(0) + 1500;
    Integer expectedPart2 = beforeValuesPart.get(1) == null ? 2000 : beforeValuesPart.get(1) + 2000;

    assertThat(afterValuesRepl1, contains(expectedRepl1, expectedRepl2));
    assertThat(afterValuesPart1, contains(expectedPart1, expectedPart2));
    assertThat(afterValuesRepl2, contains(expectedRepl1, expectedRepl2));
    assertThat(afterValuesPart2, contains(expectedPart1, expectedPart2));
    assertThat(afterValuesRepl3, contains(expectedRepl1, expectedRepl2));
    assertThat(afterValuesPart3, contains(expectedPart1, expectedPart2));
  }

  @Test
  public void testServerToServerViaPoolTxBoth() throws Exception {
    String regionNameRepl = REPLICATE_REGION_NAME;
    String regionNamePart = PARTITION_REGION_NAME;

    List<Integer> beforeValuesRepl =
        server3.invoke(() -> TxCommitMessageBackwardCompatibilityDUnitTest.doGets(regionNameRepl));
    List<Integer> beforeValuesPart =
        server3.invoke(() -> TxCommitMessageBackwardCompatibilityDUnitTest.doGets(regionNamePart));
    server3.invoke(() -> TxCommitMessageBackwardCompatibilityDUnitTest.doTxPutsBoth(regionNameRepl,
        regionNamePart));
    List<Integer> afterValuesRepl1 =
        server3.invoke(() -> TxCommitMessageBackwardCompatibilityDUnitTest.doGets(regionNameRepl));
    List<Integer> afterValuesPart1 =
        server3.invoke(() -> TxCommitMessageBackwardCompatibilityDUnitTest.doGets(regionNamePart));
    List<Integer> afterValuesRepl2 =
        server1.invoke(() -> TxCommitMessageBackwardCompatibilityDUnitTest.doGets(regionNameRepl));
    List<Integer> afterValuesPart2 =
        server1.invoke(() -> TxCommitMessageBackwardCompatibilityDUnitTest.doGets(regionNamePart));
    List<Integer> afterValuesRepl3 =
        server2.invoke(() -> TxCommitMessageBackwardCompatibilityDUnitTest.doGets(regionNameRepl));
    List<Integer> afterValuesPart3 =
        server2.invoke(() -> TxCommitMessageBackwardCompatibilityDUnitTest.doGets(regionNamePart));

    Integer expectedRepl1 = beforeValuesRepl.get(0) == null ? 500 : beforeValuesRepl.get(0) + 500;
    Integer expectedRepl2 = beforeValuesRepl.get(1) == null ? 1000 : beforeValuesRepl.get(1) + 1000;
    Integer expectedPart1 = beforeValuesPart.get(0) == null ? 1500 : beforeValuesPart.get(0) + 1500;
    Integer expectedPart2 = beforeValuesPart.get(1) == null ? 2000 : beforeValuesPart.get(1) + 2000;

    assertThat(afterValuesRepl1, contains(expectedRepl1, expectedRepl2));
    assertThat(afterValuesPart1, contains(expectedPart1, expectedPart2));
    assertThat(afterValuesRepl2, contains(expectedRepl1, expectedRepl2));
    assertThat(afterValuesPart2, contains(expectedPart1, expectedPart2));
    assertThat(afterValuesRepl3, contains(expectedRepl1, expectedRepl2));
    assertThat(afterValuesPart3, contains(expectedPart1, expectedPart2));
  }
}

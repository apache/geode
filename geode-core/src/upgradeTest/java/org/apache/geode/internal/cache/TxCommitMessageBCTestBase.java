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

import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPort;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

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
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.BackwardCompatibilityTest;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;
import org.apache.geode.test.version.VersionManager;

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
@Category(BackwardCompatibilityTest.class)
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
@SuppressWarnings("serial")
public abstract class TxCommitMessageBCTestBase extends JUnit4DistributedTestCase {
  @Parameterized.Parameter
  public String testVersion;

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

  protected static VM server1 = null;
  protected static VM server2 = null;
  protected static VM server3 = null;
  protected static VM client = null;
  protected static VM oldClient = null;

  protected static GemFireCacheImpl cache;

  protected static final String REPLICATE_REGION_NAME =
      TxCommitMessageBCTestBase.class.getSimpleName() + "_ReplicateRegion";
  protected static final String PARTITION_REGION_NAME =
      TxCommitMessageBCTestBase.class.getSimpleName() + "_PartitionRegion";

  protected static String KEY1 = "KEY1";
  protected static String KEY2 = "KEY2";

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
        server1.invoke(TxCommitMessageBCTestBase::createServerCache);
    int port2 =
        server2.invoke(TxCommitMessageBCTestBase::createServerCache);
    server3.invoke(() -> TxCommitMessageBCTestBase
        .createServerCacheWithPool(host.getHostName(), new Integer[] {port1, port2}));
    client.invoke(() -> TxCommitMessageBCTestBase
        .createClientCache(host.getHostName(), new Integer[] {port1, port2}));
    oldClient.invoke(() -> TxCommitMessageBCTestBase
        .createClientCache(host.getHostName(), new Integer[] {port1, port2}));
  }

  @Override
  public final void preTearDown() throws Exception {
    closeCache();

    server1.invoke(TxCommitMessageBCTestBase::closeCache);
    server2.invoke(TxCommitMessageBCTestBase::closeCache);
    server3.invoke(TxCommitMessageBCTestBase::closeCache);
    client.invoke(TxCommitMessageBCTestBase::closeCache);
    oldClient.invoke(TxCommitMessageBCTestBase::closeCache);
  }

  public static void closeCache() throws Exception {
    if (cache != null) {
      cache.close();
    }
  }

  @SuppressWarnings("deprecation")
  public static int createServerCache() throws Exception {
    Properties props = new Properties();
    TxCommitMessageBCTestBase test =
        new TxCommitMessageBCTestBase() {};
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
    server.setPort(getRandomAvailableTCPPort());
    server.start();
    return server.getPort();
  }

  @SuppressWarnings("deprecation")
  public static void createServerCacheWithPool(String hostName, Integer[] ports) throws Exception {
    Properties props = new Properties();
    TxCommitMessageBCTestBase test =
        new TxCommitMessageBCTestBase() {};
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
    DistributedSystem ds = new TxCommitMessageBCTestBase() {}.getSystem(props);
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

  protected static void setVersion(String field, Object value) throws Exception {
    Field targetField = value.getClass().getDeclaredField(field);
    Field modifiersField = Field.class.getDeclaredField("modifiers");
    modifiersField.setAccessible(true);
    modifiersField.set(targetField,
        targetField.getModifiers() & ~Modifier.PRIVATE & ~Modifier.FINAL);
    targetField.set(null, value);
  }

}

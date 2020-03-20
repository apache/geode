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
package org.apache.geode.internal.cache;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_CLUSTER_CONFIGURATION;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.internal.lang.SystemPropertyHelper.GEMFIRE_PREFIX;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.Disconnect.disconnectAllFromDS;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.apache.geode.test.dunit.VM.getVMId;
import static org.apache.geode.test.dunit.VM.toArray;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.ClientMetadataService;
import org.apache.geode.cache.client.internal.ClientPartitionAdvisor;
import org.apache.geode.cache.execute.FunctionAdapter;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.RegionFunctionContext;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.cache.BucketAdvisor.ServerBucketProfile;
import org.apache.geode.internal.cache.execute.data.CustId;
import org.apache.geode.internal.cache.execute.data.OrderId;
import org.apache.geode.internal.cache.execute.data.ShipmentId;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.membership.MembershipEvent;
import org.apache.geode.management.membership.UniversalMembershipListenerAdapter;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.DistributedRestoreSystemProperties;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.categories.ClientServerTest;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;


@Category(ClientServerTest.class)
@SuppressWarnings("serial")
public class PartitionedRegionSingleHopDUnitTest implements Serializable {

  private static final String PR_NAME = "single_hop_pr";
  private static final String ORDER = "ORDER";
  private static final String CUSTOMER = "CUSTOMER";
  private static final String SHIPMENT = "SHIPMENT";
  private static final AtomicReference<CountDownLatch> LATCH = new AtomicReference<>();
  private static final int LOCAL_MAX_MEMORY_DEFAULT = -1;
  private static final long TIMEOUT_MILLIS = GeodeAwaitility.getTimeout().getValueInMS();

  private static Region<Object, Object> testRegion = null;
  private static Region<Object, Object> customerRegion = null;
  private static Region<Object, Object> orderRegion = null;
  private static Region<Object, Object> shipmentRegion = null;
  private static Region<Object, Object> replicatedRegion = null;
  private static InternalCache cache;
  private static Locator locator = null;

  private String diskStoreName;
  private VM vm0;
  private VM vm1;
  private VM vm2;
  private VM vm3;

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public CacheRule cacheRule = new CacheRule();

  @Rule
  public DistributedRestoreSystemProperties restoreSystemProperties =
      new DistributedRestoreSystemProperties();

  @Rule
  public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder();

  @Before
  public void setUp() {
    IgnoredException.addIgnoredException("Connection refused");

    diskStoreName = "disk";

    vm0 = getVM(0);
    vm1 = getVM(1);
    vm2 = getVM(2);
    vm3 = getVM(3);
  }

  @After
  public void tearDown() {
    for (VM vm : toArray(vm0, vm1, vm2, vm3)) {
      vm.invoke(() -> cacheRule.closeAndNullCache());
    }

    disconnectAllFromDS();
  }

  /**
   * 2 peers 2 servers 1 accessor.No client.Should work without any exceptions.
   */
  @Test
  public void testNoClient() {
    vm0.invoke(() -> createServer(1, 4));
    vm1.invoke(() -> createServer(1, 4));

    vm2.invoke(this::createPeer);
    vm3.invoke(this::createPeer);

    createAccessorServer();

    for (VM vm : toArray(vm0, vm1, vm2, vm3)) {
      vm.invoke(this::clearMetadata);
    }
    clearMetadata();

    for (VM vm : toArray(vm0, vm1, vm2, vm3)) {
      vm.invoke(this::putIntoPartitionedRegions);
    }
    putIntoPartitionedRegions();

    for (VM vm : toArray(vm0, vm1, vm2, vm3)) {
      vm.invoke(this::getFromPartitionedRegions);
    }
    getFromPartitionedRegions();

    for (VM vm : toArray(vm0, vm1, vm2, vm3)) {
      vm.invoke(this::verifyEmptyMetadata);
    }
    verifyEmptyMetadata();

    for (VM vm : toArray(vm0, vm1, vm2, vm3)) {
      vm.invoke(this::verifyEmptyStaticData);
    }
    verifyEmptyStaticData();
  }

  /**
   * 2 AccessorServers, 2 Peers 1 Client connected to 2 AccessorServers. Hence metadata should not
   * be fetched.
   */
  @Test
  public void testClientConnectedToAccessors() {
    int port0 = vm0.invoke(this::createAccessorServer);
    int port1 = vm1.invoke(this::createAccessorServer);

    vm2.invoke(this::createPeer);

    vm3.invoke(this::createPeer);

    createClient(port0, port1);

    putIntoPartitionedRegions();

    getFromPartitionedRegions();

    verifyEmptyMetadata();

    verifyEmptyStaticData();
  }

  /**
   * 1 server 2 accesorservers 2 peers.i client connected to the server Since only 1 server hence
   * Metadata should not be fetched.
   */
  @Test
  public void testClientConnectedTo1Server() {
    int port0 = vm0.invoke(() -> createServer(1, 4));

    vm1.invoke(this::createPeer);
    vm2.invoke(this::createPeer);

    vm3.invoke(this::createAccessorServer);

    createClient(port0);

    putIntoPartitionedRegions();

    getFromPartitionedRegions();

    verifyEmptyMetadata();

    verifyEmptyStaticData();
  }

  /**
   * 4 servers, 1 client connected to all 4 servers. Put data, get data and make the metadata
   * stable. Now verify that metadata has all 8 buckets info. Now update and ensure the fetch
   * service is never called.
   */
  @Test
  public void testMetadataContents() {
    int port0 = vm0.invoke(() -> createServer(1, 4));
    int port1 = vm1.invoke(() -> createServer(1, 4));
    int port2 = vm2.invoke(() -> createServer(1, 4));
    int port3 = vm3.invoke(() -> createServer(1, 4));
    createClient(port0, port1, port2, port3);

    putIntoPartitionedRegions();

    getFromPartitionedRegions();

    verifyMetadata();
    updateIntoSinglePR();
  }

  /**
   * 2 servers, 2 clients.One client to one server. Put from c1 to s1. Now put from c2. So since
   * there will be a hop at least once, fetchservice has to be triggered. Now put again from
   * c2.There should be no hop at all.
   */
  @Test
  public void testMetadataServiceCallAccuracy() {
    int port0 = vm0.invoke(() -> createServer(1, 4));
    int port1 = vm1.invoke(() -> createServer(1, 4));

    vm2.invoke(() -> createClient(port0));
    createClient(port1);

    vm2.invoke(this::putIntoSinglePR);

    ClientMetadataService clientMetadataService = cache.getClientMetadataService();
    clientMetadataService.satisfyRefreshMetadata_TEST_ONLY(false);

    testRegion.put(0, "create0");
    testRegion.put(1, "create1");
    testRegion.put(2, "create2");
    testRegion.put(3, "create3");

    await().until(clientMetadataService::isRefreshMetadataTestOnly);

    // make sure all fetch tasks are completed
    await().until(() -> clientMetadataService.getRefreshTaskCount_TEST_ONLY() == 0);

    clientMetadataService.satisfyRefreshMetadata_TEST_ONLY(false);
    testRegion.put(0, "create0");
    testRegion.put(1, "create1");
    testRegion.put(2, "create2");
    testRegion.put(3, "create3");

    await().until(() -> !clientMetadataService.isRefreshMetadataTestOnly());
  }

  @Test
  public void testMetadataServiceCallAccuracy_FromDestroyOp() {
    int port0 = vm0.invoke(() -> createServer(0, 4));
    int port1 = vm1.invoke(() -> createServer(0, 4));

    vm2.invoke(() -> createClient(port0));
    createClient(port1);

    vm2.invoke(this::putIntoSinglePR);

    ClientMetadataService clientMetadataService = cache.getClientMetadataService();
    clientMetadataService.satisfyRefreshMetadata_TEST_ONLY(false);

    testRegion.destroy(0);
    testRegion.destroy(1);
    testRegion.destroy(2);
    testRegion.destroy(3);

    await().until(clientMetadataService::isRefreshMetadataTestOnly);
  }

  @Test
  public void testMetadataServiceCallAccuracy_FromGetOp() {
    int port0 = vm0.invoke(() -> createServer(0, 4));
    int port1 = vm1.invoke(() -> createServer(0, 4));

    vm2.invoke(() -> createClient(port0));
    createClient(port1);

    vm2.invoke(this::putIntoSinglePR);

    ClientMetadataService clientMetadataService = cache.getClientMetadataService();
    clientMetadataService.satisfyRefreshMetadata_TEST_ONLY(false);

    testRegion.get(0);
    testRegion.get(1);
    testRegion.get(2);
    testRegion.get(3);

    await().until(clientMetadataService::isRefreshMetadataTestOnly);
    clientMetadataService.satisfyRefreshMetadata_TEST_ONLY(false);
    testRegion.get(0);
    testRegion.get(1);
    testRegion.get(2);
    testRegion.get(3);

    await().until(() -> !clientMetadataService.isRefreshMetadataTestOnly());
  }

  @Test
  public void testSingleHopWithHA() {
    int port0 = vm0.invoke(() -> createServer(0, 8));
    int port1 = vm1.invoke(() -> createServer(0, 8));
    int port2 = vm2.invoke(() -> createServer(0, 8));
    int port3 = vm3.invoke(() -> createServer(0, 8));
    createClient(port0, port1, port2, port3);
    ClientMetadataService clientMetadataService = cache.getClientMetadataService();
    clientMetadataService.satisfyRefreshMetadata_TEST_ONLY(false);

    // put
    for (int i = 1; i <= 16; i++) {
      testRegion.put(i, i);
    }

    // update
    for (int i = 1; i <= 16; i++) {
      testRegion.put(i, i + 1);
    }

    await().until(clientMetadataService::isRefreshMetadataTestOnly);

    // kill server
    vm0.invoke(this::stopServer);

    // again update
    for (int i = 1; i <= 16; i++) {
      testRegion.put(i, i + 10);
    }
  }

  @Test
  public void testSingleHopWithHAWithLocator() {
    int port3 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    String host0 = NetworkUtils.getServerHostName();
    String locator = host0 + "[" + port3 + "]";
    vm3.invoke(() -> startLocatorInVM(port3));

    try {
      vm0.invoke(() -> createServerWithLocator(locator));
      vm1.invoke(() -> createServerWithLocator(locator));
      vm2.invoke(() -> createServerWithLocator(locator));

      createClientWithLocator(host0, port3);

      // put
      for (int i = 1; i <= 16; i++) {
        testRegion.put(i, i);
      }

      // update
      for (int i = 1; i <= 16; i++) {
        testRegion.put(i, i + 1);
      }

      // kill server
      vm0.invoke(this::stopServer);

      // again update
      for (int i = 1; i <= 16; i++) {
        testRegion.put(i, i + 10);
      }

    } finally {
      vm3.invoke(this::stopLocator);
    }
  }

  @Test
  public void testNoMetadataServiceCall_ForGetOp() {
    int port0 = vm0.invoke(() -> createServer(0, 4));
    int port1 = vm1.invoke(() -> createServer(0, 4));

    vm2.invoke(() -> createClientWithoutPRSingleHopEnabled(port0));
    createClientWithoutPRSingleHopEnabled(port1);
    ClientMetadataService clientMetadataService = cache.getClientMetadataService();
    clientMetadataService.satisfyRefreshMetadata_TEST_ONLY(false);
    vm2.invoke(this::putIntoSinglePR);

    testRegion.get(0);
    testRegion.get(1);
    testRegion.get(2);
    testRegion.get(3);
    await().until(() -> !clientMetadataService.isRefreshMetadataTestOnly());

    clientMetadataService.satisfyRefreshMetadata_TEST_ONLY(false);
    testRegion.get(0);
    testRegion.get(1);
    testRegion.get(2);
    testRegion.get(3);
    await().until(() -> !clientMetadataService.isRefreshMetadataTestOnly());
  }

  @Test
  public void testNoMetadataServiceCall() {
    int port0 = vm0.invoke(() -> createServer(1, 4));
    int port1 = vm1.invoke(() -> createServer(1, 4));

    vm2.invoke(() -> createClientWithoutPRSingleHopEnabled(port0));
    createClientWithoutPRSingleHopEnabled(port1);

    vm2.invoke(this::putIntoSinglePR);
    ClientMetadataService clientMetadataService = cache.getClientMetadataService();
    clientMetadataService.satisfyRefreshMetadata_TEST_ONLY(false);

    testRegion.put(0, "create0");
    boolean metadataRefreshed_get1 = clientMetadataService.isRefreshMetadataTestOnly();

    clientMetadataService.satisfyRefreshMetadata_TEST_ONLY(false);
    testRegion.put(1, "create1");
    boolean metadataRefreshed_get2 = clientMetadataService.isRefreshMetadataTestOnly();

    clientMetadataService.satisfyRefreshMetadata_TEST_ONLY(false);
    testRegion.put(2, "create2");
    boolean metadataRefreshed_get3 = clientMetadataService.isRefreshMetadataTestOnly();

    clientMetadataService.satisfyRefreshMetadata_TEST_ONLY(false);
    testRegion.put(3, "create3");
    boolean metadataRefreshed_get4 = clientMetadataService.isRefreshMetadataTestOnly();

    await().until(() -> !(metadataRefreshed_get1
        || metadataRefreshed_get2 || metadataRefreshed_get3
        || metadataRefreshed_get4));

    clientMetadataService.satisfyRefreshMetadata_TEST_ONLY(false);
    testRegion.put(0, "create0");
    testRegion.put(1, "create1");
    testRegion.put(2, "create2");
    testRegion.put(3, "create3");

    await().until(() -> !clientMetadataService.isRefreshMetadataTestOnly());
  }

  @Test
  public void testNoMetadataServiceCall_ForDestroyOp() {
    int port0 = vm0.invoke(() -> createServer(0, 4));
    int port1 = vm1.invoke(() -> createServer(0, 4));

    vm2.invoke(() -> createClientWithoutPRSingleHopEnabled(port0));
    createClientWithoutPRSingleHopEnabled(port1);

    vm2.invoke(this::putIntoSinglePR);

    ClientMetadataService clientMetadataService = cache.getClientMetadataService();
    clientMetadataService.satisfyRefreshMetadata_TEST_ONLY(false);
    testRegion.destroy(0);
    testRegion.destroy(1);
    testRegion.destroy(2);
    testRegion.destroy(3);

    await().until(() -> !clientMetadataService.isRefreshMetadataTestOnly());
  }

  @Test
  public void testServerLocationRemovalThroughPing() throws Exception {
    LATCH.set(new CountDownLatch(2));
    int redundantCopies = 3;
    int totalNumberOfBuckets = 4;
    int port0 = vm0.invoke(() -> createServer(redundantCopies, totalNumberOfBuckets));
    int port1 = vm1.invoke(() -> createServer(redundantCopies, totalNumberOfBuckets));
    int port2 = vm2.invoke(() -> createServer(redundantCopies, totalNumberOfBuckets));
    int port3 = vm3.invoke(() -> createServer(redundantCopies, totalNumberOfBuckets));
    createClient(port0, port1, port2, port3);

    ManagementService service = ManagementService.getExistingManagementService(cache);
    MyMembershipListenerImpl listener = new MyMembershipListenerImpl(LATCH);
    listener.registerMembershipListener(service);

    putIntoPartitionedRegions();
    getFromPartitionedRegions();

    ClientMetadataService clientMetadataService = cache.getClientMetadataService();
    Map<String, ClientPartitionAdvisor> regionMetaData =
        clientMetadataService.getClientPRMetadata_TEST_ONLY();

    await().until(() -> regionMetaData.size() == 4);
    assertThat(regionMetaData.containsKey(testRegion.getFullPath())).isTrue();
    assertThat(regionMetaData.containsKey(customerRegion.getFullPath())).isTrue();
    assertThat(regionMetaData.containsKey(orderRegion.getFullPath())).isTrue();
    assertThat(regionMetaData.containsKey(shipmentRegion.getFullPath())).isTrue();

    ClientPartitionAdvisor prMetaData = regionMetaData.get(testRegion.getFullPath());
    assertThat(prMetaData.getBucketServerLocationsMap_TEST_ONLY().size())
        .isEqualTo(totalNumberOfBuckets);

    for (Entry entry : prMetaData.getBucketServerLocationsMap_TEST_ONLY().entrySet()) {
      assertThat(((List) entry.getValue()).size()).isEqualTo(totalNumberOfBuckets);
    }
    vm0.invoke(this::stopServer);
    vm1.invoke(this::stopServer);

    LATCH.get().await(TIMEOUT_MILLIS, MILLISECONDS);

    getFromPartitionedRegions();
    verifyDeadServer(regionMetaData, customerRegion, port0, port1);
    verifyDeadServer(regionMetaData, testRegion, port0, port1);
  }

  @Test
  public void testMetadataFetchOnlyThroughFunctions() {
    // Workaround for 52004
    IgnoredException.addIgnoredException("InternalFunctionInvocationTargetException");
    int redundantCopies = 3;
    int totalNumberOfBuckets = 4;
    int port0 = vm0.invoke(() -> createServer(redundantCopies, totalNumberOfBuckets));
    int port1 = vm1.invoke(() -> createServer(redundantCopies, totalNumberOfBuckets));
    int port2 = vm2.invoke(() -> createServer(redundantCopies, totalNumberOfBuckets));
    int port3 = vm3.invoke(() -> createServer(redundantCopies, totalNumberOfBuckets));
    createClient(port0, port1, port2, port3);
    executeFunctions();
    ClientMetadataService clientMetadataService = cache.getClientMetadataService();
    Map<String, ClientPartitionAdvisor> regionMetaData =
        clientMetadataService.getClientPRMetadata_TEST_ONLY();

    await().until(() -> regionMetaData.size() == 1);

    assertThat(regionMetaData.size()).isEqualTo(1);
    assertThat(regionMetaData.containsKey(testRegion.getFullPath())).isTrue();

    ClientPartitionAdvisor prMetaData = regionMetaData.get(testRegion.getFullPath());

    await().pollDelay(1000, TimeUnit.MILLISECONDS).until(() -> {
      if (prMetaData.getBucketServerLocationsMap_TEST_ONLY().size() != totalNumberOfBuckets) {
        // waiting if there is another thread holding the lock
        clientMetadataService.getClientPRMetadata((LocalRegion) testRegion);
        return false;
      } else {
        return true;
      }
    });

    await()
        .until(() -> prMetaData.getBucketServerLocationsMap_TEST_ONLY()
            .size() == totalNumberOfBuckets);
  }

  @Test
  public void testMetadataFetchOnlyThroughputAll() {
    int redundantCopies = 3;
    int totalNumberOfBuckets = 4;
    int port0 = vm0.invoke(() -> createServer(redundantCopies, totalNumberOfBuckets));
    int port1 = vm1.invoke(() -> createServer(redundantCopies, totalNumberOfBuckets));
    int port2 = vm2.invoke(() -> createServer(redundantCopies, totalNumberOfBuckets));
    int port3 = vm3.invoke(() -> createServer(redundantCopies, totalNumberOfBuckets));
    createClient(port0, port1, port2, port3);
    putAll();
    ClientMetadataService clientMetadataService = cache.getClientMetadataService();
    Map<String, ClientPartitionAdvisor> regionMetaData =
        clientMetadataService.getClientPRMetadata_TEST_ONLY();

    await().until(() -> regionMetaData.size() == 1);
    assertThat(regionMetaData.containsKey(testRegion.getFullPath())).isTrue();

    ClientPartitionAdvisor prMetaData = regionMetaData.get(testRegion.getFullPath());

    await()
        .until(() -> prMetaData.getBucketServerLocationsMap_TEST_ONLY()
            .size() == totalNumberOfBuckets);
  }

  @Test
  public void testMetadataIsSameOnAllServersAndClients() {
    int redundantCopies = 3;
    int totalNumberOfBuckets = 4;
    int port0 = vm0.invoke(() -> createServer(redundantCopies, totalNumberOfBuckets));
    int port1 = vm1.invoke(() -> createServer(redundantCopies, totalNumberOfBuckets));
    int port2 = vm2.invoke(() -> createServer(redundantCopies, totalNumberOfBuckets));
    int port3 = vm3.invoke(() -> createServer(redundantCopies, totalNumberOfBuckets));
    createClient(port0, port1, port2, port3);
    put();

    for (VM vm : toArray(vm0, vm1, vm2, vm3)) {
      vm.invoke(this::waitForLocalBucketsCreation);
    }

    ClientMetadataService clientMetadataService = cache.getClientMetadataService();
    clientMetadataService.getClientPRMetadata((LocalRegion) testRegion);

    Map<String, ClientPartitionAdvisor> regionMetaData =
        clientMetadataService.getClientPRMetadata_TEST_ONLY();
    assertThat(regionMetaData.size()).isEqualTo(1);
    assertThat(regionMetaData.containsKey(testRegion.getFullPath())).isTrue();

    ClientPartitionAdvisor prMetaData = regionMetaData.get(testRegion.getFullPath());
    Map<Integer, List<BucketServerLocation66>> clientMap =
        prMetaData.getBucketServerLocationsMap_TEST_ONLY();

    await().alias("expected no metadata to be refreshed")
        .until(() -> clientMap.size() == totalNumberOfBuckets);

    for (Entry entry : clientMap.entrySet()) {
      assertThat(((List) entry.getValue()).size()).isEqualTo(4);
    }

    for (VM vm : toArray(vm0, vm1, vm2, vm3)) {
      vm.invoke(() -> verifyMetadata(clientMap));
    }
    vm0.invoke(this::stopServer);
    vm1.invoke(this::stopServer);

    vm0.invoke(() -> startServerOnPort(port0));
    vm1.invoke(() -> startServerOnPort(port1));
    put();

    for (VM vm : toArray(vm0, vm1, vm2, vm3)) {
      vm.invoke(this::waitForLocalBucketsCreation);
    }

    cache.getClientMetadataService();
    await().alias("bucket copies are not created").until(() -> {
      ClientMetadataService lambdaclientMetadataService1 = cache.getClientMetadataService();
      Map<String, ClientPartitionAdvisor> lambdaRegionMetaData =
          lambdaclientMetadataService1.getClientPRMetadata_TEST_ONLY();
      assertThat(lambdaRegionMetaData.size()).isEqualTo(1);
      assertThat(lambdaRegionMetaData.containsKey(testRegion.getFullPath())).isTrue();
      ClientPartitionAdvisor lambdaPartitionMetaData =
          lambdaRegionMetaData.get(testRegion.getFullPath());
      Map<Integer, List<BucketServerLocation66>> lambdaClientMap =
          lambdaPartitionMetaData.getBucketServerLocationsMap_TEST_ONLY();
      assertThat(lambdaClientMap.size()).isEqualTo(totalNumberOfBuckets);
      boolean finished = true;
      for (Entry entry : lambdaClientMap.entrySet()) {
        List list = (List) entry.getValue();
        if (list.size() < totalNumberOfBuckets) {
          finished = false;
          break;
        }
      }
      return finished;
    });

    clientMetadataService = cache.getClientMetadataService();
    clientMetadataService.getClientPRMetadata((LocalRegion) testRegion);

    regionMetaData = clientMetadataService.getClientPRMetadata_TEST_ONLY();
    assertThat(regionMetaData.size()).isEqualTo(1);
    assertThat(regionMetaData.containsKey(testRegion.getFullPath())).isTrue();

    prMetaData = regionMetaData.get(testRegion.getFullPath());
    Map<Integer, List<BucketServerLocation66>> clientMap2 =
        prMetaData.getBucketServerLocationsMap_TEST_ONLY();

    await().alias("expected no metadata to be refreshed")
        .until(() -> clientMap2.size() == totalNumberOfBuckets);
    for (Entry entry : clientMap.entrySet()) {
      assertThat(((List) entry.getValue()).size()).isEqualTo(totalNumberOfBuckets);
    }

    for (VM vm : toArray(vm0, vm1, vm2, vm3)) {
      vm.invoke(() -> verifyMetadata(clientMap));
    }

    vm0.invoke(() -> cacheRule.closeAndNullCache());
    vm1.invoke(() -> cacheRule.closeAndNullCache());

    put();
    vm2.invoke("aba", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        PartitionedRegion pr = (PartitionedRegion) testRegion;
        pr.getRegionAdvisor().getAllClientBucketProfilesTest();
      }
    });

    vm3.invoke("aba", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        PartitionedRegion pr = (PartitionedRegion) testRegion;
        pr.getRegionAdvisor().getAllClientBucketProfilesTest();
      }
    });

    vm2.invoke(this::waitForLocalBucketsCreation);
    vm3.invoke(this::waitForLocalBucketsCreation);

    clientMetadataService = cache.getClientMetadataService();
    clientMetadataService.getClientPRMetadata((LocalRegion) testRegion);

    regionMetaData = clientMetadataService.getClientPRMetadata_TEST_ONLY();
    assertThat(regionMetaData.size()).isEqualTo(1);
    assertThat(regionMetaData.containsKey(testRegion.getFullPath())).isTrue();

    prMetaData = regionMetaData.get(testRegion.getFullPath());
    Map<Integer, List<BucketServerLocation66>> clientMap3 =
        prMetaData.getBucketServerLocationsMap_TEST_ONLY();

    await().until(() -> clientMap3.size() == totalNumberOfBuckets);
    for (Entry entry : clientMap.entrySet()) {
      assertThat(((List) entry.getValue()).size()).isEqualTo(2);
    }

    await().alias("verification of metadata on all members").until(() -> {
      try {
        vm2.invoke(() -> verifyMetadata(clientMap));
        vm3.invoke(() -> verifyMetadata(clientMap));
      } catch (Exception e) {
        return false;
      }
      return true;
    });
  }

  @Test
  public void testMetadataIsSameOnAllServersAndClientsHA() {
    int totalNumberOfBuckets = 4;
    int port0 =
        vm0.invoke(() -> createServer(2,
            totalNumberOfBuckets));
    int port1 =
        vm1.invoke(() -> createServer(2,
            totalNumberOfBuckets));

    createClient(port0, port1, port0, port1);
    put();

    ClientMetadataService clientMetadataService = cache.getClientMetadataService();
    clientMetadataService.getClientPRMetadata((LocalRegion) testRegion);

    Map<String, ClientPartitionAdvisor> regionMetaData =
        clientMetadataService.getClientPRMetadata_TEST_ONLY();

    await().until(() -> regionMetaData.size() == 1);

    assertThat(regionMetaData.size()).isEqualTo(1);
    assertThat(regionMetaData.containsKey(testRegion.getFullPath())).isTrue();

    vm0.invoke("aba", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        PartitionedRegion pr = (PartitionedRegion) testRegion;
        pr.getRegionAdvisor().getAllClientBucketProfilesTest();
      }
    });

    vm1.invoke("aba", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        PartitionedRegion pr = (PartitionedRegion) testRegion;
        pr.getRegionAdvisor().getAllClientBucketProfilesTest();
      }
    });

    ClientPartitionAdvisor prMetaData = regionMetaData.get(testRegion.getFullPath());
    Map<Integer, List<BucketServerLocation66>> clientMap =
        prMetaData.getBucketServerLocationsMap_TEST_ONLY();
    await().until(() -> clientMap.size() == totalNumberOfBuckets);
    for (Entry entry : clientMap.entrySet()) {
      assertThat(((List) entry.getValue()).size()).isEqualTo(2);
    }
    vm0.invoke(() -> verifyMetadata(clientMap));
    vm1.invoke(() -> verifyMetadata(clientMap));

    vm0.invoke(this::stopServer);

    put();

    clientMetadataService = cache.getClientMetadataService();
    clientMetadataService.getClientPRMetadata((LocalRegion) testRegion);

    assertThat(clientMap.size()).isEqualTo(totalNumberOfBuckets/* numBuckets */);
    for (Entry entry : clientMap.entrySet()) {
      assertThat(((List) entry.getValue()).size()).isEqualTo(1);
    }

    assertThat(regionMetaData.size()).isEqualTo(1);
    assertThat(regionMetaData.containsKey(testRegion.getFullPath())).isTrue();
    assertThat(clientMap.size()).isEqualTo(totalNumberOfBuckets/* numBuckets */);
    await().until(() -> {
      int bucketId;
      int size;
      List globalList;
      boolean finished = true;
      for (Entry entry : clientMap.entrySet()) {
        List list = (List) entry.getValue();
        if (list.size() != 1) {
          size = list.size();
          globalList = list;
          bucketId = (Integer) entry.getKey();
          finished = false;
          System.out.println("bucket copies are not created, the locations size for bucket id : "
              + bucketId + " size : " + size + " the list is " + globalList);
        }
      }
      return finished;
    });
  }

  @Test
  public void testClientMetadataForPersistentPrs() throws Exception {
    LATCH.set(new CountDownLatch(4));
    int port0 = vm0
        .invoke(this::createPersistentPrsAndServer);
    int port1 = vm1
        .invoke(this::createPersistentPrsAndServer);
    int port2 = vm2
        .invoke(this::createPersistentPrsAndServer);
    int port3 = vm3
        .invoke(this::createPersistentPrsAndServer);

    vm3.invoke(this::putIntoPartitionedRegions);

    for (VM vm : toArray(vm0, vm1, vm2, vm3)) {
      vm.invoke(this::waitForLocalBucketsCreation);
    }

    createClient(port0, port1, port2, port3);

    ManagementService service = ManagementService.getExistingManagementService(cache);
    MyMembershipListenerImpl listener = new MyMembershipListenerImpl(LATCH);
    listener.registerMembershipListener(service);

    await().until(this::fetchAndValidateMetadata);

    for (VM vm : toArray(vm0, vm1, vm2, vm3)) {
      vm.invoke(() -> cacheRule.closeAndNullCache());
    }

    LATCH.get().await(TIMEOUT_MILLIS, MILLISECONDS);

    AsyncInvocation m3 = vm3.invokeAsync(() -> createPersistentPrsAndServerOnPort(port3));
    AsyncInvocation m2 = vm2.invokeAsync(() -> createPersistentPrsAndServerOnPort(port2));
    AsyncInvocation m1 = vm1.invokeAsync(() -> createPersistentPrsAndServerOnPort(port1));
    AsyncInvocation m0 = vm0.invokeAsync(() -> createPersistentPrsAndServerOnPort(port0));

    m3.await();
    m2.await();
    m1.await();
    m0.await();

    fetchAndValidateMetadata();
  }

  private boolean fetchAndValidateMetadata() {
    ClientMetadataService service = cache.getClientMetadataService();
    service.getClientPRMetadata((LocalRegion) testRegion);
    Map<ServerLocation, Set<Integer>> servers =
        service.groupByServerToAllBuckets(testRegion, true);
    if (servers == null) {
      return false;
    } else if (servers.size() == 4) {
      if (servers.size() < 4) {
        return false;
      }
    }
    return true;
  }

  private void stopServer() {
    Iterator<CacheServer> iterator = cache.getCacheServers().iterator();
    if (iterator.hasNext()) {
      CacheServer server = iterator.next();
      server.stop();
    }
  }

  private void startLocatorInVM(final int locatorPort) {

    File logFile = new File("locator-" + locatorPort + ".log");

    Properties props = new Properties();
    props.setProperty(ENABLE_CLUSTER_CONFIGURATION, "false");

    try {
      locator = Locator.startLocatorAndDS(locatorPort, logFile, null, props);
    } catch (IOException e) {
      Assertions.fail("failed to startLocatorInVM", e);
    }
  }

  private void stopLocator() {
    locator.stop();
  }

  private int createServerWithLocator(String locString) {

    Properties properties = new Properties();
    properties.setProperty(LOCATORS, locString);
    cache = cacheRule.getOrCreateCache(properties);

    CacheServer server = cache.addCacheServer();
    int redundantCopies = 0;
    server.setPort(0);
    server.setHostnameForClients("localhost");
    try {
      server.start();
    } catch (IOException e) {
      Assertions.fail("Failed to start server ", e);
    }

    int totalNumberOfBuckets = 8;
    testRegion = createBasicPartitionedRegion(redundantCopies, totalNumberOfBuckets,
        LOCAL_MAX_MEMORY_DEFAULT);

    customerRegion = createColocatedRegion(CUSTOMER, null, redundantCopies, totalNumberOfBuckets,
        LOCAL_MAX_MEMORY_DEFAULT);

    orderRegion = createColocatedRegion(ORDER, CUSTOMER, redundantCopies, totalNumberOfBuckets,
        LOCAL_MAX_MEMORY_DEFAULT);

    shipmentRegion = createColocatedRegion(SHIPMENT, ORDER, redundantCopies, totalNumberOfBuckets,
        LOCAL_MAX_MEMORY_DEFAULT);

    return server.getPort();
  }

  private void clearMetadata() {
    ClientMetadataService clientMetadataService = cache.getClientMetadataService();
    clientMetadataService.getClientPartitionAttributesMap().clear();
    clientMetadataService.getClientPRMetadata_TEST_ONLY().clear();
  }

  private void verifyMetadata(Map<Integer, List<BucketServerLocation66>> clientMap) {
    PartitionedRegion pr = (PartitionedRegion) testRegion;
    ConcurrentHashMap<Integer, Set<ServerBucketProfile>> serverMap =
        pr.getRegionAdvisor().getAllClientBucketProfilesTest();
    assertThat(serverMap.size()).isEqualTo(clientMap.size());
    assertThat(clientMap.keySet().containsAll(serverMap.keySet())).isTrue();
    for (Map.Entry<Integer, List<BucketServerLocation66>> entry : clientMap.entrySet()) {
      int bucketId = entry.getKey();
      List<BucketServerLocation66> list = entry.getValue();
      BucketServerLocation66 primaryBSL = null;
      int primaryCnt = 0;
      for (BucketServerLocation66 bsl : list) {
        if (bsl.isPrimary()) {
          primaryBSL = bsl;
          primaryCnt++;
        }
      }
      assertThat(primaryCnt).isEqualTo(1);
      Set<ServerBucketProfile> set = serverMap.get(bucketId);
      assertThat(set.size()).isEqualTo(list.size());
      primaryCnt = 0;
      for (ServerBucketProfile bp : set) {
        ServerLocation sl = (ServerLocation) bp.bucketServerLocations.toArray()[0];
        assertThat(list.contains(sl)).isTrue();
        // should be only one primary
        if (bp.isPrimary) {
          primaryCnt++;
          assertThat(sl).isEqualTo(primaryBSL);
        }
      }
      assertThat(primaryCnt).isEqualTo(1);
    }
  }

  private void waitForLocalBucketsCreation() {
    PartitionedRegion pr = (PartitionedRegion) testRegion;

    await().alias("bucket copies are not created, the total number of buckets expected are "
        + 4 + " but the total num of buckets are "
        + pr.getDataStore().getAllLocalBuckets().size())
        .until(() -> pr.getDataStore().getAllLocalBuckets().size() == 4);
  }

  private void verifyDeadServer(Map<String, ClientPartitionAdvisor> regionMetaData, Region region,
      int port0, int port1) {

    ServerLocation sl0 = new ServerLocation("localhost", port0);
    ServerLocation sl1 = new ServerLocation("localhost", port1);

    ClientPartitionAdvisor prMetaData = regionMetaData.get(region.getFullPath());

    for (Entry entry : prMetaData.getBucketServerLocationsMap_TEST_ONLY().entrySet()) {
      List servers = (List) entry.getValue();
      assertThat(servers.contains(sl0)).isFalse();
      assertThat(servers.contains(sl1)).isFalse();
    }
  }

  private void createClientWithoutPRSingleHopEnabled(int port0) {
    Properties properties = new Properties();
    properties.setProperty(MCAST_PORT, "0");
    properties.setProperty(LOCATORS, "");
    cache = cacheRule.getOrCreateCache(properties);

    System.setProperty(GEMFIRE_PREFIX + "bridge.disableShufflingOfEndpoints", "true");

    Pool p;
    try {
      p = PoolManager.createFactory().addServer("localhost", port0).setPingInterval(250)
          .setSubscriptionEnabled(true).setSubscriptionRedundancy(-1).setReadTimeout(2000)
          .setSocketBufferSize(1000).setMinConnections(6).setMaxConnections(10).setRetryAttempts(3)
          .setPRSingleHopEnabled(false).create(PR_NAME);
    } finally {
      System.setProperty(GEMFIRE_PREFIX + "bridge.disableShufflingOfEndpoints", "false");
    }

    createRegionsInClientCache(p.getName());
  }

  private int createAccessorServer() {
    int redundantCopies = 1;
    int totalNumberOfBuckets = 4;
    int localMaxMemory = 0;
    cache = cacheRule.getOrCreateCache();
    CacheServer server = cache.addCacheServer();
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    server.setPort(port);
    server.setHostnameForClients("localhost");
    try {
      server.start();
    } catch (IOException e) {
      Assertions.fail("Failed to start server ", e);
    }

    testRegion =
        createBasicPartitionedRegion(redundantCopies, totalNumberOfBuckets, localMaxMemory);

    customerRegion =
        createColocatedRegion(CUSTOMER, null, redundantCopies, totalNumberOfBuckets,
            localMaxMemory);

    orderRegion =
        createColocatedRegion(ORDER, CUSTOMER, redundantCopies, totalNumberOfBuckets,
            localMaxMemory);

    shipmentRegion =
        createColocatedRegion(SHIPMENT, ORDER, redundantCopies, totalNumberOfBuckets,
            localMaxMemory);

    RegionFactory<Object, Object> regionFactory = cache.createRegionFactory();
    replicatedRegion = regionFactory.create("rr");

    return port;
  }

  private <K, V> Region<K, V> createBasicPartitionedRegion(int redundantCopies,
      int totalNumberOfBuckets,
      int localMaxMemory) {
    PartitionAttributesFactory<K, V> paf = new PartitionAttributesFactory<>();
    paf.setRedundantCopies(redundantCopies).setTotalNumBuckets(totalNumberOfBuckets);

    if (localMaxMemory > -1) {
      paf.setLocalMaxMemory(localMaxMemory);
    }

    RegionFactory<K, V> regionFactory = cache.createRegionFactory();

    regionFactory.setPartitionAttributes(paf.create());
    regionFactory.setConcurrencyChecksEnabled(true);

    Region<K, V> region = regionFactory.create(PartitionedRegionSingleHopDUnitTest.PR_NAME);
    assertThat(region).isNotNull();

    return region;
  }

  private <K, V> Region<K, V> createColocatedRegion(String regionName,
      String colocatedRegionName,
      int redundantCopies,
      int totalNumberOfBuckets,
      int localMaxMemory) {

    PartitionAttributesFactory<K, V> paf = new PartitionAttributesFactory<>();
    paf.setRedundantCopies(redundantCopies).setTotalNumBuckets(totalNumberOfBuckets)
        .setPartitionResolver(new CustomerIDPartitionResolver<>("CustomerIDPartitionResolver"));

    if (colocatedRegionName != null) {
      paf.setColocatedWith(colocatedRegionName);
    }

    if (localMaxMemory > -1) {
      paf.setLocalMaxMemory(localMaxMemory);
    }

    RegionFactory<K, V> regionFactory = cache.createRegionFactory();
    regionFactory.setPartitionAttributes(paf.create());
    regionFactory.setConcurrencyChecksEnabled(true);
    Region<K, V> region = regionFactory.create(regionName);
    assertThat(region).isNotNull();

    return region;
  }

  private int createServer(int redundantCopies, int totalNumberOfBuckets) {
    cache = cacheRule.getOrCreateCache();
    CacheServer server = cache.addCacheServer();
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    server.setPort(port);
    server.setHostnameForClients("localhost");
    try {
      server.start();
    } catch (IOException e) {
      Assertions.fail("Failed to start server ", e);
    }

    testRegion = createBasicPartitionedRegion(redundantCopies, totalNumberOfBuckets, -1);

    // creating colocated Regions
    customerRegion =
        createColocatedRegion(CUSTOMER, null, redundantCopies, totalNumberOfBuckets,
            LOCAL_MAX_MEMORY_DEFAULT);

    orderRegion =
        createColocatedRegion(ORDER, CUSTOMER, redundantCopies, totalNumberOfBuckets,
            LOCAL_MAX_MEMORY_DEFAULT);

    shipmentRegion =
        createColocatedRegion(SHIPMENT, ORDER, redundantCopies, totalNumberOfBuckets,
            LOCAL_MAX_MEMORY_DEFAULT);

    RegionFactory<Object, Object> regionFactory = cache.createRegionFactory();
    replicatedRegion = regionFactory.create("rr");

    return port;
  }

  private int createPersistentPrsAndServer() {
    cache = cacheRule.getOrCreateCache();
    if (cache.findDiskStore(diskStoreName) == null) {
      cache.createDiskStoreFactory().setDiskDirs(getDiskDirs()).create(diskStoreName);
    }
    testRegion = createBasicPersistentPartitionRegion();

    // creating colocated Regions

    int redundantCopies = 3;
    int totalNumberOfBuckets = 4;

    customerRegion = createColocatedPersistentRegionForTest(CUSTOMER, null,
        redundantCopies, totalNumberOfBuckets, LOCAL_MAX_MEMORY_DEFAULT);

    orderRegion = createColocatedPersistentRegionForTest(ORDER, CUSTOMER, redundantCopies,
        totalNumberOfBuckets, LOCAL_MAX_MEMORY_DEFAULT);

    shipmentRegion = createColocatedPersistentRegionForTest(SHIPMENT, ORDER, redundantCopies,
        totalNumberOfBuckets, LOCAL_MAX_MEMORY_DEFAULT);

    RegionFactory<Object, Object> regionFactory = cache.createRegionFactory();
    replicatedRegion = regionFactory.create("rr");
    CacheServer server = cache.addCacheServer();
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    server.setPort(port);
    server.setHostnameForClients("localhost");
    try {
      server.start();
    } catch (IOException e) {
      Assertions.fail("Failed to start server ", e);
    }
    return port;
  }

  private <K, V> Region<K, V> createBasicPersistentPartitionRegion() {
    PartitionAttributesFactory<K, V> paf = new PartitionAttributesFactory<>();
    paf.setRedundantCopies(3).setTotalNumBuckets(4);

    RegionFactory<K, V> regionFactory = cache.createRegionFactory();
    regionFactory.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
    regionFactory.setDiskStoreName("disk");
    regionFactory.setPartitionAttributes(paf.create());

    Region<K, V> region = regionFactory.create(PartitionedRegionSingleHopDUnitTest.PR_NAME);
    assertThat(region).isNotNull();

    return region;
  }

  private <K, V> Region<K, V> createColocatedPersistentRegionForTest(final String regionName,
      String colocatedRegionName,
      int redundantCopies,
      int totalNumberOfBuckets,
      int localMaxMemory) {

    PartitionAttributesFactory<K, V> paf = new PartitionAttributesFactory<>();

    paf.setRedundantCopies(redundantCopies).setTotalNumBuckets(totalNumberOfBuckets)
        .setPartitionResolver(new CustomerIDPartitionResolver<>("CustomerIDPartitionResolver"));

    if (localMaxMemory > -1) {
      paf.setLocalMaxMemory(localMaxMemory);
    }

    if (colocatedRegionName != null) {
      paf.setColocatedWith(colocatedRegionName);
    }

    RegionFactory<K, V> regionFactory = cache.createRegionFactory();

    regionFactory.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
    regionFactory.setDiskStoreName("disk");
    regionFactory.setPartitionAttributes(paf.create());
    Region<K, V> region = regionFactory.create(regionName);

    assertThat(region).isNotNull();

    return region;
  }

  private int createPersistentPrsAndServerOnPort(int port) {
    cache = cacheRule.getOrCreateCache();

    if (cache.findDiskStore(diskStoreName) == null) {
      cache.createDiskStoreFactory().setDiskDirs(getDiskDirs()).create(diskStoreName);
    }
    testRegion = createBasicPersistentPartitionRegion();

    // creating colocated Regions
    int redundantCopies = 3;
    int totalNumberOfBuckets = 4;
    customerRegion =
        createColocatedPersistentRegionForTest(CUSTOMER, null, redundantCopies,
            totalNumberOfBuckets,
            LOCAL_MAX_MEMORY_DEFAULT);

    orderRegion =
        createColocatedPersistentRegionForTest(ORDER, CUSTOMER, redundantCopies,
            totalNumberOfBuckets,
            LOCAL_MAX_MEMORY_DEFAULT);

    shipmentRegion =
        createColocatedPersistentRegionForTest(SHIPMENT, ORDER, redundantCopies,
            totalNumberOfBuckets,
            LOCAL_MAX_MEMORY_DEFAULT);

    RegionFactory<Object, Object> regionFactory = cache.createRegionFactory();
    replicatedRegion = regionFactory.create("rr");
    CacheServer server = cache.addCacheServer();
    server.setPort(port);
    server.setHostnameForClients("localhost");
    try {
      server.start();
    } catch (IOException e) {
      Assertions.fail("Failed to start server ", e);
    }

    return port;
  }

  private void startServerOnPort(int port) {
    cache = cacheRule.getOrCreateCache();
    CacheServer server = cache.addCacheServer();
    server.setPort(port);
    server.setHostnameForClients("localhost");
    try {
      server.start();
    } catch (IOException e) {
      Assertions.fail("Failed to start server ", e);
    }
  }

  private void createPeer() {
    int redundantCopies = 1;
    int totalNumberOfBuckets = 4;
    cache = cacheRule.getOrCreateCache();

    testRegion = createBasicPartitionedRegion(redundantCopies, totalNumberOfBuckets, -1);

    customerRegion =
        createColocatedRegion(CUSTOMER, null, redundantCopies, totalNumberOfBuckets, -1);

    orderRegion =
        createColocatedRegion(ORDER, CUSTOMER, redundantCopies, totalNumberOfBuckets, -1);

    shipmentRegion =
        createColocatedRegion(SHIPMENT, ORDER, redundantCopies, totalNumberOfBuckets, -1);

    RegionFactory<Object, Object> regionFactory = cache.createRegionFactory();
    replicatedRegion = regionFactory.create("rr");
  }

  private void createClient(int port) {
    Properties properties = new Properties();
    properties.setProperty(MCAST_PORT, "0");
    properties.setProperty(LOCATORS, "");
    cache = cacheRule.getOrCreateCache(properties);
    System.setProperty(GEMFIRE_PREFIX + "bridge.disableShufflingOfEndpoints", "true");
    Pool p;
    try {
      p = PoolManager.createFactory().addServer("localhost", port).setPingInterval(250)
          .setSubscriptionEnabled(true).setSubscriptionRedundancy(-1).setReadTimeout(2000)
          .setSocketBufferSize(1000).setMinConnections(6).setMaxConnections(10).setRetryAttempts(3)
          .create(PR_NAME);
    } finally {
      System.setProperty(GEMFIRE_PREFIX + "bridge.disableShufflingOfEndpoints", "false");
    }

    createRegionsInClientCache(p.getName());
  }

  private void createClient(int port0, int port1) {
    Properties properties = new Properties();
    properties.setProperty(MCAST_PORT, "0");
    properties.setProperty(LOCATORS, "");
    cache = cacheRule.getOrCreateCache(properties);
    System.setProperty(GEMFIRE_PREFIX + "bridge.disableShufflingOfEndpoints", "true");
    Pool p;
    try {
      p = PoolManager.createFactory().addServer("localhost", port0).addServer("localhost", port1)
          .setPingInterval(250).setSubscriptionEnabled(true).setSubscriptionRedundancy(-1)
          .setReadTimeout(2000).setSocketBufferSize(1000).setMinConnections(6).setMaxConnections(10)
          .setRetryAttempts(3).create(PR_NAME);
    } finally {
      System.setProperty(GEMFIRE_PREFIX + "bridge.disableShufflingOfEndpoints", "false");
    }

    createRegionsInClientCache(p.getName());
  }

  private void createClientWithLocator(String host, int port0) {
    Properties properties = new Properties();
    properties.setProperty(MCAST_PORT, "0");
    properties.setProperty(LOCATORS, "");
    cache = cacheRule.getOrCreateCache(properties);
    System.setProperty(GEMFIRE_PREFIX + "bridge.disableShufflingOfEndpoints", "true");
    Pool p;
    try {
      p = PoolManager.createFactory().addLocator(host, port0).setPingInterval(250)
          .setSubscriptionEnabled(true).setSubscriptionRedundancy(-1).setReadTimeout(2000)
          .setSocketBufferSize(1000).setMinConnections(6).setMaxConnections(10).setRetryAttempts(3)
          .create(PR_NAME);
    } finally {
      System.setProperty(GEMFIRE_PREFIX + "bridge.disableShufflingOfEndpoints", "false");
    }

    createRegionsInClientCache(p.getName());
  }

  private void createClient(int port0, int port1, int port2, int port3) {
    Properties properties = new Properties();
    properties.setProperty(MCAST_PORT, "0");
    properties.setProperty(LOCATORS, "");
    cache = cacheRule.getOrCreateCache(properties);
    System.setProperty(GEMFIRE_PREFIX + "bridge.disableShufflingOfEndpoints", "true");
    Pool p;
    try {
      p = PoolManager.createFactory().addServer("localhost", port0).addServer("localhost", port1)
          .addServer("localhost", port2).addServer("localhost", port3).setPingInterval(100)
          .setSubscriptionEnabled(false).setReadTimeout(2000).setSocketBufferSize(1000)
          .setMinConnections(6).setMaxConnections(10).setRetryAttempts(3).create(PR_NAME);
    } finally {
      System.setProperty(GEMFIRE_PREFIX + "bridge.disableShufflingOfEndpoints", "false");
    }

    createRegionsInClientCache(p.getName());
  }

  private void createRegionsInClientCache(String poolName) {
    RegionFactory<Object, Object> regionFactory = cache.createRegionFactory();
    regionFactory.setPoolName(poolName);
    regionFactory.setDataPolicy(DataPolicy.EMPTY);
    testRegion = regionFactory.create(PR_NAME);
    assertThat(testRegion).isNotNull();

    regionFactory = cache.createRegionFactory();
    regionFactory.setPoolName(poolName);
    regionFactory.setScope(Scope.LOCAL);
    regionFactory.setConcurrencyChecksEnabled(true);
    customerRegion = regionFactory.create(CUSTOMER);
    assertThat(customerRegion).isNotNull();

    orderRegion = regionFactory.create(ORDER);
    assertThat(orderRegion).isNotNull();

    shipmentRegion = regionFactory.create(SHIPMENT);
    assertThat(shipmentRegion).isNotNull();

    regionFactory = cache.createRegionFactory();
    regionFactory.setScope(Scope.LOCAL);
    regionFactory.setConcurrencyChecksEnabled(true);
    regionFactory.setPoolName(poolName);
    replicatedRegion = regionFactory.create("rr");
  }

  private void putIntoPartitionedRegions() {
    for (int i = 0; i <= 3; i++) {
      CustId custid = new CustId(i);
      Customer customer = new Customer("name" + i, "Address" + i);
      customerRegion.put(custid, customer);
      for (int j = 1; j <= 10; j++) {
        int oid = i * 10 + j;
        OrderId orderId = new OrderId(oid, custid);
        Order order = new Order("Order" + oid);
        orderRegion.put(orderId, order);
        for (int k = 1; k <= 10; k++) {
          int sid = oid * 10 + k;
          ShipmentId shipmentId = new ShipmentId(sid, orderId);
          Shipment shipment = new Shipment("Shipment" + sid);
          shipmentRegion.put(shipmentId, shipment);
        }
      }
    }

    testRegion.put(0, "create0");
    testRegion.put(1, "create1");
    testRegion.put(2, "create2");
    testRegion.put(3, "create3");

    testRegion.put(0, "update0");
    testRegion.put(1, "update1");
    testRegion.put(2, "update2");
    testRegion.put(3, "update3");

    testRegion.put(0, "update00");
    testRegion.put(1, "update11");
    testRegion.put(2, "update22");
    testRegion.put(3, "update33");
    Map<Object, Object> map = new HashMap<>();
    map.put(1, 1);
    replicatedRegion.putAll(map);
  }

  private File getDiskDir() {
    try {
      File file = new File(temporaryFolder.getRoot(), diskStoreName + getVMId());
      if (!file.exists()) {
        temporaryFolder.newFolder(diskStoreName + getVMId());
      }
      return file.getAbsoluteFile();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private File[] getDiskDirs() {
    return new File[] {getDiskDir()};
  }

  static class MyFunctionAdapter extends FunctionAdapter implements DataSerializable {
    public MyFunctionAdapter() {}

    @Override
    public String getId() {
      return "fid";
    }

    @Override
    public void execute(FunctionContext context) {
      System.out.println("YOGS function called");
      RegionFunctionContext rc = (RegionFunctionContext) context;
      Region<Object, Object> r = rc.getDataSet();
      Set filter = rc.getFilter();
      if (rc.getFilter() == null) {
        for (int i = 0; i < 200; i++) {
          r.put(i, i);
        }
      } else {
        for (Object key : filter) {
          r.put(key, key);
        }
      }
      context.getResultSender().lastResult(Boolean.TRUE);
    }

    @Override
    public boolean isHA() {
      return false;
    }

    @Override
    public boolean optimizeForWrite() {
      return true;
    }

    @Override
    public void toData(DataOutput out) throws IOException {}

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {

    }
  }

  private void executeFunctions() {
    Set<Object> filter = new HashSet<>();
    filter.add(0);
    FunctionService.onRegion(testRegion).withFilter(filter).execute(new MyFunctionAdapter())
        .getResult();
    filter.add(1);
    FunctionService.onRegion(testRegion).withFilter(filter).execute(new MyFunctionAdapter())
        .getResult();
    filter.add(2);
    filter.add(3);
    FunctionService.onRegion(testRegion).withFilter(filter).execute(new MyFunctionAdapter())
        .getResult();
    FunctionService.onRegion(testRegion).execute(new MyFunctionAdapter()).getResult();
  }

  private void putAll() {
    Map<Object, Object> map = new HashMap<>();
    map.put(0, 0);
    map.put(1, 1);
    map.put(2, 2);
    map.put(3, 3);
    testRegion.putAll(map, "putAllCallback");
    testRegion.putAll(map);
    testRegion.putAll(map);
    testRegion.putAll(map);
  }

  private void put() {
    testRegion.put(0, "create0");
    testRegion.put(1, "create1");
    testRegion.put(2, "create2");
    testRegion.put(3, "create3");
    for (int i = 0; i < 40; i++) {
      testRegion.put(i, "create" + i);
    }
  }

  private void getFromPartitionedRegions() {
    for (int i = 0; i <= 3; i++) {
      CustId custid = new CustId(i);
      Customer customer = new Customer("name" + i, "Address" + i);
      customerRegion.get(custid, customer);
      for (int j = 1; j <= 10; j++) {
        int oid = i * 10 + j;
        OrderId orderId = new OrderId(oid, custid);
        Order order = new Order("Order" + oid);
        orderRegion.get(orderId, order);
        for (int k = 1; k <= 10; k++) {
          int sid = oid * 10 + k;
          ShipmentId shipmentId = new ShipmentId(sid, orderId);
          Shipment shipment = new Shipment("Shipment" + sid);
          shipmentRegion.get(shipmentId, shipment);
        }
      }
    }

    testRegion.get(0, "create0");
    testRegion.get(1, "create1");
    testRegion.get(2, "create2");
    testRegion.get(3, "create3");

    testRegion.get(0, "update0");
    testRegion.get(1, "update1");
    testRegion.get(2, "update2");
    testRegion.get(3, "update3");

    testRegion.get(0, "update00");
    testRegion.get(1, "update11");
    testRegion.get(2, "update22");
    testRegion.get(3, "update33");
  }

  private void putIntoSinglePR() {
    testRegion.put(0, "create0");
    testRegion.put(1, "create1");
    testRegion.put(2, "create2");
    testRegion.put(3, "create3");
  }

  private void updateIntoSinglePR() {
    ClientMetadataService clientMetadataService = cache.getClientMetadataService();

    clientMetadataService.satisfyRefreshMetadata_TEST_ONLY(false);

    testRegion.put(0, "update0");
    assertThat(clientMetadataService.isRefreshMetadataTestOnly()).isFalse();

    testRegion.put(1, "update1");
    assertThat(clientMetadataService.isRefreshMetadataTestOnly()).isFalse();

    testRegion.put(2, "update2");
    assertThat(clientMetadataService.isRefreshMetadataTestOnly()).isFalse();

    testRegion.put(3, "update3");
    assertThat(clientMetadataService.isRefreshMetadataTestOnly()).isFalse();

    testRegion.put(0, "update00");
    assertThat(clientMetadataService.isRefreshMetadataTestOnly()).isFalse();

    testRegion.put(1, "update11");
    assertThat(clientMetadataService.isRefreshMetadataTestOnly()).isFalse();

    testRegion.put(2, "update22");
    assertThat(clientMetadataService.isRefreshMetadataTestOnly()).isFalse();

    testRegion.put(3, "update33");
    assertThat(clientMetadataService.isRefreshMetadataTestOnly()).isFalse();
  }

  private void verifyEmptyMetadata() {
    ClientMetadataService clientMetadataService = cache.getClientMetadataService();
    assertThat(clientMetadataService.getClientPRMetadata_TEST_ONLY().isEmpty()).isTrue();
  }

  private void verifyEmptyStaticData() {
    ClientMetadataService clientMetadataService = cache.getClientMetadataService();
    assertThat(clientMetadataService.getClientPartitionAttributesMap().isEmpty()).isTrue();
  }

  private void verifyMetadata() {
    ClientMetadataService clientMetadataService = cache.getClientMetadataService();
    // make sure all fetch tasks are completed
    await()
        .until(() -> clientMetadataService.getRefreshTaskCount_TEST_ONLY() == 0);
  }

  private static class Customer implements DataSerializable {
    private String name;
    private String address;

    public Customer() {
      // nothing
    }

    public Customer(String name, String address) {
      this.name = name;
      this.address = address;
    }

    @Override
    public void fromData(DataInput in) throws IOException {
      name = DataSerializer.readString(in);
      address = DataSerializer.readString(in);

    }

    @Override
    public void toData(DataOutput out) throws IOException {
      DataSerializer.writeString(name, out);
      DataSerializer.writeString(address, out);
    }

    @Override
    public String toString() {
      return "Customer { name=" + name + " address=" + address + "}";
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }

      if (!(o instanceof Customer)) {
        return false;
      }

      Customer cust = (Customer) o;
      return cust.name.equals(name) && cust.address.equals(address);
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, address);
    }
  }

  private static class Order implements DataSerializable {
    private String orderName;

    public Order() {
      // nothing
    }

    private Order(String orderName) {
      this.orderName = orderName;
    }

    @Override
    public void fromData(DataInput in) throws IOException {
      orderName = DataSerializer.readString(in);
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      DataSerializer.writeString(orderName, out);
    }

    @Override
    public String toString() {
      return orderName;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }

      if (obj instanceof Order) {
        Order other = (Order) obj;
        return other.orderName != null && other.orderName.equals(orderName);
      }
      return false;
    }

    @Override
    public int hashCode() {
      if (orderName == null) {
        return super.hashCode();
      }
      return orderName.hashCode();
    }
  }

  private static class Shipment implements DataSerializable {
    private String shipmentName;

    public Shipment() {
      // nothing
    }

    private Shipment(String shipmentName) {
      this.shipmentName = shipmentName;
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      shipmentName = DataSerializer.readString(in);
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      DataSerializer.writeString(shipmentName, out);
    }

    @Override
    public String toString() {
      return shipmentName;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }

      if (obj instanceof Shipment) {
        Shipment other = (Shipment) obj;
        return other.shipmentName != null && other.shipmentName.equals(shipmentName);
      }
      return false;
    }

    @Override
    public int hashCode() {
      if (shipmentName == null) {
        return super.hashCode();
      }
      return shipmentName.hashCode();
    }
  }

  private static class MyMembershipListenerImpl extends UniversalMembershipListenerAdapter {
    private final AtomicReference<CountDownLatch> latch;

    private MyMembershipListenerImpl(AtomicReference<CountDownLatch> latch) {
      this.latch = latch;
    }

    @Override
    public void memberCrashed(MembershipEvent event) {
      latch.get().countDown();
    }
  }
}

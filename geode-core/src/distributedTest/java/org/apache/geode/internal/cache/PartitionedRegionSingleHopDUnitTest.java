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

import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_CLUSTER_CONFIGURATION;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.internal.lang.SystemPropertyHelper.GEMFIRE_PREFIX;
import static org.apache.geode.management.ManagementService.getExistingManagementService;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.awaitility.GeodeAwaitility.getTimeout;
import static org.apache.geode.test.dunit.Disconnect.disconnectAllFromDS;
import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.apache.geode.test.dunit.VM.getController;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.apache.geode.test.dunit.VM.getVMId;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EntryOperation;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.PartitionResolver;
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
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.cache.BucketAdvisor.ServerBucketProfile;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.membership.MembershipEvent;
import org.apache.geode.management.membership.UniversalMembershipListenerAdapter;
import org.apache.geode.test.dunit.AsyncInvocation;
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
  private static final int LOCAL_MAX_MEMORY_DEFAULT = -1;

  private static final AtomicReference<CountDownLatch> LATCH = new AtomicReference<>();

  private static volatile Region<Object, Object> testRegion;
  private static volatile Region<Object, Object> customerRegion;
  private static volatile Region<Object, Object> orderRegion;
  private static volatile Region<Object, Object> shipmentRegion;
  private static volatile Region<Object, Object> replicatedRegion;
  private static volatile InternalCache cache;
  private static volatile Locator locator;

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
  public DistributedRestoreSystemProperties restoreProps = new DistributedRestoreSystemProperties();
  @Rule
  public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder();

  @Before
  public void setUp() {
    addIgnoredException("Connection refused");

    diskStoreName = "disk";

    vm0 = getVM(0);
    vm1 = getVM(1);
    vm2 = getVM(2);
    vm3 = getVM(3);
  }

  @After
  public void tearDown() {
    for (VM vm : asList(getController(), vm0, vm1, vm2, vm3)) {
      vm.invoke(() -> {
        cacheRule.closeAndNullCache();
        locator = null;
        cache = null;
        testRegion = null;
        customerRegion = null;
        orderRegion = null;
        shipmentRegion = null;
        replicatedRegion = null;
      });
    }

    disconnectAllFromDS();
  }

  /**
   * 2 peers 2 servers 1 accessor.No client.Should work without any exceptions.
   */
  @Test
  public void testNoClient() throws Exception {
    vm0.invoke(() -> createServer(1, 4));
    vm1.invoke(() -> createServer(1, 4));

    vm2.invoke(() -> createPeer());
    vm3.invoke(() -> createPeer());

    createAccessorServer();

    for (VM vm : asList(getController(), vm0, vm1, vm2, vm3)) {
      vm.invoke(() -> clearMetadata());
    }

    for (VM vm : asList(getController(), vm0, vm1, vm2, vm3)) {
      vm.invoke(() -> putIntoPartitionedRegions());
    }

    for (VM vm : asList(getController(), vm0, vm1, vm2, vm3)) {
      vm.invoke(() -> getFromPartitionedRegions());
    }

    for (VM vm : asList(getController(), vm0, vm1, vm2, vm3)) {
      vm.invoke(() -> verifyEmptyMetadata());
    }

    for (VM vm : asList(getController(), vm0, vm1, vm2, vm3)) {
      vm.invoke(() -> verifyEmptyStaticData());
    }
  }

  /**
   * 2 AccessorServers, 2 Peers 1 Client connected to 2 AccessorServers. Hence metadata should not
   * be fetched.
   */
  @Test
  public void testClientConnectedToAccessors() {
    int port0 = vm0.invoke(() -> createAccessorServer());
    int port1 = vm1.invoke(() -> createAccessorServer());

    vm2.invoke(() -> createPeer());
    vm3.invoke(() -> createPeer());

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

    vm1.invoke(() -> createPeer());
    vm2.invoke(() -> createPeer());

    vm3.invoke(() -> createAccessorServer());

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

    vm2.invoke(() -> putIntoSinglePR());

    ClientMetadataService clientMetadataService = cache.getClientMetadataService();
    clientMetadataService.satisfyRefreshMetadata_TEST_ONLY(false);

    testRegion.put(0, "create0");
    testRegion.put(1, "create1");
    testRegion.put(2, "create2");
    testRegion.put(3, "create3");

    await().untilAsserted(() -> {
      assertThat(clientMetadataService.isRefreshMetadataTestOnly()).isTrue();
    });

    // make sure all fetch tasks are completed
    await().untilAsserted(() -> {
      assertThat(clientMetadataService.getRefreshTaskCount_TEST_ONLY()).isZero();
    });

    clientMetadataService.satisfyRefreshMetadata_TEST_ONLY(false);

    testRegion.put(0, "create0");
    testRegion.put(1, "create1");
    testRegion.put(2, "create2");
    testRegion.put(3, "create3");

    await().untilAsserted(() -> {
      assertThat(clientMetadataService.isRefreshMetadataTestOnly()).isFalse();
    });
  }

  @Test
  public void testMetadataServiceCallAccuracy_FromDestroyOp() {
    int port0 = vm0.invoke(() -> createServer(0, 4));
    int port1 = vm1.invoke(() -> createServer(0, 4));

    vm2.invoke(() -> createClient(port0));
    createClient(port1);

    vm2.invoke(() -> putIntoSinglePR());

    ClientMetadataService clientMetadataService = cache.getClientMetadataService();
    clientMetadataService.satisfyRefreshMetadata_TEST_ONLY(false);

    testRegion.destroy(0);
    testRegion.destroy(1);
    testRegion.destroy(2);
    testRegion.destroy(3);

    await().untilAsserted(() -> {
      assertThat(clientMetadataService.isRefreshMetadataTestOnly()).isTrue();
    });
  }

  @Test
  public void testMetadataServiceCallAccuracy_FromGetOp() {
    int port0 = vm0.invoke(() -> createServer(0, 4));
    int port1 = vm1.invoke(() -> createServer(0, 4));

    vm2.invoke(() -> createClient(port0));
    createClient(port1);

    vm2.invoke(() -> putIntoSinglePR());

    ClientMetadataService clientMetadataService = cache.getClientMetadataService();
    clientMetadataService.satisfyRefreshMetadata_TEST_ONLY(false);

    testRegion.get(0);
    testRegion.get(1);
    testRegion.get(2);
    testRegion.get(3);

    await().untilAsserted(() -> {
      assertThat(clientMetadataService.isRefreshMetadataTestOnly()).isTrue();
    });

    clientMetadataService.satisfyRefreshMetadata_TEST_ONLY(false);

    testRegion.get(0);
    testRegion.get(1);
    testRegion.get(2);
    testRegion.get(3);

    await().untilAsserted(() -> {
      assertThat(clientMetadataService.isRefreshMetadataTestOnly()).isFalse();
    });
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

    await().untilAsserted(() -> {
      assertThat(clientMetadataService.isRefreshMetadataTestOnly()).isTrue();
    });

    // kill server
    vm0.invoke(() -> stopServer());

    // again update
    for (int i = 1; i <= 16; i++) {
      testRegion.put(i, i + 10);
    }
  }

  @Test
  public void testSingleHopWithHAWithLocator() {
    int port3 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    String locator = "localhost[" + port3 + "]";

    vm3.invoke(() -> startLocatorInVM(port3));

    try {
      vm0.invoke(() -> createServerWithLocator(locator));
      vm1.invoke(() -> createServerWithLocator(locator));
      vm2.invoke(() -> createServerWithLocator(locator));

      createClientWithLocator("localhost", port3);

      // put
      for (int i = 1; i <= 16; i++) {
        testRegion.put(i, i);
      }

      // update
      for (int i = 1; i <= 16; i++) {
        testRegion.put(i, i + 1);
      }

      // kill server
      vm0.invoke(() -> stopServer());

      // again update
      for (int i = 1; i <= 16; i++) {
        testRegion.put(i, i + 10);
      }

    } finally {
      vm3.invoke(() -> stopLocator());
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

    vm2.invoke(() -> putIntoSinglePR());

    testRegion.get(0);
    testRegion.get(1);
    testRegion.get(2);
    testRegion.get(3);

    await().untilAsserted(() -> {
      assertThat(clientMetadataService.isRefreshMetadataTestOnly()).isFalse();
    });

    clientMetadataService.satisfyRefreshMetadata_TEST_ONLY(false);

    testRegion.get(0);
    testRegion.get(1);
    testRegion.get(2);
    testRegion.get(3);

    await().untilAsserted(() -> {
      assertThat(clientMetadataService.isRefreshMetadataTestOnly()).isFalse();
    });
  }

  @Test
  public void testNoMetadataServiceCall() {
    int port0 = vm0.invoke(() -> createServer(1, 4));
    int port1 = vm1.invoke(() -> createServer(1, 4));

    vm2.invoke(() -> createClientWithoutPRSingleHopEnabled(port0));
    createClientWithoutPRSingleHopEnabled(port1);

    vm2.invoke(() -> putIntoSinglePR());

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

    await().untilAsserted(() -> {
      assertThat(metadataRefreshed_get1).isFalse();
      assertThat(metadataRefreshed_get2).isFalse();
      assertThat(metadataRefreshed_get3).isFalse();
      assertThat(metadataRefreshed_get4).isFalse();
    });

    clientMetadataService.satisfyRefreshMetadata_TEST_ONLY(false);

    testRegion.put(0, "create0");
    testRegion.put(1, "create1");
    testRegion.put(2, "create2");
    testRegion.put(3, "create3");

    await().untilAsserted(() -> {
      assertThat(clientMetadataService.isRefreshMetadataTestOnly()).isFalse();
    });
  }

  @Test
  public void testNoMetadataServiceCall_ForDestroyOp() {
    int port0 = vm0.invoke(() -> createServer(0, 4));
    int port1 = vm1.invoke(() -> createServer(0, 4));

    vm2.invoke(() -> createClientWithoutPRSingleHopEnabled(port0));
    createClientWithoutPRSingleHopEnabled(port1);

    vm2.invoke(() -> putIntoSinglePR());

    ClientMetadataService clientMetadataService = cache.getClientMetadataService();
    clientMetadataService.satisfyRefreshMetadata_TEST_ONLY(false);

    testRegion.destroy(0);
    testRegion.destroy(1);
    testRegion.destroy(2);
    testRegion.destroy(3);

    await().untilAsserted(() -> {
      assertThat(clientMetadataService.isRefreshMetadataTestOnly()).isFalse();
    });
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

    ManagementService managementService = getExistingManagementService(cache);
    new MemberCrashedListener(LATCH.get()).registerMembershipListener(managementService);

    putIntoPartitionedRegions();
    getFromPartitionedRegions();

    ClientMetadataService clientMetadataService = cache.getClientMetadataService();
    Map<String, ClientPartitionAdvisor> clientPRMetadata =
        clientMetadataService.getClientPRMetadata_TEST_ONLY();

    await().untilAsserted(() -> {
      assertThat(clientPRMetadata)
          .hasSize(4)
          .containsKey(testRegion.getFullPath())
          .containsKey(customerRegion.getFullPath())
          .containsKey(orderRegion.getFullPath())
          .containsKey(shipmentRegion.getFullPath());
    });

    ClientPartitionAdvisor prMetadata = clientPRMetadata.get(testRegion.getFullPath());
    assertThat(prMetadata.getBucketServerLocationsMap_TEST_ONLY()).hasSize(totalNumberOfBuckets);

    for (Entry entry : prMetadata.getBucketServerLocationsMap_TEST_ONLY().entrySet()) {
      assertThat((Iterable<?>) entry.getValue()).hasSize(totalNumberOfBuckets);
    }

    vm0.invoke(() -> stopServer());
    vm1.invoke(() -> stopServer());

    LATCH.get().await(getTimeout().getValueInMS(), MILLISECONDS);

    getFromPartitionedRegions();

    verifyDeadServer(clientPRMetadata, customerRegion, port0, port1);
    verifyDeadServer(clientPRMetadata, testRegion, port0, port1);
  }

  @Test
  public void testMetadataFetchOnlyThroughFunctions() {
    // Workaround for 52004
    addIgnoredException("InternalFunctionInvocationTargetException");
    int redundantCopies = 3;
    int totalNumberOfBuckets = 4;

    int port0 = vm0.invoke(() -> createServer(redundantCopies, totalNumberOfBuckets));
    int port1 = vm1.invoke(() -> createServer(redundantCopies, totalNumberOfBuckets));
    int port2 = vm2.invoke(() -> createServer(redundantCopies, totalNumberOfBuckets));
    int port3 = vm3.invoke(() -> createServer(redundantCopies, totalNumberOfBuckets));

    createClient(port0, port1, port2, port3);

    executeFunctions();

    ClientMetadataService clientMetadataService = cache.getClientMetadataService();
    Map<String, ClientPartitionAdvisor> clientPRMetadata =
        clientMetadataService.getClientPRMetadata_TEST_ONLY();

    await().untilAsserted(() -> {
      assertThat(clientPRMetadata).hasSize(1);
    });

    assertThat(clientPRMetadata).containsKey(testRegion.getFullPath());

    ClientPartitionAdvisor prMetadata = clientPRMetadata.get(testRegion.getFullPath());

    await().untilAsserted(() -> {
      assertThat(prMetadata.getBucketServerLocationsMap_TEST_ONLY()).hasSize(totalNumberOfBuckets);
      clientMetadataService.getClientPRMetadata((InternalRegion) testRegion);
    });
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
    Map<String, ClientPartitionAdvisor> clientPRMetadata =
        clientMetadataService.getClientPRMetadata_TEST_ONLY();

    await().untilAsserted(() -> {
      assertThat(clientPRMetadata).hasSize(1);
    });

    assertThat(clientPRMetadata).containsKey(testRegion.getFullPath());

    ClientPartitionAdvisor prMetadata = clientPRMetadata.get(testRegion.getFullPath());

    await().untilAsserted(() -> {
      assertThat(prMetadata.getBucketServerLocationsMap_TEST_ONLY()).hasSize(totalNumberOfBuckets);
    });
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

    for (VM vm : asList(vm0, vm1, vm2, vm3)) {
      vm.invoke(() -> waitForLocalBucketsCreation());
    }

    ClientMetadataService clientMetadataService = cache.getClientMetadataService();
    clientMetadataService.getClientPRMetadata((InternalRegion) testRegion);

    Map<String, ClientPartitionAdvisor> clientPRMetadata =
        clientMetadataService.getClientPRMetadata_TEST_ONLY();

    assertThat(clientPRMetadata)
        .hasSize(1)
        .containsKey(testRegion.getFullPath());

    ClientPartitionAdvisor prMetadata = clientPRMetadata.get(testRegion.getFullPath());
    Map<Integer, List<BucketServerLocation66>> clientBucketMap =
        prMetadata.getBucketServerLocationsMap_TEST_ONLY();

    await().alias("expected no metadata to be refreshed").untilAsserted(() -> {
      assertThat(clientBucketMap).hasSize(totalNumberOfBuckets);
    });

    for (Entry<Integer, List<BucketServerLocation66>> entry : clientBucketMap.entrySet()) {
      assertThat(entry.getValue()).hasSize(4);
    }

    for (VM vm : asList(vm0, vm1, vm2, vm3)) {
      vm.invoke(() -> verifyMetadata(clientBucketMap));
    }

    vm0.invoke(() -> stopServer());
    vm1.invoke(() -> stopServer());

    vm0.invoke(() -> startServerOnPort(port0));
    vm1.invoke(() -> startServerOnPort(port1));

    put();

    for (VM vm : asList(vm0, vm1, vm2, vm3)) {
      vm.invoke(() -> waitForLocalBucketsCreation());
    }

    await().alias("bucket copies are not created").untilAsserted(() -> {
      Map<String, ClientPartitionAdvisor> clientPRMetadata_await =
          clientMetadataService.getClientPRMetadata_TEST_ONLY();

      assertThat(clientPRMetadata_await)
          .hasSize(1)
          .containsKey(testRegion.getFullPath());

      ClientPartitionAdvisor prMetadata_await =
          clientPRMetadata_await.get(testRegion.getFullPath());
      Map<Integer, List<BucketServerLocation66>> clientBucketMap_await =
          prMetadata_await.getBucketServerLocationsMap_TEST_ONLY();

      assertThat(clientBucketMap_await).hasSize(totalNumberOfBuckets);

      for (Entry<Integer, List<BucketServerLocation66>> entry : clientBucketMap_await.entrySet()) {
        assertThat(entry.getValue()).hasSize(totalNumberOfBuckets);
      }
    });

    clientMetadataService.getClientPRMetadata((InternalRegion) testRegion);

    clientPRMetadata = clientMetadataService.getClientPRMetadata_TEST_ONLY();

    assertThat(clientPRMetadata)
        .hasSize(1)
        .containsKey(testRegion.getFullPath());

    prMetadata = clientPRMetadata.get(testRegion.getFullPath());

    Map<Integer, List<BucketServerLocation66>> clientBucketMap2 =
        prMetadata.getBucketServerLocationsMap_TEST_ONLY();

    await().alias("expected no metadata to be refreshed").untilAsserted(() -> {
      assertThat(clientBucketMap2).hasSize(totalNumberOfBuckets);
    });

    for (Entry<Integer, List<BucketServerLocation66>> entry : clientBucketMap.entrySet()) {
      assertThat(entry.getValue()).hasSize(totalNumberOfBuckets);
    }

    for (VM vm : asList(vm0, vm1, vm2, vm3)) {
      vm.invoke(() -> verifyMetadata(clientBucketMap));
    }

    vm0.invoke(() -> cacheRule.closeAndNullCache());
    vm1.invoke(() -> cacheRule.closeAndNullCache());

    put();

    vm2.invoke(() -> {
      PartitionedRegion pr = (PartitionedRegion) testRegion;
      pr.getRegionAdvisor().getAllClientBucketProfilesTest();
    });

    vm3.invoke(() -> {
      PartitionedRegion pr = (PartitionedRegion) testRegion;
      pr.getRegionAdvisor().getAllClientBucketProfilesTest();
    });

    vm2.invoke(() -> waitForLocalBucketsCreation());
    vm3.invoke(() -> waitForLocalBucketsCreation());

    clientMetadataService.getClientPRMetadata((InternalRegion) testRegion);

    clientPRMetadata = clientMetadataService.getClientPRMetadata_TEST_ONLY();

    assertThat(clientPRMetadata)
        .hasSize(1)
        .containsKey(testRegion.getFullPath());

    prMetadata = clientPRMetadata.get(testRegion.getFullPath());
    Map<Integer, List<BucketServerLocation66>> clientBucketMap3 =
        prMetadata.getBucketServerLocationsMap_TEST_ONLY();

    await().untilAsserted(() -> {
      assertThat(clientBucketMap3).hasSize(totalNumberOfBuckets);
    });

    for (Entry<Integer, List<BucketServerLocation66>> entry : clientBucketMap.entrySet()) {
      assertThat(entry.getValue()).hasSize(2);
    }

    await().alias("verification of metadata on all members").untilAsserted(() -> {
      vm2.invoke(() -> verifyMetadata(clientBucketMap));
      vm3.invoke(() -> verifyMetadata(clientBucketMap));
    });
  }

  @Test
  public void testMetadataIsSameOnAllServersAndClientsHA() {
    int totalNumberOfBuckets = 4;

    int port0 = vm0.invoke(() -> createServer(2, totalNumberOfBuckets));
    int port1 = vm1.invoke(() -> createServer(2, totalNumberOfBuckets));

    createClient(port0, port1, port0, port1);

    put();

    ClientMetadataService clientMetadataService = cache.getClientMetadataService();
    clientMetadataService.getClientPRMetadata((InternalRegion) testRegion);

    Map<String, ClientPartitionAdvisor> clientPRMetadata =
        clientMetadataService.getClientPRMetadata_TEST_ONLY();

    await().untilAsserted(() -> {
      assertThat(clientPRMetadata).hasSize(1);
    });

    assertThat(clientPRMetadata).containsKey(testRegion.getFullPath());

    vm0.invoke(() -> {
      PartitionedRegion pr = (PartitionedRegion) testRegion;
      pr.getRegionAdvisor().getAllClientBucketProfilesTest();
    });

    vm1.invoke(() -> {
      PartitionedRegion pr = (PartitionedRegion) testRegion;
      pr.getRegionAdvisor().getAllClientBucketProfilesTest();
    });

    ClientPartitionAdvisor prMetadata = clientPRMetadata.get(testRegion.getFullPath());
    Map<Integer, List<BucketServerLocation66>> clientBucketMap =
        prMetadata.getBucketServerLocationsMap_TEST_ONLY();

    await().untilAsserted(() -> {
      assertThat(clientBucketMap).hasSize(totalNumberOfBuckets);
    });

    for (Entry<Integer, List<BucketServerLocation66>> entry : clientBucketMap.entrySet()) {
      assertThat(entry.getValue()).hasSize(2);
    }

    vm0.invoke(() -> verifyMetadata(clientBucketMap));
    vm1.invoke(() -> verifyMetadata(clientBucketMap));

    vm0.invoke(() -> stopServer());

    put();

    clientMetadataService.getClientPRMetadata((InternalRegion) testRegion);

    assertThat(clientBucketMap).hasSize(totalNumberOfBuckets);

    for (Entry<Integer, List<BucketServerLocation66>> entry : clientBucketMap.entrySet()) {
      assertThat(entry.getValue()).hasSize(1);
    }

    assertThat(clientPRMetadata)
        .hasSize(1)
        .containsKey(testRegion.getFullPath());

    assertThat(clientBucketMap).hasSize(totalNumberOfBuckets);

    await().untilAsserted(() -> {
      for (Entry<Integer, List<BucketServerLocation66>> entry : clientBucketMap.entrySet()) {
        assertThat(entry.getValue()).hasSize(1);
      }
    });
  }

  @Test
  public void testClientMetadataForPersistentPrs() throws Exception {
    LATCH.set(new CountDownLatch(4));

    int port0 = vm0.invoke(() -> createPersistentPrsAndServer());
    int port1 = vm1.invoke(() -> createPersistentPrsAndServer());
    int port2 = vm2.invoke(() -> createPersistentPrsAndServer());
    int port3 = vm3.invoke(() -> createPersistentPrsAndServer());

    vm3.invoke(() -> putIntoPartitionedRegions());

    for (VM vm : asList(vm0, vm1, vm2, vm3)) {
      vm.invoke(() -> waitForLocalBucketsCreation());
    }

    createClient(port0, port1, port2, port3);

    ManagementService managementService = getExistingManagementService(cache);
    MemberCrashedListener listener = new MemberCrashedListener(LATCH.get());
    listener.registerMembershipListener(managementService);

    await().until(() -> fetchAndValidateMetadata());

    for (VM vm : asList(vm0, vm1, vm2, vm3)) {
      vm.invoke(() -> cacheRule.closeAndNullCache());
    }

    LATCH.get().await(getTimeout().getValueInMS(), MILLISECONDS);

    AsyncInvocation<Void> createServerOnVM3 =
        vm3.invokeAsync(() -> createPersistentPrsAndServerOnPort(port3));
    AsyncInvocation<Void> createServerOnVM2 =
        vm2.invokeAsync(() -> createPersistentPrsAndServerOnPort(port2));
    AsyncInvocation<Void> createServerOnVM1 =
        vm1.invokeAsync(() -> createPersistentPrsAndServerOnPort(port1));
    AsyncInvocation<Void> createServerOnVM0 =
        vm0.invokeAsync(() -> createPersistentPrsAndServerOnPort(port0));

    createServerOnVM3.await();
    createServerOnVM2.await();
    createServerOnVM1.await();
    createServerOnVM0.await();

    fetchAndValidateMetadata();
  }

  private boolean fetchAndValidateMetadata() {
    ClientMetadataService clientMetadataService = cache.getClientMetadataService();
    clientMetadataService.getClientPRMetadata((InternalRegion) testRegion);

    Map<ServerLocation, Set<Integer>> serverBucketMap =
        clientMetadataService.groupByServerToAllBuckets(testRegion, true);

    return serverBucketMap != null;
  }

  private void stopServer() {
    for (CacheServer cacheServer : cache.getCacheServers()) {
      cacheServer.stop();
    }
  }

  private void startLocatorInVM(int locatorPort) throws IOException {
    Properties properties = new Properties();
    properties.setProperty(ENABLE_CLUSTER_CONFIGURATION, "false");

    File logFile = new File("locator-" + locatorPort + ".log");

    locator = Locator.startLocatorAndDS(locatorPort, logFile, null, properties);
  }

  private void stopLocator() {
    locator.stop();
  }

  private int createServerWithLocator(String locators) throws IOException {
    Properties properties = new Properties();
    properties.setProperty(LOCATORS, locators);

    cache = cacheRule.getOrCreateCache(properties);

    CacheServer cacheServer = cache.addCacheServer();
    cacheServer.setHostnameForClients("localhost");
    cacheServer.setPort(0);
    cacheServer.start();

    int redundantCopies = 0;
    int totalNumberOfBuckets = 8;

    testRegion = createBasicPartitionedRegion(redundantCopies, totalNumberOfBuckets,
        LOCAL_MAX_MEMORY_DEFAULT);

    customerRegion = createColocatedRegion(CUSTOMER, null, redundantCopies, totalNumberOfBuckets,
        LOCAL_MAX_MEMORY_DEFAULT);

    orderRegion = createColocatedRegion(ORDER, CUSTOMER, redundantCopies, totalNumberOfBuckets,
        LOCAL_MAX_MEMORY_DEFAULT);

    shipmentRegion = createColocatedRegion(SHIPMENT, ORDER, redundantCopies, totalNumberOfBuckets,
        LOCAL_MAX_MEMORY_DEFAULT);

    return cacheServer.getPort();
  }

  private void clearMetadata() {
    ClientMetadataService clientMetadataService = cache.getClientMetadataService();
    clientMetadataService.getClientPartitionAttributesMap().clear();
    clientMetadataService.getClientPRMetadata_TEST_ONLY().clear();
  }

  private void verifyMetadata(Map<Integer, List<BucketServerLocation66>> clientBucketMap) {
    PartitionedRegion pr = (PartitionedRegion) testRegion;
    Map<Integer, Set<ServerBucketProfile>> serverBucketMap =
        pr.getRegionAdvisor().getAllClientBucketProfilesTest();

    assertThat(serverBucketMap).hasSize(clientBucketMap.size());
    assertThat(clientBucketMap.keySet()).containsAll(serverBucketMap.keySet());

    for (Entry<Integer, List<BucketServerLocation66>> entry : clientBucketMap.entrySet()) {
      int bucketId = entry.getKey();
      List<BucketServerLocation66> bucketLocations = entry.getValue();

      BucketServerLocation66 primaryBucketLocation = null;
      int countOfPrimaries = 0;
      for (BucketServerLocation66 bucketLocation : bucketLocations) {
        if (bucketLocation.isPrimary()) {
          primaryBucketLocation = bucketLocation;
          countOfPrimaries++;
        }
      }

      assertThat(countOfPrimaries).isEqualTo(1);

      Set<ServerBucketProfile> bucketProfiles = serverBucketMap.get(bucketId);
      assertThat(bucketProfiles).hasSize(bucketLocations.size());

      countOfPrimaries = 0;
      for (ServerBucketProfile bucketProfile : bucketProfiles) {
        ServerLocation sl = (ServerLocation) bucketProfile.getBucketServerLocations().toArray()[0];
        assertThat(bucketLocations.contains(sl)).isTrue();
        // should be only one primary
        if (bucketProfile.isPrimary) {
          countOfPrimaries++;
          assertThat(sl).isEqualTo(primaryBucketLocation);
        }
      }
      assertThat(countOfPrimaries).isEqualTo(1);
    }
  }

  private void waitForLocalBucketsCreation() {
    PartitionedRegion pr = (PartitionedRegion) testRegion;

    await().untilAsserted(() -> assertThat(pr.getDataStore().getAllLocalBuckets()).hasSize(4));
  }

  private void verifyDeadServer(Map<String, ClientPartitionAdvisor> regionMetaData, Region region,
      int port0, int port1) {

    ClientPartitionAdvisor prMetaData = regionMetaData.get(region.getFullPath());

    for (Entry<Integer, List<BucketServerLocation66>> entry : prMetaData
        .getBucketServerLocationsMap_TEST_ONLY().entrySet()) {
      for (BucketServerLocation66 bsl : entry.getValue()) {
        assertThat(bsl.getPort())
            .isNotEqualTo(port0)
            .isNotEqualTo(port1);
      }
    }
  }

  private void createClientWithoutPRSingleHopEnabled(int port0) {
    Properties properties = new Properties();
    properties.setProperty(LOCATORS, "");

    cache = cacheRule.getOrCreateCache(properties);

    System.setProperty(GEMFIRE_PREFIX + "bridge.disableShufflingOfEndpoints", "true");

    Pool pool;
    try {
      pool = PoolManager.createFactory()
          .addServer("localhost", port0)
          .setPingInterval(250)
          .setSubscriptionEnabled(true)
          .setSubscriptionRedundancy(-1)
          .setReadTimeout(2000)
          .setSocketBufferSize(1000)
          .setMinConnections(6)
          .setMaxConnections(10)
          .setRetryAttempts(3)
          .setPRSingleHopEnabled(false)
          .create(PR_NAME);
    } finally {
      System.setProperty(GEMFIRE_PREFIX + "bridge.disableShufflingOfEndpoints", "false");
    }

    createRegionsInClientCache(pool.getName());
  }

  private int createAccessorServer() throws IOException {
    cache = cacheRule.getOrCreateCache();

    CacheServer cacheServer = cache.addCacheServer();
    cacheServer.setPort(0);
    cacheServer.setHostnameForClients("localhost");
    cacheServer.start();

    int redundantCopies = 1;
    int totalNumberOfBuckets = 4;
    int localMaxMemory = 0;

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

    return cacheServer.getPort();
  }

  private <K, V> Region<K, V> createBasicPartitionedRegion(int redundantCopies,
      int totalNumberOfBuckets,
      int localMaxMemory) {
    PartitionAttributesFactory<K, V> paf = new PartitionAttributesFactory<K, V>()
        .setRedundantCopies(redundantCopies)
        .setTotalNumBuckets(totalNumberOfBuckets);

    if (localMaxMemory > -1) {
      paf.setLocalMaxMemory(localMaxMemory);
    }

    return cache.<K, V>createRegionFactory()
        .setPartitionAttributes(paf.create())
        .setConcurrencyChecksEnabled(true)
        .create(PR_NAME);
  }

  private <K, V> Region<K, V> createColocatedRegion(String regionName, String colocatedRegionName,
      int redundantCopies, int totalNumberOfBuckets, int localMaxMemory) {
    PartitionAttributesFactory<K, V> paf = new PartitionAttributesFactory<K, V>()
        .setRedundantCopies(redundantCopies)
        .setTotalNumBuckets(totalNumberOfBuckets)
        .setPartitionResolver(new CustomerIdPartitionResolver<>("CustomerIDPartitionResolver"));

    if (colocatedRegionName != null) {
      paf.setColocatedWith(colocatedRegionName);
    }
    if (localMaxMemory > -1) {
      paf.setLocalMaxMemory(localMaxMemory);
    }

    return cache.<K, V>createRegionFactory()
        .setPartitionAttributes(paf.create())
        .setConcurrencyChecksEnabled(true)
        .create(regionName);
  }

  private int createServer(int redundantCopies, int totalNumberOfBuckets) throws IOException {
    cache = cacheRule.getOrCreateCache();

    CacheServer cacheServer = cache.addCacheServer();
    cacheServer.setPort(0);
    cacheServer.setHostnameForClients("localhost");
    cacheServer.start();

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

    replicatedRegion = cache.createRegionFactory().create("rr");

    return cacheServer.getPort();
  }

  private int createPersistentPrsAndServer() throws IOException {
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

    CacheServer cacheServer = cache.addCacheServer();
    cacheServer.setPort(0);
    cacheServer.setHostnameForClients("localhost");
    cacheServer.start();

    return cacheServer.getPort();
  }

  private <K, V> Region<K, V> createBasicPersistentPartitionRegion() {
    PartitionAttributesFactory<K, V> paf = new PartitionAttributesFactory<K, V>()
        .setRedundantCopies(3)
        .setTotalNumBuckets(4);

    return cache.<K, V>createRegionFactory()
        .setDataPolicy(DataPolicy.PERSISTENT_PARTITION)
        .setDiskStoreName("disk")
        .setPartitionAttributes(paf.create())
        .create(PR_NAME);
  }

  private <K, V> Region<K, V> createColocatedPersistentRegionForTest(String regionName,
      String colocatedRegionName, int redundantCopies, int totalNumberOfBuckets,
      int localMaxMemory) {

    PartitionAttributesFactory<K, V> paf = new PartitionAttributesFactory<K, V>()
        .setRedundantCopies(redundantCopies)
        .setTotalNumBuckets(totalNumberOfBuckets)
        .setPartitionResolver(new CustomerIdPartitionResolver<>("CustomerIDPartitionResolver"));

    if (localMaxMemory > -1) {
      paf.setLocalMaxMemory(localMaxMemory);
    }
    if (colocatedRegionName != null) {
      paf.setColocatedWith(colocatedRegionName);
    }

    RegionFactory<K, V> regionFactory = cache.<K, V>createRegionFactory()
        .setDataPolicy(DataPolicy.PERSISTENT_PARTITION)
        .setDiskStoreName("disk")
        .setPartitionAttributes(paf.create());

    return regionFactory.create(regionName);
  }

  private void createPersistentPrsAndServerOnPort(int port) throws IOException {
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
            totalNumberOfBuckets, LOCAL_MAX_MEMORY_DEFAULT);

    orderRegion =
        createColocatedPersistentRegionForTest(ORDER, CUSTOMER, redundantCopies,
            totalNumberOfBuckets, LOCAL_MAX_MEMORY_DEFAULT);

    shipmentRegion =
        createColocatedPersistentRegionForTest(SHIPMENT, ORDER, redundantCopies,
            totalNumberOfBuckets, LOCAL_MAX_MEMORY_DEFAULT);

    replicatedRegion = cache.createRegionFactory()
        .create("rr");

    CacheServer cacheServer = cache.addCacheServer();
    cacheServer.setPort(port);
    cacheServer.setHostnameForClients("localhost");
    cacheServer.start();
  }

  private void startServerOnPort(int port) throws IOException {
    cache = cacheRule.getOrCreateCache();

    CacheServer cacheServer = cache.addCacheServer();
    cacheServer.setPort(port);
    cacheServer.setHostnameForClients("localhost");
    cacheServer.start();
  }

  private void createPeer() {
    cache = cacheRule.getOrCreateCache();

    int redundantCopies = 1;
    int totalNumberOfBuckets = 4;

    testRegion = createBasicPartitionedRegion(redundantCopies, totalNumberOfBuckets, -1);

    customerRegion =
        createColocatedRegion(CUSTOMER, null, redundantCopies, totalNumberOfBuckets, -1);

    orderRegion =
        createColocatedRegion(ORDER, CUSTOMER, redundantCopies, totalNumberOfBuckets, -1);

    shipmentRegion =
        createColocatedRegion(SHIPMENT, ORDER, redundantCopies, totalNumberOfBuckets, -1);

    replicatedRegion = cache.createRegionFactory().create("rr");
  }

  private void createClient(int port) {
    Properties properties = new Properties();
    properties.setProperty(LOCATORS, "");

    cache = cacheRule.getOrCreateCache(properties);

    System.setProperty(GEMFIRE_PREFIX + "bridge.disableShufflingOfEndpoints", "true");
    Pool pool;
    try {
      pool = PoolManager.createFactory()
          .addServer("localhost", port)
          .setPingInterval(250)
          .setSubscriptionEnabled(true)
          .setSubscriptionRedundancy(-1)
          .setReadTimeout(2000)
          .setSocketBufferSize(1000)
          .setMinConnections(6)
          .setMaxConnections(10)
          .setRetryAttempts(3)
          .create(PR_NAME);
    } finally {
      System.setProperty(GEMFIRE_PREFIX + "bridge.disableShufflingOfEndpoints", "false");
    }

    createRegionsInClientCache(pool.getName());
  }

  private void createClient(int port0, int port1) {
    Properties properties = new Properties();
    properties.setProperty(LOCATORS, "");

    cache = cacheRule.getOrCreateCache(properties);

    System.setProperty(GEMFIRE_PREFIX + "bridge.disableShufflingOfEndpoints", "true");
    Pool pool;
    try {
      pool = PoolManager.createFactory()
          .addServer("localhost", port0)
          .addServer("localhost", port1)
          .setPingInterval(250)
          .setSubscriptionEnabled(true)
          .setSubscriptionRedundancy(-1)
          .setReadTimeout(2000)
          .setSocketBufferSize(1000)
          .setMinConnections(6)
          .setMaxConnections(10)
          .setRetryAttempts(3)
          .create(PR_NAME);
    } finally {
      System.clearProperty(GEMFIRE_PREFIX + "bridge.disableShufflingOfEndpoints");
    }

    createRegionsInClientCache(pool.getName());
  }

  private void createClientWithLocator(String host, int port0) {
    Properties properties = new Properties();
    properties.setProperty(LOCATORS, "");

    cache = cacheRule.getOrCreateCache(properties);

    System.setProperty(GEMFIRE_PREFIX + "bridge.disableShufflingOfEndpoints", "true");
    Pool pool;
    try {
      pool = PoolManager.createFactory()
          .addLocator(host, port0)
          .setPingInterval(250)
          .setSubscriptionEnabled(true)
          .setSubscriptionRedundancy(-1)
          .setReadTimeout(2000)
          .setSocketBufferSize(1000)
          .setMinConnections(6)
          .setMaxConnections(10)
          .setRetryAttempts(3)
          .create(PR_NAME);
    } finally {
      System.clearProperty(GEMFIRE_PREFIX + "bridge.disableShufflingOfEndpoints");
    }

    createRegionsInClientCache(pool.getName());
  }

  private void createClient(int port0, int port1, int port2, int port3) {
    Properties properties = new Properties();
    properties.setProperty(LOCATORS, "");

    cache = cacheRule.getOrCreateCache(properties);

    System.setProperty(GEMFIRE_PREFIX + "bridge.disableShufflingOfEndpoints", "true");
    Pool pool;
    try {
      pool = PoolManager.createFactory()
          .addServer("localhost", port0)
          .addServer("localhost", port1)
          .addServer("localhost", port2)
          .addServer("localhost", port3)
          .setPingInterval(100)
          .setSubscriptionEnabled(false)
          .setReadTimeout(2000)
          .setSocketBufferSize(1000)
          .setMinConnections(6)
          .setMaxConnections(10)
          .setRetryAttempts(3)
          .create(PR_NAME);
    } finally {
      System.clearProperty(GEMFIRE_PREFIX + "bridge.disableShufflingOfEndpoints");
    }

    createRegionsInClientCache(pool.getName());
  }

  private void createRegionsInClientCache(String poolName) {
    testRegion = cache.createRegionFactory()
        .setDataPolicy(DataPolicy.EMPTY)
        .setPoolName(poolName)
        .create(PR_NAME);

    customerRegion = cache.createRegionFactory()
        .setConcurrencyChecksEnabled(true)
        .setPoolName(poolName)
        .setScope(Scope.LOCAL)
        .create(CUSTOMER);

    orderRegion = cache.createRegionFactory()
        .setConcurrencyChecksEnabled(true)
        .setPoolName(poolName)
        .setScope(Scope.LOCAL)
        .create(ORDER);

    shipmentRegion = cache.createRegionFactory()
        .setConcurrencyChecksEnabled(true)
        .setPoolName(poolName)
        .setScope(Scope.LOCAL)
        .create(SHIPMENT);

    replicatedRegion = cache.createRegionFactory()
        .setConcurrencyChecksEnabled(true)
        .setPoolName(poolName)
        .setScope(Scope.LOCAL)
        .create("rr");
  }

  private void putIntoPartitionedRegions() {
    for (int i = 0; i <= 3; i++) {
      CustomerId custid = new CustomerId(i);
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

  private void executeFunctions() {
    Set<Object> filter = new HashSet<>();
    filter.add(0);
    FunctionService.onRegion(testRegion).withFilter(filter).execute(new PutFunction())
        .getResult();
    filter.add(1);
    FunctionService.onRegion(testRegion).withFilter(filter).execute(new PutFunction())
        .getResult();
    filter.add(2);
    filter.add(3);
    FunctionService.onRegion(testRegion).withFilter(filter).execute(new PutFunction())
        .getResult();
    FunctionService.onRegion(testRegion).execute(new PutFunction()).getResult();
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
      CustomerId custid = new CustomerId(i);
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
    assertThat(clientMetadataService.getClientPRMetadata_TEST_ONLY()).isEmpty();
  }

  private void verifyEmptyStaticData() {
    ClientMetadataService clientMetadataService = cache.getClientMetadataService();
    assertThat(clientMetadataService.getClientPartitionAttributesMap()).isEmpty();
  }

  private void verifyMetadata() {
    ClientMetadataService clientMetadataService = cache.getClientMetadataService();
    // make sure all fetch tasks are completed
    await().untilAsserted(() -> {
      assertThat(clientMetadataService.getRefreshTaskCount_TEST_ONLY()).isZero();
    });
  }

  private static class PutFunction extends FunctionAdapter implements DataSerializable {

    public PutFunction() {
      // required
    }

    @Override
    public String getId() {
      return "fid";
    }

    @Override
    public void execute(FunctionContext context) {
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
    public void toData(DataOutput out) throws IOException {
      // nothing
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      // nothing
    }
  }

  private static class Customer implements DataSerializable {

    private String name;
    private String address;

    public Customer() {
      // nothing
    }

    private Customer(String name, String address) {
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
      return "Customer{name='" + name + "', address='" + address + "'}";
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof Customer)) {
        return false;
      }
      Customer other = (Customer) o;
      return other.name.equals(name) && other.address.equals(address);
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, address);
    }
  }

  private static class Order implements DataSerializable {

    private String name;

    public Order() {
      // nothing
    }

    private Order(String name) {
      this.name = name;
    }

    @Override
    public void fromData(DataInput in) throws IOException {
      name = DataSerializer.readString(in);
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      DataSerializer.writeString(name, out);
    }

    @Override
    public String toString() {
      return "Order{name='" + name + "'}";
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj instanceof Order) {
        return false;
      }
      Order other = (Order) obj;
      return other.name != null && other.name.equals(name);
    }

    @Override
    public int hashCode() {
      if (name == null) {
        return super.hashCode();
      }
      return name.hashCode();
    }
  }

  private static class Shipment implements DataSerializable {

    private String name;

    public Shipment() {
      // nothing
    }

    private Shipment(String name) {
      this.name = name;
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      name = DataSerializer.readString(in);
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      DataSerializer.writeString(name, out);
    }

    @Override
    public String toString() {
      return "Shipment{name='" + name + "'}";
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof Shipment)) {
        return false;
      }
      Shipment other = (Shipment) obj;
      return other.name != null && other.name.equals(name);
    }

    @Override
    public int hashCode() {
      if (name == null) {
        return super.hashCode();
      }
      return name.hashCode();
    }
  }

  private static class MemberCrashedListener extends UniversalMembershipListenerAdapter {

    private final CountDownLatch latch;

    private MemberCrashedListener(CountDownLatch latch) {
      this.latch = latch;
    }

    @Override
    public void memberCrashed(MembershipEvent event) {
      latch.countDown();
    }

    @Override
    public String toString() {
      return "MemberCrashedListener{latch=" + latch + '}';
    }
  }

  private static class CustomerId implements DataSerializable {

    private int id;

    public CustomerId() {
      // required
    }

    private CustomerId(int id) {
      this.id = id;
    }

    private int getId() {
      return id;
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      id = DataSerializer.readInteger(in);
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      DataSerializer.writeInteger(id, out);
    }

    @Override
    public String toString() {
      return "CustId{id=" + id + '}';
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof CustomerId)) {
        return false;
      }
      CustomerId other = (CustomerId) o;
      return other.id == id;
    }

    @Override
    public int hashCode() {
      return id;
    }
  }

  private static class OrderId implements DataSerializable {

    private int id;
    private CustomerId customerId;

    public OrderId() {
      // required
    }

    private OrderId(int id, CustomerId customerId) {
      this.id = id;
      this.customerId = customerId;
    }

    private CustomerId getCustomerId() {
      return customerId;
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      id = DataSerializer.readInteger(in);
      customerId = DataSerializer.readObject(in);
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      DataSerializer.writeInteger(id, out);
      DataSerializer.writeObject(customerId, out);
    }

    @Override
    public String toString() {
      return "OrderId{id=" + id + ", custId=" + customerId + '}';
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof OrderId)) {
        return false;
      }
      OrderId other = (OrderId) o;
      return other.id == id && other.customerId.equals(customerId);

    }

    @Override
    public int hashCode() {
      return customerId.hashCode();
    }
  }

  private static class ShipmentId implements DataSerializable {

    private int id;
    private OrderId orderId;

    public ShipmentId() {
      // required
    }

    private ShipmentId(int id, OrderId orderId) {
      this.id = id;
      this.orderId = orderId;
    }

    public OrderId getOrderId() {
      return orderId;
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      id = DataSerializer.readInteger(in);
      orderId = DataSerializer.readObject(in);
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      DataSerializer.writeInteger(id, out);
      DataSerializer.writeObject(orderId, out);
    }

    @Override
    public String toString() {
      return "ShipmentId{id=" + id + ", orderId=" + orderId + '}';
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof ShipmentId)) {
        return false;
      }
      ShipmentId other = (ShipmentId) obj;
      return orderId.equals(other.orderId) && id == other.id;
    }

    @Override
    public int hashCode() {
      return orderId.getCustomerId().hashCode();
    }
  }

  public static class CustomerIdPartitionResolver<K, V> implements PartitionResolver<K, V> {

    private String id;

    public CustomerIdPartitionResolver() {
      // required
    }

    public CustomerIdPartitionResolver(String id) {
      this.id = id;
    }

    @Override
    public String getName() {
      return null;
    }

    @Override
    public Serializable getRoutingObject(EntryOperation opDetails) {
      Serializable routingObject = null;
      if (opDetails.getKey() instanceof ShipmentId) {
        ShipmentId shipmentId = (ShipmentId) opDetails.getKey();
        routingObject = shipmentId.getOrderId().getCustomerId();
      }
      if (opDetails.getKey() instanceof OrderId) {
        OrderId orderId = (OrderId) opDetails.getKey();
        routingObject = orderId.getCustomerId();
      } else if (opDetails.getKey() instanceof CustomerId) {
        CustomerId customerId = (CustomerId) opDetails.getKey();
        routingObject = customerId.getId();
      }
      return routingObject;
    }

    @Override
    public String toString() {
      return "CustomerIdPartitionResolver{id='" + id + "'}";
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof CustomerIdPartitionResolver)) {
        return false;
      }
      CustomerIdPartitionResolver other = (CustomerIdPartitionResolver) o;
      return other.id.equals(id);
    }

    @Override
    public int hashCode() {
      return Objects.hash(id);
    }
  }
}

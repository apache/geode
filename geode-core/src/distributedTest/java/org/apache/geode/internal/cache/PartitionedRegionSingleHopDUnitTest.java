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
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.geode.cache.RegionShortcut.LOCAL;
import static org.apache.geode.cache.RegionShortcut.PARTITION;
import static org.apache.geode.cache.RegionShortcut.PARTITION_PERSISTENT;
import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_CLUSTER_CONFIGURATION;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.internal.cache.util.UncheckedUtils.cast;
import static org.apache.geode.internal.lang.SystemPropertyHelper.GEMFIRE_PREFIX;
import static org.apache.geode.management.ManagementService.getExistingManagementService;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.awaitility.GeodeAwaitility.getTimeout;
import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.apache.geode.test.dunit.VM.getController;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.apache.geode.test.dunit.VM.getVMId;
import static org.apache.geode.test.dunit.rules.DistributedRule.getDistributedSystemProperties;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EntryOperation;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.PartitionResolver;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.PoolFactory;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.ClientMetadataService;
import org.apache.geode.cache.client.internal.ClientPartitionAdvisor;
import org.apache.geode.cache.client.internal.InternalClientCache;
import org.apache.geode.cache.execute.FunctionAdapter;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.RegionFunctionContext;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.LocatorLauncher;
import org.apache.geode.distributed.ServerLauncher;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.cache.BucketAdvisor.ServerBucketProfile;
import org.apache.geode.internal.cache.execute.InternalFunctionInvocationTargetException;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.membership.MembershipEvent;
import org.apache.geode.management.membership.UniversalMembershipListenerAdapter;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.DUnitEnv;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.DistributedRestoreSystemProperties;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.categories.ClientServerTest;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;

@Category(ClientServerTest.class)
@SuppressWarnings("serial")
public class PartitionedRegionSingleHopDUnitTest implements Serializable {

  private static final String PARTITIONED_REGION_NAME = "single_hop_pr";
  private static final String ORDER_REGION_NAME = "ORDER";
  private static final String CUSTOMER_REGION_NAME = "CUSTOMER";
  private static final String SHIPMENT_REGION_NAME = "SHIPMENT";
  private static final String REPLICATE_REGION_NAME = "rr";

  private static final int LOCAL_MAX_MEMORY_DEFAULT = -1;

  private static final ServerLauncher DUMMY_SERVER = mock(ServerLauncher.class);
  private static final LocatorLauncher DUMMY_LOCATOR = mock(LocatorLauncher.class);
  private static final InternalClientCache DUMMY_CLIENT = mock(InternalClientCache.class);
  private static final InternalCache DUMMY_CACHE = mock(InternalCache.class);
  private static final AtomicReference<ServerLauncher> SERVER = new AtomicReference<>();
  private static final AtomicReference<LocatorLauncher> LOCATOR = new AtomicReference<>();
  private static final AtomicReference<InternalClientCache> CLIENT = new AtomicReference<>();
  private static final AtomicReference<InternalCache> CACHE = new AtomicReference<>();

  private static final AtomicReference<CountDownLatch> LATCH = new AtomicReference<>();

  private String diskStoreName;

  private VM vm0;
  private VM vm1;
  private VM vm2;
  private VM vm3;

  @Rule
  public DistributedRule distributedRule = new DistributedRule();
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

    for (VM vm : asList(getController(), vm0, vm1, vm2, vm3)) {
      vm.invoke(() -> {
        CLIENT.set(DUMMY_CLIENT);
        CACHE.set(DUMMY_CACHE);
        SERVER.set(DUMMY_SERVER);
        LOCATOR.set(DUMMY_LOCATOR);
      });
    }
  }

  @After
  public void tearDown() {
    for (VM vm : asList(getController(), vm0, vm1, vm2, vm3)) {
      vm.invoke(() -> {
        CLIENT.getAndSet(DUMMY_CLIENT).close();
        CACHE.getAndSet(DUMMY_CACHE).close();
        SERVER.getAndSet(DUMMY_SERVER).stop();
        LOCATOR.getAndSet(DUMMY_LOCATOR).stop();
      });
    }
  }

  /**
   * 2 peers 2 servers 1 accessor.No client.Should work without any exceptions.
   */
  @Test
  public void testNoClient() throws Exception {
    vm0.invoke(() -> createServer(-1, 1, 4));
    vm1.invoke(() -> createServer(-1, 1, 4));
    vm2.invoke(() -> createAccessorPeer(1, 4));
    vm3.invoke(() -> createAccessorPeer(1, 4));
    createAccessorServer(1, 4);

    for (VM vm : asList(getController(), vm0, vm1, vm2, vm3)) {
      vm.invoke(() -> clearMetadata());
    }

    for (VM vm : asList(getController(), vm0, vm1, vm2, vm3)) {
      vm.invoke(() -> putIntoPartitionedRegions());
    }

    for (VM vm : asList(getController(), vm0, vm1, vm2, vm3)) {
      vm.invoke(() -> doGets());
    }

    for (VM vm : asList(getController(), vm0, vm1, vm2, vm3)) {
      vm.invoke(() -> {
        ClientMetadataService clientMetadataService =
            getInternalCache(SERVER.get()).getClientMetadataService();

        assertThat(clientMetadataService.getClientPRMetadata_TEST_ONLY()).isEmpty();
        assertThat(clientMetadataService.getClientPartitionAttributesMap()).isEmpty();
      });
    }
  }

  /**
   * 2 AccessorServers, 2 Peers 1 Client connected to 2 AccessorServers. Hence metadata should not
   * be fetched.
   */
  @Test
  public void testClientConnectedToAccessors() {
    int port0 = vm0.invoke(() -> createAccessorServer(1, 4));
    int port1 = vm1.invoke(() -> createAccessorServer(1, 4));
    vm2.invoke(() -> createAccessorPeer(1, 4));
    vm3.invoke(() -> createAccessorPeer(1, 4));
    createClient(250, true, true, true, port0, port1);

    putIntoPartitionedRegions();
    doGets();

    ClientMetadataService clientMetadataService = CLIENT.get().getClientMetadataService();

    assertThat(clientMetadataService.getClientPRMetadata_TEST_ONLY()).isEmpty();
    assertThat(clientMetadataService.getClientPartitionAttributesMap()).isEmpty();
  }

  /**
   * 1 server 2 accesorservers 2 peers.i client connected to the server Since only 1 server hence
   * Metadata should not be fetched.
   */
  @Test
  public void testClientConnectedTo1Server() {
    int port0 = vm0.invoke(() -> createServer(-1, 1, 4));
    vm1.invoke(() -> createAccessorPeer(1, 4));
    vm2.invoke(() -> createAccessorPeer(1, 4));
    vm3.invoke(() -> createAccessorServer(1, 4));
    createClient(250, true, true, true, port0);

    putIntoPartitionedRegions();
    doGets();

    ClientMetadataService clientMetadataService = CLIENT.get().getClientMetadataService();

    assertThat(clientMetadataService.getClientPRMetadata_TEST_ONLY()).isEmpty();
    assertThat(clientMetadataService.getClientPartitionAttributesMap()).isEmpty();
  }

  /**
   * 4 servers, 1 client connected to all 4 servers. Put data, get data and make the metadata
   * stable. Now verify that metadata has all 8 buckets info. Now update and ensure the fetch
   * service is never called.
   */
  @Test
  public void testMetadataContents() {
    int port0 = vm0.invoke(() -> createServer(-1, 1, 4));
    int port1 = vm1.invoke(() -> createServer(-1, 1, 4));
    int port2 = vm2.invoke(() -> createServer(-1, 1, 4));
    int port3 = vm3.invoke(() -> createServer(-1, 1, 4));
    createClient(100, true, false, true, port0, port1, port2, port3);

    putIntoPartitionedRegions();
    doGets();

    ClientMetadataService clientMetadataService = CLIENT.get().getClientMetadataService();

    await().untilAsserted(() -> {
      assertThat(clientMetadataService.getRefreshTaskCount_TEST_ONLY()).isZero();
    });

    clientMetadataService.satisfyRefreshMetadata_TEST_ONLY(false);

    Region<Object, Object> partitionedRegion = getRegion(PARTITIONED_REGION_NAME);

    partitionedRegion.put(0, "update0");
    assertThat(clientMetadataService.isRefreshMetadataTestOnly()).isFalse();

    partitionedRegion.put(1, "update1");
    assertThat(clientMetadataService.isRefreshMetadataTestOnly()).isFalse();

    partitionedRegion.put(2, "update2");
    assertThat(clientMetadataService.isRefreshMetadataTestOnly()).isFalse();

    partitionedRegion.put(3, "update3");
    assertThat(clientMetadataService.isRefreshMetadataTestOnly()).isFalse();

    partitionedRegion.put(0, "update00");
    assertThat(clientMetadataService.isRefreshMetadataTestOnly()).isFalse();

    partitionedRegion.put(1, "update11");
    assertThat(clientMetadataService.isRefreshMetadataTestOnly()).isFalse();

    partitionedRegion.put(2, "update22");
    assertThat(clientMetadataService.isRefreshMetadataTestOnly()).isFalse();

    partitionedRegion.put(3, "update33");
    assertThat(clientMetadataService.isRefreshMetadataTestOnly()).isFalse();
  }

  /**
   * 2 servers, 2 clients.One client to one server. Put from c1 to s1. Now put from c2. So since
   * there will be a hop at least once, fetchservice has to be triggered. Now put again from
   * c2.There should be no hop at all.
   */
  @Test
  public void testMetadataServiceCallAccuracy() {
    int port0 = vm0.invoke(() -> createServer(-1, 1, 4));
    int port1 = vm1.invoke(() -> createServer(-1, 1, 4));
    vm2.invoke(() -> createClient(250, true, true, true, port0));
    createClient(250, true, true, true, port1);

    vm2.invoke(() -> doPuts(getRegion(PARTITIONED_REGION_NAME)));

    ClientMetadataService clientMetadataService = CLIENT.get().getClientMetadataService();
    clientMetadataService.satisfyRefreshMetadata_TEST_ONLY(false);

    Region<Object, Object> partitionedRegion = getRegion(PARTITIONED_REGION_NAME);

    partitionedRegion.put(0, "create0");
    partitionedRegion.put(1, "create1");
    partitionedRegion.put(2, "create2");
    partitionedRegion.put(3, "create3");

    await().untilAsserted(() -> {
      assertThat(clientMetadataService.isRefreshMetadataTestOnly()).isTrue();
    });

    // make sure all fetch tasks are completed
    await().untilAsserted(() -> {
      assertThat(clientMetadataService.getRefreshTaskCount_TEST_ONLY()).isZero();
    });

    clientMetadataService.satisfyRefreshMetadata_TEST_ONLY(false);

    partitionedRegion.put(0, "create0");
    partitionedRegion.put(1, "create1");
    partitionedRegion.put(2, "create2");
    partitionedRegion.put(3, "create3");

    await().untilAsserted(() -> {
      assertThat(clientMetadataService.isRefreshMetadataTestOnly()).isFalse();
    });
  }

  @Test
  public void testMetadataServiceCallAccuracy_FromDestroyOp() {
    int port0 = vm0.invoke(() -> createServer(-1, 0, 4));
    int port1 = vm1.invoke(() -> createServer(-1, 0, 4));
    vm2.invoke(() -> createClient(250, true, true, true, port0));
    createClient(250, true, true, true, port1);

    vm2.invoke(() -> doPuts(getRegion(PARTITIONED_REGION_NAME)));

    ClientMetadataService clientMetadataService = CLIENT.get().getClientMetadataService();
    clientMetadataService.satisfyRefreshMetadata_TEST_ONLY(false);

    Region<Object, Object> partitionedRegion = getRegion(PARTITIONED_REGION_NAME);

    partitionedRegion.destroy(0);
    partitionedRegion.destroy(1);
    partitionedRegion.destroy(2);
    partitionedRegion.destroy(3);

    await().untilAsserted(() -> {
      assertThat(clientMetadataService.isRefreshMetadataTestOnly()).isTrue();
    });
  }

  @Test
  public void testMetadataServiceCallAccuracy_FromGetOp() {
    int port0 = vm0.invoke(() -> createServer(-1, 0, 4));
    int port1 = vm1.invoke(() -> createServer(-1, 0, 4));
    vm2.invoke(() -> createClient(250, true, true, true, port0));
    createClient(250, true, true, true, port1);

    vm2.invoke(() -> doPuts(getRegion(PARTITIONED_REGION_NAME)));

    ClientMetadataService clientMetadataService = CLIENT.get().getClientMetadataService();
    clientMetadataService.satisfyRefreshMetadata_TEST_ONLY(false);

    Region<Object, Object> partitionedRegion = getRegion(PARTITIONED_REGION_NAME);

    partitionedRegion.get(0);
    partitionedRegion.get(1);
    partitionedRegion.get(2);
    partitionedRegion.get(3);

    await().untilAsserted(() -> {
      assertThat(clientMetadataService.isRefreshMetadataTestOnly()).isTrue();
    });

    clientMetadataService.satisfyRefreshMetadata_TEST_ONLY(false);

    partitionedRegion.get(0);
    partitionedRegion.get(1);
    partitionedRegion.get(2);
    partitionedRegion.get(3);

    await().untilAsserted(() -> {
      assertThat(clientMetadataService.isRefreshMetadataTestOnly()).isFalse();
    });
  }

  @Test
  public void testSingleHopWithHA() {
    int port0 = vm0.invoke(() -> createServer(-1, 0, 8));
    int port1 = vm1.invoke(() -> createServer(-1, 0, 8));
    int port2 = vm2.invoke(() -> createServer(-1, 0, 8));
    int port3 = vm3.invoke(() -> createServer(-1, 0, 8));
    createClient(100, true, false, true, port0, port1, port2, port3);

    ClientMetadataService clientMetadataService = CLIENT.get().getClientMetadataService();
    clientMetadataService.satisfyRefreshMetadata_TEST_ONLY(false);

    Region<Object, Object> partitionedRegion = getRegion(PARTITIONED_REGION_NAME);

    // put
    for (int i = 1; i <= 16; i++) {
      partitionedRegion.put(i, i);
    }

    // update
    for (int i = 1; i <= 16; i++) {
      partitionedRegion.put(i, i + 1);
    }

    await().untilAsserted(() -> {
      assertThat(clientMetadataService.isRefreshMetadataTestOnly()).isTrue();
    });

    // kill server
    vm0.invoke(() -> stopServer());

    // again update
    for (int i = 1; i <= 16; i++) {
      partitionedRegion.put(i, i + 10);
    }
  }

  @Test
  public void testSingleHopWithHAWithLocator() {
    int locatorPort = vm3.invoke(() -> startLocator());
    String locators = "localhost[" + locatorPort + "]";

    vm0.invoke(() -> createServer(locators, null, LOCAL_MAX_MEMORY_DEFAULT, 0, 8));
    vm1.invoke(() -> createServer(locators, null, LOCAL_MAX_MEMORY_DEFAULT, 0, 8));
    vm2.invoke(() -> createServer(locators, null, LOCAL_MAX_MEMORY_DEFAULT, 0, 8));
    createClient(250, true, true, false, locatorPort);

    Region<Object, Object> partitionedRegion = getRegion(PARTITIONED_REGION_NAME);

    // put
    for (int i = 1; i <= 16; i++) {
      partitionedRegion.put(i, i);
    }

    // update
    for (int i = 1; i <= 16; i++) {
      partitionedRegion.put(i, i + 1);
    }

    // kill server
    vm0.invoke(() -> stopServer());

    // again update
    for (int i = 1; i <= 16; i++) {
      partitionedRegion.put(i, i + 10);
    }
  }

  @Test
  public void testNoMetadataServiceCall_ForGetOp() {
    int port0 = vm0.invoke(() -> createServer(-1, 0, 4));
    int port1 = vm1.invoke(() -> createServer(-1, 0, 4));
    vm2.invoke(() -> createClient(250, false, true, true, port0));
    createClient(250, false, true, true, port1);

    ClientMetadataService clientMetadataService = CLIENT.get().getClientMetadataService();
    clientMetadataService.satisfyRefreshMetadata_TEST_ONLY(false);

    vm2.invoke(() -> doPuts(getRegion(PARTITIONED_REGION_NAME)));

    Region<Object, Object> partitionedRegion = getRegion(PARTITIONED_REGION_NAME);

    partitionedRegion.get(0);
    partitionedRegion.get(1);
    partitionedRegion.get(2);
    partitionedRegion.get(3);

    await().untilAsserted(() -> {
      assertThat(clientMetadataService.isRefreshMetadataTestOnly()).isFalse();
    });

    clientMetadataService.satisfyRefreshMetadata_TEST_ONLY(false);

    partitionedRegion.get(0);
    partitionedRegion.get(1);
    partitionedRegion.get(2);
    partitionedRegion.get(3);

    await().untilAsserted(() -> {
      assertThat(clientMetadataService.isRefreshMetadataTestOnly()).isFalse();
    });
  }

  @Test
  public void testNoMetadataServiceCall() {
    int port0 = vm0.invoke(() -> createServer(-1, 1, 4));
    int port1 = vm1.invoke(() -> createServer(-1, 1, 4));
    vm2.invoke(() -> createClient(250, false, true, true, port0));
    createClient(250, false, true, true, port1);

    vm2.invoke(() -> doPuts(getRegion(PARTITIONED_REGION_NAME)));

    ClientMetadataService clientMetadataService = CLIENT.get().getClientMetadataService();

    Region<Object, Object> partitionedRegion = getRegion(PARTITIONED_REGION_NAME);

    clientMetadataService.satisfyRefreshMetadata_TEST_ONLY(false);
    partitionedRegion.put(0, "create0");
    boolean metadataRefreshed_get1 = clientMetadataService.isRefreshMetadataTestOnly();

    clientMetadataService.satisfyRefreshMetadata_TEST_ONLY(false);
    partitionedRegion.put(1, "create1");
    boolean metadataRefreshed_get2 = clientMetadataService.isRefreshMetadataTestOnly();

    clientMetadataService.satisfyRefreshMetadata_TEST_ONLY(false);
    partitionedRegion.put(2, "create2");
    boolean metadataRefreshed_get3 = clientMetadataService.isRefreshMetadataTestOnly();

    clientMetadataService.satisfyRefreshMetadata_TEST_ONLY(false);
    partitionedRegion.put(3, "create3");
    boolean metadataRefreshed_get4 = clientMetadataService.isRefreshMetadataTestOnly();

    await().untilAsserted(() -> {
      assertThat(metadataRefreshed_get1).isFalse();
      assertThat(metadataRefreshed_get2).isFalse();
      assertThat(metadataRefreshed_get3).isFalse();
      assertThat(metadataRefreshed_get4).isFalse();
    });

    clientMetadataService.satisfyRefreshMetadata_TEST_ONLY(false);

    partitionedRegion.put(0, "create0");
    partitionedRegion.put(1, "create1");
    partitionedRegion.put(2, "create2");
    partitionedRegion.put(3, "create3");

    await().untilAsserted(() -> {
      assertThat(clientMetadataService.isRefreshMetadataTestOnly()).isFalse();
    });
  }

  @Test
  public void testNoMetadataServiceCall_ForDestroyOp() {
    int port0 = vm0.invoke(() -> createServer(-1, 0, 4));
    int port1 = vm1.invoke(() -> createServer(-1, 0, 4));
    vm2.invoke(() -> createClient(250, false, true, true, port0));
    createClient(250, false, true, true, port1);

    vm2.invoke(() -> doPuts(getRegion(PARTITIONED_REGION_NAME)));

    ClientMetadataService clientMetadataService = CLIENT.get().getClientMetadataService();
    clientMetadataService.satisfyRefreshMetadata_TEST_ONLY(false);

    Region<Object, Object> partitionedRegion = getRegion(PARTITIONED_REGION_NAME);

    partitionedRegion.destroy(0);
    partitionedRegion.destroy(1);
    partitionedRegion.destroy(2);
    partitionedRegion.destroy(3);

    await().untilAsserted(() -> {
      assertThat(clientMetadataService.isRefreshMetadataTestOnly()).isFalse();
    });
  }

  @Test
  public void testServerLocationRemovalThroughPing() throws Exception {
    LATCH.set(new CountDownLatch(2));

    int redundantCopies = 3;
    int totalNumberOfBuckets = 4;

    int port0 = vm0.invoke(() -> createServer(-1, redundantCopies, totalNumberOfBuckets));
    int port1 = vm1.invoke(() -> createServer(-1, redundantCopies, totalNumberOfBuckets));
    int port2 = vm2.invoke(() -> createServer(-1, redundantCopies, totalNumberOfBuckets));
    int port3 = vm3.invoke(() -> createServer(-1, redundantCopies, totalNumberOfBuckets));
    createOldClient(100, true, false, true, port0, port1, port2, port3);

    ManagementService managementService = getExistingManagementService(CACHE.get());
    new MemberCrashedListener(LATCH.get()).registerMembershipListener(managementService);

    putIntoPartitionedRegions();
    doGets();

    ClientMetadataService clientMetadataService = CACHE.get().getClientMetadataService();
    Map<String, ClientPartitionAdvisor> clientPRMetadata =
        clientMetadataService.getClientPRMetadata_TEST_ONLY();

    Region<Object, Object> partitionedRegion = getRegion(PARTITIONED_REGION_NAME);
    Region<Object, Object> customerRegion = getRegion(CUSTOMER_REGION_NAME);
    Region<Object, Object> orderRegion = getRegion(ORDER_REGION_NAME);
    Region<Object, Object> shipmentRegion = getRegion(SHIPMENT_REGION_NAME);

    await().untilAsserted(() -> {
      assertThat(clientPRMetadata)
          .hasSize(4)
          .containsKey(partitionedRegion.getFullPath())
          .containsKey(customerRegion.getFullPath())
          .containsKey(orderRegion.getFullPath())
          .containsKey(shipmentRegion.getFullPath());
    });

    ClientPartitionAdvisor prMetadata = clientPRMetadata.get(partitionedRegion.getFullPath());
    assertThat(prMetadata.getBucketServerLocationsMap_TEST_ONLY()).hasSize(totalNumberOfBuckets);

    for (Entry entry : prMetadata.getBucketServerLocationsMap_TEST_ONLY().entrySet()) {
      assertThat((Iterable<?>) entry.getValue()).hasSize(totalNumberOfBuckets);
    }

    vm0.invoke(() -> stopServer());
    vm1.invoke(() -> stopServer());

    LATCH.get().await(getTimeout().getValueInMS(), MILLISECONDS);

    doGets();

    verifyDeadServer(clientPRMetadata, customerRegion, port0, port1);
    verifyDeadServer(clientPRMetadata, partitionedRegion, port0, port1);
  }

  @Test
  public void testMetadataFetchOnlyThroughFunctions() {
    // Workaround for 52004
    addIgnoredException(InternalFunctionInvocationTargetException.class);

    int redundantCopies = 3;
    int totalNumberOfBuckets = 4;

    int port0 = vm0.invoke(() -> createServer(-1, redundantCopies, totalNumberOfBuckets));
    int port1 = vm1.invoke(() -> createServer(-1, redundantCopies, totalNumberOfBuckets));
    int port2 = vm2.invoke(() -> createServer(-1, redundantCopies, totalNumberOfBuckets));
    int port3 = vm3.invoke(() -> createServer(-1, redundantCopies, totalNumberOfBuckets));
    createClient(100, true, false, true, port0, port1, port2, port3);

    Region<Object, Object> partitionedRegion = getRegion(PARTITIONED_REGION_NAME);

    executeFunctions(partitionedRegion);

    ClientMetadataService clientMetadataService = CLIENT.get().getClientMetadataService();
    Map<String, ClientPartitionAdvisor> clientPRMetadata =
        clientMetadataService.getClientPRMetadata_TEST_ONLY();

    await().untilAsserted(() -> {
      assertThat(clientPRMetadata)
          .hasSize(1)
          .containsKey(partitionedRegion.getFullPath());
    });

    ClientPartitionAdvisor prMetadata = clientPRMetadata.get(partitionedRegion.getFullPath());

    await().untilAsserted(() -> {
      clientMetadataService.getClientPRMetadata((InternalRegion) partitionedRegion);
      assertThat(prMetadata.getBucketServerLocationsMap_TEST_ONLY()).hasSize(totalNumberOfBuckets);
    });
  }

  @Test
  public void testMetadataFetchOnlyThroughputAll() {
    int redundantCopies = 3;
    int totalNumberOfBuckets = 4;

    int port0 = vm0.invoke(() -> createServer(-1, redundantCopies, totalNumberOfBuckets));
    int port1 = vm1.invoke(() -> createServer(-1, redundantCopies, totalNumberOfBuckets));
    int port2 = vm2.invoke(() -> createServer(-1, redundantCopies, totalNumberOfBuckets));
    int port3 = vm3.invoke(() -> createServer(-1, redundantCopies, totalNumberOfBuckets));
    createClient(100, true, false, true, port0, port1, port2, port3);

    Region<Object, Object> partitionedRegion = getRegion(PARTITIONED_REGION_NAME);

    doPutAlls(partitionedRegion);

    ClientMetadataService clientMetadataService = CLIENT.get().getClientMetadataService();
    Map<String, ClientPartitionAdvisor> clientPRMetadata =
        clientMetadataService.getClientPRMetadata_TEST_ONLY();
    ClientPartitionAdvisor prMetadata = clientPRMetadata.get(partitionedRegion.getFullPath());

    await().untilAsserted(() -> {
      assertThat(clientPRMetadata)
          .hasSize(1)
          .containsKey(partitionedRegion.getFullPath());
      assertThat(prMetadata.getBucketServerLocationsMap_TEST_ONLY())
          .hasSize(totalNumberOfBuckets);
    });
  }

  @Test
  public void testMetadataIsSameOnAllServersAndClients() {
    int locatorPort = DUnitEnv.get().getLocatorPort();
    String locators = "localhost[" + locatorPort + "]";

    int redundantCopies = 3;
    int totalNumberOfBuckets = 4;

    vm0.invoke(() -> createServer(locators, null, -1, redundantCopies, totalNumberOfBuckets));
    vm1.invoke(() -> createServer(locators, null, -1, redundantCopies, totalNumberOfBuckets));
    vm2.invoke(() -> createServer(locators, null, -1, redundantCopies, totalNumberOfBuckets));
    vm3.invoke(() -> createServer(locators, null, -1, redundantCopies, totalNumberOfBuckets));
    createClient(100, true, false, false, locatorPort);

    Region<Object, Object> partitionedRegion = getRegion(PARTITIONED_REGION_NAME);

    doManyPuts(partitionedRegion);

    for (VM vm : asList(vm0, vm1, vm2, vm3)) {
      vm.invoke(() -> waitForLocalBucketsCreation());
    }

    ClientMetadataService clientMetadataService = CLIENT.get().getClientMetadataService();
    clientMetadataService.getClientPRMetadata((InternalRegion) partitionedRegion);

    Map<String, ClientPartitionAdvisor> clientPRMetadata =
        clientMetadataService.getClientPRMetadata_TEST_ONLY();

    assertThat(clientPRMetadata)
        .hasSize(1)
        .containsKey(partitionedRegion.getFullPath());

    ClientPartitionAdvisor prMetadata = clientPRMetadata.get(partitionedRegion.getFullPath());
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

    vm0.invoke(() -> startServer());
    vm1.invoke(() -> startServer());

    doManyPuts(partitionedRegion);

    for (VM vm : asList(vm0, vm1, vm2, vm3)) {
      vm.invoke(() -> waitForLocalBucketsCreation());
    }

    await().atMost(2, SECONDS).untilAsserted(() -> {
      Map<String, ClientPartitionAdvisor> clientPRMetadata_await =
          clientMetadataService.getClientPRMetadata_TEST_ONLY();

      assertThat(clientPRMetadata_await)
          .hasSize(1)
          .containsKey(partitionedRegion.getFullPath());

      ClientPartitionAdvisor prMetadata_await =
          clientPRMetadata_await.get(partitionedRegion.getFullPath());
      Map<Integer, List<BucketServerLocation66>> clientBucketMap_await =
          prMetadata_await.getBucketServerLocationsMap_TEST_ONLY();

      assertThat(clientBucketMap_await).hasSize(totalNumberOfBuckets);

      for (Entry<Integer, List<BucketServerLocation66>> entry : clientBucketMap_await.entrySet()) {
        assertThat(entry.getValue()).hasSize(totalNumberOfBuckets);
      }
    });

    clientMetadataService.getClientPRMetadata((InternalRegion) partitionedRegion);

    clientPRMetadata = clientMetadataService.getClientPRMetadata_TEST_ONLY();

    assertThat(clientPRMetadata)
        .hasSize(1)
        .containsKey(partitionedRegion.getFullPath());

    prMetadata = clientPRMetadata.get(partitionedRegion.getFullPath());

    Map<Integer, List<BucketServerLocation66>> clientBucketMap2 =
        prMetadata.getBucketServerLocationsMap_TEST_ONLY();

    await().untilAsserted(() -> {
      assertThat(clientBucketMap2).hasSize(totalNumberOfBuckets);
    });

    for (Entry<Integer, List<BucketServerLocation66>> entry : clientBucketMap.entrySet()) {
      assertThat(entry.getValue()).hasSize(totalNumberOfBuckets);
    }

    for (VM vm : asList(vm0, vm1, vm2, vm3)) {
      vm.invoke(() -> verifyMetadata(clientBucketMap));
    }

    for (VM vm : asList(vm0, vm1)) {
      vm.invoke(() -> {
        SERVER.getAndSet(DUMMY_SERVER).stop();
      });
    }

    doManyPuts(partitionedRegion);

    vm2.invoke(() -> {
      PartitionedRegion pr = (PartitionedRegion) getRegion(PARTITIONED_REGION_NAME);
      pr.getRegionAdvisor().getAllClientBucketProfilesTest();
    });
    vm3.invoke(() -> {
      PartitionedRegion pr = (PartitionedRegion) getRegion(PARTITIONED_REGION_NAME);
      pr.getRegionAdvisor().getAllClientBucketProfilesTest();
    });

    vm2.invoke(() -> waitForLocalBucketsCreation());
    vm3.invoke(() -> waitForLocalBucketsCreation());

    clientMetadataService.getClientPRMetadata((InternalRegion) partitionedRegion);
    clientPRMetadata = clientMetadataService.getClientPRMetadata_TEST_ONLY();

    assertThat(clientPRMetadata)
        .hasSize(1)
        .containsKey(partitionedRegion.getFullPath());

    prMetadata = clientPRMetadata.get(partitionedRegion.getFullPath());
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

    int port0 = vm0.invoke(() -> createServer(-1, 2, totalNumberOfBuckets));
    int port1 = vm1.invoke(() -> createServer(-1, 2, totalNumberOfBuckets));
    createClient(100, true, false, true, port0, port1, port0, port1);

    Region<Object, Object> partitionedRegion = getRegion(PARTITIONED_REGION_NAME);

    doManyPuts(partitionedRegion);

    ClientMetadataService clientMetadataService = CLIENT.get().getClientMetadataService();
    clientMetadataService.getClientPRMetadata((InternalRegion) partitionedRegion);
    Map<String, ClientPartitionAdvisor> clientPRMetadata =
        clientMetadataService.getClientPRMetadata_TEST_ONLY();

    await().untilAsserted(() -> {
      assertThat(clientPRMetadata)
          .hasSize(1)
          .containsKey(partitionedRegion.getFullPath());
    });

    vm0.invoke(() -> {
      PartitionedRegion pr = (PartitionedRegion) getRegion(PARTITIONED_REGION_NAME);
      pr.getRegionAdvisor().getAllClientBucketProfilesTest();
    });
    vm1.invoke(() -> {
      PartitionedRegion pr = (PartitionedRegion) getRegion(PARTITIONED_REGION_NAME);
      pr.getRegionAdvisor().getAllClientBucketProfilesTest();
    });

    ClientPartitionAdvisor prMetadata = clientPRMetadata.get(partitionedRegion.getFullPath());
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

    doManyPuts(partitionedRegion);

    clientMetadataService.getClientPRMetadata((InternalRegion) partitionedRegion);

    assertThat(clientBucketMap).hasSize(totalNumberOfBuckets);

    for (Entry<Integer, List<BucketServerLocation66>> entry : clientBucketMap.entrySet()) {
      assertThat(entry.getValue()).hasSize(1);
    }

    assertThat(clientPRMetadata)
        .hasSize(1)
        .containsKey(partitionedRegion.getFullPath());
    assertThat(clientBucketMap)
        .hasSize(totalNumberOfBuckets);

    await().untilAsserted(() -> {
      for (Entry<Integer, List<BucketServerLocation66>> entry : clientBucketMap.entrySet()) {
        assertThat(entry.getValue()).hasSize(1);
      }
    });
  }

  @Test
  public void testClientMetadataForPersistentPrs() throws Exception {
    LATCH.set(new CountDownLatch(4));

    int locatorPort = DUnitEnv.get().getLocatorPort();

    vm0.invoke(() -> createServer("disk", -1, 3, 4));
    vm1.invoke(() -> createServer("disk", -1, 3, 4));
    vm2.invoke(() -> createServer("disk", -1, 3, 4));
    vm3.invoke(() -> createServer("disk", -1, 3, 4));

    vm3.invoke(() -> putIntoPartitionedRegions());

    for (VM vm : asList(vm0, vm1, vm2, vm3)) {
      vm.invoke(() -> waitForLocalBucketsCreation());
    }

    createOldClient(100, true, false, false, locatorPort);

    ManagementService managementService = getExistingManagementService(CACHE.get());
    new MemberCrashedListener(LATCH.get()).registerMembershipListener(managementService);
    ClientMetadataService clientMetadataService = CACHE.get().getClientMetadataService();

    Region<Object, Object> partitionedRegion = getRegion(PARTITIONED_REGION_NAME);

    await().until(() -> {
      clientMetadataService.getClientPRMetadata((InternalRegion) partitionedRegion);

      Map<ServerLocation, Set<Integer>> serverBucketMap =
          clientMetadataService.groupByServerToAllBuckets(partitionedRegion, true);

      return serverBucketMap != null;
    });

    for (VM vm : asList(vm0, vm1, vm2, vm3)) {
      vm.invoke(() -> {
        SERVER.getAndSet(DUMMY_SERVER).stop();
      });
    }

    LATCH.get().await(getTimeout().getValueInMS(), MILLISECONDS);

    AsyncInvocation<Integer> createServerOnVM3 =
        vm3.invokeAsync(() -> createServer("disk", -1, 3, 4));
    AsyncInvocation<Integer> createServerOnVM2 =
        vm2.invokeAsync(() -> createServer("disk", -1, 3, 4));
    AsyncInvocation<Integer> createServerOnVM1 =
        vm1.invokeAsync(() -> createServer("disk", -1, 3, 4));
    AsyncInvocation<Integer> createServerOnVM0 =
        vm0.invokeAsync(() -> createServer("disk", -1, 3, 4));

    createServerOnVM3.await();
    createServerOnVM2.await();
    createServerOnVM1.await();
    createServerOnVM0.await();

    await().untilAsserted(() -> {
      clientMetadataService.getClientPRMetadata((InternalRegion) partitionedRegion);
      Map<ServerLocation, Set<Integer>> serverBucketMap =
          clientMetadataService.groupByServerToAllBuckets(partitionedRegion, true);

      assertThat(serverBucketMap).hasSize(4);
    });
  }

  private int createServer(int localMaxMemory, int redundantCopies, int totalNumberOfBuckets)
      throws IOException {
    return createServer(null, null, localMaxMemory, redundantCopies, totalNumberOfBuckets);
  }

  private int createServer(String diskStoreName, int localMaxMemory, int redundantCopies,
      int totalNumberOfBuckets) throws IOException {
    return createServer(null, diskStoreName, localMaxMemory, redundantCopies, totalNumberOfBuckets);
  }

  private int createServer(String locators, String diskStoreName, int localMaxMemory,
      int redundantCopies, int totalNumberOfBuckets) throws IOException {
    return doCreateServer(locators, 0, diskStoreName, localMaxMemory, LOCAL_MAX_MEMORY_DEFAULT,
        redundantCopies, totalNumberOfBuckets);
  }

  private int createAccessorServer(int redundantCopies, int totalNumberOfBuckets)
      throws IOException {
    return doCreateServer(null, 0, null, 0, 0, redundantCopies, totalNumberOfBuckets);
  }

  private void createAccessorPeer(int redundantCopies, int totalNumberOfBuckets)
      throws IOException {
    ServerLauncher serverLauncher = new ServerLauncher.Builder()
        .setDeletePidFileOnStop(true)
        .setDisableDefaultServer(true)
        .setWorkingDirectory(getWorkingDirectory())
        .set(getDistributedSystemProperties())
        .build();
    serverLauncher.start();

    SERVER.set(serverLauncher);

    createRegions(null, -1, -1, redundantCopies, totalNumberOfBuckets);
  }

  private int doCreateServer(String locators, int serverPortInput, String diskStoreName,
      int localMaxMemory, int localMaxMemoryOthers, int redundantCopies, int totalNumberOfBuckets)
      throws IOException {
    ServerLauncher.Builder serverBuilder = new ServerLauncher.Builder()
        .setDeletePidFileOnStop(true)
        .setDisableDefaultServer(true)
        .setWorkingDirectory(getWorkingDirectory())
        .set(getDistributedSystemProperties());

    if (locators != null) {
      serverBuilder.set(LOCATORS, locators);
    }

    ServerLauncher serverLauncher = serverBuilder.build();
    serverLauncher.start();

    SERVER.set(serverLauncher);

    CacheServer cacheServer = serverLauncher.getCache().addCacheServer();
    cacheServer.setHostnameForClients("localhost");
    cacheServer.setPort(serverPortInput);
    cacheServer.start();

    int serverPort = cacheServer.getPort();

    createRegions(diskStoreName, localMaxMemory, localMaxMemoryOthers, redundantCopies,
        totalNumberOfBuckets);

    return serverPort;
  }

  private void createRegions(String diskStoreName, int localMaxMemory, int localMaxMemoryOthers,
      int redundantCopies, int totalNumberOfBuckets) throws IOException {
    createPartitionedRegion(PARTITIONED_REGION_NAME, null, diskStoreName, localMaxMemory, null,
        redundantCopies, totalNumberOfBuckets);

    createPartitionedRegion(CUSTOMER_REGION_NAME, null, diskStoreName, localMaxMemoryOthers,
        new CustomerIdPartitionResolver<>(), redundantCopies, totalNumberOfBuckets);

    createPartitionedRegion(ORDER_REGION_NAME, CUSTOMER_REGION_NAME, diskStoreName,
        localMaxMemoryOthers,
        new CustomerIdPartitionResolver<>(), redundantCopies, totalNumberOfBuckets);

    createPartitionedRegion(SHIPMENT_REGION_NAME, ORDER_REGION_NAME, diskStoreName,
        localMaxMemoryOthers,
        new CustomerIdPartitionResolver<>(), redundantCopies, totalNumberOfBuckets);

    SERVER.get().getCache().createRegionFactory().create(REPLICATE_REGION_NAME);
  }

  private void createClient(int pingInterval, boolean prSingleHopEnabled,
      boolean subscriptionEnabled, boolean useServerPool, int... ports) {
    CLIENT.set((InternalClientCache) new ClientCacheFactory().set(LOCATORS, "").create());
    String poolName =
        createPool(pingInterval, prSingleHopEnabled, subscriptionEnabled, useServerPool, ports);
    createRegionsInClientCache(poolName);
  }

  private void createOldClient(int pingInterval, boolean prSingleHopEnabled,
      boolean subscriptionEnabled, boolean useServerPool, int... ports) {
    CACHE.set((InternalCache) new CacheFactory().set(LOCATORS, "").create());
    String poolName =
        createPool(pingInterval, prSingleHopEnabled, subscriptionEnabled, useServerPool, ports);
    createRegionsInOldClient(poolName);
  }

  private String createPool(long pingInterval, boolean prSingleHopEnabled,
      boolean subscriptionEnabled, boolean useServerPool, int... ports) {
    System.setProperty(GEMFIRE_PREFIX + "bridge.disableShufflingOfEndpoints", "true");
    String poolName = PARTITIONED_REGION_NAME;
    try {
      PoolFactory poolFactory = PoolManager.createFactory()
          .setMaxConnections(10)
          .setMinConnections(6)
          .setPingInterval(pingInterval)
          .setPRSingleHopEnabled(prSingleHopEnabled)
          .setReadTimeout(2000)
          .setRetryAttempts(3)
          .setSocketBufferSize(1000)
          .setSubscriptionEnabled(subscriptionEnabled)
          .setSubscriptionRedundancy(-1);

      if (useServerPool) {
        for (int port : ports) {
          poolFactory.addServer("localhost", port);
        }
      } else {
        for (int port : ports) {
          poolFactory.addLocator("localhost", port);
        }
      }

      poolFactory.create(poolName);
    } finally {
      System.clearProperty(GEMFIRE_PREFIX + "bridge.disableShufflingOfEndpoints");
    }

    return poolName;
  }

  private <K, V> Region<K, V> createPartitionedRegion(String regionName, String colocatedRegionName,
      String diskStoreName, int localMaxMemory, PartitionResolver<K, V> partitionResolver,
      int redundantCopies, int totalNumberOfBuckets) throws IOException {
    InternalCache cache = getCache();

    PartitionAttributesFactory<K, V> paf = new PartitionAttributesFactory<K, V>()
        .setRedundantCopies(redundantCopies)
        .setTotalNumBuckets(totalNumberOfBuckets);

    if (colocatedRegionName != null) {
      paf.setColocatedWith(colocatedRegionName);
    }
    if (localMaxMemory > -1) {
      paf.setLocalMaxMemory(localMaxMemory);
    }
    if (partitionResolver != null) {
      paf.setPartitionResolver(partitionResolver);
    }

    RegionFactory<K, V> regionFactory;
    if (diskStoreName != null) {
      // create DiskStore
      if (cache.findDiskStore(diskStoreName) == null) {
        cache.createDiskStoreFactory()
            .setDiskDirs(getDiskDirs())
            .create(diskStoreName);
      }

      regionFactory = cache.createRegionFactory(PARTITION_PERSISTENT);
      regionFactory.setDiskStoreName(diskStoreName);
    } else {
      regionFactory = cache.createRegionFactory(PARTITION);
    }

    return regionFactory
        .setConcurrencyChecksEnabled(true)
        .setPartitionAttributes(paf.create())
        .create(regionName);
  }

  private void createRegionsInClientCache(String poolName) {
    ClientRegionFactory<Object, Object> proxyRegionFactory =
        CLIENT.get().createClientRegionFactory(ClientRegionShortcut.PROXY);
    proxyRegionFactory.setPoolName(poolName);

    proxyRegionFactory.create(PARTITIONED_REGION_NAME);

    ClientRegionFactory<Object, Object> localRegionFactory =
        CLIENT.get().createClientRegionFactory(ClientRegionShortcut.LOCAL);
    localRegionFactory.setConcurrencyChecksEnabled(true);
    localRegionFactory.setPoolName(poolName);

    localRegionFactory.create(CUSTOMER_REGION_NAME);
    localRegionFactory.create(ORDER_REGION_NAME);
    localRegionFactory.create(SHIPMENT_REGION_NAME);
    localRegionFactory.create(REPLICATE_REGION_NAME);
  }

  private void createRegionsInOldClient(String poolName) {
    CACHE.get().createRegionFactory()
        .setDataPolicy(DataPolicy.EMPTY)
        .setPoolName(poolName)
        .create(PARTITIONED_REGION_NAME);

    RegionFactory<Object, Object> localRegionFactory = CACHE.get().createRegionFactory(LOCAL)
        .setConcurrencyChecksEnabled(true)
        .setPoolName(poolName);

    localRegionFactory.create(CUSTOMER_REGION_NAME);
    localRegionFactory.create(ORDER_REGION_NAME);
    localRegionFactory.create(SHIPMENT_REGION_NAME);
    localRegionFactory.create(REPLICATE_REGION_NAME);
  }

  private int startServer() throws IOException {
    CacheServer cacheServer = SERVER.get().getCache().addCacheServer();
    cacheServer.setHostnameForClients("localhost");
    cacheServer.setPort(0);
    cacheServer.start();

    return cacheServer.getPort();
  }

  private int startLocator() throws IOException {
    LocatorLauncher locatorLauncher = new LocatorLauncher.Builder()
        .setDeletePidFileOnStop(true)
        .setPort(0)
        .setWorkingDirectory(getWorkingDirectory())
        .set(ENABLE_CLUSTER_CONFIGURATION, "false")
        .build();
    locatorLauncher.start();

    LOCATOR.set(locatorLauncher);

    return locatorLauncher.getLocator().getPort();
  }

  private void stopServer() {
    for (CacheServer cacheServer : SERVER.get().getCache().getCacheServers()) {
      cacheServer.stop();
    }
  }

  private void doPuts(Region<Object, Object> region) {
    region.put(0, "create0");
    region.put(1, "create1");
    region.put(2, "create2");
    region.put(3, "create3");
  }

  private void doManyPuts(Region<Object, Object> region) {
    region.put(0, "create0");
    region.put(1, "create1");
    region.put(2, "create2");
    region.put(3, "create3");
    for (int i = 0; i < 40; i++) {
      region.put(i, "create" + i);
    }
  }

  private void putIntoPartitionedRegions() {
    Region<Object, Object> partitionedRegion = getRegion(PARTITIONED_REGION_NAME);
    Region<Object, Object> customerRegion = getRegion(CUSTOMER_REGION_NAME);
    Region<Object, Object> orderRegion = getRegion(ORDER_REGION_NAME);
    Region<Object, Object> shipmentRegion = getRegion(SHIPMENT_REGION_NAME);
    Region<Object, Object> replicateRegion = getRegion(REPLICATE_REGION_NAME);

    for (int i = 0; i <= 3; i++) {
      CustomerId customerId = new CustomerId(i);
      Customer customer = new Customer("name" + i, "Address" + i);
      customerRegion.put(customerId, customer);

      for (int j = 1; j <= 10; j++) {
        int oid = i * 10 + j;
        OrderId orderId = new OrderId(oid, customerId);
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

    partitionedRegion.put(0, "create0");
    partitionedRegion.put(1, "create1");
    partitionedRegion.put(2, "create2");
    partitionedRegion.put(3, "create3");

    partitionedRegion.put(0, "update0");
    partitionedRegion.put(1, "update1");
    partitionedRegion.put(2, "update2");
    partitionedRegion.put(3, "update3");

    partitionedRegion.put(0, "update00");
    partitionedRegion.put(1, "update11");
    partitionedRegion.put(2, "update22");
    partitionedRegion.put(3, "update33");

    Map<Object, Object> map = new HashMap<>();
    map.put(1, 1);
    replicateRegion.putAll(map);
  }

  private void doPutAlls(Region<Object, Object> region) {
    Map<Object, Object> map = new HashMap<>();
    map.put(0, 0);
    map.put(1, 1);
    map.put(2, 2);
    map.put(3, 3);

    region.putAll(map, "putAllCallback");
    region.putAll(map);
    region.putAll(map);
    region.putAll(map);
  }

  private void doGets() {
    Region<Object, Object> partitionedRegion = getRegion(PARTITIONED_REGION_NAME);
    Region<Object, Object> customerRegion = getRegion(CUSTOMER_REGION_NAME);
    Region<Object, Object> orderRegion = getRegion(ORDER_REGION_NAME);
    Region<Object, Object> shipmentRegion = getRegion(SHIPMENT_REGION_NAME);

    for (int i = 0; i <= 3; i++) {
      CustomerId customerId = new CustomerId(i);
      Customer customer = new Customer("name" + i, "Address" + i);
      customerRegion.get(customerId, customer);

      for (int j = 1; j <= 10; j++) {
        int oid = i * 10 + j;
        OrderId orderId = new OrderId(oid, customerId);
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

    partitionedRegion.get(0, "create0");
    partitionedRegion.get(1, "create1");
    partitionedRegion.get(2, "create2");
    partitionedRegion.get(3, "create3");

    partitionedRegion.get(0, "update0");
    partitionedRegion.get(1, "update1");
    partitionedRegion.get(2, "update2");
    partitionedRegion.get(3, "update3");

    partitionedRegion.get(0, "update00");
    partitionedRegion.get(1, "update11");
    partitionedRegion.get(2, "update22");
    partitionedRegion.get(3, "update33");
  }

  private void executeFunctions(Region<Object, Object> region) {
    cast(FunctionService.onRegion(region))
        .withFilter(filter(0))
        .execute(new PutFunction())
        .getResult();

    cast(FunctionService.onRegion(region))
        .withFilter(filter(0, 1))
        .execute(new PutFunction())
        .getResult();

    cast(FunctionService.onRegion(region))
        .withFilter(filter(0, 1, 2, 3))
        .execute(new PutFunction())
        .getResult();

    cast(FunctionService.onRegion(region))
        .execute(new PutFunction())
        .getResult();
  }

  private Set<Object> filter(Object... values) {
    return Arrays.stream(values).collect(Collectors.toSet());
  }

  private void clearMetadata() {
    ClientMetadataService clientMetadataService =
        getInternalCache(SERVER.get()).getClientMetadataService();
    clientMetadataService.getClientPartitionAttributesMap().clear();
    clientMetadataService.getClientPRMetadata_TEST_ONLY().clear();
  }

  private String getWorkingDirectory() throws IOException {
    int vmId = getVMId();
    File directory = new File(temporaryFolder.getRoot(), "VM-" + vmId);
    if (!directory.exists()) {
      temporaryFolder.newFolder("VM-" + vmId);
    }
    return directory.getAbsolutePath();
  }

  private File getDiskDir() throws IOException {
    File file = new File(temporaryFolder.getRoot(), diskStoreName + getVMId());
    if (!file.exists()) {
      temporaryFolder.newFolder(diskStoreName + getVMId());
    }
    return file.getAbsoluteFile();
  }

  private File[] getDiskDirs() throws IOException {
    return new File[] {getDiskDir()};
  }

  private InternalCache getInternalCache(ServerLauncher serverLauncher) {
    return cast(serverLauncher.getCache());
  }

  private void waitForLocalBucketsCreation() {
    PartitionedRegion pr = (PartitionedRegion) getRegion(PARTITIONED_REGION_NAME);

    await().untilAsserted(() -> assertThat(pr.getDataStore().getAllLocalBuckets()).hasSize(4));
  }

  private void verifyDeadServer(Map<String, ClientPartitionAdvisor> regionMetaData, Region region,
      int port0, int port1) {
    ClientPartitionAdvisor prMetaData = regionMetaData.get(region.getFullPath());
    Set<Entry<Integer, List<BucketServerLocation66>>> bucketLocationsMap =
        prMetaData.getBucketServerLocationsMap_TEST_ONLY().entrySet();

    for (Entry<Integer, List<BucketServerLocation66>> entry : bucketLocationsMap) {
      for (BucketServerLocation66 bucketLocation : entry.getValue()) {
        assertThat(bucketLocation.getPort())
            .isNotEqualTo(port0)
            .isNotEqualTo(port1);
      }
    }
  }

  private void verifyMetadata(Map<Integer, List<BucketServerLocation66>> clientBucketMap) {
    PartitionedRegion pr = (PartitionedRegion) getRegion(PARTITIONED_REGION_NAME);
    Map<Integer, Set<ServerBucketProfile>> serverBucketMap =
        pr.getRegionAdvisor().getAllClientBucketProfilesTest();

    assertThat(serverBucketMap)
        .hasSize(clientBucketMap.size());
    assertThat(serverBucketMap.keySet())
        .containsAll(clientBucketMap.keySet());

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
        for (BucketServerLocation66 bucketLocation : bucketProfile.getBucketServerLocations()) {

          assertThat(bucketLocations).contains(bucketLocation);

          // should be only one primary
          if (bucketProfile.isPrimary) {
            countOfPrimaries++;

            assertThat(bucketLocation).isEqualTo(primaryBucketLocation);
          }
        }
      }

      assertThat(countOfPrimaries).isEqualTo(1);
    }
  }

  private InternalCache getCache() {
    if (CACHE.get() != DUMMY_CACHE) {
      return CACHE.get();
    }
    if (SERVER.get() != DUMMY_SERVER) {
      return (InternalCache) SERVER.get().getCache();
    }
    if (CLIENT.get() != DUMMY_CACHE) {
      return (InternalCache) CLIENT.get();
    }
    return null;
  }

  private Region<Object, Object> getRegion(String name) {
    InternalCache cache = getCache();
    if (cache != null) {
      return cache.getRegion(name);
    }
    throw new IllegalStateException("Cache or region not found");
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

    private final String id = getClass().getSimpleName();

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

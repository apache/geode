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

import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import com.jayway.awaitility.Awaitility;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.ClientMetadataService;
import org.apache.geode.cache.client.internal.ClientPartitionAdvisor;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.cache.execute.data.CustId;
import org.apache.geode.internal.cache.execute.data.OrderId;
import org.apache.geode.internal.cache.execute.data.ShipmentId;
import org.apache.geode.internal.cache.tier.sockets.CacheServerTestUtil;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.DistributedTestUtils;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

@Category(DistributedTest.class)
public class SingleHopStatsDUnitTest extends JUnit4CacheTestCase {

  private final String Region_Name = "42010";
  private final String ORDER_REGION_NAME = "ORDER";
  private final String SHIPMENT_REGION_NAME = "SHIPMENT";
  private final String CUSTOMER_REGION_NAME = "CUSTOMER";
  private VM member0 = null;
  private VM member1 = null;
  private VM member2 = null;
  private VM member3 = null;

  private static long metaDataRefreshCount;
  private static long nonSingleHopsCount;
  private static long metaDataRefreshCount_Customer;
  private static long nonSingleHopsCount_Customer;
  private static long metaDataRefreshCount_Order;
  private static long nonSingleHopsCount_Order;
  private static long metaDataRefreshCount_Shipment;
  private static long nonSingleHopsCount_Shipment;

  @Override
  public final void postSetUp() throws Exception {
    Host host = Host.getHost(0);
    member0 = host.getVM(0);
    member1 = host.getVM(1);
    member2 = host.getVM(2);
    member3 = host.getVM(3);
  }

  @Override
  public final void preTearDownCacheTestCase() throws Exception {
    // close the clients first
    member0.invoke(() -> closeCacheAndDisconnect());
    member1.invoke(() -> closeCacheAndDisconnect());
    member2.invoke(() -> closeCacheAndDisconnect());
    member3.invoke(() -> closeCacheAndDisconnect());
    closeCacheAndDisconnect();
  }

  @Override
  public final void postTearDownCacheTestCase() throws Exception {
    try {
      member0 = null;
      member1 = null;
      member2 = null;
      member3 = null;
    } finally {
      DistributedTestUtils.unregisterAllDataSerializersFromAllVms();
    }
  }

  private void closeCacheAndDisconnect() {
    try {
      Cache cache = CacheFactory.getAnyInstance();
      if (cache != null && !cache.isClosed()) {
        cache.close();
        cache.getDistributedSystem().disconnect();
      }
    } catch (CacheClosedException e) {
    }
  }

  @Test
  public void testClientStatsPR() {
    Integer port0 = (Integer) member0.invoke(() -> createServerForStats(0, 113, "No_Colocation"));
    Integer port1 = (Integer) member1.invoke(() -> createServerForStats(0, 113, "No_Colocation"));
    Integer port2 = (Integer) member2.invoke(() -> createServerForStats(0, 113, "No_Colocation"));

    member3.invoke("createClient", () -> createClient(port0, port1, port2, "No_Colocation"));
    System.out.println("createClient");
    createClient(port0, port1, port2, "No_Colocation");

    member3.invoke("createPR", () -> createPR("FirstClient", "No_Colocation"));
    System.out.println("createPR");
    createPR("SecondClient", "No_Colocation");

    member3.invoke("getPR", () -> getPR("FirstClient", "No_Colocation"));
    System.out.println("getPR");
    getPR("SecondClient", "No_Colocation");

    member3.invoke("updatePR", () -> updatePR("FirstClient", "No_Colocation"));
  }

  @Test
  public void testClientStatsColocationPR() {
    Integer port0 = (Integer) member0.invoke(() -> createServerForStats(0, 4, "Colocation"));
    Integer port1 = (Integer) member1.invoke(() -> createServerForStats(0, 4, "Colocation"));
    Integer port2 = (Integer) member2.invoke(() -> createServerForStats(0, 4, "Colocation"));
    member3.invoke(() -> createClient(port0, port1, port2, "Colocation"));
    createClient(port0, port1, port2, "Colocation");

    member3.invoke(() -> createPR("FirstClient", "Colocation"));

    member3.invoke(() -> getPR("FirstClient", "Colocation"));
  }

  private void createClient(int port0, int port1, int port2, String colocation) {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    DistributedSystem distributedSystem = getSystem(props);
    Cache cache = CacheFactory.create(distributedSystem);
    assertNotNull(cache);
    CacheServerTestUtil.disableShufflingOfEndpoints();
    Pool p;
    try {
      p = PoolManager.createFactory().addServer("localhost", port0).addServer("localhost", port1)
          .addServer("localhost", port2).setRetryAttempts(5).setMinConnections(1)
          .setMaxConnections(-1).setSubscriptionEnabled(false).create(Region_Name);
    } finally {
      CacheServerTestUtil.enableShufflingOfEndpoints();
    }
    createRegionInClientCache(p.getName(), colocation, cache);
  }

  private int createServerForStats(int redundantCopies, int totalNoofBuckets, String colocation) {
    Cache cache = getCache();
    CacheServer server = cache.addCacheServer();
    server.setPort(0);
    server.setHostnameForClients("localhost");
    try {
      server.start();
    } catch (IOException e) {
      Assert.fail("Failed to start server ", e);
    }

    Region region = null;

    if (colocation.equals("No_Colocation")) {
      if (totalNoofBuckets == 0) { // DR
        AttributesFactory attr = new AttributesFactory();
        attr.setScope(Scope.DISTRIBUTED_ACK);
        attr.setDataPolicy(DataPolicy.REPLICATE);
        region = cache.createRegion(Region_Name, attr.create());
        assertNotNull(region);
        LogWriterUtils.getLogWriter().info(
            "Distributed Region " + Region_Name + " created Successfully :" + region.toString());
      } else {
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(redundantCopies).setTotalNumBuckets(totalNoofBuckets);
        AttributesFactory attr = new AttributesFactory();
        attr.setPartitionAttributes(paf.create());
        region = cache.createRegion(Region_Name, attr.create());
        assertNotNull(region);
        LogWriterUtils.getLogWriter().info(
            "Partitioned Region " + Region_Name + " created Successfully :" + region.toString());
      }
    } else {
      PartitionAttributesFactory paf = new PartitionAttributesFactory();
      paf.setRedundantCopies(redundantCopies).setTotalNumBuckets(totalNoofBuckets)
          .setPartitionResolver(
              new CustomerIDPartitionResolver("CustomerIDPartitio" + "nResolver"));
      AttributesFactory attr = new AttributesFactory();
      attr.setPartitionAttributes(paf.create());
      Region customerRegion = cache.createRegion(CUSTOMER_REGION_NAME, attr.create());
      assertNotNull(customerRegion);
      LogWriterUtils.getLogWriter()
          .info("Partitioned Region CUSTOMER created Successfully :" + customerRegion.toString());

      paf = new PartitionAttributesFactory();
      paf.setRedundantCopies(redundantCopies).setTotalNumBuckets(totalNoofBuckets)
          .setColocatedWith(CUSTOMER_REGION_NAME)
          .setPartitionResolver(new CustomerIDPartitionResolver("CustomerIDPartitionResolver"));
      attr = new AttributesFactory();
      attr.setPartitionAttributes(paf.create());
      Region orderRegion = cache.createRegion(ORDER_REGION_NAME, attr.create());
      assertNotNull(orderRegion);
      LogWriterUtils.getLogWriter()
          .info("Partitioned Region ORDER created Successfully :" + orderRegion.toString());

      paf = new PartitionAttributesFactory();
      paf.setRedundantCopies(redundantCopies).setTotalNumBuckets(totalNoofBuckets)
          .setColocatedWith(ORDER_REGION_NAME)
          .setPartitionResolver(new CustomerIDPartitionResolver("CustomerIDPartitionResolver"));
      attr = new AttributesFactory();
      attr.setPartitionAttributes(paf.create());
      Region shipmentRegion = cache.createRegion(SHIPMENT_REGION_NAME, attr.create());
      assertNotNull(shipmentRegion);
      LogWriterUtils.getLogWriter()
          .info("Partitioned Region SHIPMENT created Successfully :" + shipmentRegion.toString());
    }
    return server.getPort();
  }

  private void createRegionInClientCache(String poolName, String colocation, Cache cache) {
    Region region = null;
    if (colocation.equals("No_Colocation")) {
      AttributesFactory factory = new AttributesFactory();
      factory.setPoolName(poolName);
      factory.setDataPolicy(DataPolicy.EMPTY);
      RegionAttributes attrs = factory.create();
      region = cache.createRegion(Region_Name, attrs);
      assertNotNull(region);
      LogWriterUtils.getLogWriter()
          .info("Region " + Region_Name + " created Successfully :" + region.toString());
    } else {
      AttributesFactory factory = new AttributesFactory();
      factory.setPoolName(poolName);
      RegionAttributes attrs = factory.create();
      Region customerRegion = cache.createRegion(CUSTOMER_REGION_NAME, attrs);
      assertNotNull(customerRegion);
      LogWriterUtils.getLogWriter()
          .info("Partitioned Region CUSTOMER created Successfully :" + customerRegion.toString());

      factory = new AttributesFactory();
      factory.setPoolName(poolName);
      attrs = factory.create();
      Region orderRegion = cache.createRegion(ORDER_REGION_NAME, attrs);
      assertNotNull(orderRegion);
      LogWriterUtils.getLogWriter()
          .info("Partitioned Region ORDER created Successfully :" + orderRegion.toString());

      factory = new AttributesFactory();
      factory.setPoolName(poolName);
      attrs = factory.create();
      Region shipmentRegion = cache.createRegion("SHIPMENT", attrs);
      assertNotNull(shipmentRegion);
      LogWriterUtils.getLogWriter()
          .info("Partitioned Region SHIPMENT created Successfully :" + shipmentRegion.toString());
    }
  }

  private void createPR(String fromClient, String colocation) {
    GemFireCacheImpl cache = (GemFireCacheImpl) CacheFactory.getAnyInstance();
    Region region = cache.getRegion(Region_Name);


    if (colocation.equals("No_Colocation")) {
      if (fromClient.equals("FirstClient")) {

        System.out.println("first pass...");
        for (int i = 0; i < 113; i++) {
          region.create(new Integer(i), "create" + i);
        }
        ClientMetadataService cms = ((GemFireCacheImpl) cache).getClientMetadataService();
        final Map<String, ClientPartitionAdvisor> regionMetaData =
            cms.getClientPRMetadata_TEST_ONLY();
        assertEquals(0, regionMetaData.size());

        System.out.println("second pass...");
        for (int i = 113; i < 226; i++) {
          region.create(new Integer(i), "create" + i);
        }
        cms = ((GemFireCacheImpl) cache).getClientMetadataService();
        // since PR metadata is fetched in a background executor thread
        // we need to wait for it to arrive for a bit
        Awaitility.await().timeout(120, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
            .pollInterval(500, TimeUnit.MILLISECONDS).until(() -> {
              return regionMetaData.size() == 1;
            });

        assertTrue(regionMetaData.containsKey(region.getFullPath()));
        regionMetaData.get(region.getFullPath());
        metaDataRefreshCount = ((LocalRegion) region).getCachePerfStats().getMetaDataRefreshCount();
        nonSingleHopsCount = ((LocalRegion) region).getCachePerfStats().getNonSingleHopsCount();
        assertTrue(metaDataRefreshCount != 0); // hops are not predictable
        assertTrue(nonSingleHopsCount != 0);

        System.out.println("metadata refresh count after second pass is " + metaDataRefreshCount);
      } else {
        System.out.println("creating keys in second client");
        for (int i = 0; i < 226; i++) {
          region.create(new Integer(i), "create" + i);
        }
        ClientMetadataService cms = ((GemFireCacheImpl) cache).getClientMetadataService();
        Map<String, ClientPartitionAdvisor> regionMetaData = cms.getClientPRMetadata_TEST_ONLY();
        assertEquals(1, regionMetaData.size());
        assertTrue(regionMetaData.containsKey(region.getFullPath()));

        regionMetaData.get(region.getFullPath());
        metaDataRefreshCount = ((LocalRegion) region).getCachePerfStats().getMetaDataRefreshCount();
        nonSingleHopsCount = ((LocalRegion) region).getCachePerfStats().getNonSingleHopsCount();
        assertTrue(metaDataRefreshCount != 0); // hops are not predictable
        assertTrue(nonSingleHopsCount != 0);
        System.out.println("metadata refresh count in second client is " + metaDataRefreshCount);
      }
    } else {
      createdColocatedPRData(cache);
    }
  }

  private void createdColocatedPRData(GemFireCacheImpl cache) {
    Region customerRegion = cache.getRegion(CUSTOMER_REGION_NAME);
    Region orderRegion = cache.getRegion(ORDER_REGION_NAME);
    Region shipmentRegion = cache.getRegion(SHIPMENT_REGION_NAME);
    for (int i = 0; i <= 20; i++) {
      CustId custid = new CustId(i);
      Customer customer = new Customer("name" + i, "Address" + i);

      customerRegion.put(custid, customer);
      for (int j = 1; j <= 10; j++) {
        int oid = (i * 10) + j;
        OrderId orderId = new OrderId(oid, custid);
        Order order = new Order(ORDER_REGION_NAME + oid);
        orderRegion.put(orderId, order);
        for (int k = 1; k <= 10; k++) {
          int sid = (oid * 10) + k;
          ShipmentId shipmentId = new ShipmentId(sid, orderId);
          Shipment shipment = new Shipment("Shipment" + sid);
          shipmentRegion.put(shipmentId, shipment);
        }
      }
    }
    ClientMetadataService cms = cache.getClientMetadataService();
    Map<String, ClientPartitionAdvisor> regionMetaData = cms.getClientPRMetadata_TEST_ONLY();
    assertEquals(3, regionMetaData.size());
    assertTrue(regionMetaData.containsKey(customerRegion.getFullPath()));
    regionMetaData.get(customerRegion.getFullPath());
    metaDataRefreshCount_Customer =
        ((LocalRegion) customerRegion).getCachePerfStats().getMetaDataRefreshCount();
    nonSingleHopsCount_Customer =
        ((LocalRegion) customerRegion).getCachePerfStats().getNonSingleHopsCount();
    assertTrue(metaDataRefreshCount_Customer != 0); // hops are not predictable
    assertTrue(nonSingleHopsCount_Customer != 0);

    regionMetaData.get(orderRegion.getFullPath());
    metaDataRefreshCount_Order =
        ((LocalRegion) orderRegion).getCachePerfStats().getMetaDataRefreshCount();
    nonSingleHopsCount_Order =
        ((LocalRegion) orderRegion).getCachePerfStats().getNonSingleHopsCount();
    assertTrue(metaDataRefreshCount_Order == 0);
    assertTrue(nonSingleHopsCount_Order != 0);

    regionMetaData.get(shipmentRegion.getFullPath());
    metaDataRefreshCount_Shipment =
        ((LocalRegion) shipmentRegion).getCachePerfStats().getMetaDataRefreshCount();
    nonSingleHopsCount_Shipment =
        ((LocalRegion) shipmentRegion).getCachePerfStats().getNonSingleHopsCount();
    assertTrue(metaDataRefreshCount_Shipment == 0);
    assertTrue(nonSingleHopsCount_Shipment != 0);
  }

  private void getPR(String FromClient, String colocation) {
    Cache cache = CacheFactory.getAnyInstance();
    Region region = cache.getRegion(Region_Name);
    Region customerRegion = cache.getRegion(CUSTOMER_REGION_NAME);
    Region orderRegion = cache.getRegion(ORDER_REGION_NAME);
    Region shipmentRegion = cache.getRegion("SHIPMENT");
    if (colocation.equals("No_Colocation")) {
      if (FromClient.equals("FirstClient")) {
        for (int i = 0; i < 226; i++) {
          region.get(new Integer(i));
        }
        ClientMetadataService cms = ((GemFireCacheImpl) cache).getClientMetadataService();
        Map<String, ClientPartitionAdvisor> regionMetaData = cms.getClientPRMetadata_TEST_ONLY();
        assertEquals(1, regionMetaData.size());
        regionMetaData.get(region.getFullPath());
        assertEquals(metaDataRefreshCount,
            ((LocalRegion) region).getCachePerfStats().getMetaDataRefreshCount());
        assertEquals(nonSingleHopsCount,
            ((LocalRegion) region).getCachePerfStats().getNonSingleHopsCount());
      } else {
        for (int i = 0; i < 226; i++) {
          region.get(new Integer(i));
        }
        ClientMetadataService cms = ((GemFireCacheImpl) cache).getClientMetadataService();
        Map<String, ClientPartitionAdvisor> regionMetaData = cms.getClientPRMetadata_TEST_ONLY();
        assertEquals(1, regionMetaData.size());
        regionMetaData.get(region.getFullPath());
        assertEquals(metaDataRefreshCount,
            ((LocalRegion) region).getCachePerfStats().getMetaDataRefreshCount());
        assertEquals(nonSingleHopsCount,
            ((LocalRegion) region).getCachePerfStats().getNonSingleHopsCount());
      }
    } else {
      for (int i = 0; i <= 20; i++) {
        CustId custid = new CustId(i);
        customerRegion.get(custid);
        for (int j = 1; j <= 10; j++) {
          int oid = (i * 10) + j;
          OrderId orderId = new OrderId(oid, custid);
          orderRegion.get(orderId);
          for (int k = 1; k <= 10; k++) {
            int sid = (oid * 10) + k;
            ShipmentId shipmentId = new ShipmentId(sid, orderId);
            shipmentRegion.get(shipmentId);
          }
        }
      }
      ClientMetadataService cms = ((GemFireCacheImpl) cache).getClientMetadataService();
      Map<String, ClientPartitionAdvisor> regionMetaData = cms.getClientPRMetadata_TEST_ONLY();
      assertEquals(3, regionMetaData.size());
      assertTrue(regionMetaData.containsKey(customerRegion.getFullPath()));
      regionMetaData.get(customerRegion.getFullPath());
      assertEquals(metaDataRefreshCount_Customer,
          ((LocalRegion) customerRegion).getCachePerfStats().getMetaDataRefreshCount());
      assertEquals(nonSingleHopsCount_Customer,
          ((LocalRegion) customerRegion).getCachePerfStats().getNonSingleHopsCount());

      regionMetaData.get(orderRegion.getFullPath());
      assertEquals(metaDataRefreshCount_Order,
          ((LocalRegion) orderRegion).getCachePerfStats().getMetaDataRefreshCount());
      assertEquals(nonSingleHopsCount_Order,
          ((LocalRegion) orderRegion).getCachePerfStats().getNonSingleHopsCount());

      regionMetaData.get(shipmentRegion.getFullPath());
      assertEquals(metaDataRefreshCount_Shipment,
          ((LocalRegion) shipmentRegion).getCachePerfStats().getMetaDataRefreshCount());
      assertEquals(nonSingleHopsCount_Shipment,
          ((LocalRegion) shipmentRegion).getCachePerfStats().getNonSingleHopsCount());
    }
  }

  private void updatePR(String FromClient, String colocation) {
    Cache cache = CacheFactory.getAnyInstance();
    Region region = cache.getRegion(Region_Name);
    if (colocation.equals("No_Colocation")) {
      if (FromClient.equals("FirstClient")) {
        for (int i = 0; i < 226; i++) {
          region.put(new Integer(i), "Update" + i);
        }
        ClientMetadataService cms = ((GemFireCacheImpl) cache).getClientMetadataService();
        Map<String, ClientPartitionAdvisor> regionMetaData = cms.getClientPRMetadata_TEST_ONLY();
        assertEquals(1, regionMetaData.size());
        regionMetaData.get(region.getFullPath());
        assertEquals(metaDataRefreshCount,
            ((LocalRegion) region).getCachePerfStats().getMetaDataRefreshCount());
        assertEquals(nonSingleHopsCount,
            ((LocalRegion) region).getCachePerfStats().getNonSingleHopsCount());
      }
    }
  }
}

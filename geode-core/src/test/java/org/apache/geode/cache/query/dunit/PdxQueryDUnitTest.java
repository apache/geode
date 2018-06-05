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
package org.apache.geode.cache.query.dunit;

import static org.apache.geode.internal.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.query.CacheUtils;
import org.apache.geode.cache.query.IndexType;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.Struct;
import org.apache.geode.cache.query.data.PortfolioPdx;
import org.apache.geode.cache.query.data.PositionPdx;
import org.apache.geode.cache.query.internal.Undefined;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.cache30.ClientServerTestCase;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.pdx.FieldType;
import org.apache.geode.pdx.JSONFormatter;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.pdx.PdxInstanceFactory;
import org.apache.geode.pdx.internal.ClientTypeRegistration;
import org.apache.geode.pdx.internal.PdxInstanceFactoryImpl;
import org.apache.geode.pdx.internal.PdxType;
import org.apache.geode.pdx.internal.PeerTypeRegistration;
import org.apache.geode.pdx.internal.TypeRegistration;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.DistributedTestCase;
import org.apache.geode.test.dunit.DistributedTestUtils;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.SerializableRunnableIF;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.OQLQueryTest;

@Category({DistributedTest.class, OQLQueryTest.class})
public class PdxQueryDUnitTest extends PDXQueryTestBase {
  public static final Logger logger = LogService.getLogger();

  public PdxQueryDUnitTest() {
    super();
  }

  /**
   * Tests client-server query on PdxInstance. The client receives projected value.
   */
  @Test
  public void testServerQuery() throws CacheException {

    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    final int numberOfEntries = 5;

    // Start server1
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer();
        Region region = getRootRegion().getSubregion(regionName);
        for (int i = 0; i < numberOfEntries; i++) {
          region.put("key-" + i, new TestObject(i, "vmware"));
        }
      }
    });


    // Start server2
    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer();
        Region region = getRootRegion().getSubregion(regionName);
        assertEquals(0, TestObject.numInstance);

        // Execute query with different type of Results.
        QueryService qs = getCache().getQueryService();
        Query query = null;
        SelectResults sr = null;
        for (int i = 0; i < queryString.length; i++) {
          try {
            query = qs.newQuery(queryString[i]);
            sr = (SelectResults) query.execute();
          } catch (Exception ex) {
            fail("Failed to execute query, " + ex.getMessage());
          }

          for (Object o : sr.asSet()) {
            if (i == 0 && !(o instanceof Integer)) {
              fail("Expected type Integer, not found in result set. Found type :" + o.getClass());
            } else if (i == 1 && !(o instanceof TestObject)) {
              fail(
                  "Expected type TestObject, not found in result set. Found type :" + o.getClass());
            } else if (i == 2 && !(o instanceof String)) {
              fail("Expected type String, not found in result set. Found type :" + o.getClass());
            }
          }
        }
        // Pdx objects for local queries now get deserialized when results are iterated.
        // So the deserialized objects are no longer cached in VMCachedDeserializable.
        assertEquals(numberOfEntries * 2, TestObject.numInstance);
      }
    });

    this.closeClient(vm1);
    this.closeClient(vm0);
  }


  /**
   * Tests client-server query on PdxInstance. The client receives projected value.
   */
  @Test
  public void testClientServerQueryWithProjections() throws CacheException {

    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    final int numberOfEntries = 10;

    // Start server1
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer();
        Region region = getRootRegion().getSubregion(regionName);
        class PdxObject extends TestObject {
          PdxObject() {}

          PdxObject(int id, String ticker) {
            super(id, ticker);
          }
        };
        for (int i = 0; i < numberOfEntries; i++) {
          region.put("key-" + i, new PdxObject(i, "vmware"));
        }
      }
    });

    // Start server2
    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer();
        Region region = getRootRegion().getSubregion(regionName);
        System.out.println("##### Region size is: " + region.size());
        assertEquals(0, TestObject.numInstance);
      }
    });

    // Create client region
    final int port0 = vm0.invoke(() -> PdxQueryDUnitTest.getCacheServerPort());
    final int port1 = vm1.invoke(() -> PdxQueryDUnitTest.getCacheServerPort());

    final String host0 = NetworkUtils.getServerHostName(vm0.getHost());

    // Create client pool.
    final String poolName = "testClientServerQueriesWithParamsPool";
    createPool(vm2, poolName, new String[] {host0}, new int[] {port0}, true);
    createPool(vm3, poolName, new String[] {host0}, new int[] {port1}, true);

    // Execute client queries
    SerializableRunnable executeQueries = new CacheSerializableRunnable("Execute queries") {
      public void run2() throws CacheException {
        SelectResults results = null;
        QueryService qService = null;
        try {
          qService = (PoolManager.find(poolName)).getQueryService();
        } catch (Exception e) {
          Assert.fail("Failed to get QueryService.", e);
        }
        try {
          logger.info("### Executing Query :" + queryString[0]);
          Query query = qService.newQuery(queryString[0]);
          results = (SelectResults) query.execute();
        } catch (Exception e) {
          Assert.fail("Failed executing " + queryString[0], e);
        }
        assertEquals(numberOfEntries, results.size());
      }
    };

    vm3.invoke(executeQueries);

    // Check for TestObject instances on Server2.
    // It should be 0
    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        assertEquals(0, TestObject.numInstance);
      }
    });

    this.closeClient(vm2);
    this.closeClient(vm3);
    this.closeClient(vm1);
    this.closeClient(vm0);
  }

  /**
   * Tests client-server query on compressed PdxInstance. The client receives uncompressed value.
   */
  @Test
  public void testClientServerQueryWithCompression() throws CacheException {
    final String randomString =
        "asddfjkhaskkfdjhzjc0943509328kvnhfjkldsg09q3485ibjafdp9q8y43p9u7hgavpiuaha48uy9afliasdnuaiuqa498qa4"
            + "asddfjkhaskkfdjhzjc0943509328kvnhfjkldsg09q3485ibjafdp9q8y43p9u7hgavpiuaha48uy9afliasdnuaiuqa498qa4"
            + "asddfjkhaskkfdjhzjc0943509328kvnhfjkldsg09q3485ibjafdp9q8y43p9u7hgavpiuaha48uy9afliasdnuaiuqa498qa4"
            + "asddfjkhaskkfdjhzjc0943509328kvnhfjkldsg09q3485ibjafdp9q8y43p9u7hgavpiuaha48uy9afliasdnuaiuqa498qa4";

    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    final int numberOfEntries = 10;

    // Start server1
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer(false, false, false, compressor);
        Region region = getRootRegion().getSubregion(regionName);
        assert (region.getAttributes().getCompressor() != null);
        for (int i = 0; i < numberOfEntries; i++) {
          region.put("key-" + i, new TestObject(i, randomString));
        }
      }
    });

    // Start server2
    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer();
        Region region = getRootRegion().getSubregion(regionName);
        System.out.println("##### Region size is: " + region.size());
        assertEquals(0, TestObject.numInstance);
      }
    });

    // Create client region
    final int port0 = vm0.invoke(() -> PdxQueryDUnitTest.getCacheServerPort());
    final int port1 = vm1.invoke(() -> PdxQueryDUnitTest.getCacheServerPort());

    final String host0 = NetworkUtils.getServerHostName(vm0.getHost());

    // Create client pool.
    final String poolName = "testClientServerQueriesWithParamsPool";
    createPool(vm2, poolName, new String[] {host0}, new int[] {port0}, true);
    createPool(vm3, poolName, new String[] {host0}, new int[] {port1}, true);

    // Execute client queries
    SerializableRunnable executeQueries = new CacheSerializableRunnable("Execute queries") {
      @SuppressWarnings("unchecked")
      public void run2() throws CacheException {
        SelectResults<String> results = null;
        QueryService qService = null;
        try {
          qService = (PoolManager.find(poolName)).getQueryService();
        } catch (Exception e) {
          Assert.fail("Failed to get QueryService.", e);
        }
        try {
          logger.info("### Executing Query :" + queryString[2]);
          Query query = qService.newQuery(queryString[2]);
          results = (SelectResults<String>) query.execute();
        } catch (Exception e) {
          Assert.fail("Failed executing " + queryString[2], e);
        }
        assertEquals(numberOfEntries, results.size());
        for (String result : results) {
          assertEquals(randomString, result);
        }
      }
    };

    vm3.invoke(executeQueries);

    // Check for TestObject instances on Server2.
    // It should be 0
    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        assertEquals(0, TestObject.numInstance);
      }
    });

    this.closeClient(vm2);
    this.closeClient(vm3);
    this.closeClient(vm1);
    this.closeClient(vm0);
  }

  /**
   * Tests client-server query on PdxInstance. The client receives projected value.
   */
  @Test
  public void testVersionedClass() throws CacheException {

    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    final int numberOfEntries = 10;

    // Start server1
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer();
        Region region = getRootRegion().getSubregion(regionName);
        try {
          for (int i = 0; i < numberOfEntries; i++) {
            PdxInstanceFactory pdxFactory =
                PdxInstanceFactoryImpl.newCreator("PdxTestObject", false, getCache());
            pdxFactory.writeInt("id", i);
            pdxFactory.writeString("ticker", "vmware");
            pdxFactory.writeString("idTickers", i + "vmware");
            PdxInstance pdxInstance = pdxFactory.create();
            region.put("key-" + i, pdxInstance);
          }
        } catch (Exception ex) {
          Assert.fail("Failed to load the class.", ex);
        }
      }
    });

    // Start server2
    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer();
        Region region = getRootRegion().getSubregion(regionName);
        System.out.println("##### Region size is: " + region.size());
        assertEquals(0, TestObject.numInstance);

      }
    });

    // Create client region
    final int port0 = vm0.invoke(() -> PdxQueryDUnitTest.getCacheServerPort());
    final int port1 = vm1.invoke(() -> PdxQueryDUnitTest.getCacheServerPort());

    final String host0 = NetworkUtils.getServerHostName(vm0.getHost());

    // Create client pool.
    final String poolName = "testClientServerQueriesWithParamsPool";
    createPool(vm2, poolName, new String[] {host0}, new int[] {port0}, true);
    createPool(vm3, poolName, new String[] {host0}, new int[] {port1}, true);

    // Execute client queries
    SerializableRunnable executeQueries = new CacheSerializableRunnable("Execute queries") {
      public void run2() throws CacheException {
        SelectResults results = null;
        QueryService qService = null;
        try {
          qService = (PoolManager.find(poolName)).getQueryService();
        } catch (Exception e) {
          Assert.fail("Failed to get QueryService.", e);
        }
        try {
          logger.info("### Executing Query :" + queryString[0]);
          Query query = qService.newQuery(queryString[0]);
          results = (SelectResults) query.execute();
        } catch (Exception e) {
          Assert.fail("Failed executing " + queryString[0], e);
        }

        assertEquals(numberOfEntries, results.size());
      }
    };

    vm3.invoke(executeQueries);

    // Check for TestObject instances on Server2.
    // It should be 0
    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        assertEquals(0, TestObject.numInstance);
      }
    });

    this.closeClient(vm2);
    this.closeClient(vm3);
    this.closeClient(vm1);
    this.closeClient(vm0);
  }

  /**
   * Tests client-server query on PdxInstance.
   */
  @Test
  public void testClientServerQuery() throws CacheException {

    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    final int numberOfEntries = 10;

    // Start server1
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer();
        Region region = getRootRegion().getSubregion(regionName);
        for (int i = 0; i < numberOfEntries; i++) {
          region.put("key-" + i, new TestObject(i, "vmware"));
        }
      }
    });

    // Start server2
    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer();
        Region region = getRootRegion().getSubregion(regionName);
      }
    });


    // Client pool.
    final int port0 = vm0.invoke(() -> PdxQueryDUnitTest.getCacheServerPort());
    final int port1 = vm1.invoke(() -> PdxQueryDUnitTest.getCacheServerPort());

    final String host0 = NetworkUtils.getServerHostName(vm0.getHost());

    // Create client pool.
    final String poolName = "testClientServerQueryPool";
    createPool(vm2, poolName, new String[] {host0}, new int[] {port0}, true);
    createPool(vm3, poolName, new String[] {host0}, new int[] {port1}, true);

    // Create client region
    vm3.invoke(new CacheSerializableRunnable("Create region") {
      public void run2() throws CacheException {
        QueryService localQueryService = null;

        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.LOCAL);
        ClientServerTestCase.configureConnectionPool(factory, host0, port1, -1, true, -1, -1, null);
        Region region = createRegion(regionName, rootRegionName, factory.create());
        for (int i = 0; i < numberOfEntries; i++) {
          region.put("key-" + i, new TestObject(i, "vmware"));
        }

        // Execute query locally.
        try {
          localQueryService = getCache().getQueryService();
        } catch (Exception e) {
          Assert.fail("Failed to get QueryService.", e);
        }
        for (int i = 0; i < 3; i++) {
          try {
            Query query = localQueryService.newQuery(queryString[i]);
            SelectResults results = (SelectResults) query.execute();
            assertEquals(numberOfEntries, results.size());
          } catch (Exception e) {
            Assert.fail("Failed executing " + queryString[i], e);
          }
        }
      }
    });

    // Execute client queries
    SerializableRunnable executeQueries = new CacheSerializableRunnable("Execute queries") {
      public void run2() throws CacheException {
        QueryService remoteQueryService = null;
        QueryService localQueryService = null;
        SelectResults[][] rs = new SelectResults[1][2];
        try {
          remoteQueryService = (PoolManager.find(poolName)).getQueryService();
          localQueryService = getCache().getQueryService();
        } catch (Exception e) {
          Assert.fail("Failed to get QueryService.", e);
        }

        for (int i = 1; i < 3; i++) {
          try {
            logger.info("### Executing Query on server:" + queryString[i]);
            Query query = remoteQueryService.newQuery(queryString[i]);
            rs[0][0] = (SelectResults) query.execute();
            assertEquals(numberOfEntries, rs[0][0].size());

            logger.info("### Executing Query locally:" + queryString[i]);
            query = localQueryService.newQuery(queryString[i]);
            rs[0][1] = (SelectResults) query.execute();
            assertEquals(numberOfEntries, rs[0][1].size());

            // Compare local and remote query results.
            if (!CacheUtils.compareResultsOfWithAndWithoutIndex(rs)) {
              fail("Local and Remote Query Results are not matching for query :" + queryString[i]);
            }

          } catch (Exception e) {
            Assert.fail("Failed executing " + queryString[i], e);
          }

        }
        assertEquals(2 * numberOfEntries, TestObject.numInstance);
      }
    };

    vm3.invoke(executeQueries);

    // Check for TestObject instances on Server2.
    // It should be 0
    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        assertEquals(0, TestObject.numInstance);
      }
    });

    this.closeClient(vm2);
    this.closeClient(vm3);
    this.closeClient(vm1);
    this.closeClient(vm0);
  }

  /**
   * Tests client-server query on PdxInstance.
   */
  @Test
  public void testClientServerQueryWithRangeIndex() throws CacheException {

    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);

    final String[] qs = new String[] {"SELECT * FROM " + regName + " p WHERE p.ID > 0",
        "SELECT p FROM " + regName + " p WHERE p.ID > 0",
        "SELECT * FROM " + regName + " p WHERE p.ID = 1",
        "SELECT * FROM " + regName + " p WHERE p.ID < 10",
        "SELECT * FROM " + regName + " p WHERE p.ID != 10",
        "SELECT * FROM " + regName + " p, p.positions.values pos WHERE p.ID > 0",
        "SELECT * FROM " + regName + " p, p.positions.values pos WHERE p.ID = 10",
        "SELECT p, pos FROM " + regName + " p, p.positions.values pos WHERE p.ID > 0",
        "SELECT p, pos FROM " + regName + " p, p.positions.values pos WHERE p.ID = 10",
        "SELECT pos FROM " + regName + " p, p.positions.values pos WHERE p.ID > 0",
        "SELECT p, pos FROM " + regName + " p, p.positions.values pos WHERE pos.secId != 'XXX'",
        "SELECT pos FROM " + regName + " p, p.positions.values pos WHERE pos.secId != 'XXX'",
        "SELECT pos FROM " + regName + " p, p.positions.values pos WHERE pos.secId = 'SUN'",
        "SELECT p, pos FROM " + regName + " p, p.positions.values pos WHERE pos.secId = 'SUN'",
        "SELECT p, pos FROM " + regName + " p, p.positions.values pos WHERE pos.secId = 'DELL'",
        "SELECT * FROM " + regName + " p, p.positions.values pos WHERE pos.secId = 'SUN'",
        "SELECT * FROM " + regName + " p, p.positions.values pos WHERE pos.secId = 'DELL'",
        "SELECT p, p.position1 FROM " + regName + " p where p.position1.secId != 'XXX'",
        "SELECT p, p.position1 FROM " + regName + " p where p.position1.secId = 'SUN'",
        "SELECT p.position1 FROM " + regName + " p WHERE p.ID > 0",
        "SELECT * FROM " + regName + " p WHERE p.status = 'active'",
        "SELECT p FROM " + regName + " p WHERE p.status != 'active'",};

    // Start server1
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer(false, false, true, null); // Async index
        Region region = getRootRegion().getSubregion(regionName);
        // Create Range index.
        QueryService qs = getCache().getQueryService();
        try {
          qs.createIndex("idIndex", "p.ID", regName + " p");
          qs.createIndex("statusIndex", "p.status", regName + " p");
          qs.createIndex("secIdIndex", "pos.secId", regName + " p, p.positions.values pos");
          qs.createIndex("pSecIdIdIndex", "p.position1.secId", regName + " p");
        } catch (Exception ex) {
          fail("Failed to create index." + ex.getMessage());
        }
      }
    });

    // Start server2
    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer(false, false, true, null); // Async index
        Region region = getRootRegion().getSubregion(regionName);
      }
    });

    // Client pool.
    final int port0 = vm0.invoke(() -> PdxQueryDUnitTest.getCacheServerPort());
    final int port1 = vm1.invoke(() -> PdxQueryDUnitTest.getCacheServerPort());

    final String host0 = NetworkUtils.getServerHostName(vm0.getHost());

    // Create client pool.
    final String poolName = "testClientServerQueryPool";
    createPool(vm2, poolName, new String[] {host0}, new int[] {port0}, true);
    createPool(vm3, poolName, new String[] {host0}, new int[] {port1}, true);

    // Create client region
    vm3.invoke(new CacheSerializableRunnable("Create region") {
      public void run2() throws CacheException {

        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.LOCAL);
        ClientServerTestCase.configureConnectionPool(factory, host0, port1, -1, true, -1, -1, null);
        Region region = createRegion(regionName, rootRegionName, factory.create());
        int j = 0;
        for (int i = 0; i < 100; i++) {
          region.put("key-" + i, new PortfolioPdx(j, j++));
          // To add duplicate:
          if (i % 24 == 0) {
            j = 0; // reset
          }
        }
      }
    });


    // Execute query and make sure there is no PdxInstance in the results.
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        // Execute query locally.
        QueryService queryService = getCache().getQueryService();
        for (int i = 0; i < qs.length; i++) {
          try {
            Query query = queryService.newQuery(qs[i]);
            SelectResults results = (SelectResults) query.execute();
            for (Object o : results.asList()) {
              if (o instanceof Struct) {
                Object[] values = ((Struct) o).getFieldValues();
                for (int c = 0; c < values.length; c++) {
                  if (values[c] instanceof PdxInstance) {
                    fail("Found unexpected PdxInstance in the query results. At struct field [" + c
                        + "] query :" + qs[i] + " Object is: " + values[c]);
                  }
                }
              } else {
                if (o instanceof PdxInstance) {
                  fail("Found unexpected PdxInstance in the query results. " + qs[i]);
                }
              }
            }
          } catch (Exception e) {
            Assert.fail("Failed executing " + qs[i], e);
          }

        }
      }
    });

    // Re-execute query to fetch PdxInstance in the results.
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        // Execute query locally.
        GemFireCacheImpl cache = (GemFireCacheImpl) getCache();
        cache.setReadSerializedForTest(true);
        try {
          QueryService queryService = getCache().getQueryService();
          for (int i = 0; i < qs.length; i++) {
            try {
              Query query = queryService.newQuery(qs[i]);
              SelectResults results = (SelectResults) query.execute();
              for (Object o : results.asList()) {
                if (o instanceof Struct) {
                  Object[] values = ((Struct) o).getFieldValues();
                  for (int c = 0; c < values.length; c++) {
                    if (!(values[c] instanceof PdxInstance)) {
                      fail(
                          "Didn't found expected PdxInstance in the query results. At struct field ["
                              + c + "] query :" + qs[i] + " Object is: " + values[c]);
                    }
                  }
                } else {
                  if (!(o instanceof PdxInstance)) {
                    fail("Didn't found expected PdxInstance in the query results. " + qs[i]
                        + " Object is: " + o);
                  }
                }
              }
            } catch (Exception e) {
              Assert.fail("Failed executing " + qs[i], e);
            }
          }
        } finally {
          cache.setReadSerializedForTest(false);
        }
      }
    });

    this.closeClient(vm2);
    this.closeClient(vm3);
    this.closeClient(vm1);
    this.closeClient(vm0);
  }

  /**
   * Tests client-server query on PdxInstance.
   */
  @Test
  public void testClientServerCountQuery() throws CacheException {

    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    final int numberOfEntries = 10;
    final String queryStr = "SELECT COUNT(*) FROM " + regName + " WHERE id >= 0";

    // Start server1
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer();
        Region region = getRootRegion().getSubregion(regionName);
        for (int i = 0; i < numberOfEntries; i++) {
          region.put("key-" + i, new TestObject(i, "vmware"));
        }
      }
    });

    // Start server2
    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer();
        Region region = getRootRegion().getSubregion(regionName);
      }
    });

    // Client pool.
    final int port0 = vm0.invoke(() -> PdxQueryDUnitTest.getCacheServerPort());
    final int port1 = vm1.invoke(() -> PdxQueryDUnitTest.getCacheServerPort());

    final String host0 = NetworkUtils.getServerHostName(vm0.getHost());

    // Create client pool.
    final String poolName = "testClientServerQueryPool";
    createPool(vm2, poolName, new String[] {host0}, new int[] {port0}, true);
    createPool(vm3, poolName, new String[] {host0}, new int[] {port1}, true);

    // Create client region
    vm3.invoke(new CacheSerializableRunnable("Create region") {
      public void run2() throws CacheException {
        QueryService localQueryService = null;

        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.LOCAL);
        ClientServerTestCase.configureConnectionPool(factory, host0, port1, -1, true, -1, -1, null);
        Region region = createRegion(regionName, rootRegionName, factory.create());
        for (int i = 0; i < numberOfEntries; i++) {
          region.put("key-" + i, new TestObject(i, "vmware"));
        }

        // Execute query locally.
        try {
          localQueryService = getCache().getQueryService();
        } catch (Exception e) {
          Assert.fail("Failed to get QueryService.", e);
        }
        try {
          Query query = localQueryService.newQuery(queryStr);
          SelectResults results = (SelectResults) query.execute();
          assertEquals(numberOfEntries, ((Integer) results.asList().get(0)).intValue());
        } catch (Exception e) {
          Assert.fail("Failed executing " + queryStr, e);
        }

      }
    });

    // Execute client queries
    SerializableRunnable executeQueries = new CacheSerializableRunnable("Execute queries") {
      public void run2() throws CacheException {
        QueryService remoteQueryService = null;
        QueryService localQueryService = null;
        SelectResults[][] rs = new SelectResults[1][2];
        try {
          remoteQueryService = (PoolManager.find(poolName)).getQueryService();
          localQueryService = getCache().getQueryService();
        } catch (Exception e) {
          Assert.fail("Failed to get QueryService.", e);
        }

        try {
          logger.info("### Executing Query on server:" + queryStr);
          Query query = remoteQueryService.newQuery(queryStr);
          rs[0][0] = (SelectResults) query.execute();
          assertEquals(numberOfEntries, ((Integer) rs[0][0].asList().get(0)).intValue());

          logger.info("### Executing Query locally:" + queryStr);
          query = localQueryService.newQuery(queryStr);
          rs[0][1] = (SelectResults) query.execute();
          assertEquals(numberOfEntries, ((Integer) rs[0][1].asList().get(0)).intValue());

          // Compare local and remote query results.
          if (!CacheUtils.compareResultsOfWithAndWithoutIndex(rs)) {
            fail("Local and Remote Query Results are not matching for query :" + queryStr);
          }

        } catch (Exception e) {
          Assert.fail("Failed executing " + queryStr, e);
        }

        assertEquals(numberOfEntries, TestObject.numInstance);
      }
    };

    vm3.invoke(executeQueries);

    // Check for TestObject instances on Server2.
    // It should be 0
    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        assertEquals(0, TestObject.numInstance);
      }
    });

    this.closeClient(vm2);
    this.closeClient(vm3);
    this.closeClient(vm1);
    this.closeClient(vm0);
  }

  /**
   * Tests client-server query on PdxInstance.
   */
  @Test
  public void testVersionedClientServerQuery() throws CacheException {

    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    final int numberOfEntries = 10;

    final String[] queryStr = new String[] {"SELECT DISTINCT ID FROM " + regName, // 0
        "SELECT * FROM " + regName, // 1
        "SELECT pkid FROM " + regName, // 2
        "SELECT * FROM " + regName + " WHERE ID > 5", // 3
        "SELECT p FROM " + regName + " p, p.positions pos WHERE p.pkid != 'vmware'", // 4
        "SELECT entry.value FROM " + this.regName + ".entries entry WHERE entry.value.ID > 0",
        "SELECT entry.value FROM  " + this.regName + ".entries entry WHERE entry.key = 'key-1'",
        "SELECT e.value FROM " + this.regName + ".entrySet e where  e.value.pkid >= '0'",
        "SELECT * FROM " + this.regName + ".values p WHERE p.pkid in SET('1', '2','3')",
        "SELECT * FROM " + this.regName + " pf where pf.position1.secId > '2'",
        "SELECT * FROM " + this.regName + " p where p.position3[1].portfolioId = 2",
        "SELECT * FROM " + this.regName + " p, p.positions.values AS pos WHERE pos.secId != '1'",
        "SELECT key, positions FROM " + this.regName + ".entrySet, value.positions.values "
            + "positions WHERE positions.mktValue >= 25.00",
        "SELECT * FROM " + this.regName + " portfolio1, " + this.regName + " portfolio2 WHERE "
            + "portfolio1.status = portfolio2.status",
        "SELECT portfolio1.ID, portfolio2.status FROM " + this.regName + " portfolio1, "
            + this.regName + " portfolio2  WHERE portfolio1.status = portfolio2.status",
        "SELECT * FROM " + this.regName + " portfolio1, portfolio1.positions.values positions1, "
            + this.regName + " portfolio2,  portfolio2.positions.values positions2 WHERE "
            + "positions1.secId = positions1.secId ",
        "SELECT * FROM " + this.regName + " portfolio, portfolio.positions.values positions WHERE "
            + "portfolio.Pk IN SET ('1', '2') AND positions.secId = '1'",
        "SELECT DISTINCT pf1, pf2 FROM " + this.regName
            + "  pf1, pf1.collectionHolderMap.values coll1," + " pf1.positions.values posit1, "
            + this.regName + "  pf2, pf2.collectionHolderMap.values "
            + " coll2, pf2.positions.values posit2 WHERE pf1.ID = pf2.ID",};



    // Start server1
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer();
        Region region = getRootRegion().getSubregion(regionName);
      }
    });

    // Start server2
    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer();
        Region region = getRootRegion().getSubregion(regionName);
      }
    });


    // Client pool.
    final int port0 = vm0.invoke(() -> PdxQueryDUnitTest.getCacheServerPort());
    final int port1 = vm1.invoke(() -> PdxQueryDUnitTest.getCacheServerPort());

    final String host0 = NetworkUtils.getServerHostName(vm0.getHost());

    // Create client pool.
    final String poolName = "testClientServerQueryPool";
    createPool(vm2, poolName, new String[] {host0}, new int[] {port0}, true);
    createPool(vm3, poolName, new String[] {host0}, new int[] {port1}, true);

    // Create client region
    vm3.invoke(new CacheSerializableRunnable("Create region") {
      public void run2() throws CacheException {

        // Load client/server region.
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.LOCAL);
        ClientServerTestCase.configureConnectionPool(factory, host0, port1, -1, true, -1, -1, null);
        Region region = createRegion(regionName, rootRegionName, factory.create());

        try {

          // Load TestObject
          for (int i = 0; i < numberOfEntries; i++) {
            PortfolioPdxVersion portfolioPdxVersion =
                new PortfolioPdxVersion(new Integer(i), new Integer(i));
            PdxInstanceFactory pdxFactory =
                PdxInstanceFactoryImpl.newCreator("PortfolioPdxVersion", false, getCache());
            PdxInstance pdxInstance = portfolioPdxVersion.createPdxInstance(pdxFactory);
            region.put("key-" + i, pdxInstance);
          }
        } catch (Exception ex) {
          fail("Failed to load the class.");
        }

        // Execute query:
        QueryService remoteQueryService = null;
        QueryService localQueryService = null;
        SelectResults[][] rs = new SelectResults[1][2];

        try {
          remoteQueryService = (PoolManager.find(poolName)).getQueryService();
          localQueryService = getCache().getQueryService();
        } catch (Exception e) {
          Assert.fail("Failed to get QueryService.", e);
        }

        for (int i = 0; i < queryStr.length; i++) {
          try {
            logger.info("### Executing Query on server:" + queryStr[i]);
            Query query = remoteQueryService.newQuery(queryStr[i]);
            rs[0][0] = (SelectResults) query.execute();
            logger.info("### Executing Query locally:" + queryStr[i]);
            query = localQueryService.newQuery(queryStr[i]);
            rs[0][1] = (SelectResults) query.execute();
            logger.info("### Remote Query rs size: " + (rs[0][0]).size() + "Local Query rs size: "
                + (rs[0][1]).size());
            // Compare local and remote query results.
            if (!CacheUtils.compareResultsOfWithAndWithoutIndex(rs)) {
              fail("Local and Remote Query Results are not matching for query :" + queryStr[i]);
            }
          } catch (Exception e) {
            Assert.fail("Failed executing " + queryStr[i], e);
          }

        }

      }
    });

    this.closeClient(vm2);
    this.closeClient(vm3);
    this.closeClient(vm1);
    this.closeClient(vm0);
  }

  /**
   * Tests client-server query on PdxInstance with mixed types.
   */
  @Test
  public void testClientServerQueryMixedTypes() throws CacheException {

    final String[] testQueries = new String[] {"select ticker from /root/" + regionName,
        "select ticker from /root/" + regionName + " p where IS_DEFINED(p.ticker)",
        "select ticker from /root/" + regionName + " where ticker = 'vmware'",};
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    final int numberOfEntries = 10;

    // Start server1
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer();
        Region region = getRootRegion().getSubregion(regionName);
        for (int i = 0; i < numberOfEntries; i++) {
          region.put("key-" + i, new TestObject(i, "vmware"));
        }
        for (int i = numberOfEntries; i < (numberOfEntries + 10); i++) {
          region.put("key-" + i, new TestObject2(i));
        }
      }
    });

    // Start server2
    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer();
        Region region = getRootRegion().getSubregion(regionName);
      }
    });

    // Client pool.
    final int port0 = vm0.invoke(() -> PdxQueryDUnitTest.getCacheServerPort());
    final int port1 = vm1.invoke(() -> PdxQueryDUnitTest.getCacheServerPort());

    final String host0 = NetworkUtils.getServerHostName(vm0.getHost());

    // Create client pool.
    final String poolName = "testClientServerQueryPool";
    createPool(vm2, poolName, new String[] {host0}, new int[] {port0}, true);
    createPool(vm3, poolName, new String[] {host0}, new int[] {port1}, true);

    // Create client region
    vm3.invoke(new CacheSerializableRunnable("Create region") {
      public void run2() throws CacheException {
        QueryService localQueryService = null;

        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.LOCAL);
        ClientServerTestCase.configureConnectionPool(factory, host0, port1, -1, true, -1, -1, null);
        Region region = createRegion(regionName, rootRegionName, factory.create());
        for (int i = 0; i < numberOfEntries; i++) {
          region.put("key-" + i, new TestObject(i, "vmware"));
        }
        for (int i = numberOfEntries; i < (numberOfEntries + 10); i++) {
          region.put("key-" + i, new TestObject2(i));
        }
        // Execute query locally.
        try {
          localQueryService = getCache().getQueryService();
        } catch (Exception e) {
          Assert.fail("Failed to get QueryService.", e);
        }
        for (int i = 0; i < 3; i++) {
          try {
            Query query = localQueryService.newQuery(testQueries[i]);
            SelectResults results = (SelectResults) query.execute();
            if (i == 0) {
              assertEquals(numberOfEntries + 10, results.size());
            } else {
              assertEquals(numberOfEntries, results.size());
            }
          } catch (Exception e) {
            Assert.fail("Failed executing " + testQueries[i], e);
          }
        }
      }
    });

    // Execute client queries
    SerializableRunnable executeQueries = new CacheSerializableRunnable("Execute queries") {
      public void run2() throws CacheException {
        QueryService remoteQueryService = null;
        QueryService localQueryService = null;
        SelectResults[][] rs = new SelectResults[1][2];
        try {
          remoteQueryService = (PoolManager.find(poolName)).getQueryService();
          localQueryService = getCache().getQueryService();
        } catch (Exception e) {
          Assert.fail("Failed to get QueryService.", e);
        }

        for (int i = 0; i < 3; i++) {
          try {
            logger.info("### Executing Query on server:" + testQueries[i]);
            Query query = remoteQueryService.newQuery(testQueries[i]);
            rs[0][0] = (SelectResults) query.execute();
            if (i == 0) {
              // defined and undefined values returned
              assertEquals(numberOfEntries + 10, rs[0][0].size());
            } else {
              // only defined values of ticker
              assertEquals(numberOfEntries, rs[0][0].size());
            }
            logger.info("### Executing Query locally:" + testQueries[i]);
            query = localQueryService.newQuery(testQueries[i]);
            rs[0][1] = (SelectResults) query.execute();
            if (i == 0) {
              assertEquals(numberOfEntries + 10, rs[0][1].size());
            } else {
              assertEquals(numberOfEntries, rs[0][1].size());
            }
            assertEquals(rs[0][0].size(), rs[0][1].size());
          } catch (Exception e) {
            Assert.fail("Failed executing " + testQueries[i], e);
          }

        }
        assertEquals(2 * (numberOfEntries + 5), (TestObject.numInstance + TestObject2.numInstance));
      }
    };

    vm3.invoke(executeQueries);

    // Check for TestObject instances on Server2.
    // It should be 0
    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        assertEquals(0, TestObject.numInstance);
      }
    });

    this.closeClient(vm2);
    this.closeClient(vm3);
    this.closeClient(vm1);
    this.closeClient(vm0);
  }

  /**
   * Tests query on with PR.
   */
  @Test
  public void testQueryOnPR() throws CacheException {

    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    final int numberOfEntries = 100;

    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer(true, true);
      }
    });

    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer(true, false);
      }
    });

    vm2.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer(true, false);
      }
    });

    // Load region.
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        for (int i = 0; i < numberOfEntries; i++) {
          region.put("key-" + i, new TestObject(i, "vmware"));
        }
      }
    });

    // Client pool.
    final int port1 = vm1.invoke(() -> PdxQueryDUnitTest.getCacheServerPort());

    final String host0 = NetworkUtils.getServerHostName(vm0.getHost());

    // Create client pool.
    final String poolName = "testClientServerQueryPool";
    createPool(vm3, poolName, new String[] {host0}, new int[] {port1}, true);

    // Execute client queries
    vm3.invoke(new CacheSerializableRunnable("Execute queries") {
      public void run2() throws CacheException {
        QueryService remoteQueryService = null;

        try {
          remoteQueryService = (PoolManager.find(poolName)).getQueryService();
        } catch (Exception e) {
          Assert.fail("Failed to get QueryService.", e);
        }

        for (int i = 1; i < 3; i++) {
          try {
            logger.info("### Executing Query on server:" + queryString[i]);
            Query query = remoteQueryService.newQuery(queryString[i]);
            SelectResults rs = (SelectResults) query.execute();
            assertEquals(numberOfEntries, rs.size());
          } catch (Exception e) {
            Assert.fail("Failed executing " + queryString[i], e);
          }
        }
        assertEquals(numberOfEntries, TestObject.numInstance);
      }
    });

    // Check for TestObject instances.
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        assertEquals(numberOfEntries, TestObject.numInstance);
      }
    });

    // Check for TestObject instances.
    // It should be 0
    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        assertEquals(0, TestObject.numInstance);
      }
    });

    // Check for TestObject instances.
    // It should be 0
    vm2.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        assertEquals(0, TestObject.numInstance);
      }
    });

    // Execute Query on Server2.
    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        QueryService qs = getCache().getQueryService();
        Query query = null;
        SelectResults sr = null;
        for (int i = 0; i < queryString.length; i++) {
          try {
            query = qs.newQuery(queryString[i]);
            sr = (SelectResults) query.execute();
          } catch (Exception ex) {
            fail("Failed to execute query, " + ex.getMessage());
          }

          for (Object o : sr.asSet()) {
            if (i == 0 && !(o instanceof Integer)) {
              fail("Expected type Integer, not found in result set. Found type :" + o.getClass());
            } else if (i == 1 && !(o instanceof TestObject)) {
              fail(
                  "Expected type TestObject, not found in result set. Found type :" + o.getClass());
            } else if (i == 2 && !(o instanceof String)) {
              fail("Expected type String, not found in result set. Found type :" + o.getClass());
            }
          }
        }
        if (TestObject.numInstance <= 0) {
          fail("Expected TestObject instance to be >= 0.");
        }
      }
    });

    // Check for TestObject instances.
    // It should be 0
    vm2.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        assertEquals(0, TestObject.numInstance);
      }
    });


    this.closeClient(vm2);
    this.closeClient(vm3);
    this.closeClient(vm1);
    this.closeClient(vm0);
  }

  /**
   * Tests query on with PR.
   */
  @Test
  public void testLocalPRQuery() throws CacheException {

    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    final int numberOfEntries = 100;

    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer(true, true);
      }
    });

    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer(true, false);
      }
    });

    vm2.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer(true, false);
      }
    });

    vm3.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer(true, false);
      }
    });


    // Load region using class loader and execute query on the same thread.
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        try {
          // Load TestObject
          for (int i = 0; i < numberOfEntries; i++) {
            PdxInstanceFactory pdxInstanceFactory =
                PdxInstanceFactoryImpl.newCreator("PortfolioPdxVersion", false, getCache());
            PortfolioPdxVersion portfolioPdxVersion =
                new PortfolioPdxVersion(new Integer(i), new Integer(i));
            PdxInstance pdxInstance = portfolioPdxVersion.createPdxInstance(pdxInstanceFactory);
            region.put("key-" + i, pdxInstance);
          }
        } catch (Exception ex) {
          Assert.fail("Failed to load the class.", ex);
        }

        QueryService localQueryService = null;

        try {
          localQueryService = region.getCache().getQueryService();
        } catch (Exception e) {
          Assert.fail("Failed to get QueryService.", e);
        }

        for (int i = 1; i < 3; i++) {
          try {
            logger.info("### Executing Query on server:" + queryString[i]);
            Query query = localQueryService.newQuery(queryString[i]);
            SelectResults rs = (SelectResults) query.execute();
            assertEquals(numberOfEntries, rs.size());
          } catch (Exception e) {
            Assert.fail("Failed executing " + queryString[i], e);
          }
        }
      }
    });

    this.closeClient(vm2);
    this.closeClient(vm3);
    this.closeClient(vm1);
    this.closeClient(vm0);
  }

  /**
   * Tests query on with PR.
   */
  @Test
  public void testPdxReadSerializedForPRQuery() throws CacheException {

    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    final int numberOfEntries = 100;

    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer(true, true);
      }
    });

    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer(true, false);
      }
    });

    vm2.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer(true, false);
      }
    });

    vm3.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer(true, false);
      }
    });


    // Load region using class loader and execute query on the same thread.
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        try {
          // Load TestObject
          for (int i = 0; i < numberOfEntries; i++) {
            PortfolioPdxVersion portfolioPdxVersion =
                new PortfolioPdxVersion(new Integer(i), new Integer(i));
            PdxInstanceFactory pdxInstanceFactory =
                PdxInstanceFactoryImpl.newCreator("PortfolioPdxVersion", false, getCache());
            PdxInstance pdxInstance = portfolioPdxVersion.createPdxInstance(pdxInstanceFactory);
            region.put("key-" + i, pdxInstance);
          }
        } catch (Exception ex) {
          fail("Failed to load the class.");
        }

        QueryService localQueryService = null;

        try {
          localQueryService = region.getCache().getQueryService();
        } catch (Exception e) {
          Assert.fail("Failed to get QueryService.", e);
        }

        for (int i = 1; i < 3; i++) {
          try {
            logger.info("### Executing Query on server:" + queryString[i]);
            Query query = localQueryService.newQuery(queryString[i]);
            SelectResults rs = (SelectResults) query.execute();
            assertEquals(numberOfEntries, rs.size());
          } catch (Exception e) {
            Assert.fail("Failed executing " + queryString[i], e);
          }
        }
      }
    });

    final String[] qs = new String[] {"SELECT * FROM " + regName,
        "SELECT * FROM " + regName + " WHERE ID > 5", "SELECT p FROM " + regName
            + " p, p.positions.values pos WHERE p.ID > 2 or pos.secId = 'vmware'",};

    // Execute query on node without class and with pdxReadSerialized.
    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        GemFireCacheImpl c = (GemFireCacheImpl) region.getCache();
        try {
          // Set read serialized.
          c.setReadSerializedForTest(true);

          QueryService localQueryService = null;

          try {
            localQueryService = region.getCache().getQueryService();
          } catch (Exception e) {
            Assert.fail("Failed to get QueryService.", e);
          }

          // This should not throw class not found exception.
          for (int i = 1; i < qs.length; i++) {
            try {
              logger.info("### Executing Query on server:" + qs[i]);
              Query query = localQueryService.newQuery(qs[i]);
              SelectResults rs = (SelectResults) query.execute();
              for (Object o : rs.asSet()) {
                if (!(o instanceof PdxInstance)) {
                  fail("Expected type PdxInstance, not found in result set. Found type :"
                      + o.getClass());
                }
              }
            } catch (Exception e) {
              Assert.fail("Failed executing " + qs[i], e);
            }
          }
        } finally {
          c.setReadSerializedForTest(false);
        }
      }
    });

    this.closeClient(vm2);
    this.closeClient(vm3);
    this.closeClient(vm1);
    this.closeClient(vm0);
  }


  /**
   * Tests index on PdxInstance.
   */
  @Test
  public void testIndex() throws CacheException {

    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    final int numberOfEntries = 10;

    // Start server1
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer();
        Region region = getRootRegion().getSubregion(regionName);
        for (int i = 0; i < numberOfEntries; i++) {
          if (i % 2 == 0) {
            region.put("key-" + i, new TestObject(i, "vmware"));
          } else {
            region.put("key-" + i, new TestObject(i, "vmware" + i));
          }
        }

        try {
          QueryService qs = getCache().getQueryService();
          qs.createIndex("idIndex", IndexType.FUNCTIONAL, "id", regName);
          qs.createIndex("tickerIndex", IndexType.FUNCTIONAL, "p.ticker", regName + " p");
          qs.createIndex("tickerIdTickerMapIndex", IndexType.FUNCTIONAL, "p.ticker",
              regName + " p, p.idTickers idTickers");
        } catch (Exception ex) {
          fail("Unable to create index. " + ex.getMessage());
        }
        assertEquals(numberOfEntries, TestObject.numInstance);
      }
    });

    // Start server2
    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer();
        Region region = getRootRegion().getSubregion(regionName);

        try {
          QueryService qs = getCache().getQueryService();
          qs.createIndex("idIndex", IndexType.FUNCTIONAL, "id", regName);
          qs.createIndex("tickerIndex", IndexType.FUNCTIONAL, "p.ticker", regName + " p");
          qs.createIndex("tickerIdTickerMapIndex", IndexType.FUNCTIONAL, "p.ticker",
              regName + " p, p.idTickers idTickers");
        } catch (Exception ex) {
          fail("Unable to create index. " + ex.getMessage());
        }
        assertEquals(0, TestObject.numInstance);
      }
    });

    // Client pool.
    final int port0 = vm0.invoke(() -> PdxQueryDUnitTest.getCacheServerPort());
    final int port1 = vm1.invoke(() -> PdxQueryDUnitTest.getCacheServerPort());

    final String host0 = NetworkUtils.getServerHostName(vm0.getHost());

    // Create client pool.
    final String poolName = "testClientServerQueryPool";
    createPool(vm2, poolName, new String[] {host0}, new int[] {port0}, true);
    createPool(vm3, poolName, new String[] {host0}, new int[] {port1}, true);

    // Create client region
    SerializableRunnable createClientRegions = new CacheSerializableRunnable("Create region") {
      public void run2() throws CacheException {

        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.LOCAL);
        ClientServerTestCase.configureConnectionPool(factory, host0, port1, -1, true, -1, -1, null);
        Region region = createRegion(regionName, rootRegionName, factory.create());
        for (int i = 0; i < numberOfEntries * 2; i++) {
          if (i % 2 == 0) {
            region.put("key-" + i, new TestObject(i, "vmware"));
          } else {
            region.put("key-" + i, new TestObject(i, "vmware" + i));
          }
        }
      }
    };

    vm2.invoke(createClientRegions);
    vm3.invoke(createClientRegions);

    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        assertEquals(numberOfEntries, TestObject.numInstance);
      }
    });

    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        assertEquals(0, TestObject.numInstance);
      }
    });


    // Execute client queries
    SerializableRunnable executeQueries = new CacheSerializableRunnable("Execute queries") {
      public void run2() throws CacheException {
        QueryService remoteQueryService = null;
        QueryService localQueryService = null;
        SelectResults[][] rs = new SelectResults[1][2];
        try {
          remoteQueryService = (PoolManager.find(poolName)).getQueryService();
          localQueryService = getCache().getQueryService();
        } catch (Exception e) {
          Assert.fail("Failed to get QueryService.", e);
        }

        for (int i = 0; i < 3; i++) {
          try {
            logger.info("### Executing Query on server:" + queryString[i]);
            Query query = remoteQueryService.newQuery(queryString[i]);
            rs[0][0] = (SelectResults) query.execute();
            assertEquals(numberOfEntries * 2, rs[0][0].size());

            logger.info("### Executing Query locally:" + queryString[i]);
            query = localQueryService.newQuery(queryString[i]);
            rs[0][1] = (SelectResults) query.execute();
            assertEquals(numberOfEntries * 2, rs[0][1].size());

            // Compare local and remote query results.
            if (!CacheUtils.compareResultsOfWithAndWithoutIndex(rs)) {
              fail("Local and Remote Query Results are not matching for query :" + queryString[i]);
            }

          } catch (Exception e) {
            Assert.fail("Failed executing " + queryString[i], e);
          }

        }
        assertEquals(4 * numberOfEntries, TestObject.numInstance);

        for (int i = 3; i < queryString.length; i++) {
          try {
            logger.info("### Executing Query on server:" + queryString[i]);
            Query query = remoteQueryService.newQuery(queryString[i]);
            rs[0][0] = (SelectResults) query.execute();

            logger.info("### Executing Query locally:" + queryString[i]);
            query = localQueryService.newQuery(queryString[i]);
            rs[0][1] = (SelectResults) query.execute();

            // Compare local and remote query results.
            if (!CacheUtils.compareResultsOfWithAndWithoutIndex(rs)) {
              fail("Local and Remote Query Results are not matching for query :" + queryString[i]);
            }

          } catch (Exception e) {
            Assert.fail("Failed executing " + queryString[i], e);
          }

        }

      }
    };

    vm2.invoke(executeQueries);
    vm3.invoke(executeQueries);

    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        assertEquals(numberOfEntries, TestObject.numInstance);
      }
    });

    // Check for TestObject instances on Server2.
    // It should be 0
    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        assertEquals(0, TestObject.numInstance);
      }
    });

    this.closeClient(vm2);
    this.closeClient(vm3);
    this.closeClient(vm1);
    this.closeClient(vm0);
  }

  /**
   * Tests client-server query with region iterators.
   */
  @Test
  public void testRegionIterators() throws CacheException {

    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    final int numberOfEntries = 10;

    final String[] queries = new String[] {
        "SELECT entry.value FROM " + this.regName + ".entries entry WHERE entry.value.id > 0",
        "SELECT entry.value FROM  " + this.regName + ".entries entry WHERE entry.key = 'key-1'",
        "SELECT e.value FROM " + this.regName + ".entrySet e where  e.value.id >= 0",
        "SELECT * FROM " + this.regName + ".values p WHERE p.ticker = 'vmware'",};

    // Start server1
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer();
        Region region = getRootRegion().getSubregion(regionName);
        for (int i = 0; i < numberOfEntries; i++) {
          region.put("key-" + i, new TestObject(i, "vmware"));
        }
      }
    });

    // Start server2
    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer();
        Region region = getRootRegion().getSubregion(regionName);
      }
    });


    // Client pool.
    final int port0 = vm0.invoke(() -> PdxQueryDUnitTest.getCacheServerPort());
    final int port1 = vm1.invoke(() -> PdxQueryDUnitTest.getCacheServerPort());

    final String host0 = NetworkUtils.getServerHostName(vm0.getHost());

    // Create client pool.
    final String poolName = "testClientServerQueryPool";
    createPool(vm2, poolName, new String[] {host0}, new int[] {port0}, true);
    createPool(vm3, poolName, new String[] {host0}, new int[] {port1}, true);

    // Create client region
    vm3.invoke(new CacheSerializableRunnable("Create region") {
      public void run2() throws CacheException {
        QueryService localQueryService = null;

        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.LOCAL);
        ClientServerTestCase.configureConnectionPool(factory, host0, port1, -1, true, -1, -1, null);
        Region region = createRegion(regionName, rootRegionName, factory.create());
        for (int i = 0; i < numberOfEntries; i++) {
          region.put("key-" + i, new TestObject(i, "vmware"));
        }
      }
    });

    // Execute client queries
    SerializableRunnable executeQueries = new CacheSerializableRunnable("Execute queries") {
      public void run2() throws CacheException {
        QueryService remoteQueryService = null;
        QueryService localQueryService = null;
        SelectResults[][] rs = new SelectResults[1][2];
        try {
          remoteQueryService = (PoolManager.find(poolName)).getQueryService();
          localQueryService = getCache().getQueryService();
        } catch (Exception e) {
          Assert.fail("Failed to get QueryService.", e);
        }

        for (int i = 0; i < queries.length; i++) {
          try {
            logger.info("### Executing Query on server:" + queries[i]);
            Query query = remoteQueryService.newQuery(queries[i]);
            rs[0][0] = (SelectResults) query.execute();

            logger.info("### Executing Query locally:" + queries[i]);
            query = localQueryService.newQuery(queries[i]);
            rs[0][1] = (SelectResults) query.execute();

            // Compare local and remote query results.
            if (!CacheUtils.compareResultsOfWithAndWithoutIndex(rs)) {
              fail("Local and Remote Query Results are not matching for query :" + queries[i]);
            }

          } catch (Exception e) {
            Assert.fail("Failed executing " + queries[i], e);
          }

        }
      }
    };

    vm3.invoke(executeQueries);

    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        assertEquals(0, TestObject.numInstance);
      }
    });


    // Create index
    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        QueryService qs = getCache().getQueryService();
        try {
          qs.createIndex("idIndex", IndexType.FUNCTIONAL, "entry.value.id",
              regName + ".entries entry");
          qs.createIndex("tickerIndex", IndexType.FUNCTIONAL, "p.ticker", regName + ".values p");
        } catch (Exception ex) {
          fail("Unable to create index. " + ex.getMessage());
        }
      }
    });

    vm3.invoke(executeQueries);

    // Check for TestObject instances.
    // It should be 0
    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        assertEquals(0, TestObject.numInstance);
      }
    });

    this.closeClient(vm2);
    this.closeClient(vm3);
    this.closeClient(vm1);
    this.closeClient(vm0);
  }

  /**
   * Tests client-server query with nested and collection of Pdx.
   */
  @Test
  public void testNestedAndCollectionPdx() throws CacheException {

    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    final int numberOfEntries = 50;

    final String[] queries = new String[] {
        "SELECT * FROM " + this.regName + " pf where pf.position1.secId > '2'",
        "SELECT * FROM " + this.regName + " p where p.position3[1].portfolioId = 2",
        "SELECT * FROM " + this.regName + " p, p.positions.values AS pos WHERE pos.secId != '1'",
        "SELECT key, positions FROM " + this.regName + ".entrySet, value.positions.values "
            + "positions WHERE positions.mktValue >= 25.00",
        "SELECT * FROM " + this.regName + " portfolio1, " + this.regName2 + " portfolio2 WHERE "
            + "portfolio1.status = portfolio2.status",
        "SELECT portfolio1.ID, portfolio2.status FROM " + this.regName + " portfolio1, "
            + this.regName + " portfolio2  WHERE portfolio1.status = portfolio2.status",
        "SELECT * FROM " + this.regName + " portfolio1, portfolio1.positions.values positions1, "
            + this.regName + " portfolio2,  portfolio2.positions.values positions2 WHERE "
            + "positions1.secId = positions2.secId ",
        "SELECT * FROM " + this.regName + " portfolio, portfolio.positions.values positions WHERE "
            + "portfolio.Pk IN SET ('1', '2') AND positions.secId = '1'",
        "SELECT DISTINCT * FROM " + this.regName + "  pf1, pf1.collectionHolderMap.values coll1,"
            + " pf1.positions.values posit1, " + this.regName2
            + "  pf2, pf2.collectionHolderMap.values "
            + " coll2, pf2.positions.values posit2 WHERE posit1.secId='IBM' AND posit2.secId='IBM'",};

    // Start server1
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer();
        Region region1 = getRootRegion().getSubregion(regionName);
        Region region2 = getRootRegion().getSubregion(regionName2);

        for (int i = 0; i < numberOfEntries; i++) {
          region1.put("key-" + i, new PortfolioPdx(i, i));
          region2.put("key-" + i, new PortfolioPdx(i, i));
        }
      }
    });

    // Start server2
    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer();
        Region region = getRootRegion().getSubregion(regionName);
        Region region2 = getRootRegion().getSubregion(regionName2);
      }
    });


    // Client pool.
    final int port0 = vm0.invoke(() -> PdxQueryDUnitTest.getCacheServerPort());
    final int port1 = vm1.invoke(() -> PdxQueryDUnitTest.getCacheServerPort());

    final String host0 = NetworkUtils.getServerHostName(vm0.getHost());

    // Create client pool.
    final String poolName = "testClientServerQueryPool";
    createPool(vm2, poolName, new String[] {host0}, new int[] {port0}, true);
    createPool(vm3, poolName, new String[] {host0}, new int[] {port1}, true);

    // Create client region
    vm3.invoke(new CacheSerializableRunnable("Create region") {
      public void run2() throws CacheException {

        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.LOCAL);
        ClientServerTestCase.configureConnectionPool(factory, host0, port1, -1, true, -1, -1, null);
        Region region1 = createRegion(regionName, rootRegionName, factory.create());
        Region region2 = createRegion(regionName2, rootRegionName, factory.create());

        for (int i = 0; i < numberOfEntries; i++) {
          region1.put("key-" + i, new PortfolioPdx(i, i));
          region2.put("key-" + i, new PortfolioPdx(i, i));
        }
      }
    });

    // Execute client queries
    SerializableRunnable executeQueries = new CacheSerializableRunnable("Execute queries") {
      public void run2() throws CacheException {
        QueryService remoteQueryService = null;
        QueryService localQueryService = null;
        SelectResults[][] rs = new SelectResults[1][2];
        try {
          remoteQueryService = (PoolManager.find(poolName)).getQueryService();
          localQueryService = getCache().getQueryService();
        } catch (Exception e) {
          Assert.fail("Failed to get QueryService.", e);
        }

        for (int i = 0; i < queries.length; i++) {
          try {
            logger.info("### Executing Query on server:" + queries[i]);
            Query query = remoteQueryService.newQuery(queries[i]);
            rs[0][0] = (SelectResults) query.execute();

            logger.info("### Executing Query locally:" + queries[i]);
            query = localQueryService.newQuery(queries[i]);
            rs[0][1] = (SelectResults) query.execute();

            // Compare local and remote query results.
            if (!CacheUtils.compareResultsOfWithAndWithoutIndex(rs)) {
              fail("Local and Remote Query Results are not matching for query :" + queries[i]);
            }

          } catch (Exception e) {
            Assert.fail("Failed executing " + queries[i], e);
          }

        }
      }
    };

    vm3.invoke(executeQueries);

    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        assertEquals(0, PortfolioPdx.numInstance);
        assertEquals(0, PositionPdx.numInstance);
      }
    });


    // Create index
    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        QueryService qs = getCache().getQueryService();
        try {
          qs.createIndex("pkIndex", IndexType.FUNCTIONAL, "portfolio.Pk", regName + " portfolio");
          qs.createIndex("secIdIndex", IndexType.FUNCTIONAL, "pos.secId",
              regName + " p, p.positions.values AS pos");
          qs.createIndex("tickerIndex", IndexType.FUNCTIONAL, "pf.position1.secId",
              regName + " pf");
          qs.createIndex("secIdIndexPf1", IndexType.FUNCTIONAL, "pos11.secId",
              regName + " pf1, pf1.collectionHolderMap.values coll1, pf1.positions.values pos11");
          qs.createIndex("secIdIndexPf2", IndexType.FUNCTIONAL, "pos22.secId",
              regName2 + " pf2, pf2.collectionHolderMap.values coll2, pf2.positions.values pos22");
        } catch (Exception ex) {
          fail("Unable to create index. " + ex.getMessage());
        }
      }
    });

    vm3.invoke(executeQueries);

    // index is created on portfolio.Pk field which does not exists in
    // PorfolioPdx object
    // but there is a method getPk(), so for #44436, the objects are
    // deserialized to get the value in vm1
    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        assertEquals(numberOfEntries, PortfolioPdx.numInstance);
        assertEquals(325, PositionPdx.numInstance); // 50 PorforlioPdx objects
        // create (50*3)+50+50+50+25
        // = 325 PositionPdx objects
        // when deserialized
      }
    });

    this.closeClient(vm2);
    this.closeClient(vm3);
    this.closeClient(vm1);
    this.closeClient(vm0);
  }

  /**
   * Tests client-server query with nested and collection of Pdx.
   */
  @Test
  public void testNestedAndCollectionPdxWithPR() throws CacheException {

    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    final int numberOfEntries = 50;

    final String[] queries = new String[] {
        "SELECT * FROM " + this.regName + " pf where pf.position1.secId > '2'",
        "SELECT * FROM " + this.regName + " p where p.position3[1].portfolioId = 2",
        "SELECT * FROM " + this.regName + " p, p.positions.values AS pos WHERE pos.secId != '1'",
        "SELECT key, positions FROM " + this.regName + ".entrySet, value.positions.values "
            + "positions WHERE positions.mktValue >= 25.00",
        "SELECT * FROM " + this.regName + " portfolio1, " + this.regName2 + " portfolio2 WHERE "
            + "portfolio1.status = portfolio2.status",
        "SELECT portfolio1.ID, portfolio2.status FROM " + this.regName + " portfolio1, "
            + this.regName + " portfolio2  WHERE portfolio1.status = portfolio2.status",
        "SELECT * FROM " + this.regName + " portfolio1, portfolio1.positions.values positions1, "
            + this.regName + " portfolio2,  portfolio2.positions.values positions2 WHERE "
            + "positions1.secId = positions2.secId ",
        "SELECT * FROM " + this.regName + " portfolio, portfolio.positions.values positions WHERE "
            + "portfolio.Pk IN SET ('1', '2') AND positions.secId = '1'",
        "SELECT DISTINCT * FROM " + this.regName + "  pf1, pf1.collectionHolderMap.values coll1,"
            + " pf1.positions.values posit1, " + this.regName2
            + "  pf2, pf2.collectionHolderMap.values "
            + " coll2, pf2.positions.values posit2 WHERE posit1.secId='IBM' AND posit2.secId='IBM'",};

    // Start server1
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer(false, true);
        Region region1 = getRootRegion().getSubregion(regionName);
        Region region2 = getRootRegion().getSubregion(regionName2);
      }
    });

    // Start server2
    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer(false, true);
        Region region = getRootRegion().getSubregion(regionName);
        Region region2 = getRootRegion().getSubregion(regionName2);
      }
    });

    // Start server2
    vm2.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer(false, true);
        Region region = getRootRegion().getSubregion(regionName);
        Region region2 = getRootRegion().getSubregion(regionName2);
      }
    });

    // Client pool.
    final int port0 = vm0.invoke(() -> PdxQueryDUnitTest.getCacheServerPort());
    final int port1 = vm1.invoke(() -> PdxQueryDUnitTest.getCacheServerPort());

    final String host0 = NetworkUtils.getServerHostName(vm0.getHost());

    // Create client pool.
    final String poolName = "testClientServerQueryPool";
    createPool(vm2, poolName, new String[] {host0}, new int[] {port0}, true);
    createPool(vm3, poolName, new String[] {host0}, new int[] {port1}, true);

    // Create client region
    vm3.invoke(new CacheSerializableRunnable("Create region") {
      public void run2() throws CacheException {
        QueryService localQueryService = null;

        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.LOCAL);
        ClientServerTestCase.configureConnectionPool(factory, host0, port1, -1, true, -1, -1, null);
        Region region1 = createRegion(regionName, rootRegionName, factory.create());
        Region region2 = createRegion(regionName2, rootRegionName, factory.create());

        for (int i = 0; i < numberOfEntries; i++) {
          region1.put("key-" + i, new PortfolioPdx(i, i));
          region2.put("key-" + i, new PortfolioPdx(i, i));
        }
      }
    });

    // Execute client queries
    SerializableRunnable executeQueries = new CacheSerializableRunnable("Execute queries") {
      public void run2() throws CacheException {
        QueryService remoteQueryService = null;
        QueryService localQueryService = null;
        SelectResults[][] rs = new SelectResults[1][2];
        try {
          remoteQueryService = (PoolManager.find(poolName)).getQueryService();
          localQueryService = getCache().getQueryService();
        } catch (Exception e) {
          Assert.fail("Failed to get QueryService.", e);
        }

        for (int i = 0; i < queries.length; i++) {
          try {
            logger.info("### Executing Query on server:" + queries[i]);
            Query query = remoteQueryService.newQuery(queries[i]);
            rs[0][0] = (SelectResults) query.execute();

            logger.info("### Executing Query locally:" + queries[i]);
            query = localQueryService.newQuery(queries[i]);
            rs[0][1] = (SelectResults) query.execute();

            // Compare local and remote query results.
            if (!CacheUtils.compareResultsOfWithAndWithoutIndex(rs)) {
              fail("Local and Remote Query Results are not matching for query :" + queries[i]);
            }

          } catch (Exception e) {
            Assert.fail("Failed executing " + queries[i], e);
          }

        }
      }
    };

    vm3.invoke(executeQueries);

    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        assertEquals(0, PortfolioPdx.numInstance);
        assertEquals(0, PositionPdx.numInstance);
      }
    });


    // Create index
    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        QueryService qs = getCache().getQueryService();
        try {
          qs.createIndex("pkIndex", IndexType.FUNCTIONAL, "portfolio.Pk", regName + " portfolio");
          qs.createIndex("idIndex", IndexType.FUNCTIONAL, "pos.secId",
              regName + " p, p.positions.values AS pos");
          qs.createIndex("tickerIndex", IndexType.FUNCTIONAL, "pf.position1.secId",
              regName + " pf");
          qs.createIndex("secIdIndexPf1", IndexType.FUNCTIONAL, "pos11.secId",
              regName + " pf1, pf1.collectionHolderMap.values coll1, pf1.positions.values pos11");
          qs.createIndex("secIdIndexPf2", IndexType.FUNCTIONAL, "pos22.secId",
              regName2 + " pf2, pf2.collectionHolderMap.values coll2, pf2.positions.values pos22");
        } catch (Exception ex) {
          fail("Unable to create index. " + ex.getMessage());
        }
      }
    });

    vm3.invoke(executeQueries);

    // Check for TestObject instances.
    // It should be 0
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        assertEquals(0, PortfolioPdx.numInstance);
        assertEquals(0, PositionPdx.numInstance);
      }
    });

    // index is created on portfolio.Pk field which does not exists in
    // PorfolioPdx object
    // but there is a method getPk(), so for #44436, the objects are
    // deserialized to get the value in vm1
    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        assertEquals(numberOfEntries, PortfolioPdx.numInstance);
        // 50 PorforlioPdx objects create (50*3)+50+50+50+25 = 325 PositionPdx
        // objects when deserialized
        assertEquals(325, PositionPdx.numInstance);
      }
    });

    vm2.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        assertEquals(0, PortfolioPdx.numInstance);
        assertEquals(0, PositionPdx.numInstance);
      }
    });

    this.closeClient(vm2);
    this.closeClient(vm3);
    this.closeClient(vm1);
    this.closeClient(vm0);
  }



  /**
   * Tests identity of Pdx.
   */
  @Test
  public void testPdxIdentity() throws CacheException {

    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    final int numberOfEntries = 10;
    final String queryStr =
        "SELECT DISTINCT * FROM " + this.regName + " pf where pf.ID > 2 and pf.ID < 10";
    // Start server1
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer();
        Region region = getRootRegion().getSubregion(regionName);
      }
    });

    // Start server2
    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer();
        Region region = getRootRegion().getSubregion(regionName);
      }
    });


    // Client pool.
    final int port0 = vm0.invoke(() -> PdxQueryDUnitTest.getCacheServerPort());
    final int port1 = vm1.invoke(() -> PdxQueryDUnitTest.getCacheServerPort());

    final String host0 = NetworkUtils.getServerHostName(vm0.getHost());

    // Create client pool.
    final String poolName = "testClientServerQueryPool";
    createPool(vm2, poolName, new String[] {host0}, new int[] {port0}, true);
    createPool(vm3, poolName, new String[] {host0}, new int[] {port1}, true);

    final int dupIndex = 2;

    // Create client region
    vm3.invoke(new CacheSerializableRunnable("Create region") {
      public void run2() throws CacheException {

        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.LOCAL);
        ClientServerTestCase.configureConnectionPool(factory, host0, port1, -1, true, -1, -1, null);
        Region region = createRegion(regionName, rootRegionName, factory.create());
        int j = 0;
        for (int i = 0; i < numberOfEntries * 2; i++) {
          // insert duplicate values.
          if (i % dupIndex == 0) {
            j++;
          }
          region.put("key-" + i, new PortfolioPdx(j));
        }
      }
    });

    // Execute client queries
    SerializableRunnable executeQueries = new CacheSerializableRunnable("Execute queries") {
      public void run2() throws CacheException {
        QueryService remoteQueryService = null;
        QueryService localQueryService = null;
        SelectResults[][] rs = new SelectResults[1][2];
        try {
          remoteQueryService = (PoolManager.find(poolName)).getQueryService();
          localQueryService = getCache().getQueryService();
        } catch (Exception e) {
          Assert.fail("Failed to get QueryService.", e);
        }

        int expectedResultSize = 7;

        try {
          logger.info("### Executing Query on server:" + queryStr);
          Query query = remoteQueryService.newQuery(queryStr);
          rs[0][0] = (SelectResults) query.execute();
          assertEquals(expectedResultSize, rs[0][0].size());

          logger.info("### Executing Query locally:" + queryStr);
          query = localQueryService.newQuery(queryStr);
          rs[0][1] = (SelectResults) query.execute();
          assertEquals(expectedResultSize, rs[0][1].size());
          logger.info("### Remote Query rs size: " + (rs[0][0]).size() + "Local Query rs size: "
              + (rs[0][1]).size());

          // Compare local and remote query results.
          if (!CacheUtils.compareResultsOfWithAndWithoutIndex(rs)) {
            fail("Local and Remote Query Results are not matching for query :" + queryStr);
          }
        } catch (Exception e) {
          Assert.fail("Failed executing " + queryStr, e);
        }
      }
    };

    // vm2.invoke(executeQueries);
    vm3.invoke(executeQueries);

    // Check for TestObject instances on Server2.
    // It should be 0
    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        assertEquals(0, PortfolioPdx.numInstance);
      }
    });

    this.closeClient(vm2);
    this.closeClient(vm3);
    this.closeClient(vm1);
    this.closeClient(vm0);
  }

  /**
   * Tests function calls in the query.
   */
  @Test
  public void testFunctionCalls() throws CacheException {
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    final int numberOfEntries = 10;
    final String[] queryStr =
        new String[] {"SELECT * FROM " + this.regName + " pf where pf.getIdValue() > 0", // 0
            "SELECT * FROM " + this.regName + " pf where pf.test.getId() > 0", // 1
            "SELECT * FROM " + this.regName + " pf, pf.positions.values pos where "
                + "pos.getSecId() != 'VMWARE'", // 2
            "SELECT * FROM " + this.regName + " pf, pf.positions.values pos where "
                + "pf.getIdValue() > 0 and pos.getSecId() != 'VMWARE'", // 3
            "SELECT * FROM " + this.regName + " pf, pf.getPositions('test').values pos where "
                + "pos.getSecId() != 'VMWARE'", // 4
            "SELECT * FROM " + this.regName + " pf, pf.getPositions('test').values pos where "
                + "pf.id > 0 and pos.getSecId() != 'IBM'", // 5
            "SELECT * FROM " + this.regName + " pf, pf.getPositions('test').values pos where "
                + "pf.getIdValue() > 0 and pos.secId != 'IBM'", // 6
        };

    final int numPositionsPerTestObject = 2;

    final int[] numberOfTestObjectForAllQueries = new int[] {numberOfEntries, // Query 0
        0, // Query 1
        0, // Query 2
        numberOfEntries, // Query 3
        numberOfEntries, // Query 4
        numberOfEntries, // Query 5
        numberOfEntries, // Query 6
    };

    final int[] numberOfTestObject2ForAllQueries = new int[] {numberOfEntries, // Query 0
        numberOfEntries, // Query 1
        0, // Query 2
        numberOfEntries, // Query 3
        numberOfEntries, // Query 4
        numberOfEntries, // Query 5
        numberOfEntries, // Query 6
    };

    final int[] numberOfPositionPdxForAllQueries =
        new int[] {(numberOfEntries * numPositionsPerTestObject), // Query 0
            0, // Query 1
            (numberOfEntries * numPositionsPerTestObject), // 2
            (numberOfEntries * numPositionsPerTestObject * 2), // 3
            (numberOfEntries * numPositionsPerTestObject), // 4
            (numberOfEntries * numPositionsPerTestObject), // 5
            (numberOfEntries * numPositionsPerTestObject), // 6
        };


    // Start server1
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer();
        Region region = getRootRegion().getSubregion(regionName);
      }
    });

    // Start server2
    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer();
        Region region = getRootRegion().getSubregion(regionName);
      }
    });


    // Client pool.
    final int port0 = vm0.invoke(() -> PdxQueryDUnitTest.getCacheServerPort());
    final int port1 = vm1.invoke(() -> PdxQueryDUnitTest.getCacheServerPort());

    final String host0 = NetworkUtils.getServerHostName(vm0.getHost());

    // Create client pool.
    final String poolName = "testClientServerQueryPool";
    createPool(vm2, poolName, new String[] {host0}, new int[] {port0}, true);
    createPool(vm3, poolName, new String[] {host0}, new int[] {port1}, true);

    final int dupIndex = 2;

    // Create client region
    vm3.invoke(new CacheSerializableRunnable("Create region") {
      public void run2() throws CacheException {

        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.LOCAL);
        ClientServerTestCase.configureConnectionPool(factory, host0, port1, -1, true, -1, -1, null);
        Region region = createRegion(regionName, rootRegionName, factory.create());
        int j = 0;
        for (int i = 0; i < numberOfEntries; i++) {
          // insert duplicate values.
          if (i % dupIndex == 0) {
            j++;
          }
          region.put("key-" + i, new TestObject(j, "vmware", numPositionsPerTestObject));
        }
      }
    });


    for (int i = 0; i < queryStr.length; i++) {
      final int testObjectCnt = numberOfTestObjectForAllQueries[i];
      final int positionObjectCnt = numberOfPositionPdxForAllQueries[i];
      final int testObjCnt = numberOfTestObject2ForAllQueries[i];

      executeClientQueries(vm3, poolName, queryStr[i]);
      // Check for TestObject instances on Server2.

      vm1.invoke(new CacheSerializableRunnable("validate") {
        public void run2() throws CacheException {
          assertEquals(testObjectCnt, TestObject.numInstance);
          assertEquals(positionObjectCnt, PositionPdx.numInstance);
          assertEquals(testObjCnt, TestObject2.numInstance);

          // Reset the instances
          TestObject.numInstance = 0;
          PositionPdx.numInstance = 0;
          TestObject2.numInstance = 0;
        }
      });
    }

    this.closeClient(vm2);
    this.closeClient(vm3);
    this.closeClient(vm1);
    this.closeClient(vm0);
  }

  /**
   * This test creates 3 cache servers with a PR and one client which puts some PDX values in PR and
   * runs a query. This was failing randomly in a POC.
   */
  @Test
  public void testPutAllWithIndexes() {
    final String name = "testRegion";
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);

    final Properties config = new Properties();
    config.setProperty("locators", "localhost[" + DistributedTestUtils.getDUnitLocatorPort() + "]");

    // Start server
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        Cache cache = new CacheFactory(config).create();
        AttributesFactory factory = new AttributesFactory();
        PartitionAttributesFactory prfactory = new PartitionAttributesFactory();
        prfactory.setRedundantCopies(0);
        factory.setPartitionAttributes(prfactory.create());
        cache.createRegionFactory(factory.create()).create(name);
        try {
          startCacheServer(0, false);
        } catch (Exception ex) {
          Assert.fail("While starting CacheServer", ex);
        }
        // Create Index on empty region
        try {
          cache.getQueryService().createIndex("myFuncIndex", "intId", "/" + name);
        } catch (Exception e) {
          Assert.fail("index creation failed", e);
        }
      }
    });

    // Start server
    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        Cache cache = new CacheFactory(config).create();
        AttributesFactory factory = new AttributesFactory();
        PartitionAttributesFactory prfactory = new PartitionAttributesFactory();
        prfactory.setRedundantCopies(0);
        factory.setPartitionAttributes(prfactory.create());
        cache.createRegionFactory(factory.create()).create(name);
        try {
          startCacheServer(0, false);
        } catch (Exception ex) {
          Assert.fail("While starting CacheServer", ex);
        }
      }
    });

    // Start server
    vm2.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        Cache cache = new CacheFactory(config).create();
        AttributesFactory factory = new AttributesFactory();
        PartitionAttributesFactory prfactory = new PartitionAttributesFactory();
        prfactory.setRedundantCopies(0);
        factory.setPartitionAttributes(prfactory.create());
        cache.createRegionFactory(factory.create()).create(name);
        try {
          startCacheServer(0, false);
        } catch (Exception ex) {
          Assert.fail("While starting CacheServer", ex);
        }
      }
    });

    // Create client region
    final int port = vm0.invoke(() -> PdxQueryDUnitTest.getCacheServerPort());
    final String host0 = NetworkUtils.getServerHostName(vm2.getHost());
    vm3.invoke(new CacheSerializableRunnable("Create region") {
      public void run2() throws CacheException {
        Properties config = new Properties();
        config.setProperty("mcast-port", "0");
        ClientCache cache = new ClientCacheFactory(config).addPoolServer(host0, port)
            .setPoolPRSingleHopEnabled(true).setPoolSubscriptionEnabled(true).create();
        AttributesFactory factory = new AttributesFactory();
        cache.createClientRegionFactory(ClientRegionShortcut.PROXY).create(name);
      }
    });

    vm3.invoke(new CacheSerializableRunnable("putAll() test") {

      @Override
      public void run2() throws CacheException {
        try {
          ClientCache cache = new ClientCacheFactory().create();
          Region region = cache.getRegion(name);
          QueryService queryService = cache.getQueryService();
          String k;
          for (int x = 0; x < 285; x++) {
            k = Integer.valueOf(x).toString();
            PortfolioPdx v = new PortfolioPdx(x, x);
            region.put(k, v);
          }
          Query q = queryService.newQuery("SELECT DISTINCT * from /" + name + " WHERE ID = 2");
          SelectResults qResult = (SelectResults) q.execute();
          for (Object o : qResult.asList()) {
            System.out.println("o = " + o);
          }
        } catch (Exception e) {
          Assert.fail("Querying failed: ", e);
        }
      }
    });

    Invoke.invokeInEveryVM(DistributedTestCase.class, "disconnectFromDS");
    // }
  }

  /**
   * In PeerTypeRegistration when a PdxType is updated, a local map of class => PdxTypes is
   * populated. This map is used to search a field for a class in different versions (PdxTypes) This
   * test verifies that the map is being updated by the cacheListener
   *
   */
  @Test
  public void testLocalMapInPeerTypePdxRegistry() throws CacheException {

    final Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    final VM vm1 = host.getVM(1);
    final VM vm2 = host.getVM(2);
    final VM vm3 = host.getVM(3);
    final int numberOfEntries = 10;
    final String name = "/" + regionName;
    final String[] qs = {"select * from " + name + " where pdxStatus = 'active'",
        "select pdxStatus from " + name + " where id > 4"};

    // Start server1
    final int port1 = (Integer) vm0.invoke(new SerializableCallable("Create Server1") {
      @Override
      public Object call() throws Exception {
        getCache().createRegionFactory(RegionShortcut.REPLICATE).create(regionName);

        CacheServer server = getCache().addCacheServer();
        int port = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
        server.setPort(port);
        server.start();
        return port;
      }
    });

    // Start server2
    final int port2 = (Integer) vm1.invoke(new SerializableCallable("Create Server2") {
      @Override
      public Object call() throws Exception {
        Region r1 = getCache().createRegionFactory(RegionShortcut.REPLICATE).create(regionName);

        CacheServer server = getCache().addCacheServer();
        int port = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
        server.setPort(port);
        server.start();
        return port;
      }
    });

    // client1 loads version 1 objects on server1
    vm2.invoke(new SerializableCallable("Create client") {
      @Override
      public Object call() throws Exception {
        ClientCacheFactory cf = new ClientCacheFactory();
        cf.addPoolServer(NetworkUtils.getServerHostName(vm0.getHost()), port1);
        ClientCache cache = getClientCache(cf);
        Region region =
            cache.createClientRegionFactory(ClientRegionShortcut.PROXY).create(regionName);

        // Load version 1 objects
        for (int i = 0; i < numberOfEntries; i++) {
          PdxInstanceFactory pdxInstanceFactory =
              PdxInstanceFactoryImpl.newCreator("PdxVersionedNewPortfolio", false, getCache());
          pdxInstanceFactory.writeInt("id", i);
          pdxInstanceFactory.writeString("pdxStatus", (i % 2 == 0 ? "active" : "inactive"));
          PdxInstance pdxInstance = pdxInstanceFactory.create();
          region.put("key-" + i, pdxInstance);
        }

        return null;
      }
    });

    // client 2 loads version 2 objects on server2
    vm3.invoke(new SerializableCallable("Create client") {
      @Override
      public Object call() throws Exception {
        ClientCacheFactory cf = new ClientCacheFactory();
        cf.addPoolServer(NetworkUtils.getServerHostName(vm1.getHost()), port2);
        ClientCache cache = getClientCache(cf);
        Region region =
            cache.createClientRegionFactory(ClientRegionShortcut.PROXY).create(regionName);
        // Load version 2 objects
        for (int i = numberOfEntries; i < numberOfEntries * 2; i++) {
          PdxInstanceFactory pdxInstanceFactory =
              PdxInstanceFactoryImpl.newCreator("PdxVersionedNewPortfolio", false, getCache());
          pdxInstanceFactory.writeInt("id", i);
          pdxInstanceFactory.writeString("status", (i % 2 == 0 ? "active" : "inactive"));
          PdxInstance pdxInstance = pdxInstanceFactory.create();
          region.put("key-" + i, pdxInstance);
        }
        return null;
      }
    });

    final SerializableRunnableIF verifyTwoPdxTypesArePresent = () -> {
      TypeRegistration registration = getCache().getPdxRegistry().getTypeRegistration();
      assertTrue(registration instanceof PeerTypeRegistration);

      final Map<Integer, PdxType> types = registration.types();
      assertEquals(2, types.size());
      for (PdxType type : types.values()) {
        assertEquals("PdxVersionedNewPortfolio", type.getClassName());
      }
    };

    vm0.invoke(verifyTwoPdxTypesArePresent);
    vm1.invoke(verifyTwoPdxTypesArePresent);
  }

  /**
   * Test to query a field that is not present in the Pdx object but has a get method
   *
   */
  @Test
  public void testPdxInstanceWithMethodButNoField() throws CacheException {

    final Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    final VM vm1 = host.getVM(1);
    final VM vm2 = host.getVM(2);
    final VM vm3 = host.getVM(3);
    final int numberOfEntries = 10;
    final String name = "/" + regionName;
    final String[] qs = {"select * from " + name + " where status = 'active'",
        "select status from " + name + " where id >= 5"};

    // Start server1
    final int port1 = (Integer) vm0.invoke(new SerializableCallable("Create Server1") {
      @Override
      public Object call() throws Exception {
        Region r1 = getCache().createRegionFactory(RegionShortcut.PARTITION).create(regionName);

        CacheServer server = getCache().addCacheServer();
        int port = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
        server.setPort(port);
        server.start();
        return port;
      }
    });

    // Start server2
    final int port2 = (Integer) vm1.invoke(new SerializableCallable("Create Server2") {
      @Override
      public Object call() throws Exception {
        Region r1 = getCache().createRegionFactory(RegionShortcut.PARTITION).create(regionName);

        CacheServer server = getCache().addCacheServer();
        int port = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
        server.setPort(port);
        server.start();
        return port;
      }
    });

    // Start server3
    final int port3 = (Integer) vm2.invoke(new SerializableCallable("Create Server3") {
      @Override
      public Object call() throws Exception {
        Region r1 = getCache().createRegionFactory(RegionShortcut.PARTITION).create(regionName);

        CacheServer server = getCache().addCacheServer();
        int port = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
        server.setPort(port);
        server.start();
        return port;
      }
    });

    // create client
    vm3.invoke(new SerializableCallable("Create client") {
      @Override
      public Object call() throws Exception {
        ClientCacheFactory cf = new ClientCacheFactory();
        cf.addPoolServer(NetworkUtils.getServerHostName(vm0.getHost()), port1);
        cf.addPoolServer(NetworkUtils.getServerHostName(vm1.getHost()), port2);
        cf.addPoolServer(NetworkUtils.getServerHostName(vm2.getHost()), port3);
        ClientCache cache = getClientCache(cf);
        Region region =
            cache.createClientRegionFactory(ClientRegionShortcut.PROXY).create(regionName);

        for (int i = 0; i < numberOfEntries; i++) {
          region.put("key-" + i, new TestObject(i, "vmware"));
        }
        return null;
      }
    });

    vm3.invoke(new SerializableCallable("Create client") {
      @Override
      public Object call() throws Exception {
        QueryService remoteQueryService = null;
        // Execute query remotely
        try {
          remoteQueryService = getCache().getQueryService();
        } catch (Exception e) {
          Assert.fail("Failed to get QueryService.", e);
        }

        for (int i = 0; i < qs.length; i++) {
          try {
            SelectResults sr = (SelectResults) remoteQueryService.newQuery(qs[i]).execute();
            assertEquals(5, sr.size());
          } catch (Exception e) {
            Assert.fail("Failed executing " + qs[i], e);
          }
        }
        return null;
      }
    });

    // create index
    vm0.invoke(new SerializableCallable("Query") {
      @Override
      public Object call() throws Exception {
        QueryService qs = null;
        try {
          qs = getCache().getQueryService();
          qs.createIndex("status", "status", name);
        } catch (Exception e) {
          Assert.fail("Exception getting query service ", e);
        }

        return null;
      }
    });

    // create client
    vm3.invoke(new SerializableCallable("Create client") {
      @Override
      public Object call() throws Exception {

        QueryService remoteQueryService = null;
        // Execute query remotely
        try {
          remoteQueryService = getCache().getQueryService();
        } catch (Exception e) {
          Assert.fail("Failed to get QueryService.", e);
        }

        for (int i = 0; i < qs.length; i++) {
          try {
            SelectResults sr = (SelectResults) remoteQueryService.newQuery(qs[i]).execute();
            assertEquals(5, sr.size());
          } catch (Exception e) {
            Assert.fail("Failed executing " + qs[i], e);
          }
        }
        return null;
      }
    });
    Invoke.invokeInEveryVM(DistributedTestCase.class, "disconnectFromDS");
  }


  /**
   * Test to query a field that is not present in the Pdx object but is present in some other
   * version of the pdx instance
   *
   */
  @Test
  public void testPdxInstanceFieldInOtherVersion() throws CacheException {

    final Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    final VM vm1 = host.getVM(1);
    final VM vm2 = host.getVM(2);
    final VM vm3 = host.getVM(3);
    final int numberOfEntries = 10;
    final String name = "/" + regionName;
    final String[] qs = {"select pdxStatus from " + name + " where pdxStatus = 'active'",
        "select pdxStatus from " + name + " where id > 8 and id < 14"};

    // Start server1
    final int port1 = (Integer) vm0.invoke(new SerializableCallable("Create Server1") {
      @Override
      public Object call() throws Exception {
        Region r1 = getCache().createRegionFactory(RegionShortcut.REPLICATE).create(regionName);

        CacheServer server = getCache().addCacheServer();
        int port = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
        server.setPort(port);
        server.start();
        return port;
      }
    });

    // Start server2
    final int port2 = (Integer) vm1.invoke(new SerializableCallable("Create Server2") {
      @Override
      public Object call() throws Exception {
        Region r1 = getCache().createRegionFactory(RegionShortcut.REPLICATE).create(regionName);

        CacheServer server = getCache().addCacheServer();
        int port = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
        server.setPort(port);
        server.start();
        return port;
      }
    });

    // client1 loads version 1 objects on server1
    vm2.invoke(new SerializableCallable("Create client") {
      @Override
      public Object call() throws Exception {
        ClientCacheFactory cf = new ClientCacheFactory();
        cf.addPoolServer(NetworkUtils.getServerHostName(vm0.getHost()), port1);
        ClientCache cache = getClientCache(cf);
        Region region =
            cache.createClientRegionFactory(ClientRegionShortcut.PROXY).create(regionName);

        // Load version 1 objects
        for (int i = 0; i < numberOfEntries; i++) {
          PdxInstanceFactory pdxInstanceFactory =
              PdxInstanceFactoryImpl.newCreator("PdxVersionedNewPortfolio", false, getCache());
          pdxInstanceFactory.writeInt("id", i);
          pdxInstanceFactory.writeString("pdxStatus", (i % 2 == 0 ? "active" : "inactive"));
          PdxInstance pdxInstance = pdxInstanceFactory.create();
          region.put("key-" + i, pdxInstance);
        }

        return null;
      }
    });

    // client 2 loads version 2 objects on server2
    vm3.invoke(new SerializableCallable("Create client") {
      @Override
      public Object call() throws Exception {
        ClientCacheFactory cf = new ClientCacheFactory();
        cf.addPoolServer(NetworkUtils.getServerHostName(vm1.getHost()), port2);
        ClientCache cache = getClientCache(cf);
        Region region =
            cache.createClientRegionFactory(ClientRegionShortcut.PROXY).create(regionName);
        // Load version 2 objects
        for (int i = numberOfEntries; i < numberOfEntries * 2; i++) {
          PdxInstanceFactory pdxInstanceFactory =
              PdxInstanceFactoryImpl.newCreator("PdxVersionedNewPortfolio", false, getCache());
          pdxInstanceFactory.writeInt("id", i);
          pdxInstanceFactory.writeString("status", (i % 2 == 0 ? "active" : "inactive"));
          PdxInstance pdxInstance = pdxInstanceFactory.create();
          region.put("key-" + i, pdxInstance);
        }
        return null;
      }
    });

    // query remotely from client 1 with version 1 in classpath
    vm2.invoke(new SerializableCallable("Create client") {
      @Override
      public Object call() throws Exception {
        QueryService remoteQueryService = null;
        // Execute query remotely
        try {
          remoteQueryService = getCache().getQueryService();
        } catch (Exception e) {
          Assert.fail("Failed to get QueryService.", e);
        }

        for (int i = 0; i < qs.length; i++) {
          try {
            SelectResults sr = (SelectResults) remoteQueryService.newQuery(qs[i]).execute();
            assertEquals(5, sr.size());
            if (i == 1) {
              for (Object o : sr) {
                if (o == null) {
                } else if (o instanceof String) {
                } else {
                  fail("Result should be either null or String and not " + o.getClass());
                }
              }
            }
          } catch (Exception e) {
            Assert.fail("Failed executing " + qs[i], e);
          }
        }
        return null;
      }
    });

    // query remotely from client 2 with version 2 in classpath
    vm3.invoke(new SerializableCallable("Create client") {
      @Override
      public Object call() throws Exception {
        QueryService remoteQueryService = null;
        // Execute query remotely
        try {
          remoteQueryService = getCache().getQueryService();
        } catch (Exception e) {
          Assert.fail("Failed to get QueryService.", e);
        }

        for (int i = 0; i < qs.length; i++) {
          try {
            SelectResults sr = (SelectResults) remoteQueryService.newQuery(qs[i]).execute();
            assertEquals(5, sr.size());
            if (i == 1) {
              for (Object o : sr) {
                if (o == null) {
                } else if (o instanceof String) {
                } else {
                  fail("Result should be either null or String and not " + o.getClass());
                }
              }
            }
          } catch (Exception e) {
            Assert.fail("Failed executing " + qs[i], e);
          }
        }
        return null;
      }
    });

    // query locally on server
    vm0.invoke(new SerializableCallable("Create client") {
      @Override
      public Object call() throws Exception {
        GemFireCacheImpl cache = (GemFireCacheImpl) getCache();
        cache.setReadSerializedForTest(true);
        QueryService queryService = null;
        try {
          queryService = getCache().getQueryService();
        } catch (Exception e) {
          Assert.fail("Failed to get QueryService.", e);
        }

        for (int i = 0; i < qs.length; i++) {
          try {
            SelectResults sr = (SelectResults) queryService.newQuery(qs[i]).execute();
            assertEquals(5, sr.size());
            if (i == 1) {
              for (Object o : sr) {
                if (o == null) {
                } else if (o instanceof String) {
                } else {
                  fail("Result should be either null or String and not " + o.getClass());
                }
              }
            }
          } catch (Exception e) {
            Assert.fail("Failed executing " + qs[i], e);
          }
        }
        return null;
      }
    });

    // create index
    vm0.invoke(new SerializableCallable("Query") {
      @Override
      public Object call() throws Exception {
        QueryService qs = null;
        try {
          qs = getCache().getQueryService();
          qs.createIndex("status", "status", name);
        } catch (Exception e) {
          Assert.fail("Exception getting query service ", e);
        }

        return null;
      }
    });

    // query from client 1 with version 1 in classpath
    vm2.invoke(new SerializableCallable("Create client") {
      @Override
      public Object call() throws Exception {

        QueryService remoteQueryService = null;
        // Execute query remotely
        try {
          remoteQueryService = getCache().getQueryService();
        } catch (Exception e) {
          Assert.fail("Failed to get QueryService.", e);
        }

        for (int i = 0; i < qs.length; i++) {
          try {
            SelectResults sr = (SelectResults) remoteQueryService.newQuery(qs[i]).execute();
            assertEquals(5, sr.size());
          } catch (Exception e) {
            Assert.fail("Failed executing " + qs[i], e);
          }
        }
        return null;
      }
    });

    Invoke.invokeInEveryVM(DistributedTestCase.class, "disconnectFromDS");
  }

  /**
   * 2 servers(replicated) and 2 clients. client2 puts version1 and version2 objects on server1
   * client1 had registered interest to server2, hence gets the pdx objects for both versions Test
   * local query on client1 Test if client1 fetched pdxtypes from server
   *
   */
  @Test
  public void testClientForFieldInOtherVersion() throws CacheException {

    final Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    final VM vm1 = host.getVM(1);
    final VM vm2 = host.getVM(2);
    final VM vm3 = host.getVM(3);
    final int numberOfEntries = 10;
    final String name = "/" + regionName;
    final String[] qs = {"select pdxStatus from " + name + " where pdxStatus = 'active'",
        "select status from " + name + " where id > 8 and id < 14"};

    // Start server1
    final int port1 = (Integer) vm0.invoke(new SerializableCallable("Create Server1") {
      @Override
      public Object call() throws Exception {
        Region r1 = getCache().createRegionFactory(RegionShortcut.REPLICATE).create(regionName);

        CacheServer server = getCache().addCacheServer();
        int port = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
        server.setPort(port);
        server.start();
        return port;
      }
    });

    // Start server2
    final int port2 = (Integer) vm1.invoke(new SerializableCallable("Create Server2") {
      @Override
      public Object call() throws Exception {
        Region r1 = getCache().createRegionFactory(RegionShortcut.REPLICATE).create(regionName);

        CacheServer server = getCache().addCacheServer();
        int port = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
        server.setPort(port);
        server.start();
        return port;
      }
    });

    // client 1 registers interest for server2
    vm2.invoke(new SerializableCallable("Create client") {
      @Override
      public Object call() throws Exception {
        ClientCacheFactory cf = new ClientCacheFactory();
        cf.setPoolSubscriptionEnabled(true);
        cf.addPoolServer(NetworkUtils.getServerHostName(vm1.getHost()), port2);
        ClientCache cache = getClientCache(cf);
        Region region =
            cache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY).create(regionName);
        region.registerInterest("ALL_KEYS");
        return null;
      }
    });

    // client2 loads both version objects on server1
    vm3.invoke(new SerializableCallable("Create client") {
      @Override
      public Object call() throws Exception {
        ClientCacheFactory cf = new ClientCacheFactory();
        cf.addPoolServer(NetworkUtils.getServerHostName(vm0.getHost()), port1);
        ClientCache cache = getClientCache(cf);
        Region region =
            cache.createClientRegionFactory(ClientRegionShortcut.PROXY).create(regionName);

        // Load version 1 objects
        for (int i = 0; i < numberOfEntries; i++) {
          PdxInstanceFactory pdxFactory = cache.createPdxInstanceFactory("PdxPortfolio");
          pdxFactory.writeString("pdxStatus", (i % 2 == 0 ? "active" : "inactive"));
          pdxFactory.writeInt("id", i);
          PdxInstance pdxInstance = pdxFactory.create();
          region.put("key-" + i, pdxInstance);

        } ;
        // Load version 2 objects
        for (int i = numberOfEntries; i < numberOfEntries * 2; i++) {
          PdxInstanceFactory pdxFactory = cache.createPdxInstanceFactory("PdxPortfolio");
          pdxFactory.writeString("status", i % 2 == 0 ? "active" : "inactive");
          pdxFactory.writeInt("id", i);
          PdxInstance pdxInstance = pdxFactory.create();
          region.put("key-" + i, pdxInstance);
        }

        return null;
      }
    });

    // query locally on client 1 which has registered interest
    vm2.invoke(new SerializableCallable("Create client") {
      @Override
      public Object call() throws Exception {
        GemFireCacheImpl cache = (GemFireCacheImpl) getCache();
        cache.setReadSerializedForTest(true);
        QueryService localQueryService = null;
        // Execute query remotely
        try {
          localQueryService = ((ClientCache) getCache()).getLocalQueryService();
        } catch (Exception e) {
          Assert.fail("Failed to get QueryService.", e);
        }

        for (int i = 0; i < qs.length; i++) {
          try {
            SelectResults sr = (SelectResults) localQueryService.newQuery(qs[i]).execute();
            assertEquals(5, sr.size());
            if (i == 1) {
              for (Object o : sr) {
                if (o == null) {
                } else if (o instanceof String) {
                } else {
                  fail("Result should be either null or String and not " + o.getClass());
                }
              }
            }
          } catch (Exception e) {
            Assert.fail("Failed executing " + qs[i], e);
          }
        }
        // check if the types registered on server are fetched by the client
        TypeRegistration registration = getCache().getPdxRegistry().getTypeRegistration();
        assertTrue(registration instanceof ClientTypeRegistration);
        Map<Integer, PdxType> m = ((ClientTypeRegistration) registration).types();
        assertEquals(2, m.size());
        for (PdxType type : m.values()) {
          assertEquals("PdxPortfolio", type.getClassName());
        }
        return null;
      }
    });
    Invoke.invokeInEveryVM("Disconnecting from the Distributed system", () -> disconnectFromDS());
  }

  /**
   * Test to query a field that is not present in the Pdx object Also the implicit method is absent
   * in the class
   *
   */
  @Test
  public void testPdxInstanceNoFieldNoMethod() throws CacheException {

    final Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    final VM vm3 = host.getVM(3);
    final int numberOfEntries = 10;
    final String name = "/" + regionName;
    final String[] qs = {"select * from " + name + " where pdxStatus = 'active'",
        "select pdxStatus from " + name + " where id > 4"};

    // Start server1
    final int port1 = (Integer) vm0.invoke(new SerializableCallable("Create Server1") {
      @Override
      public Object call() throws Exception {
        Region r1 = getCache().createRegionFactory(RegionShortcut.REPLICATE).create(regionName);

        CacheServer server = getCache().addCacheServer();
        int port = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
        server.setPort(port);
        server.start();
        return port;
      }
    });

    // create client and load only version 1 objects with no pdxStatus field
    vm3.invoke(new SerializableCallable("Create client") {
      @Override
      public Object call() throws Exception {
        ClientCacheFactory cf = new ClientCacheFactory();
        cf.addPoolServer(NetworkUtils.getServerHostName(vm0.getHost()), port1);
        ClientCache cache = getClientCache(cf);
        Region region =
            cache.createClientRegionFactory(ClientRegionShortcut.PROXY).create(regionName);

        // Load version 1 objects
        for (int i = 0; i < numberOfEntries; i++) {
          PdxInstanceFactory pdxInstanceFactory =
              PdxInstanceFactoryImpl.newCreator("PdxVersionedNewPortfolio", false, getCache());
          pdxInstanceFactory.writeInt("id", i);
          pdxInstanceFactory.writeString("status", (i % 2 == 0 ? "active" : "inactive"));
          PdxInstance pdxInstance = pdxInstanceFactory.create();
          region.put("key-" + i, pdxInstance);
        }
        return null;
      }
    });

    // Version1 class loader
    vm3.invoke(new SerializableCallable("Create client") {
      @Override
      public Object call() throws Exception {
        // Load version 1 classloader
        QueryService remoteQueryService = null;
        // Execute query remotely
        try {
          remoteQueryService = getCache().getQueryService();
        } catch (Exception e) {
          Assert.fail("Failed to get QueryService.", e);
        }

        for (int i = 0; i < qs.length; i++) {
          try {
            SelectResults sr = (SelectResults) remoteQueryService.newQuery(qs[i]).execute();
            if (i == 1) {
              assertEquals(5, sr.size());
              for (Object o : sr) {
                if (!(o instanceof Undefined)) {
                  fail("Result should be Undefined and not " + o.getClass());
                }
              }
            } else {
              assertEquals(0, sr.size());
            }
          } catch (Exception e) {
            Assert.fail("Failed executing " + qs[i], e);
          }
        }
        return null;
      }
    });

    Invoke.invokeInEveryVM(DistributedTestCase.class, "disconnectFromDS");
  }

  /**
   * Test query execution when default values of {@link FieldType} are used. This happens when one
   * version of Pdx object does not have a field but other version has.
   *
   */
  @Test
  public void testDefaultValuesInPdxFieldTypes() throws Exception {
    final Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    final VM vm1 = host.getVM(1);
    final int numberOfEntries = 10;
    final String name = "/" + regionName;
    final String query =
        "select stringField, booleanField, charField, shortField, intField, longField, floatField, doubleField from "
            + name;

    // Start server1
    final int port1 = (Integer) vm0.invoke(new SerializableCallable("Create Server1") {
      @Override
      public Object call() throws Exception {
        Region r1 = getCache().createRegionFactory(RegionShortcut.REPLICATE).create(regionName);
        CacheServer server = getCache().addCacheServer();
        int port = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
        server.setPort(port);
        server.start();
        return port;
      }
    });

    // client loads version1 and version2 objects on server
    vm1.invoke(new SerializableCallable("Create client") {
      @Override
      public Object call() throws Exception {
        ClientCacheFactory cf = new ClientCacheFactory();
        cf.addPoolServer(NetworkUtils.getServerHostName(vm0.getHost()), port1);
        ClientCache cache = getClientCache(cf);
        Region region =
            cache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY).create(regionName);

        // Load version 1 objects
        for (int i = 0; i < numberOfEntries; i++) {
          PdxInstanceFactory pdxInstanceFactory =
              PdxInstanceFactoryImpl.newCreator("PdxVersionedFieldType", false, getCache());
          pdxInstanceFactory.writeString("stringField", "" + i);
          pdxInstanceFactory.writeBoolean("booleanField", (i % 2 == 0 ? true : false));
          pdxInstanceFactory.writeChar("charField", ((char) i));
          pdxInstanceFactory.writeShort("shortField", new Integer(i).shortValue());
          PdxInstance pdxInstance = pdxInstanceFactory.create();
          logger.info("Putting object: " + pdxInstance);
          region.put("key-" + i, pdxInstance);
        }

        // Load version 2 objects
        for (int i = numberOfEntries; i < numberOfEntries * 2; i++) {
          PdxInstanceFactory pdxInstanceFactory =
              PdxInstanceFactoryImpl.newCreator("PdxVersionedFieldType", false, getCache());
          pdxInstanceFactory.writeInt("intField", i);
          pdxInstanceFactory.writeLong("longField", new Integer(i + 1).longValue());
          pdxInstanceFactory.writeFloat("floatField", new Integer(i + 2).floatValue());
          pdxInstanceFactory.writeDouble("doubleField", new Integer(i + 3).doubleValue());
          PdxInstance pdxInstance = pdxInstanceFactory.create();
          logger.info("Putting object: " + pdxInstance);
          region.put("key-" + i, pdxInstance);
        }

        return null;
      }
    });

    // query locally on server, create index, verify results with and without index
    vm0.invoke(new SerializableCallable("Create index") {
      @Override
      public Object call() throws Exception {
        GemFireCacheImpl cache = (GemFireCacheImpl) getCache();
        cache.setReadSerializedForTest(true);

        QueryService qs = null;
        SelectResults[][] sr = new SelectResults[1][2];
        // Execute query locally
        try {
          qs = getCache().getQueryService();
        } catch (Exception e) {
          Assert.fail("Failed to get QueryService.", e);
        }

        try {
          sr[0][0] = (SelectResults) qs.newQuery(query).execute();
          assertEquals(20, sr[0][0].size());

        } catch (Exception e) {
          Assert.fail("Failed executing " + qs, e);
        }
        // create index
        try {
          qs.createIndex("stringIndex", "stringField", name);
          qs.createIndex("booleanIndex", "booleanField", name);
          qs.createIndex("shortIndex", "shortField", name);
          qs.createIndex("charIndex", "charField", name);
          qs.createIndex("intIndex", "intField", name);
          qs.createIndex("longIndex", "longField", name);
          qs.createIndex("floatIndex", "floatField", name);
          qs.createIndex("doubleIndex", "doubleField", name);
        } catch (Exception e) {
          Assert.fail("Exception creating index ", e);
        }

        // query after index creation
        try {
          sr[0][1] = (SelectResults) qs.newQuery(query).execute();
          assertEquals(20, sr[0][1].size());

        } catch (Exception e) {
          Assert.fail("Failed executing " + qs, e);
        }

        CacheUtils.compareResultsOfWithAndWithoutIndex(sr);
        return null;
      }
    });

    // Update index
    vm1.invoke(new SerializableCallable("update index") {
      @Override
      public Object call() throws Exception {

        Region region = getCache().getRegion(regionName);

        // Load version 1 objects
        for (int i = numberOfEntries; i < numberOfEntries * 2; i++) {
          PdxInstanceFactory pdxInstanceFactory =
              PdxInstanceFactoryImpl.newCreator("PdxVersionedFieldType", false, getCache());
          pdxInstanceFactory.writeString("stringField", "" + i);
          pdxInstanceFactory.writeBoolean("booleanField", (i % 2 == 0 ? true : false));
          pdxInstanceFactory.writeChar("charField", ((char) i));
          pdxInstanceFactory.writeShort("shortField", new Integer(i).shortValue());
          PdxInstance pdxInstance = pdxInstanceFactory.create();
          logger.info("Putting object: " + pdxInstance);
          region.put("key-" + i, pdxInstance);
        }

        // Load version 2 objects
        for (int i = 0; i < numberOfEntries; i++) {
          PdxInstanceFactory pdxInstanceFactory =
              PdxInstanceFactoryImpl.newCreator("PdxVersionedFieldType", false, getCache());
          pdxInstanceFactory.writeInt("intField", i);
          pdxInstanceFactory.writeLong("longField", new Integer(i + 1).longValue());
          pdxInstanceFactory.writeFloat("floatField", new Integer(i + 2).floatValue());
          pdxInstanceFactory.writeDouble("doubleField", new Integer(i + 3).doubleValue());
          PdxInstance pdxInstance = pdxInstanceFactory.create();
          logger.info("Putting object: " + pdxInstance);
          region.put("key-" + i, pdxInstance);
        }
        return null;
      }
    });

    // query remotely from client
    vm1.invoke(new SerializableCallable("query") {
      @Override
      public Object call() throws Exception {
        QueryService remoteQueryService = null;
        QueryService localQueryService = null;
        SelectResults[][] sr = new SelectResults[1][2];
        // Execute query locally
        try {
          remoteQueryService = getCache().getQueryService();
          localQueryService = ((ClientCache) getCache()).getLocalQueryService();
        } catch (Exception e) {
          Assert.fail("Failed to get QueryService.", e);
        }
        try {
          sr[0][0] = (SelectResults) remoteQueryService.newQuery(query).execute();
          assertEquals(20, sr[0][0].size());
          sr[0][1] = (SelectResults) localQueryService.newQuery(query).execute();
          assertEquals(20, sr[0][1].size());
        } catch (Exception e) {
          fail("Failed executing query " + e);
        }

        CacheUtils.compareResultsOfWithAndWithoutIndex(sr);

        return null;
      }
    });

    Invoke.invokeInEveryVM(DistributedTestCase.class, "disconnectFromDS");
  }

  /**
   * Tests client-server query on PdxInstance. The client receives projected value.
   */
  @Test
  public void testJSONWithHeterogenousObjectsDifferingFields() throws CacheException {

    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    // Start server1
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer();
        Region region = getCache().createRegionFactory(RegionShortcut.REPLICATE).create("testJson");
        String obj1 =
            "{ \"FirstName\": \"Vinay\", \"LastName\": \"Upadhya\", \"Address\": [ { \"Line1\": \"NYC\", \"phones\": [ { \"number\": \"212\" }, { \"number\": \"313\" } ] }, { \"Line1\": \"NJ\", \"phones\": [ { \"number\": \"412\" }, { \"number\": \"513\" } ] } ] }";
        String obj2 =
            "{ \"FirstName\": \"Vinay\", \"LastName\": \"Upadhya\", \"Address\": [ { \"Line1\": \"NYC\", \"phones\": [ { \"number\": \"212\" }, { \"number\": \"313\" } ] }, { \"Line1\": \"NJ\" } ] }";
        region.put("value1", JSONFormatter.fromJSON(obj1));
        region.put("value2", JSONFormatter.fromJSON(obj2));
      }
    });


    // Start server2
    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer();
        getCache().createRegionFactory(RegionShortcut.REPLICATE).create("testJson");

        // Execute query with different type of Results.
        QueryService qs = getCache().getQueryService();

        try {
          SelectResults results = (SelectResults) qs
              .newQuery(
                  "select r from /testJson r, r.Address a, a.phones pn where pn.number = '412'")
              .execute();
          assertEquals(results.size(), 1);
        } catch (Exception e) {
          e.printStackTrace();
          throw new CacheException(e) {};
        }
      }
    });

    this.closeClient(vm1);
    this.closeClient(vm0);
  }

  /**
   * Tests client-server query on PdxInstance. The client receives projected value.
   */
  @Test
  public void testJSONWithHeterogenousObjectsDifferingFieldTypes() throws CacheException {

    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    // Start server1
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer();
        Region region = getCache().createRegionFactory(RegionShortcut.REPLICATE).create("testJson");
        String obj1 =
            "{\"FirstName\": \"Vinay\", \"LastName\": \"Upadhya\", \"Address\": [ { \"Line1\": \"NYC\" }, { \"Line1\": \"NJ\" } ] }";
        String obj2 =
            "{ \"FirstName\": \"Vinay\", \"LastName\": \"Upadhya\", \"Address\": \"my address\" }";
        region.put("value1", JSONFormatter.fromJSON(obj2));
        region.put("value2", JSONFormatter.fromJSON(obj1));
      }
    });


    // Start server2
    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer();
        getCache().createRegionFactory(RegionShortcut.REPLICATE).create("testJson");
        // Execute query with different type of Results.
        QueryService qs = getCache().getQueryService();
        try {
          SelectResults result = (SelectResults) qs
              .newQuery("select r from /testJson r, r.Address a where a.Line1 = 'NYC'").execute();
          assertEquals(1, result.size());
          result = (SelectResults) qs
              .newQuery("select r from /testJson r, r.Address a where a = 'my address'").execute();
          assertEquals(1, result.size());
        } catch (Exception e) {
          e.printStackTrace();
          throw new CacheException(e) {};
        }
      }
    });

    this.closeClient(vm1);
    this.closeClient(vm0);
  }

  /**
   * Test Aggregate queries on Pdx instances
   */

  @Test
  public void testAggregateQueries() {
    VM vm0 = VM.getVM(0);
    VM vm1 = VM.getVM(1);
    VM vm2 = VM.getVM(2);

    final int port0 = vm0.invoke(() -> {
      configAndStartBridgeServer();
      Region region = getRootRegion().getSubregion(regionName);
      for (int i = 0; i < 10; i++) {
        region.put("key-" + i, new TestObject(i, "val-" + i));
      }
      return getCacheServerPort();
    });

    final int port1 = vm1.invoke(() -> {
      configAndStartBridgeServer();
      return getCacheServerPort();
    });

    final String host0 = NetworkUtils.getServerHostName();

    final String poolName = "testClientServerQueryPool";
    createPool(vm2, poolName, new String[] {host0}, new int[] {port1}, true);

    vm2.invoke(() -> {
      QueryService queryService = PoolManager.find(poolName).getQueryService();
      Query query = queryService.newQuery("select SUM(price) from " + regName + " where id > 0");
      SelectResults<Object> selectResults = (SelectResults) query.execute();
      assertEquals(1, selectResults.size());
      assertEquals(45, selectResults.asList().get(0));
    });

    vm2.invoke(() -> {
      QueryService queryService = PoolManager.find(poolName).getQueryService();
      Query query = queryService.newQuery("select SUM(price) from " + regName + " where id > 9");
      SelectResults<Long> selectResults = (SelectResults) query.execute();
      assertEquals(0, selectResults.size());
      assertEquals(0, selectResults.asList().size());
    });

    this.closeClient(vm0);
    this.closeClient(vm1);
    this.closeClient(vm2);
  }

}

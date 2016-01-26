/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.cache.query.dunit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Comparator;
import java.util.Properties;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.query.QueryInvocationTargetException;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.Struct;
import com.gemstone.gemfire.cache.query.internal.QueryObserverAdapter;
import com.gemstone.gemfire.cache.query.internal.QueryObserverHolder;
import com.gemstone.gemfire.cache.query.internal.ResultsBag;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.cache30.ClientServerTestCase;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;

import cacheRunner.Portfolio;
import cacheRunner.Position;

/**
 * Tests remote (client/server) query execution.
 *
 * @author Barry Oglesby
 * @author Asif
 * @since 5.0.1
 */
public class RemoteQueryDUnitTest extends CacheTestCase {

  /** The port on which the bridge server was started in this VM */
  private static int bridgeServerPort;

  public RemoteQueryDUnitTest(String name) {
    super(name);
  }

  ////////  Test Methods

  @Override
  public void setUp() throws Exception {
    super.setUp();
    disconnectAllFromDS();
  }

  @Override
  public void tearDown2() throws Exception {
    try {
      super.tearDown2();
    }
    finally {
      disconnectAllFromDS();
    }
  }

  /**
   * Tests remote predicate query execution.
   */
  public void testRemotePredicateQueries() throws CacheException {

    final String name = this.getName();
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    final int numberOfEntries = 100;

    // Start server
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
        public void run2() throws CacheException {
          Properties config = new Properties();
          config.setProperty("locators", "localhost["+getDUnitLocatorPort()+"]");
          system = (InternalDistributedSystem) DistributedSystem.connect(config);
          AttributesFactory factory = new AttributesFactory();
          factory.setScope(Scope.LOCAL);
          createRegion(name, factory.create());
          pause(1000);
          try {
            startBridgeServer(0, false);
          } catch (Exception ex) {
            fail("While starting CacheServer", ex);
          }
        }
      });

    // Initialize server region
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          for (int i=0; i<numberOfEntries; i++) {
            region.put("key-"+i, new TestObject(i, "ibm"));
          }
        }
      });

    // Create client region
    final int port = vm0.invokeInt(RemoteQueryDUnitTest.class, "getCacheServerPort");
    final String host0 = getServerHostName(vm0.getHost());
    vm1.invoke(new CacheSerializableRunnable("Create region") {
        public void run2() throws CacheException {
          Properties config = new Properties();
          config.setProperty("mcast-port", "0");
          system = (InternalDistributedSystem) DistributedSystem.connect(config);
          getCache();
          AttributesFactory factory = new AttributesFactory();
          factory.setScope(Scope.LOCAL);
          ClientServerTestCase.configureConnectionPool(factory, host0, port,-1, true, -1, -1, null);
          createRegion(name, factory.create());
        }
      });

    // Execute client queries
    vm1.invoke(new CacheSerializableRunnable("Execute queries") {
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          String queryString = null;
          SelectResults results = null;

          queryString = "ticker = 'ibm'";
          try {
            results = region.query(queryString);
          } catch (Exception e) {
            fail("Failed executing " + queryString, e);
          }
          assertEquals(numberOfEntries, results.size());
          assertTrue(results.getClass() == ResultsBag.class);
          assertTrue(results.getCollectionType().allowsDuplicates() && !results.getCollectionType().getElementType().isStructType());

          queryString = "ticker = 'IBM'";
          try {
            results = region.query(queryString);
          } catch (Exception e) {
            fail("Failed executing " + queryString, e);
          }
          assertEquals(0, results.size());
          assertTrue(results.getClass() == ResultsBag.class);
          assertTrue(results.getCollectionType().allowsDuplicates() && !results.getCollectionType().getElementType().isStructType());

          queryString = "price > 49";
          try {
            results = region.query(queryString);
          } catch (Exception e) {
            fail("Failed executing " + queryString, e);
          }
          assertEquals(numberOfEntries/2, results.size());
          assertTrue(results.getClass() == ResultsBag.class);
          assertTrue(results.getCollectionType().allowsDuplicates() && !results.getCollectionType().getElementType().isStructType());

          queryString = "price = 50";
          try {
            results = region.query(queryString);
          } catch (Exception e) {
            fail("Failed executing " + queryString, e);
          }
          assertEquals(1, results.size());
          assertTrue(results.getClass() == ResultsBag.class);
          assertTrue(results.getCollectionType().allowsDuplicates() && !results.getCollectionType().getElementType().isStructType());

          queryString = "ticker = 'ibm' and price = 50";
          try {
            results = region.query(queryString);
          } catch (Exception e) {
            fail("Failed executing " + queryString, e);
          }
          assertEquals(1, results.size());
          assertTrue(results.getClass() == ResultsBag.class);
          assertTrue(results.getCollectionType().allowsDuplicates() && !results.getCollectionType().getElementType().isStructType());

          /* Non-distinct order by query not yet supported
          queryString = "id < 101 ORDER BY id";
          try {
            results = region.query(queryString);
          } catch (Exception e) {
            fail("Failed executing " + queryString, e);
        }
          assertEquals(100, results.size());
          assertTrue(results instanceof ResultsCollectionWrapper);
          IdComparator comparator = new IdComparator();
          Object[] resultsArray = results.toArray();
          for (int i=0; i<resultsArray.length; i++) {
            if (i+1 != resultsArray.length) {
              // The id of the current element in the result set must be less
              // than the id of the next one to pass.
              assertTrue("The id for " + resultsArray[i] + " should be less than the id for " + resultsArray[i+1], comparator.compare(resultsArray[i], resultsArray[i+1]) == -1);
            }
          }
          */
        }
      });


    // Stop server
    vm0.invoke(new SerializableRunnable("Stop CacheServer") {
      public void run() {
        stopBridgeServer(getCache());
      }
    });
  }

  /**
   * Tests remote import query execution.
   */
  public void testRemoteImportQueries() throws CacheException {

    final String name = this.getName();
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    final int numberOfEntries = 100;

    // Start server
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
        public void run2() throws CacheException {
          Properties config = new Properties();
          config.setProperty("locators", "localhost["+getDUnitLocatorPort()+"]");
          system = (InternalDistributedSystem) DistributedSystem.connect(config);
          AttributesFactory factory = new AttributesFactory();
          factory.setScope(Scope.LOCAL);
          createRegion(name, factory.create());
          pause(1000);
          try {
            startBridgeServer(0, false);
          } catch (Exception ex) {
            fail("While starting CacheServer", ex);
          }
        }
      });

    // Initialize server region
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          for (int i=0; i<numberOfEntries; i++) {
            region.put("key-"+i, new TestObject(i, "ibm"));
          }
        }
      });

    // Create client region
    final int port = vm0.invokeInt(RemoteQueryDUnitTest.class, "getCacheServerPort");
    final String host0 = getServerHostName(vm0.getHost());
    vm1.invoke(new CacheSerializableRunnable("Create region") {
        public void run2() throws CacheException {
          Properties config = new Properties();
          config.setProperty("mcast-port", "0");
          system = (InternalDistributedSystem) DistributedSystem.connect(config);
          getCache();
          AttributesFactory factory = new AttributesFactory();
          factory.setScope(Scope.LOCAL);
          
          ClientServerTestCase.configureConnectionPool(factory, host0, port,-1, true, -1, -1, null);
          createRegion(name, factory.create());
        }
      });

    // Execute client queries
    vm1.invoke(new CacheSerializableRunnable("Execute queries") {
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          String queryString = null;
          SelectResults results = null;

          queryString = "import com.gemstone.gemfire.admin.RemoteQueryDUnitTest.TestObject; select distinct * from " + region.getFullPath();
          try {
            results = region.query(queryString);
          } catch (Exception e) {
            fail("Failed executing " + queryString, e);
          }
          assertEquals(numberOfEntries, results.size());
          assertTrue(!results.getCollectionType().allowsDuplicates());

          queryString = "import com.gemstone.gemfire.admin.RemoteQueryDUnitTest.TestObject; select distinct * from " + region.getFullPath() + " where ticker = 'ibm'";
          try {
            results = region.query(queryString);
          } catch (Exception e) {
            fail("Failed executing " + queryString, e);
          }
          assertEquals(numberOfEntries, results.size());
          assertTrue(!results.getCollectionType().allowsDuplicates());

          queryString = "import com.gemstone.gemfire.admin.RemoteQueryDUnitTest.TestObject; select distinct * from " + region.getFullPath() + " where ticker = 'IBM'";
          try {
            results = region.query(queryString);
          } catch (Exception e) {
            fail("Failed executing " + queryString, e);
          }
          assertEquals(0, results.size());
          assertTrue(!results.getCollectionType().allowsDuplicates());

          queryString = "import com.gemstone.gemfire.admin.RemoteQueryDUnitTest.TestObject; select distinct * from " + region.getFullPath() + " where price > 49";
          try {
            results = region.query(queryString);
          } catch (Exception e) {
            fail("Failed executing " + queryString, e);
          }
          assertEquals(numberOfEntries/2, results.size());
          assertTrue(!results.getCollectionType().allowsDuplicates());

          queryString = "import com.gemstone.gemfire.admin.RemoteQueryDUnitTest.TestObject; select distinct * from " + region.getFullPath() + " where price = 50";
          try {
            results = region.query(queryString);
          } catch (Exception e) {
            fail("Failed executing " + queryString, e);
          }
          assertEquals(1, results.size());
          assertTrue(!results.getCollectionType().allowsDuplicates());

          queryString = "import com.gemstone.gemfire.admin.RemoteQueryDUnitTest.TestObject; select distinct * from " + region.getFullPath() + " where ticker = 'ibm' and price = 50";
          try {
            results = region.query(queryString);
          } catch (Exception e) {
            fail("Failed executing " + queryString, e);
          }
          assertEquals(1, results.size());
          assertTrue(!results.getCollectionType().allowsDuplicates());
        }
      });


    // Stop server
    vm0.invoke(new SerializableRunnable("Stop CacheServer") {
      public void run() {
        stopBridgeServer(getCache());
      }
    });
  }

  /**
   * Tests remote struct query execution.
   */
  public void testRemoteStructQueries() throws CacheException {

    final String name = this.getName();
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    final int numberOfEntries = 100;

    // Start server
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
        public void run2() throws CacheException {
          Properties config = new Properties();
          config.setProperty("locators", "localhost["+getDUnitLocatorPort()+"]");
          system = (InternalDistributedSystem) DistributedSystem.connect(config);
          AttributesFactory factory = new AttributesFactory();
          factory.setScope(Scope.LOCAL);
          createRegion(name, factory.create());
          pause(1000);
          try {
            startBridgeServer(0, false);
          } catch (Exception ex) {
            fail("While starting CacheServer", ex);
          }
        }
      });

    // Initialize server region
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          for (int i=0; i<numberOfEntries; i++) {
            region.put("key-"+i, new TestObject(i, "ibm"));
          }
        }
      });

    // Create client region
    final int port = vm0.invokeInt(RemoteQueryDUnitTest.class, "getCacheServerPort");
    final String host0 = getServerHostName(vm0.getHost());
    vm1.invoke(new CacheSerializableRunnable("Create region") {
        public void run2() throws CacheException {
          Properties config = new Properties();
          config.setProperty("mcast-port", "0");
          system = (InternalDistributedSystem) DistributedSystem.connect(config);
          getCache();
          AttributesFactory factory = new AttributesFactory();
          factory.setScope(Scope.LOCAL);
          ClientServerTestCase.configureConnectionPool(factory, host0, port,-1, true, -1, -1, null);
          createRegion(name, factory.create());
        }
      });

    // Execute client queries
    vm1.invoke(new CacheSerializableRunnable("Execute queries") {
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          String queryString = null;
          SelectResults results = null;

          queryString = "import com.gemstone.gemfire.admin.RemoteQueryDUnitTest.TestObject; select distinct ticker, price from " + region.getFullPath();
          try {
            results = region.query(queryString);
          } catch (Exception e) {
            fail("Failed executing " + queryString, e);
          }
          assertEquals(numberOfEntries, results.size());
          assertTrue(!results.getCollectionType().allowsDuplicates() && results.getCollectionType().getElementType().isStructType());

          queryString = "import com.gemstone.gemfire.admin.RemoteQueryDUnitTest.TestObject; select distinct ticker, price from " + region.getFullPath() + " where ticker = 'ibm'";
          try {
            results = region.query(queryString);
          } catch (Exception e) {
            fail("Failed executing " + queryString, e);
          }
          assertEquals(numberOfEntries, results.size());
          assertTrue(!results.getCollectionType().allowsDuplicates() && results.getCollectionType().getElementType().isStructType());

          queryString = "import com.gemstone.gemfire.admin.RemoteQueryDUnitTest.TestObject; select distinct ticker, price from " + region.getFullPath() + " where ticker = 'IBM'";
          try {
            results = region.query(queryString);
          } catch (Exception e) {
            fail("Failed executing " + queryString, e);
          }
          assertEquals(0, results.size());
          assertTrue(!results.getCollectionType().allowsDuplicates() && results.getCollectionType().getElementType().isStructType());

          queryString = "import com.gemstone.gemfire.admin.RemoteQueryDUnitTest.TestObject; select distinct ticker, price from " + region.getFullPath() + " where price > 49";
          try {
            results = region.query(queryString);
          } catch (Exception e) {
            fail("Failed executing " + queryString, e);
          }
          assertEquals(numberOfEntries/2, results.size());
          assertTrue(!results.getCollectionType().allowsDuplicates() && results.getCollectionType().getElementType().isStructType());

          queryString = "import com.gemstone.gemfire.admin.RemoteQueryDUnitTest.TestObject; select distinct ticker, price from " + region.getFullPath() + " where price = 50";
          try {
            results = region.query(queryString);
          } catch (Exception e) {
            fail("Failed executing " + queryString, e);
          }
          assertEquals(1, results.size());
          assertTrue(!results.getCollectionType().allowsDuplicates() && results.getCollectionType().getElementType().isStructType());

          queryString = "import com.gemstone.gemfire.admin.RemoteQueryDUnitTest.TestObject; select distinct ticker, price from " + region.getFullPath() + " where ticker = 'ibm' and price = 50";
          try {
            results = region.query(queryString);
          } catch (Exception e) {
            fail("Failed executing " + queryString, e);
          }
          assertEquals(1, results.size());
          assertTrue(!results.getCollectionType().allowsDuplicates() && results.getCollectionType().getElementType().isStructType());
        }
      });

    // Stop server
    vm0.invoke(new SerializableRunnable("Stop CacheServer") {
      public void run() {
        stopBridgeServer(getCache());
      }
    });
  }

  /**
   * Tests remote complex query execution.
   */
  public void __testRemoteComplexQueries() throws CacheException {

    final String name = this.getName();
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
//    final int numberOfEntries = 100;

    // Start server
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
        public void run2() throws CacheException {
          Properties config = new Properties();
          config.setProperty("locators", "localhost["+getDUnitLocatorPort()+"]");
          system = (InternalDistributedSystem) DistributedSystem.connect(config);
          AttributesFactory factory = new AttributesFactory();
          factory.setScope(Scope.LOCAL);
          createRegion(name, factory.create());
          pause(1000);
          try {
            startBridgeServer(0, false);
          } catch (Exception ex) {
            fail("While starting CacheServer", ex);
          }
        }
      });

    // Initialize server region
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          Portfolio portfolio = null;
          Position position1 = null;
          Position position2 = null;
          Properties portfolioProperties= null;
          Properties position1Properties = null;
          Properties position2Properties = null;

          // Create portfolio 1
          portfolio = new Portfolio();
          portfolioProperties = new Properties();
          portfolioProperties.put("id", new Integer(1));
          portfolioProperties.put("type", "type1");
          portfolioProperties.put("status", "active");

          position1 = new Position();
          position1Properties = new Properties();
          position1Properties.put("secId", "SUN");
          position1Properties.put("qty", new Double(34000.0));
          position1Properties.put("mktValue", new Double(24.42));
          position1.init(position1Properties);
          portfolioProperties.put("position1", position1);

          position2 = new Position();
          position2Properties = new Properties();
          position2Properties.put("secId", "IBM");
          position2Properties.put("qty", new Double(8765.0));
          position2Properties.put("mktValue", new Double(34.29));
          position2.init(position2Properties);
          portfolioProperties.put("position2", position2);

          portfolio.init(portfolioProperties);
          region.put(new Integer(1), portfolio);

          // Create portfolio 2
          portfolio = new Portfolio();
          portfolioProperties = new Properties();
          portfolioProperties.put("id", new Integer(2));
          portfolioProperties.put("type", "type2");
          portfolioProperties.put("status", "inactive");

          position1 = new Position();
          position1Properties = new Properties();
          position1Properties.put("secId", "YHOO");
          position1Properties.put("qty", new Double(9834.0));
          position1Properties.put("mktValue", new Double(12.925));
          position1.init(position1Properties);
          portfolioProperties.put("position1", position1);

          position2 = new Position();
          position2Properties = new Properties();
          position2Properties.put("secId", "GOOG");
          position2Properties.put("qty", new Double(12176.0));
          position2Properties.put("mktValue", new Double(21.972));
          position2.init(position2Properties);
          portfolioProperties.put("position2", position2);

          portfolio.init(portfolioProperties);
          region.put(new Integer(2), portfolio);

          // Create portfolio 3
          portfolio = new Portfolio();
          portfolioProperties = new Properties();
          portfolioProperties.put("id", new Integer(3));
          portfolioProperties.put("type", "type3");
          portfolioProperties.put("status", "active");

          position1 = new Position();
          position1Properties = new Properties();
          position1Properties.put("secId", "MSFT");
          position1Properties.put("qty", new Double(98327.0));
          position1Properties.put("mktValue", new Double(23.32));
          position1.init(position1Properties);
          portfolioProperties.put("position1", position1);

          position2 = new Position();
          position2Properties = new Properties();
          position2Properties.put("secId", "AOL");
          position2Properties.put("qty", new Double(978.0));
          position2Properties.put("mktValue", new Double(40.373));
          position2.init(position2Properties);
          portfolioProperties.put("position2", position2);

          portfolio.init(portfolioProperties);
          region.put(new Integer(3), portfolio);

          // Create portfolio 4
          portfolio = new Portfolio();
          portfolioProperties = new Properties();
          portfolioProperties.put("id", new Integer(4));
          portfolioProperties.put("type", "type1");
          portfolioProperties.put("status", "inactive");

          position1 = new Position();
          position1Properties = new Properties();
          position1Properties.put("secId", "APPL");
          position1Properties.put("qty", new Double(90.0));
          position1Properties.put("mktValue", new Double(67.356572));
          position1.init(position1Properties);
          portfolioProperties.put("position1", position1);

          position2 = new Position();
          position2Properties = new Properties();
          position2Properties.put("secId", "ORCL");
          position2Properties.put("qty", new Double(376.0));
          position2Properties.put("mktValue", new Double(101.34));
          position2.init(position2Properties);
          portfolioProperties.put("position2", position2);

          portfolio.init(portfolioProperties);
          region.put(new Integer(4), portfolio);
        }
      });

    // Create client region
    final int port = vm0.invokeInt(RemoteQueryDUnitTest.class, "getCacheServerPort");
    final String host0 = getServerHostName(vm0.getHost());
    vm1.invoke(new CacheSerializableRunnable("Create region") {
        public void run2() throws CacheException {
          Properties config = new Properties();
          config.setProperty("mcast-port", "0");
          system = (InternalDistributedSystem) DistributedSystem.connect(config);
          getCache();
          AttributesFactory factory = new AttributesFactory();
          factory.setScope(Scope.LOCAL);
          ClientServerTestCase.configureConnectionPool(factory, host0, port,-1, true, -1, -1, null);
          createRegion(name, factory.create());
        }
      });

    // Execute client queries
    vm1.invoke(new CacheSerializableRunnable("Execute queries") {
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          String queryString = null;
          SelectResults results = null;

          queryString =
            "IMPORT cacheRunner.Position; " +
            "SELECT DISTINCT id, status FROM " + region.getFullPath() +
            "WHERE NOT (SELECT DISTINCT * FROM positions.values posnVal TYPE Position " +
            "WHERE posnVal.secId='AOL' OR posnVal.secId='SAP').isEmpty";
          try {
            results = region.query(queryString);
          } catch (Exception e) {
            fail("Failed executing " + queryString, e);
          }
          getLogWriter().fine("size: " + results.size());
          //assertEquals(numberOfEntries, results.size());
          assertTrue(!results.getCollectionType().allowsDuplicates() && results.getCollectionType().getElementType().isStructType());
        }
      });


    // Stop server
    vm0.invoke(new SerializableRunnable("Stop CacheServer") {
      public void run() {
        stopBridgeServer(getCache());
      }
    });
  }

  /**
   * Tests remote full region query execution.
   */
  public void testRemoteFullRegionQueries() throws CacheException {

    final String name = this.getName();
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    final int numberOfEntries = 100;

    // Start server
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
        public void run2() throws CacheException {
          Properties config = new Properties();
          config.setProperty("locators", "localhost["+getDUnitLocatorPort()+"]");
          system = (InternalDistributedSystem) DistributedSystem.connect(config);
          AttributesFactory factory = new AttributesFactory();
          factory.setScope(Scope.LOCAL);
          createRegion(name, factory.create());
          pause(1000);
          try {
            startBridgeServer(0, false);
          } catch (Exception ex) {
            fail("While starting CacheServer", ex);
          }
        }
      });

    // Initialize server region
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          for (int i=0; i<numberOfEntries; i++) {
            region.put("key-"+i, new TestObject(i, "ibm"));
          }
        }
      });

    // Create client region
    final int port = vm0.invokeInt(RemoteQueryDUnitTest.class, "getCacheServerPort");
    final String host0 = getServerHostName(vm0.getHost());
    vm1.invoke(new CacheSerializableRunnable("Create region") {
        public void run2() throws CacheException {
          Properties config = new Properties();
          config.setProperty("mcast-port", "0");
          system = (InternalDistributedSystem) DistributedSystem.connect(config);
          getCache();
          AttributesFactory factory = new AttributesFactory();
          factory.setScope(Scope.LOCAL);
          ClientServerTestCase.configureConnectionPool(factory, host0, port,-1, true, -1, -1, null);
          createRegion(name, factory.create());
        }
      });

    // Execute client queries
    vm1.invoke(new CacheSerializableRunnable("Execute queries") {
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          String queryString = null;
          SelectResults results = null;
          Comparator comparator = null;
          Object[] resultsArray = null;

          // value query
          queryString = "SELECT DISTINCT itr.value FROM " + region.getFullPath() + ".entries itr where itr.key = 'key-1'";
          try {
            results = region.query(queryString);
          } catch (Exception e) {
            fail("Failed executing " + queryString, e);
          }
          assertEquals(1, results.size());
          assertTrue(!results.getCollectionType().allowsDuplicates());
          assertTrue(results.asList().get(0) instanceof TestObject);

          // key query
          queryString = "SELECT DISTINCT itr.key FROM " + region.getFullPath() + ".entries itr where itr.key = 'key-1'";
          try {
            results = region.query(queryString);
          } catch (Exception e) {
            fail("Failed executing " + queryString, e);
          }
          assertEquals(1, results.size());
          assertTrue(!results.getCollectionType().allowsDuplicates());
          assertEquals("key-1", results.asList().get(0));

          // order by value query
          queryString = "SELECT DISTINCT * FROM " + region.getFullPath() + " WHERE id < 101 ORDER BY id";
          try {
            results = region.query(queryString);
          } catch (Exception e) {
            fail("Failed executing " + queryString, e);
        }
          assertEquals(numberOfEntries, results.size());
          // All order-by query results are stored in a ResultsCollectionWrapper
          // wrapping a list, so the assertion below is not correct even though
          // it should be.
          //assertTrue(!results.getCollectionType().allowsDuplicates());
          assertTrue(results.getCollectionType().isOrdered());
          comparator = new IdComparator();
          resultsArray = results.toArray();
          for (int i=0; i<resultsArray.length; i++) {
            if (i+1 != resultsArray.length) {
              // The id of the current element in the result set must be less
              // than the id of the next one to pass.
              assertTrue("The id for " + resultsArray[i] + " should be less than the id for " + resultsArray[i+1], comparator.compare(resultsArray[i], resultsArray[i+1]) == -1);
            }
          }

          // order by struct query
          queryString = "SELECT DISTINCT id, ticker, price FROM " + region.getFullPath() + " WHERE id < 101 ORDER BY id";
          try {
            results = region.query(queryString);
          } catch (Exception e) {
            fail("Failed executing " + queryString, e);
          }
          assertEquals(numberOfEntries, results.size());
          // All order-by query results are stored in a ResultsCollectionWrapper
          // wrapping a list, so the assertion below is not correct even though
          // it should be.
          //assertTrue(!results.getCollectionType().allowsDuplicates());
          assertTrue(results.getCollectionType().isOrdered());
          comparator = new StructIdComparator();
          resultsArray = results.toArray();
          for (int i=0; i<resultsArray.length; i++) {
            if (i+1 != resultsArray.length) {
              // The id of the current element in the result set must be less
              // than the id of the next one to pass.
              assertTrue("The id for " + resultsArray[i] + " should be less than the id for " + resultsArray[i+1], comparator.compare(resultsArray[i], resultsArray[i+1]) == -1);
            }
          }

          // size query
          queryString = "(SELECT DISTINCT * FROM " + region.getFullPath() + " WHERE id < 101).size";
          try {
            results = region.query(queryString);
          } catch (Exception e) {
            fail("Failed executing " + queryString, e);
          }
          assertEquals(1, results.size());
          Object result = results.iterator().next();
          assertTrue(result instanceof Integer);
          int resultInt = ((Integer) result).intValue();
          assertEquals(resultInt, 100);

          // query with leading/trailing spaces
          queryString = " SELECT DISTINCT itr.key FROM " + region.getFullPath() + ".entries itr where itr.key = 'key-1' ";
          try {
            results = region.query(queryString);
          } catch (Exception e) {
            fail("Failed executing " + queryString, e);
          }
          assertEquals(1, results.size());
          assertEquals("key-1", results.asList().get(0));
        }
      });

    // Stop server
    vm0.invoke(new SerializableRunnable("Stop CacheServer") {
      public void run() {
        stopBridgeServer(getCache());
      }
    });
  }

  /**
   * Tests remote join query execution.
   */
  public void testRemoteJoinRegionQueries() throws CacheException {

    final String name = this.getName();
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    final int numberOfEntries = 100;

    // Start server
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
        public void run2() throws CacheException {
          Properties config = new Properties();
          config.setProperty("locators", "localhost["+getDUnitLocatorPort()+"]");
          system = (InternalDistributedSystem) DistributedSystem.connect(config);
          AttributesFactory factory = new AttributesFactory();
          factory.setScope(Scope.LOCAL);
          createRegion(name+"1", factory.create());
          createRegion(name+"2", factory.create());
          pause(1000);
          try {
            startBridgeServer(0, false);
          } catch (Exception ex) {
            fail("While starting CacheServer", ex);
          }
        }
      });

    // Initialize server region
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
        public void run2() throws CacheException {
          Region region1 = getRootRegion().getSubregion(name+"1");
          for (int i=0; i<numberOfEntries; i++) {
            region1.put("key-"+i, new TestObject(i, "ibm"));
          }
          Region region2 = getRootRegion().getSubregion(name+"2");
          for (int i=0; i<numberOfEntries; i++) {
            region2.put("key-"+i, new TestObject(i, "ibm"));
          }
        }
      });

    // Create client region
    final int port = vm0.invokeInt(RemoteQueryDUnitTest.class, "getCacheServerPort");
    final String host0 = getServerHostName(vm0.getHost());
    vm1.invoke(new CacheSerializableRunnable("Create region") {
        public void run2() throws CacheException {
          Properties config = new Properties();
          config.setProperty("mcast-port", "0");
          system = (InternalDistributedSystem) DistributedSystem.connect(config);
          getCache();
          AttributesFactory factory = new AttributesFactory();
          factory.setScope(Scope.LOCAL);
          ClientServerTestCase.configureConnectionPool(factory, host0, port,-1, true, -1, -1, null);
          createRegion(name+"1", factory.create());
          createRegion(name+"2", factory.create());
        }
      });

    // Execute client queries
    vm1.invoke(new CacheSerializableRunnable("Execute queries") {
        public void run2() throws CacheException {
          Region region1 = getRootRegion().getSubregion(name+"1");
          Region region2 = getRootRegion().getSubregion(name+"2");
          String queryString = null;
          SelectResults results = null;

          queryString =
            "select distinct a, b.price from " + region1.getFullPath() + " a, " + region2.getFullPath() + " b where a.price = b.price";
          try {
            results = region1.query(queryString);
          } catch (Exception e) {
            fail("Failed executing " + queryString, e);
          }
          assertEquals(numberOfEntries, results.size());
          assertTrue(!results.getCollectionType().allowsDuplicates() && results.getCollectionType().getElementType().isStructType());

          queryString =
            "select distinct a, b.price from " + region1.getFullPath() + " a, " + region2.getFullPath() + " b where a.price = b.price and a.price = 50";
          try {
            results = region1.query(queryString);
          } catch (Exception e) {
            fail("Failed executing " + queryString, e);
          }
          assertEquals(1, results.size());
          assertTrue(!results.getCollectionType().allowsDuplicates() && results.getCollectionType().getElementType().isStructType());
        }
      });

          // Stop server
    vm0.invoke(new SerializableRunnable("Stop CacheServer") {
      public void run() {
        stopBridgeServer(getCache());
      }
    });
  }

  /**
   * Tests remote query execution using a BridgeClient as the CacheWriter
   * and CacheLoader.
   */
  public void testRemoteBridgeClientQueries() throws CacheException {

    final String name = this.getName();
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    final int numberOfEntries = 100;

    // Start server
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
        public void run2() throws CacheException {
          Properties config = new Properties();
          config.setProperty("locators", "localhost["+getDUnitLocatorPort()+"]");
          system = (InternalDistributedSystem) DistributedSystem.connect(config);
          AttributesFactory factory = new AttributesFactory();
          factory.setScope(Scope.LOCAL);
          createRegion(name, factory.create());
          try {
            startBridgeServer(0, false);
          } catch (Exception ex) {
            fail("While starting CacheServer", ex);
          }
        }
      });

    // Initialize server region
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          for (int i=0; i<numberOfEntries; i++) {
            region.put("key-"+i, new TestObject(i, "ibm"));
          }
        }
      });

    final int port = vm0.invokeInt(RemoteQueryDUnitTest.class, "getCacheServerPort");
    final String host0 = getServerHostName(vm0.getHost());

    // Create client region in VM1
    vm1.invoke(new CacheSerializableRunnable("Create region") {
        public void run2() throws CacheException {
          Properties config = new Properties();
          config.setProperty("mcast-port", "0");
          system = (InternalDistributedSystem) DistributedSystem.connect(config);
          PoolManager.createFactory().addServer(host0, port).setSubscriptionEnabled(true).create("clientPool");
          getCache();
          AttributesFactory factory = new AttributesFactory();
          factory.setScope(Scope.LOCAL);
          factory.setPoolName("clientPool");
          createRegion(name, factory.create());
        }
      });

    // Create client region in VM2
    vm2.invoke(new CacheSerializableRunnable("Create region") {
        public void run2() throws CacheException {
          Properties config = new Properties();
          config.setProperty("mcast-port", "0");
          system = (InternalDistributedSystem) DistributedSystem.connect(config);
          PoolManager.createFactory().addServer(host0, port).setSubscriptionEnabled(true).create("clientPool");
          getCache();
          AttributesFactory factory = new AttributesFactory();
          factory.setScope(Scope.LOCAL);
          factory.setPoolName("clientPool");
          createRegion(name, factory.create());
        }
      });

    // Execute client queries in VM1
    vm1.invoke(new CacheSerializableRunnable("Execute queries") {
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          String queryString = null;
          SelectResults results = null;

          queryString = "SELECT DISTINCT itr.value FROM " + region.getFullPath() + ".entries itr where itr.key = 'key-1'";
          try {
            results = region.query(queryString);
          } catch (Exception e) {
            fail("Failed executing " + queryString, e);
          }
          assertEquals(1, results.size());
          assertTrue(!results.getCollectionType().allowsDuplicates() && !results.getCollectionType().getElementType().isStructType());
          assertTrue(results.asList().get(0) instanceof TestObject);

          queryString = "SELECT DISTINCT itr.key FROM " + region.getFullPath() + ".entries itr where itr.key = 'key-1'";
          try {
            results = region.query(queryString);
          } catch (Exception e) {
            fail("Failed executing " + queryString, e);
          }
          assertEquals(1, results.size());
          assertTrue(!results.getCollectionType().allowsDuplicates() && !results.getCollectionType().getElementType().isStructType());
          assertEquals("key-1", results.asList().get(0));
        }
      });

    // Execute client queries in VM2
    vm2.invoke(new CacheSerializableRunnable("Execute queries") {
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          String queryString = null;
          SelectResults results = null;

          queryString = "SELECT DISTINCT itr.value FROM " + region.getFullPath() + ".entries itr where itr.key = 'key-1'";
          try {
            results = region.query(queryString);
          } catch (Exception e) {
            fail("Failed executing " + queryString, e);
          }
          assertEquals(1, results.size());
          assertTrue(!results.getCollectionType().allowsDuplicates() && !results.getCollectionType().getElementType().isStructType());
          assertTrue(results.asList().get(0) instanceof TestObject);

          queryString = "SELECT DISTINCT itr.key FROM " + region.getFullPath() + ".entries itr where itr.key = 'key-1'";
          try {
            results = region.query(queryString);
          } catch (Exception e) {
            fail("Failed executing " + queryString, e);
          }
          assertEquals(1, results.size());
          assertTrue(!results.getCollectionType().allowsDuplicates() && !results.getCollectionType().getElementType().isStructType());
          assertEquals("key-1", results.asList().get(0));
        }
      });

      // Close client VM1
      vm1.invoke(new CacheSerializableRunnable("Close client") {
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          region.close();
          PoolManager.find("clientPool").destroy();
        }
      });

      // Close client VM2
      vm2.invoke(new CacheSerializableRunnable("Close client") {
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          region.close();
          PoolManager.find("clientPool").destroy();
        }
      });

    // Stop server
    vm0.invoke(new SerializableRunnable("Stop CacheServer") {
      public void run() {
        stopBridgeServer(getCache());
      }
    });
  }


  /**
   * This the dunit test for the bug no : 36434
   * @throws Exception
   */

   public void testBug36434() throws Exception
   {
     final String name = this.getName();
     final Host host = Host.getHost(0);
     VM vm0 = host.getVM(0);
     VM vm1 = host.getVM(1);

     final int numberOfEntries = 100;

     // Start server
     vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
         public void run2() throws CacheException {
           Properties config = new Properties();
           config.setProperty("locators", "localhost["+getDUnitLocatorPort()+"]");
           system = (InternalDistributedSystem) DistributedSystem.connect(config);
           AttributesFactory factory = new AttributesFactory();
           factory.setScope(Scope.LOCAL);
           createRegion(name, factory.createRegionAttributes());
           try {
             startBridgeServer(0, false);
           } catch (Exception ex) {
             fail("While starting CacheServer", ex);
           }
         }
       });

     // Initialize server region
     vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
         public void run2() throws CacheException {
           Region region = getRootRegion().getSubregion(name);
           for (int i=0; i<numberOfEntries; i++) {
             region.put("key-"+i, new TestObject(i, "ibm"));
           }
         }
       });

     final int port = vm0.invokeInt(RemoteQueryDUnitTest.class, "getCacheServerPort");
     final String host0 = getServerHostName(vm0.getHost());

     // Create client region in VM1
     vm1.invoke(new CacheSerializableRunnable("Create region") {
         public void run2() throws CacheException {
           Properties config = new Properties();
           config.setProperty("mcast-port", "0");
           system = (InternalDistributedSystem) DistributedSystem.connect(config);
           PoolManager.createFactory().addServer(host0, port).setSubscriptionEnabled(true).create("clientPool");
           getCache();
           AttributesFactory factory = new AttributesFactory();
           factory.setScope(Scope.LOCAL);
           factory.setPoolName("clientPool");
           createRegion(name, factory.createRegionAttributes());
         }
       });


     // Execute client queries in VM1
     vm1.invoke(new CacheSerializableRunnable("Execute queries") {
         public void run2() throws CacheException {
           Region region = getRootRegion().getSubregion(name);
           String queryStrings [] ={"id<9", "selection<9", "important<9","\"select\"<9" };
           for(int i =0 ; i <queryStrings.length;++i) {
             SelectResults results = null;

             try {
               results = region.query(queryStrings[i]);
             } catch (Exception e) {
               fail("Failed executing " + queryStrings[i], e);
             }
             assertEquals(9, results.size());
             String msg = "results expected to be instance of ResultsBag,"
               + " but was found to be is instance of '";
             assertTrue(msg + results.getClass().getName() + "'",
                        results instanceof ResultsBag);
             assertTrue(results.asList().get(0) instanceof TestObject);
           }
         }
       });

       // Close client VM1
       vm1.invoke(new CacheSerializableRunnable("Close client") {
         public void run2() throws CacheException {
           Region region = getRootRegion().getSubregion(name);
           region.close();
           PoolManager.find("clientPool").destroy();
         }
       });


     // Stop server
     vm0.invoke(new SerializableRunnable("Stop CacheServer") {
       public void run() {
         stopBridgeServer(getCache());
       }
     });



   }

  /**
    * This the dunit test for the bug no : 36969
    * @throws Exception
    */

    public void testBug36969() throws Exception
    {
      final String name = this.getName();
      final Host host = Host.getHost(0);
      VM vm0 = host.getVM(0);
      VM vm1 = host.getVM(1);

      final int numberOfEntries = 100;

      // Start server
      vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
          public void run2() throws CacheException {
            Properties config = new Properties();
            config.setProperty("locators", "localhost["+getDUnitLocatorPort()+"]");
            system = (InternalDistributedSystem) DistributedSystem.connect(config);
            AttributesFactory factory = new AttributesFactory();
            factory.setScope(Scope.LOCAL);
            final Region region = createRegion(name, factory.createRegionAttributes());
            QueryObserverHolder.setInstance(new QueryObserverAdapter() {
              public void afterQueryEvaluation(Object result) {
                //Destroy the region in the test
                region.close();
              }

            });
            try {
              startBridgeServer(0, false);
            } catch (Exception ex) {
              fail("While starting CacheServer", ex);
            }
          }
        });

      // Initialize server region
      vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
          public void run2() throws CacheException {
            Region region = getRootRegion().getSubregion(name);
            for (int i=0; i<numberOfEntries; i++) {
              region.put("key-"+i, new TestObject(i, "ibm"));
            }
          }
        });

      final int port = vm0.invokeInt(RemoteQueryDUnitTest.class, "getCacheServerPort");
      final String host0 = getServerHostName(vm0.getHost());

      // Create client region in VM1
      vm1.invoke(new CacheSerializableRunnable("Create region") {
          public void run2() throws CacheException {
            Properties config = new Properties();
            config.setProperty("mcast-port", "0");
            system = (InternalDistributedSystem) DistributedSystem.connect(config);
            PoolManager.createFactory().addServer(host0, port).setSubscriptionEnabled(true).create("clientPool");
            getCache();
            AttributesFactory factory = new AttributesFactory();
            factory.setScope(Scope.LOCAL);
            factory.setPoolName("clientPool");
            createRegion(name, factory.createRegionAttributes());
          }
        });


      // Execute client queries in VM1
      vm1.invoke(new CacheSerializableRunnable("Execute queries") {
          public void run2() throws CacheException {
            Region region = getRootRegion().getSubregion(name);
            String queryStrings = "id<9";
//            SelectResults results = null;
              try {
                region.query(queryStrings);
                fail("The query should have experienced RegionDestroyedException");
              } catch (QueryInvocationTargetException qte) {
                //Ok test passed
              } catch(Exception e) {
                fail("Failed executing query " + queryStrings+ " due  to unexpected Excecption", e);
            }
          }
        });

      // Start server
      vm0.invoke(new CacheSerializableRunnable("Create two regions") {
          public void run2() throws CacheException {
            AttributesFactory factory = new AttributesFactory();
            factory.setScope(Scope.LOCAL);
            final Region region1 = createRegion(name, factory.createRegionAttributes());
            final Region region2 = createRegion(name+"_2", factory.createRegionAttributes());
            QueryObserverHolder.setInstance(new QueryObserverAdapter() {
              public void afterQueryEvaluation(Object result) {
                //Destroy the region in the test
                region1.close();
              }

            });
            for (int i=0; i<numberOfEntries; i++) {
              region1.put("key-"+i, new TestObject(i, "ibm"));
              region2.put("key-"+i, new TestObject(i, "ibm"));
            }

          }
        });

   // Execute client queries in VM1
      vm1.invoke(new CacheSerializableRunnable("Execute queries") {
          public void run2() throws CacheException {
            Region region = getRootRegion().getSubregion(name);
            String queryString = "select distinct * from /"+name+",/"+name + "_2";
//            SelectResults results = null;
              try {
                region.query(queryString);
                fail("The query should have experienced RegionDestroyedException");
              } catch (QueryInvocationTargetException qte) {
                //Ok test passed
              } catch(Exception e) {
                fail("Failed executing query " + queryString+ " due  to unexpected Excecption", e);
            }
          }
        });

        // Close client VM1
        vm1.invoke(new CacheSerializableRunnable("Close client") {
          public void run2() throws CacheException {
            Region region = getRootRegion().getSubregion(name);
            region.close();
            PoolManager.find("clientPool").destroy();
          }
        });


      // Stop server
      vm0.invoke(new SerializableRunnable("Stop CacheServer") {
        public void run() {
          QueryObserverHolder.setInstance(new QueryObserverAdapter());
          stopBridgeServer(getCache());
        }
      });



    }



  /**
   * Starts a bridge server on the given port, using the given
   * deserializeValues and notifyBySubscription to serve up the
   * given region.
   */
  protected void startBridgeServer(int port, boolean notifyBySubscription)
    throws IOException {

    Cache cache = getCache();
    CacheServer bridge = cache.addCacheServer();
    bridge.setPort(port);
    bridge.setNotifyBySubscription(notifyBySubscription);
    bridge.start();
    bridgeServerPort = bridge.getPort();
  }

  /**
   * Stops the bridge server that serves up the given cache.
   */
  protected void stopBridgeServer(Cache cache) {
    CacheServer bridge =
      (CacheServer) cache.getCacheServers().iterator().next();
    bridge.stop();
    assertFalse(bridge.isRunning());
  }

  private static int getCacheServerPort() {
    return bridgeServerPort;
  }

  public static class TestObject implements DataSerializable {
    protected String _ticker;
    protected int _price;
    public int id;
    public int important;
    public int selection;
    public int select;

    public TestObject() {}

    public TestObject(int id, String ticker) {
      this.id = id;
      this._ticker = ticker;
      this._price = id;
      this.important = id;
      this.selection =id;
      this.select =id;
    }

    public int getId() {
      return this.id;
    }

    public String getTicker() {
      return this._ticker;
    }

    public int getPrice() {
      return this._price;
    }

    public void toData(DataOutput out) throws IOException
    {
      //System.out.println("Is serializing in WAN: " + GatewayEventImpl.isSerializingValue());
      out.writeInt(this.id);
      DataSerializer.writeString(this._ticker, out);
      out.writeInt(this._price);
    }

    public void fromData(DataInput in) throws IOException, ClassNotFoundException
    {
      //System.out.println("Is deserializing in WAN: " + GatewayEventImpl.isDeserializingValue());
      this.id = in.readInt();
      this._ticker = DataSerializer.readString(in);
      this._price = in.readInt();
    }

    public String toString() {
      StringBuffer buffer = new StringBuffer();
      buffer
        .append("TestObject [")
        .append("id=")
        .append(this.id)
        .append("; ticker=")
        .append(this._ticker)
        .append("; price=")
        .append(this._price)
        .append("]");
      return buffer.toString();
    }
  }

  public static class IdComparator implements Comparator {

    public int compare(Object obj1, Object obj2) {
      int obj1Id = ((TestObject) obj1).getId();
      int obj2Id = ((TestObject) obj2).getId();
      if (obj1Id > obj2Id) {
        return 1;
      } else if (obj1Id < obj2Id) {
        return -1;
      } else {
        return 0;
      }
    }
  }

  public static class StructIdComparator implements Comparator {

    public int compare(Object obj1, Object obj2) {
      int obj1Id = ((Integer) ((Struct) obj1).get("id")).intValue();
      int obj2Id = ((Integer) ((Struct) obj2).get("id")).intValue();
      if (obj1Id > obj2Id) {
        return 1;
      } else if (obj1Id < obj2Id) {
        return -1;
      } else {
        return 0;
      }
    }
  }
}

/*
    String queryString = "ticker = 'ibm'";
    SelectResults results = region.query(queryString);

    String queryString = "ticker = 'ibm' and price = 50";

    String queryString = "select distinct * from /trade";

    String queryString = "import TestObject; select distinct * from /trade";

    String queryString =
      "IMPORT cacheRunner.Position; " +
      "SELECT DISTINCT id, status FROM /root/portfolios " +
      "WHERE NOT (SELECT DISTINCT * FROM positions.values posnVal TYPE Position " +
      "WHERE posnVal.secId='AOL' OR posnVal.secId='SAP').isEmpty";

    queryString = "SELECT DISTINCT itr.value FROM /trade.entries itr where itr.key = 'key-1'";

    queryString = "SELECT DISTINCT itr.key FROM /trade.entries itr where itr.key = 'key-1'";

    String queryString =
      "select distinct a, b.price from /generic/app1/Trade a, /generic/app1/Trade b where a.price = b.price";

    String queryString =
        "SELECT DISTINCT a, b.unitPrice from /newegg/arinv a, /newegg/itemPriceSetting b where a.item = b.itemNumber and a.item = '26-106-934'";

    String queryString =
        "SELECT DISTINCT a, UNDEFINED from /newegg/arinv a, /newegg/itemPriceSetting b where a.item = b.itemNumber and a.item = '26-106-934'";

*/

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

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.test.dunit.Assert.assertFalse;
import static org.apache.geode.test.dunit.Assert.fail;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.geode.LogWriter;
import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.client.PoolFactory;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.query.CacheUtils;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.Struct;
import org.apache.geode.cache.query.data.PortfolioPdx;
import org.apache.geode.cache.query.data.PositionPdx;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.compression.Compressor;
import org.apache.geode.compression.SnappyCompressor;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.pdx.PdxReader;
import org.apache.geode.pdx.PdxSerializable;
import org.apache.geode.pdx.PdxWriter;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;

public abstract class PDXQueryTestBase extends JUnit4CacheTestCase {

  /** The port on which the cache server was started in this VM */
  private static int bridgeServerPort;
  protected static final Compressor compressor = SnappyCompressor.getDefaultInstance();
  protected final String rootRegionName = "root";
  protected final String regionName = "PdxTest";
  protected final String regionName2 = "PdxTest2";
  protected final String regName = SEPARATOR + rootRegionName + SEPARATOR + regionName;
  protected final String regName2 = SEPARATOR + rootRegionName + SEPARATOR + regionName2;
  protected final String[] queryString = new String[] {"SELECT DISTINCT id FROM " + regName, // 0
      "SELECT * FROM " + regName, // 1
      "SELECT ticker FROM " + regName, // 2
      "SELECT * FROM " + regName + " WHERE id > 5", // 3
      "SELECT p FROM " + regName + " p, p.idTickers idTickers WHERE p.ticker = 'vmware'", // 4
  };

  protected static int getCacheServerPort() {
    return bridgeServerPort;
  }

  @Override
  public final void preTearDownCacheTestCase() throws Exception {
    preTearDownPDXQueryTestBase();
    disconnectAllFromDS(); // tests all expect to create a new ds
    // Reset the testObject numinstance for the next test.
    TestObject.numInstance.set(0);
    // In all VM.
    resetTestObjectInstanceCount();
  }

  protected void preTearDownPDXQueryTestBase() throws Exception {}

  private void resetTestObjectInstanceCount() {
    final Host host = Host.getHost(0);
    for (int i = 0; i < 4; i++) {
      VM vm = host.getVM(i);
      vm.invoke(new CacheSerializableRunnable("Create cache server") {
        @Override
        public void run2() throws CacheException {
          TestObject.numInstance.set(0);
          PortfolioPdx.numInstance = 0;
          PositionPdx.numInstance = 0;
          PositionPdx.cnt = 0;
          TestObject2.numInstance.set(0);
        }
      });
    }
  }

  public void createPool(VM vm, String poolName, String server, int port,
      boolean subscriptionEnabled) {
    createPool(vm, poolName, new String[] {server}, new int[] {port}, subscriptionEnabled);
  }

  public void createPool(VM vm, String poolName, String server, int port) {
    createPool(vm, poolName, new String[] {server}, new int[] {port}, false);
  }

  public void createPool(VM vm, final String poolName, final String[] servers, final int[] ports,
      final boolean subscriptionEnabled) {
    createPool(vm, poolName, servers, ports, subscriptionEnabled, 0);
  }

  public void createPool(VM vm, final String poolName, final String[] servers, final int[] ports,
      final boolean subscriptionEnabled, final int redundancy) {
    vm.invoke(new CacheSerializableRunnable("createPool :" + poolName) {
      @Override
      public void run2() throws CacheException {
        // Create Cache.
        Properties props = new Properties();
        props.setProperty("mcast-port", "0");
        props.setProperty("locators", "");
        getSystem(props);
        getCache();
        PoolFactory cpf = PoolManager.createFactory();
        cpf.setSubscriptionEnabled(subscriptionEnabled);
        cpf.setSubscriptionRedundancy(redundancy);
        for (int i = 0; i < servers.length; i++) {
          cpf.addServer(servers[i], ports[i]);
        }
        cpf.create(poolName);
      }
    });
  }

  public void executeClientQueries(VM vm, final String poolName, final String queryStr) {
    vm.invoke(new CacheSerializableRunnable("Execute queries") {
      @Override
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
          Query query = remoteQueryService.newQuery(queryStr);
          rs[0][0] = (SelectResults) query.execute();
          query = localQueryService.newQuery(queryStr);
          rs[0][1] = (SelectResults) query.execute();
          // Compare local and remote query results.
          if (!CacheUtils.compareResultsOfWithAndWithoutIndex(rs)) {
            fail("Local and Remote Query Results are not matching for query :" + queryStr);
          }
        } catch (Exception e) {
          Assert.fail("Failed executing " + queryStr, e);
        }
      }
    });
  }

  public void printResults(SelectResults results, String message) {
    Object r;
    Struct s;
    LogWriter logger = GemFireCacheImpl.getInstance().getLogger();
    logger.fine(message);
    int row = 0;
    for (final Object result : results) {
      r = result;
      row++;
      if (r instanceof Struct) {
        s = (Struct) r;
        String[] fieldNames = ((Struct) r).getStructType().getFieldNames();
        for (final String fieldName : fieldNames) {
          logger.fine("### Row " + row + "\n" + "Field: " + fieldName + " > "
              + s.get(fieldName).toString());
        }
      } else {
        logger.fine("#### Row " + row + "\n" + r);
      }
    }
  }

  protected void configAndStartBridgeServer() {
    configAndStartBridgeServer(false, false, false, null);
  }

  protected void configAndStartBridgeServer(boolean isPr, boolean isAccessor) {
    configAndStartBridgeServer(isPr, isAccessor, false, null);
  }

  protected void configAndStartBridgeServer(boolean isPr, boolean isAccessor, boolean asyncIndex,
      Compressor compressor) {
    AttributesFactory factory = new AttributesFactory();
    if (isPr) {
      PartitionAttributesFactory paf = new PartitionAttributesFactory();
      if (isAccessor) {
        paf.setLocalMaxMemory(0);
      }
      PartitionAttributes prAttr = paf.setTotalNumBuckets(20).setRedundantCopies(0).create();
      factory.setPartitionAttributes(prAttr);
    } else {
      factory.setScope(Scope.DISTRIBUTED_ACK);
      factory.setDataPolicy(DataPolicy.REPLICATE);
    }
    if (asyncIndex) {
      factory.setIndexMaintenanceSynchronous(!asyncIndex);
    }
    if (compressor != null) {
      factory.setCompressor(compressor);
    }

    createRegion(regionName, rootRegionName, factory.create());
    createRegion(regionName2, rootRegionName, factory.create());

    try {
      startBridgeServer(0, false);
    } catch (Exception ex) {
      Assert.fail("While starting CacheServer", ex);
    }
  }

  protected void executeCompiledQueries(String poolName, Object[][] params) {
    SelectResults results = null;
    QueryService qService = null;

    try {
      qService = (PoolManager.find(poolName)).getQueryService();
    } catch (Exception e) {
      Assert.fail("Failed to get QueryService.", e);
    }

    for (int i = 0; i < queryString.length; i++) {
      try {
        Query query = qService.newQuery(queryString[i]);
        results = (SelectResults) query.execute(params[i]);
      } catch (Exception e) {
        Assert.fail("Failed executing " + queryString[i], e);
      }
    }
  }

  /**
   * Starts a cache server on the given port, using the given deserializeValues and
   * notifyBySubscription to serve up the given region.
   */
  protected void startBridgeServer(int port, boolean notifyBySubscription) throws IOException {
    Cache cache = getCache();
    CacheServer bridge = cache.addCacheServer();
    bridge.setPort(port);
    bridge.setNotifyBySubscription(notifyBySubscription);
    bridge.start();
    bridgeServerPort = bridge.getPort();
  }

  /**
   * Stops the cache server that serves up the given cache.
   */
  protected void stopBridgeServer(Cache cache) {
    CacheServer bridge = cache.getCacheServers().iterator().next();
    bridge.stop();
    assertFalse(bridge.isRunning());
  }

  public void closeClient(VM client) {
    SerializableRunnable closeCache = new CacheSerializableRunnable("Close Client") {
      @Override
      public void run2() throws CacheException {
        try {
          closeCache();
          disconnectFromDS();
        } catch (Exception ignored) {
        }
      }
    };

    client.invoke(closeCache);
  }

  /**
   * Starts a cache server on the given port, using the given deserializeValues and
   * notifyBySubscription to serve up the given region.
   */
  protected void startCacheServer(int port, boolean notifyBySubscription) throws IOException {
    Cache cache = CacheFactory.getAnyInstance();
    CacheServer bridge = cache.addCacheServer();
    bridge.setPort(port);
    bridge.setNotifyBySubscription(notifyBySubscription);
    bridge.start();
    bridgeServerPort = bridge.getPort();
  }

  public static class TestObject2 implements PdxSerializable {
    public int _id;
    public static AtomicInteger numInstance = new AtomicInteger();

    public TestObject2() {
      numInstance.incrementAndGet();
    }

    public TestObject2(int id) {
      _id = id;
      numInstance.incrementAndGet();
    }

    public int getId() {
      return _id;
    }

    @Override
    public void toData(PdxWriter out) {
      out.writeInt("id", _id);
    }

    @Override
    public void fromData(PdxReader in) {
      _id = in.readInt("id");
    }

    @Override
    public boolean equals(Object o) {
      GemFireCacheImpl.getInstance().getLogger()
          .fine("In TestObject2.equals() this: " + this + " other :" + o);
      TestObject2 other = (TestObject2) o;
      return _id == other._id;
    }

    @Override
    public int hashCode() {
      GemFireCacheImpl.getInstance().getLogger()
          .fine("In TestObject2.hashCode() : " + _id);
      return _id;
    }
  }

  public static class TestObject implements PdxSerializable {
    public static LogWriter log;
    protected String _ticker;
    protected int _price;
    public int id;
    public int important;
    public int selection;
    public int select;
    public static final AtomicInteger numInstance = new AtomicInteger();
    public Map idTickers = new HashMap();
    public HashMap positions = new HashMap();
    public TestObject2 test;

    public TestObject() {
      if (log != null) {
        log.info("TestObject ctor stack trace", new Exception());
      }
      numInstance.incrementAndGet();
    }

    public TestObject(int id, String ticker) {
      if (log != null) {
        log.info("TestObject ctor stack trace", new Exception());
      }
      this.id = id;
      _ticker = ticker;
      _price = id;
      important = id;
      selection = id;
      select = id;
      numInstance.incrementAndGet();
      idTickers.put(id + "", ticker);
      test = new TestObject2(id);
    }

    public TestObject(int id, String ticker, int numPositions) {
      this(id, ticker);
      for (int i = 0; i < numPositions; i++) {
        positions.put(id + i, new PositionPdx(ticker + ":" + id + ":" + i, (id + 100)));
      }
    }

    public int getIdValue() {
      return id;
    }

    public String getTicker() {
      return _ticker;
    }

    public int getPriceValue() {
      return _price;
    }

    public HashMap getPositions(String id) {
      return positions;
    }

    public String getStatus() {
      return (id % 2 == 0) ? "active" : "inactive";
    }

    @Override
    public void toData(PdxWriter out) {
      out.writeInt("id", id);
      out.writeString("ticker", _ticker);
      out.writeInt("price", _price);
      out.writeObject("idTickers", idTickers);
      out.writeObject("positions", positions);
      out.writeObject("test", test);
    }

    @Override
    public void fromData(PdxReader in) {
      id = in.readInt("id");
      _ticker = in.readString("ticker");
      _price = in.readInt("price");
      idTickers = (Map) in.readObject("idTickers");
      positions = (HashMap) in.readObject("positions");
      test = (TestObject2) in.readObject("test");
    }

    public String toString() {
      return "TestObject [" + "id=" + id + "; ticker="
          + _ticker + "; price=" + _price + "]";
    }

    @Override
    public boolean equals(Object o) {
      TestObject other = (TestObject) o;
      return (id == other.id) && (_ticker.equals(other._ticker));
    }

    @Override
    public int hashCode() {
      GemFireCacheImpl.getInstance().getLogger().fine("In TestObject.hashCode() : " + id);
      return id;
    }

  }
}

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

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.client.PoolFactory;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.query.CacheUtils;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.Struct;
import com.gemstone.gemfire.cache.query.data.PortfolioPdx;
import com.gemstone.gemfire.cache.query.data.PositionPdx;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.compression.Compressor;
import com.gemstone.gemfire.compression.SnappyCompressor;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.pdx.PdxReader;
import com.gemstone.gemfire.pdx.PdxSerializable;
import com.gemstone.gemfire.pdx.PdxWriter;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;

public abstract class PdxQueryCQTestBase extends CacheTestCase {

  /** The port on which the bridge server was started in this VM */
  private static int bridgeServerPort;
  protected static final Compressor compressor = SnappyCompressor.getDefaultInstance();
  protected final String rootRegionName = "root";
  protected final String regionName = "PdxTest";
  protected final String regionName2 = "PdxTest2";
  protected final String regName = "/" + rootRegionName + "/" + regionName;
  protected final String regName2 = "/" + rootRegionName + "/" + regionName2;
  protected final String[] queryString = new String[] {
      "SELECT DISTINCT id FROM " + regName, // 0
      "SELECT * FROM " + regName, // 1
      "SELECT ticker FROM " + regName, // 2
      "SELECT * FROM " + regName + " WHERE id > 5", // 3
      "SELECT p FROM " + regName + " p, p.idTickers idTickers WHERE p.ticker = 'vmware'", // 4
    };

  private static int getCacheServerPort() {
    return bridgeServerPort;
  }

  public void tearDown2() throws Exception {
    disconnectAllFromDS(); // tests all expect to create a new ds
    // Reset the testObject numinstance for the next test.
    TestObject.numInstance = 0;
    // In all VM.
    resetTestObjectInstanceCount();    
  }

  private void resetTestObjectInstanceCount() {
    final Host host = Host.getHost(0);
    for (int i=0; i < 4; i++) {
      VM vm = host.getVM(i);
      vm.invoke(new CacheSerializableRunnable("Create Bridge Server") {
        public void run2() throws CacheException {
          TestObject.numInstance = 0;
          PortfolioPdx.numInstance = 0;
          PositionPdx.numInstance = 0;
          PositionPdx.cnt = 0;
          TestObject2.numInstance = 0;
        }
      });
    }
  }

  public void createPool(VM vm, String poolName, String server, int port,
      boolean subscriptionEnabled) {
        createPool(vm, poolName, new String[]{server}, new int[]{port}, subscriptionEnabled);  
      }

  public void createPool(VM vm, String poolName, String server, int port) {
    createPool(vm, poolName, new String[]{server}, new int[]{port}, false);  
  }

  public void createPool(VM vm, final String poolName, final String[] servers, final int[] ports,
      final boolean subscriptionEnabled) {
        createPool(vm, poolName, servers, ports, subscriptionEnabled, 0);    
      }

  public void createPool(VM vm, final String poolName, final String[] servers, final int[] ports,
      final boolean subscriptionEnabled, final int redundancy) {
        vm.invoke(new CacheSerializableRunnable("createPool :" + poolName) {
          public void run2() throws CacheException {
            // Create Cache.
            Properties props = new Properties();
            props.setProperty("mcast-port", "0");
            props.setProperty("locators", "");
            getSystem(props );
            getCache();        
            PoolFactory cpf = PoolManager.createFactory();
            cpf.setSubscriptionEnabled(subscriptionEnabled);
            cpf.setSubscriptionRedundancy(redundancy);
            for (int i=0; i < servers.length; i++){
              getLogWriter().info("### Adding to Pool. ### Server : " + servers[i] + " Port : " + ports[i]);
              cpf.addServer(servers[i], ports[i]);
            }
            cpf.create(poolName);
          }
        });   
      }

  public void executeClientQueries(VM vm, final String poolName, final String queryStr) {
    vm.invoke(new CacheSerializableRunnable("Execute queries") {
      public void run2() throws CacheException {
        SelectResults results = null;
        Comparator comparator = null;
        Object[] resultsArray = null;
        QueryService remoteQueryService = null;
        QueryService localQueryService = null;
        SelectResults[][] rs = new SelectResults[1][2];
        
        try {
          remoteQueryService = (PoolManager.find(poolName)).getQueryService();
          localQueryService = getCache().getQueryService();
        } catch (Exception e) {
          fail("Failed to get QueryService.", e);
        }          
  
        try {
          getLogWriter().info("### Executing Query on server:" + queryStr);
          Query query = remoteQueryService.newQuery(queryStr);
          rs[0][0] = (SelectResults)query.execute();
          //printResults (rs[0][0], " ### Remote Query Results : ####");
          getLogWriter().info("### Executing Query locally:" + queryStr);
          query = localQueryService.newQuery(queryStr);
          rs[0][1] = (SelectResults)query.execute();
          getLogWriter().info("### Remote Query rs size: " + (rs[0][0]).size() + 
              "Local Query rs size: " + (rs[0][1]).size());
          //printResults (rs[0][1], " ### Local Query Results : ####");
          // Compare local and remote query results.
          if (!CacheUtils.compareResultsOfWithAndWithoutIndex(rs))
          {
             fail("Local and Remote Query Results are not matching for query :" + queryStr);  
          }
        } catch (Exception e) {
          fail("Failed executing " + queryStr, e);
        }
      }
    });
  }

  public void printResults(SelectResults results, String message) {
    Object r;
    Struct s;
    LogWriterI18n logger = GemFireCacheImpl.getInstance().getLoggerI18n();
    logger.fine(message);
    int row = 0;
    for (Iterator iter = results.iterator(); iter.hasNext(); ) {
      r = iter.next();
      row++;
      if (r instanceof Struct) {
         s = (Struct)r;
         String[] fieldNames = ((Struct)r).getStructType().getFieldNames();
         for (int i=0; i < fieldNames.length; i++){
            logger.fine("### Row " + row  + "\n" + "Field: " +
                fieldNames[i] + " > " + s.get(fieldNames[i]).toString());
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
    configAndStartBridgeServer(false, false, false, null); 
  }

  protected void configAndStartBridgeServer(boolean isPr, boolean isAccessor,
      boolean asyncIndex, Compressor compressor) {
        AttributesFactory factory = new AttributesFactory();
        if (isPr) {
          PartitionAttributesFactory paf = new PartitionAttributesFactory();
          //factory.setDataPolicy(DataPolicy.PARTITION);
          if (isAccessor){
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
        
        createRegion(this.regionName, this.rootRegionName, factory.create());
        createRegion(this.regionName2, this.rootRegionName, factory.create());
        
        try {
          startBridgeServer(0, false);
        } catch (Exception ex) {
          fail("While starting CacheServer", ex);
        }
      }

  protected void executeCompiledQueries(String poolName, Object[][] params) {
    SelectResults results = null;
    Comparator comparator = null;
    Object[] resultsArray = null;
    QueryService qService = null;
  
    try {
      qService = (PoolManager.find(poolName)).getQueryService();
    } catch (Exception e) {
      fail("Failed to get QueryService.", e);
    }          
  
    for (int i=0; i < queryString.length; i++){
      try {
        getLogWriter().info("### Executing Query :" + queryString[i]);
        Query query = qService.newQuery(queryString[i]);
        results = (SelectResults)query.execute(params[i]);
      } catch (Exception e) {
        fail("Failed executing " + queryString[i], e);
      }
    }        
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

  public void closeClient(VM client) {
    SerializableRunnable closeCache =
      new CacheSerializableRunnable("Close Client") {
      public void run2() throws CacheException {
        getLogWriter().info("### Close Client. ###");
        try {
          closeCache();
          disconnectFromDS();
        } catch (Exception ex) {
          getLogWriter().info("### Failed to get close client. ###");
        }
      }
    };
    
    client.invoke(closeCache);
  }

  public static class TestObject2 implements PdxSerializable {
    public int _id;
    public static int numInstance = 0;
    
    public TestObject2(){
      numInstance++;
    }
    
    public TestObject2(int id){
      this._id = id;
      numInstance++;
    }
    
    public int getId() {
      return this._id;
    }
    
    public void toData(PdxWriter out) {
      out.writeInt("id", this._id);
    }
    
    public void fromData(PdxReader in) {
      this._id = in.readInt("id");
    }
    
    @Override
    public boolean equals(Object o){
      getLogWriter().info("In TestObject2.equals() this: " + this + " other :" + o);
      GemFireCacheImpl.getInstance().getLoggerI18n().fine("In TestObject2.equals() this: " + this + " other :" + o);
      TestObject2 other = (TestObject2)o;
      if (_id == other._id) {
        return true;
      } else {
        getLogWriter().info("NOT EQUALS");  
        return false;
      }
    }
    
    @Override
    public int hashCode(){
      GemFireCacheImpl.getInstance().getLoggerI18n().fine("In TestObject2.hashCode() : " + this._id);
      return this._id;
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
      public static int numInstance = 0;
      public Map idTickers = new HashMap();
      public HashMap positions = new HashMap();
      public TestObject2 test;
      
      public TestObject() {
        if (log != null) {
          log.info("TestObject ctor stack trace", new Exception());
        }
        numInstance++;
        //GemFireCacheImpl.getInstance().getLoggerI18n().fine(new Exception("DEBUG"));
      }
  
      public TestObject(int id, String ticker) {
        if (log != null) {
          log.info("TestObject ctor stack trace", new Exception());
        }
        this.id = id;
        this._ticker = ticker;
        this._price = id;
        this.important = id;
        this.selection =id;
        this.select =id;
        //GemFireCacheImpl.getInstance().getLoggerI18n().fine(new Exception("DEBUG"));
        numInstance++;
        idTickers.put(id + "", ticker);
        this.test = new TestObject2(id);
      }
  
      public TestObject(int id, String ticker, int numPositions) {
        this(id, ticker);
        for (int i=0; i < numPositions; i++) {
          positions.put(id + i, new PositionPdx(ticker + ":" +  id + ":" + i , (id + 100)));
        }
      }
      
      public int getIdValue() {
        return this.id;
      }
  
      public String getTicker() {
        return this._ticker;
      }
  
      public int getPriceValue() {
        return this._price;
      }
  
      public HashMap getPositions(String id) {
        return this.positions;  
      }
  
      public String getStatus(){
        return (id % 2 == 0) ? "active" : "inactive";
      }
  
      public void toData(PdxWriter out)
      {
        //System.out.println("Is serializing in WAN: " + GatewayEventImpl.isSerializingValue());
        out.writeInt("id", this.id);
        out.writeString("ticker", this._ticker);
        out.writeInt("price", this._price);
        out.writeObject("idTickers", this.idTickers);
        out.writeObject("positions", this.positions);
        out.writeObject("test", this.test);
      }
  
      public void fromData(PdxReader in)
      {
        //System.out.println("Is deserializing in WAN: " + GatewayEventImpl.isDeserializingValue());
        this.id = in.readInt("id");
        this._ticker = in.readString("ticker");
        this._price = in.readInt("price");
        this.idTickers = (Map)in.readObject("idTickers");
        this.positions = (HashMap)in.readObject("positions");
        this.test = (TestObject2)in.readObject("test");
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
  
      @Override
      public boolean equals(Object o){
  //      getLogWriter().info("In TestObject.equals() this: " + this + " other :" + o);
  //      GemFireCacheImpl.getInstance().getLoggerI18n().fine("In TestObject.equals() this: " + this + " other :" + o);
        TestObject other = (TestObject)o;
        if ((id == other.id) && (_ticker.equals(other._ticker))) {
          return true;
        } else {
  //        getLogWriter().info("NOT EQUALS");  
          return false;
        }
      }
      
      @Override
      public int hashCode(){
        GemFireCacheImpl.getInstance().getLoggerI18n().fine("In TestObject.hashCode() : " + this.id);
        return this.id;
      }
  
    }

  /**
   * Starts a bridge server on the given port, using the given
   * deserializeValues and notifyBySubscription to serve up the
   * given region.
   */
  protected void startCacheServer(int port, boolean notifyBySubscription)
      throws IOException {
      
        Cache cache = CacheFactory.getAnyInstance();
        CacheServer bridge = cache.addCacheServer();
        bridge.setPort(port);
        bridge.setNotifyBySubscription(notifyBySubscription);
        bridge.start();
        bridgeServerPort = bridge.getPort();
      }

  public PdxQueryCQTestBase(String name) {
    super(name);
  }

}

package com.gemstone.gemfire.tools.databrowser.dunit;

import hydra.BridgeHelper;
import hydra.CacheHelper;
import hydra.ConfigHashtable;
import hydra.GemFireDescription;
import hydra.GemFirePrms;
import hydra.HostHelper;
import hydra.Log;
import hydra.RegionHelper;
import hydra.TestConfig;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import junit.framework.Assert;
import util.NameFactory;

import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.admin.DistributedSystemConfig;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.mgmt.DataBrowser.connection.ClientConfiguration;
import com.gemstone.gemfire.mgmt.DataBrowser.connection.ConnectionFactory;
import com.gemstone.gemfire.mgmt.DataBrowser.connection.ConnectionFailureException;
import com.gemstone.gemfire.mgmt.DataBrowser.connection.EndPoint;
import com.gemstone.gemfire.mgmt.DataBrowser.connection.GemFireConnection;
import com.gemstone.gemfire.mgmt.DataBrowser.model.member.GemFireMember;
import com.gemstone.gemfire.mgmt.DataBrowser.query.ColumnNotFoundException;
import com.gemstone.gemfire.mgmt.DataBrowser.query.IntrospectionResult;
import com.gemstone.gemfire.mgmt.DataBrowser.query.QueryExecutionException;
import com.gemstone.gemfire.mgmt.DataBrowser.query.QueryResult;
import com.gemstone.gemfire.mgmt.DataBrowser.query.internal.PrimitiveTypeResultImpl;

import dunit.DistributedTestCase;
import dunit.Host;
import dunit.SerializableRunnable;
import dunit.SerializableCallable;
import dunit.VM;

public abstract class DataBrowserDUnitTestCase extends DistributedTestCase {
  static ConfigHashtable conftab = TestConfig.tab();
  public static final String DEFAULT_SERVER_CACHE_CONFIG = "server";
  public static final String DEFAULT_CACHE_SERVER_CONFIG_NAME = "bridge";
  
  public static final String DEFAULT_REGION_NAME = "Customer";
  public static final String DEFAULT_REGION_PATH = "/"+DEFAULT_REGION_NAME;
  
  protected VM server;  
  protected VM browser;
  protected static GemFireConnection connection;
    
  public DataBrowserDUnitTestCase(String name) {
    super(name); 
  }
  
  public boolean startCacheServer() {
    return true;
  }
  
  public boolean shutdown() {
    return false;
  }
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    final Host host = Host.getHost(0);
    server = host.getVM(0);
    browser = host.getVM(1);
    
    /**
     * During Test setup, following steps are executed.
     * 1. Start the GemFire distributed system.
     * 2. Prepare the regions to be used.
     * 3. Populate the regions.
     **/    
    final EndPoint endpt = (EndPoint)server.invoke(new SerializableCallable() {
      public Object call() {
        boolean isConnected = isConnectedToDS();
        if(!isConnected) {
          Log.getLogWriter().info("connecting to GemFire distributed system.");
          DistributedSystem system = getSystem();
          initCache(system, startCacheServer());        
          
          Log.getLogWriter().info("connected to GemFire distributed system.");
         }   
        
        CacheServer server = BridgeHelper.getBridgeServer();
        
        InetAddress addr = HostHelper.getIPAddress();
        EndPoint endpt = new EndPoint();
        endpt.setHostName(addr.getHostName());
        endpt.setPort(server.getPort());  
        
        Log.getLogWriter().info("CACHE-SERVER ENDPOINT :"+endpt);
         
        populateData();     
        
        return endpt;
        }
      });
     

    final ClientConfiguration conf = new ClientConfiguration();
    conf.addCacheServer(endpt);    
    Log.getLogWriter().info("CACHE-SERVER ENDPOINT :"+endpt);
    
      browser.invoke(new SerializableRunnable() {
       public void run() {
        if (connection != null)
          return;

        try {
          Log.getLogWriter().info("DataBrowser connecting "+endpt);
          connection = ConnectionFactory.createGemFireConnection(conf);
          Log.getLogWriter().info("DataBrowser connected successfully.");
          Assert.assertNotNull(connection);
        }
        catch (ConnectionFailureException e) {
          fail("DataBrowser failed to connect to the GemFire system...", e);
        }
      }
    });
      
      Log.getLogWriter().info("completed setUp :"+this.getName());
  }

  public void tearDown2() throws Exception {
    super.tearDown2();
    
    server.invoke(new SerializableRunnable() {
      public void run() {
        if(isConnectedToDS()) {
          cleanUpData();  
        }
         
        if(shutdown()) {
          Log.getLogWriter().info("disconnecting from GemFire distributed system.");
          disconnectFromDS();
          Log.getLogWriter().info("disconnected from GemFire distributed system.");
        }
       }
      });
  }
  
  
     
  public Cache initCache(DistributedSystem system, boolean isCacheServer) {
    Cache cache = CacheHelper.createCache(DEFAULT_SERVER_CACHE_CONFIG);    
       
    Map regions = TestConfig.getInstance().getRegionDescriptions();
    Set keySet = regions.keySet();
    Iterator iter = keySet.iterator();
    while(iter.hasNext()) {
      String name = (String)iter.next();
      if(name.startsWith(DEFAULT_SERVER_CACHE_CONFIG)) {
        RegionHelper.createRegion(name);       
      }     
    }
    
    Region root = cache.getRegion(DEFAULT_REGION_PATH);
    Assert.assertNotNull(root);
  
    if(isCacheServer)
      BridgeHelper.startBridgeServer(DEFAULT_CACHE_SERVER_CONFIG_NAME);    
    return cache;
  }
  
  public void populateData() {
   //Do nothing. The individual test has to populate the data that is required for testing.  
  }
  
  public void cleanUpData() {
    cleanUpRegion(DEFAULT_REGION_PATH);
  }
  
  public void populateRegion(String regionPath, Object[] objects) {
    Log.getLogWriter().info("Inside populateRegion 1"); 
    Cache cache = CacheHelper.getCache();
    Assert.assertNotNull(cache);
    
    Region region = cache.getRegion(regionPath);
    Assert.assertNotNull(region);
    
    Log.getLogWriter().info("Inside populateRegion 2"); 
    for(int i = 0 ; i < objects.length ; i++) {
      Object key = NameFactory.getNextPositiveObjectName();
      Object value = objects[i];
      region.put(key, value);
      Log.getLogWriter().info("Put object :"+value+" in Region :"+regionPath);      
    }  
    Log.getLogWriter().info("Inside populateRegion 3"); 
  }  
  
  public void cleanUpRegion(String regionPath) {
    Cache cache = CacheHelper.getCache();
    Assert.assertNotNull(cache);
    
    Region region = cache.getRegion(regionPath);
    Assert.assertNotNull(region);
    
    region.clear();    
    Log.getLogWriter().info("All the data is cleared. Region :"+regionPath); 
  }
  
  protected int executeDirectQuery(QueryService svc, String query) {   
    Query queryObj = svc.newQuery(query) ;
    Object gfe_result = null;
  
     try {
       gfe_result = queryObj.execute();
     } catch (Exception e) {
       fail("Failed to execute query : "+query, e);          
     }
  
     Assert.assertTrue((gfe_result instanceof SelectResults));
     SelectResults gfe_selectresult = (SelectResults)gfe_result;
     return gfe_selectresult.size();
  }
  
  protected int executeQueryThroughBrowser(GemFireConnection connection, GemFireMember member, String query) {
    int count = 0;
    try {
      QueryResult db_result = connection.executeQuery(query, member);
      count = db_result.getQueryResult().size();                
    }
    catch (QueryExecutionException e) {
      fail("Failed to execute query through browser : "+query, e);
    } 
    
    return count;
  }
  
  protected void verifyType(IntrospectionResult metaInfo, Class javaType, List expFields, List expClassTypes, List<Integer> expTypes) {
    Assert.assertEquals(javaType, metaInfo.getJavaType());
    Assert.assertEquals(expFields.size(), metaInfo.getColumnCount());
    
    for(int i = 0 ; i < metaInfo.getColumnCount() ; i++) {
      try {
        String columnName = metaInfo.getColumnName(i);
        String columnClassType = metaInfo.getColumnClass(i).getCanonicalName();
        int columnType = metaInfo.getColumnType(i);
        
        Assert.assertTrue("Column-Name :"+columnName, expFields.contains(columnName));
        int index = expFields.indexOf(columnName);        
        Assert.assertTrue("Column Name :"+columnName+getMessage(expClassTypes.get(index).toString(), columnClassType), expClassTypes.get(index).equals(columnClassType));
        Assert.assertTrue("Column Name :"+columnName+getMessage(expTypes.get(index).toString(), String.valueOf(columnType)), expTypes.get(index).intValue() == columnType);
      }
      catch (ColumnNotFoundException e) {
        fail("Column with index "+i+" is not found.",e);      
      }           
     }     
  }
  
  private static String getMessage(String expectedVal, String actualVal) {
    String result = "Expected value : "+expectedVal+" actual value : "+actualVal;
    return result;
  }
  
  protected void verifyPrimitiveType(IntrospectionResult metaInfo, Class type) {
    List fields = Arrays.asList(new String[] {PrimitiveTypeResultImpl.CONST_COLUMN_NAME});
    List columnclasstypes = Arrays.asList(new String[] {type.getCanonicalName()}); 
    List<Integer> columntypes = Arrays.asList(new Integer[] {IntrospectionResult.PRIMITIVE_TYPE_COLUMN});
    Log.getLogWriter().info("Verifying "+type+" type.");
    verifyType(metaInfo, type, fields, columnclasstypes, columntypes);    
  } 
}

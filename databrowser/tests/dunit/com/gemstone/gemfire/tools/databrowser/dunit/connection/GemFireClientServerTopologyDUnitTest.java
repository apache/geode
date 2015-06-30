package com.gemstone.gemfire.tools.databrowser.dunit.connection;

import hydra.BridgeHelper;
import hydra.CacheHelper;
import hydra.Log;
import hydra.BridgeHelper.Endpoint;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

import junit.framework.Assert;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.mgmt.DataBrowser.model.member.CacheServerInfo;
import com.gemstone.gemfire.mgmt.DataBrowser.model.member.GemFireMember;
import com.gemstone.gemfire.mgmt.DataBrowser.model.region.GemFireRegion;
import com.gemstone.gemfire.tools.databrowser.dunit.DataBrowserDUnitTestCase;

import dunit.Host;
import dunit.SerializableRunnable;

public class GemFireClientServerTopologyDUnitTest extends DataBrowserDUnitTestCase {
  
  public static final String SUBREGION_NAME = "Region1";
  public static final String SUBREGION_PATH = DEFAULT_REGION_PATH+"/Region1";
  
  public GemFireClientServerTopologyDUnitTest(String name) {
    super(name); 
  }
  
//  @Override
  public void setUp() throws Exception {
    super.setUp();
    
    server.invoke(new SerializableRunnable() {
      public void run() {
        Cache cache = CacheHelper.getCache();
        Assert.assertNotNull(cache);
        
        Assert.assertTrue("Cache Server is down", cache.getCacheServers().size() > 0);
        
        Region root = cache.getRegion(DEFAULT_REGION_PATH);
        Assert.assertNotNull(root);
        
        AttributesFactory factory = new AttributesFactory();
        factory.setDataPolicy(DataPolicy.EMPTY);
        factory.setScope(Scope.DISTRIBUTED_NO_ACK);
        RegionAttributes attr = factory.create();
        
        Region sub = root.createSubregion(SUBREGION_NAME, attr);
        Assert.assertNotNull(sub);        
        
        Assert.assertNotNull(cache.getRegion(SUBREGION_PATH));
        
        Assert.assertTrue("Cache Server is down", cache.getCacheServers().size() > 0);     
        
      }});
   }
  
   @Override
  public void tearDown2() throws Exception {
    super.tearDown2();
    
    server.invoke(new SerializableRunnable() {
      public void run() {
        Cache cache = CacheHelper.getCache();
        Assert.assertNotNull(cache);
        
        Region region = cache.getRegion(SUBREGION_PATH);
        Assert.assertNotNull(region);
        
        //region.destroyRegion();  
               
      }}); 
    
  }
   
  public void testGemFireCacheServerConfiguration() {
    final Host host = Host.getHost(0);
    
    try {
      Thread.sleep(10000);
    }
    catch (InterruptedException e1) {
      //Do nothing.
    }

    browser.invoke(new SerializableRunnable() {
      public void run() {
//        GemFireMember[] members = connection.getMembers();
//        Assert.assertEquals(1, members.length);
//        Assert.assertEquals(GemFireMember.GEMFIRE_CACHE_SERVER, members[0].getType());
//        CacheServerInfo[] info = members[0].getCacheServers();
//        Assert.assertEquals(1, info.length);
//        Log.getLogWriter().info(members[0].toString()); 
//        
//        String id = members[0].getId();
//        
//        
//        try {
//         InetAddress address = InetAddress.getByName(host.getHostName());
//         Assert.assertNotNull(address);
//         Assert.assertEquals(address.getHostAddress(), members[0].getHost());
//        }
//        catch (UnknownHostException e) {
//          fail("Could not identify the host information of the cache-server", e);          
//        }       
//        
//        List endpts = BridgeHelper.getEndpoints();
//        Assert.assertEquals(1, endpts.size());
//        Endpoint endp = (Endpoint)endpts.get(0);
//        Assert.assertEquals(endp.getPort(), info[0].getPort());
//        
//        Log.getLogWriter().info(members[0].toString());
//        GemFireRegion[] rootRegions = (connection.getMember(id)).getRootRegions();
//        //GemFireRegion[] rootRegions = (members[0]).getRootRegions();
//        Assert.assertEquals(1, rootRegions.length);
//        Assert.assertEquals(DEFAULT_REGION_NAME, rootRegions[0].getName());
//        Assert.assertEquals(DEFAULT_REGION_PATH, rootRegions[0].getFullPath());
//        Assert.assertEquals(String.valueOf(Scope.DISTRIBUTED_NO_ACK), rootRegions[0].getScope());
//        Assert.assertEquals(String.valueOf(DataPolicy.REPLICATE), rootRegions[0].getDataPolicy());
//
//        Assert.assertEquals(1, ((connection.getMember(id)).getRootRegions()[0]).getSubRegions().length);    
//        
//        GemFireRegion sub = rootRegions[0].getSubRegions()[0];
//        Assert.assertEquals(SUBREGION_NAME, sub.getName());
//        Assert.assertEquals(SUBREGION_PATH, sub.getFullPath());
//        Assert.assertEquals(String.valueOf(Scope.DISTRIBUTED_NO_ACK), sub.getScope());
//        Assert.assertEquals(String.valueOf(DataPolicy.EMPTY), sub.getDataPolicy());
//        Assert.assertEquals(0, sub.getSubRegions().length); 
      }
    });
  }
 
}

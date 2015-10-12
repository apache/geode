/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.cache.ha.HAContainerWrapper;
import com.gemstone.gemfire.internal.cache.ha.HARegionQueue;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * Test to verify that each bridge sever creates its own client_messages_region
 * at its start and destroys it when it stops.
 * 
 * @author aingle
 * @since 5.7
 */
@Category(IntegrationTest.class)
public class ClientMessagesRegionCreationAndDestroyJUnitTest {

  /** The cache instance */
  private Cache cache = null;
  // max number of bridge server can attached to the cache 
  private int brigeNum = 5;
  // stores corresponding names of client messages region created by bridge
  // server
  private HashSet regionNames = new HashSet();
  
  /**
   * Create the cache in setup.
   * 
   * @throws Exception -
   *           thrown if any exception occurs in setUp
   */
  @Before
  public void setUp() throws Exception
  {
    cache = createCache();
  }
  
  /**
   * Create and attach bridge server to cache
   * @throws IOException
   */
  
  private void attachBridgeServer() throws IOException {
    CacheServerImpl server = (CacheServerImpl)cache.addCacheServer();
    assertNotNull(server);
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    server.setPort(port);
    server.getClientSubscriptionConfig().setEvictionPolicy(HARegionQueue.HA_EVICTION_POLICY_ENTRY);
    server.start();
    assertNotNull("client messages region is null ", server.getAcceptor().getCacheClientNotifier().getHaContainer());
//  check for is LIFO Enable
    String regionName = ((HAContainerWrapper)server.getAcceptor().getCacheClientNotifier().getHaContainer()).getName();
    EvictionAttributesImpl ea = (EvictionAttributesImpl)cache.getRegion(
        Region.SEPARATOR + regionName).getAttributes().getEvictionAttributes();
    assertTrue("Eviction Algorithm is not LIFO", ea.isLIFO());
    // The CacheClientNotifier is a singleton. 
    if (cache.getCacheServers().size() <= 1) {
      assertTrue("client messages region name should not be present ", (regionNames).add(regionName));
    } else {
      assertTrue("client messages region name should have been already present ", (regionNames).contains(regionName));      
    }
  }
  
  /**
   * This test does the following :<br>
   * 1)Verify client messages region get created when bridge serve start's <br>
   * 2)Verify client messages region get destroy when bridge serve stop's <br>
   */
  @Test
  public void testCreationAndDestroyOfClientMessagesRegion() {
    for (int i = 0; i < 5; i++) {
      // sequential
      attachmentOfBridgeServer();
    }
    // making sure stop all servers
    dettachmentOfBridgeServer();
  }
  
  /**
   * Attach bridge server
   */
  private void attachmentOfBridgeServer() {
    if (cache.getCacheServers().size() < brigeNum) {
      try {
        // attaching and starting bridge server
        attachBridgeServer();
      }
      catch (IOException e) {
        e.printStackTrace();
      }
    }
  }
  
  /**
   * Stop's all bridge servers attached
   */
  private void dettachmentOfBridgeServer() {
    // detach all bridge server to test destroy of client_messages_region
    for (Iterator itr = cache.getCacheServers().iterator(); itr.hasNext();) {
      CacheServerImpl server = (CacheServerImpl)itr.next();
      String rName = ((HAContainerWrapper)server.getAcceptor().getCacheClientNotifier().getHaContainer()).getName();
      assertNotNull("client messages region is null ", cache.getRegion(Region.SEPARATOR + rName));
      server.stop();
      
      if (!itr.hasNext()) {
        assertNull("client messages region is not null ", cache.getRegion(Region.SEPARATOR + rName));
      }
    }
  }
  
  /**
   * Close the cache in tear down *
   * 
   * @throws Exception -
   *           thrown if any exception occurs in tearDown
   */
  @After
  public void tearDown() throws Exception {
    // its stops all bridge serves associated with cache 
    // delete client_messages_region
    cache.close();
  }
  
  /**
   * Creates the cache instance for the test
   * 
   * @return the cache instance
   * @throws CacheException -
   *           thrown if any exception occurs in cache creation
   */
  private Cache createCache() throws CacheException
  {
    return CacheFactory.create(DistributedSystem.connect(new Properties()));
  }
  
}

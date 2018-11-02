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

import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
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

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.cache.ha.HAContainerWrapper;
import org.apache.geode.internal.cache.ha.HARegionQueue;

/**
 * Test to verify that each bridge sever creates its own client_messages_region at its start and
 * destroys it when it stops.
 *
 * @since GemFire 5.7
 */
public class ClientMessagesRegionCreationAndDestroyJUnitTest {

  /** The cache instance */
  private Cache cache = null;
  // max number of cache server can attached to the cache
  private int brigeNum = 5;
  // stores corresponding names of client messages region created by bridge
  // server
  private HashSet regionNames = new HashSet();

  /**
   * Create the cache in setup.
   *
   * @throws Exception - thrown if any exception occurs in setUp
   */
  @Before
  public void setUp() throws Exception {
    cache = createCache();
  }

  /**
   * Create and attach cache server to cache
   *
   */

  private void attachBridgeServer() throws IOException {
    CacheServerImpl server = (CacheServerImpl) cache.addCacheServer();
    assertNotNull(server);
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    server.setPort(port);
    server.getClientSubscriptionConfig().setEvictionPolicy(HARegionQueue.HA_EVICTION_POLICY_ENTRY);
    server.start();
    assertNotNull("client messages region is null ",
        server.getAcceptor().getCacheClientNotifier().getHaContainer());
    // check for is LIFO Enable
    String regionName =
        ((HAContainerWrapper) server.getAcceptor().getCacheClientNotifier().getHaContainer())
            .getName();
    EvictionAttributesImpl ea = (EvictionAttributesImpl) cache
        .getRegion(Region.SEPARATOR + regionName).getAttributes().getEvictionAttributes();
    assertTrue("Eviction Algorithm is not LIFO", ea.isLIFO());
    // The CacheClientNotifier is a singleton.
    if (cache.getCacheServers().size() <= 1) {
      assertTrue("client messages region name should not be present ",
          (regionNames).add(regionName));
    } else {
      assertTrue("client messages region name should have been already present ",
          (regionNames).contains(regionName));
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
   * Attach cache server
   */
  private void attachmentOfBridgeServer() {
    if (cache.getCacheServers().size() < brigeNum) {
      try {
        // attaching and starting cache server
        attachBridgeServer();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * Stop's all cache servers attached
   */
  private void dettachmentOfBridgeServer() {
    // detach all cache server to test destroy of client_messages_region
    for (Iterator itr = cache.getCacheServers().iterator(); itr.hasNext();) {
      CacheServerImpl server = (CacheServerImpl) itr.next();
      String rName =
          ((HAContainerWrapper) server.getAcceptor().getCacheClientNotifier().getHaContainer())
              .getName();
      assertNotNull("client messages region is null ", cache.getRegion(Region.SEPARATOR + rName));
      server.stop();

      if (!itr.hasNext()) {
        assertNull("client messages region is not null ",
            cache.getRegion(Region.SEPARATOR + rName));
      }
    }
  }

  /**
   * Close the cache in tear down *
   *
   * @throws Exception - thrown if any exception occurs in tearDown
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
   * @throws CacheException - thrown if any exception occurs in cache creation
   */
  private Cache createCache() throws CacheException {
    Properties p = new Properties();
    p.put(MCAST_PORT, "0");
    return CacheFactory.create(DistributedSystem.connect(p));
  }

}

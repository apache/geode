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
package org.apache.geode.cache30;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.ClientServerTest;
import org.apache.geode.test.junit.categories.MembershipTest;
import org.apache.geode.util.internal.GeodeGlossary;

@Category({MembershipTest.class, ClientServerTest.class})
public class ReconnectedCacheServerDUnitTest extends JUnit4CacheTestCase {

  private static final long serialVersionUID = 1L;

  private boolean addedCacheServer = false;

  private Cache cache;

  @Override
  public final void postSetUp() {
    cache = getCache();
    if (cache.getCacheServers().isEmpty()) {
      CacheServer server = cache.addCacheServer();
      server.setPort(0);
      addedCacheServer = true;
    }
  }

  @Override
  public Properties getDistributedSystemProperties() {
    Properties props = new Properties(super.getDistributedSystemProperties());
    props.setProperty(ConfigurationProperties.USE_CLUSTER_CONFIGURATION, "true");
    return props;
  }

  @Override
  public final void preTearDownCacheTestCase() throws Exception {
    if (addedCacheServer && cache != null && !cache.isClosed()) {
      // since I polluted the cache I should shut it down in order
      // to avoid affecting other tests
      cache.close();
    }
  }

  @Test
  public void testCacheServerConfigRetained() {
    // make sure the environment isn't polluted
    assertFalse(
        Boolean.getBoolean(GeodeGlossary.GEMFIRE_PREFIX + "autoReconnect-useCacheXMLFile"));

    InternalCache gc = (InternalCache) cache;

    // fool the system into thinking cluster-config is being used
    gc.saveCacheXmlForReconnect();

    // the cache server config should now be stored in the cache's config
    assertFalse(gc.getCacheServers().isEmpty());
    assertNotNull(gc.getCacheConfig().getCacheServerCreation());
  }

  @Test
  public void testDefaultCacheServerNotCreatedOnReconnect() {

    assertFalse(
        Boolean.getBoolean(GeodeGlossary.GEMFIRE_PREFIX + "autoReconnect-useCacheXMLFile"));

    GemFireCacheImpl gc = (GemFireCacheImpl) cache;

    gc.saveCacheXmlForReconnect();

    // the cache server config should now be stored in the cache's config
    assertFalse(gc.getCacheServers().isEmpty());
    int numServers = gc.getCacheServers().size();

    assertNotNull(gc.getCacheConfig().getCacheServerCreation());

    InternalDistributedSystem system = gc.getInternalDistributedSystem();
    system.createAndStartCacheServers(gc.getCacheConfig().getCacheServerCreation(), gc);

    assertEquals("found these cache servers:" + gc.getCacheServers(), numServers,
        gc.getCacheServers().size());

  }
}

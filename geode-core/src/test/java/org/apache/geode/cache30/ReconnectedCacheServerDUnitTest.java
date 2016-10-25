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
package org.apache.geode.cache30;

import org.junit.experimental.categories.Category;
import org.junit.Test;

import static org.junit.Assert.*;

import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.gms.MembershipManagerHelper;
import org.apache.geode.distributed.internal.membership.gms.mgr.GMSMembershipManager;
import org.apache.geode.internal.cache.GemFireCacheImpl;


@Category(DistributedTest.class)
public class ReconnectedCacheServerDUnitTest extends JUnit4CacheTestCase {

  public ReconnectedCacheServerDUnitTest() {
    super();
  }

  private static final long serialVersionUID = 1L;
  
  private boolean addedCacheServer = false;

  private Cache cache;

  @Override
  public final void postSetUp() {
    this.cache = getCache();
    if (this.cache.getCacheServers().isEmpty()) {
      CacheServer server = this.cache.addCacheServer();
      server.setPort(0);
      addedCacheServer = true;
    }
  }
  
  @Override
  public final void preTearDownCacheTestCase() throws Exception {
    if (addedCacheServer && this.cache != null && !this.cache.isClosed()) {
      // since I polluted the cache I should shut it down in order
      // to avoid affecting other tests
      this.cache.close();
    }
  }

  @Test
  public void testCacheServerConfigRetained() {
    // make sure the environment isn't polluted
    assertFalse(Boolean.getBoolean(DistributionConfig.GEMFIRE_PREFIX + "autoReconnect-useCacheXMLFile"));

    GemFireCacheImpl gc = (GemFireCacheImpl)this.cache;
    
    // fool the system into thinking cluster-config is being used
    GMSMembershipManager mgr = (GMSMembershipManager)MembershipManagerHelper
           .getMembershipManager(gc.getDistributedSystem());
    mgr.saveCacheXmlForReconnect(true);
    
    // the cache server config should now be stored in the cache's config
    assertFalse(gc.getCacheServers().isEmpty());
    assertNotNull(gc.getCacheConfig().getCacheServerCreation());
  }

  @Test
  public void testDefaultCacheServerNotCreatedOnReconnect() {

    assertFalse(Boolean.getBoolean(DistributionConfig.GEMFIRE_PREFIX + "autoReconnect-useCacheXMLFile"));
    
    GemFireCacheImpl gc = (GemFireCacheImpl)this.cache;

    // fool the system into thinking cluster-config is being used
    GMSMembershipManager mgr = (GMSMembershipManager)MembershipManagerHelper
        .getMembershipManager(gc.getDistributedSystem());
    final boolean sharedConfigEnabled = true;
    mgr.saveCacheXmlForReconnect(sharedConfigEnabled);

    // the cache server config should now be stored in the cache's config
    assertFalse(gc.getCacheServers().isEmpty());
    int numServers = gc.getCacheServers().size();

    assertNotNull(gc.getCacheConfig().getCacheServerCreation());

    InternalDistributedSystem system = gc.getDistributedSystem();
    system.createAndStartCacheServers(gc.getCacheConfig().getCacheServerCreation(), gc);

    assertEquals("found these cache servers:" + gc.getCacheServers(),
        numServers, gc.getCacheServers().size());
      
  }
}

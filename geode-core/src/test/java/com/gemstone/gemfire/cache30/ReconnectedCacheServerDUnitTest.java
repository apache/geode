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
package com.gemstone.gemfire.cache30;

import com.gemstone.gemfire.distributed.internal.membership.gms.MembershipManagerHelper;
import com.gemstone.gemfire.distributed.internal.membership.gms.mgr.GMSMembershipManager;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;


public class ReconnectedCacheServerDUnitTest extends CacheTestCase {

  public ReconnectedCacheServerDUnitTest(String name) {
    super(name);
  }

  private static final long serialVersionUID = 1L;
  
  private boolean addedCacheServer = false;
  
  @Override
  public void setUp() {
    getCache();
    if (cache.getCacheServers().isEmpty()) {
      cache.addCacheServer();
      addedCacheServer = true;
    }
  }
  
  @Override
  protected final void preTearDownCacheTestCase() throws Exception {
    if (addedCacheServer && cache != null && !cache.isClosed()) {
      // since I polluted the cache I should shut it down in order
      // to avoid affecting other tests
      cache.close();
    }
  }

  public void testCacheServerConfigRetained() {
    // make sure the environment isn't polluted
    assertFalse(Boolean.getBoolean("gemfire.autoReconnect-useCacheXMLFile"));

    GemFireCacheImpl gc = (GemFireCacheImpl)cache;
    
    // fool the system into thinking cluster-config is being used
    GMSMembershipManager mgr = (GMSMembershipManager)MembershipManagerHelper
           .getMembershipManager(gc.getDistributedSystem());
    mgr.saveCacheXmlForReconnect(true);
    
    // the cache server config should now be stored in the cache's config
    assertFalse(gc.getCacheServers().isEmpty());
    assertNotNull(gc.getCacheConfig().getCacheServerCreation());
  }

}

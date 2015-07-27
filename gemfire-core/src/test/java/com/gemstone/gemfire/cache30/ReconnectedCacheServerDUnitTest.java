/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
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
  public void tearDown2() {
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

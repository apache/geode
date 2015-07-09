/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache30;

//import java.util.*;
import com.gemstone.gemfire.cache.*;


import dunit.*;

/**
 * This class tests the functionality of a cache {@link Region region}
 * that has a scope of {@link Scope#DISTRIBUTED_ACK distributed ACK}.
 *
 * @author David Whitlock
 * @since 3.0
 */
public class PreloadedRegionTestCase extends MultiVMRegionTestCase {

  public PreloadedRegionTestCase(String name) {
    super(name);
  }

  /**
   * Returns region attributes for a <code>GLOBAL</code> region
   */
  protected RegionAttributes getRegionAttributes() {
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.PRELOADED);
    return factory.create();
  } 

  /**
   * Tests that created entries are not propagated to other caches
   */
  public void testDistributedCreate() throws Exception {
    final String rgnName = getUniqueName();

    SerializableRunnable create = new SerializableRunnable("testDistributedCreate: Create Region") {
      public void run() {
        try {
          createRegion(rgnName);
          getSystem().getLogWriter().info("testDistributedCreate: Created Region");
        }
        catch (CacheException e) {
          fail("While creating region", e);
        }
      }
    };

    SerializableRunnable newKey = new SerializableRunnable("testDistributedCreate: Create Key") {
      public void run() {
        try {
          Region root = getRootRegion("root");
          Region rgn = root.getSubregion(rgnName);
          rgn.create("key", null);
          getSystem().getLogWriter().info("testDistributedCReate: Created Key");
        }
        catch (CacheException e) {
          fail("While creating region", e);
        }
      }
    };


    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);

    // Create empty region
    vm0.invoke(create);

    // Create empty version locally
    Region rgn = createRegion(rgnName);
    
    // Add a key in first cache
    vm0.invoke(newKey);
    
    // We should NOT see the update here.
    assertTrue(rgn.getEntry("key") == null);
  }

}

/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache30;

//import java.util.*;
import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.Delta;
import com.gemstone.gemfire.InvalidDeltaException;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.Token;
import com.gemstone.gemfire.internal.cache.TombstoneService;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

import junit.framework.AssertionFailedError;

import dunit.*;

/**
 * This class tests the functionality of a cache {@link Region region}
 * that has a scope of {@link Scope#DISTRIBUTED_ACK distributed ACK}.
 *
 * @author David Whitlock
 * @author Bruce Schuchardt
 * @since 3.0
 */
public class DistributedAckRegionDUnitTest extends MultiVMRegionTestCase {


  
  public DistributedAckRegionDUnitTest(String name) {
    super(name);
  }

  /**
   * Returns region attributes for a <code>GLOBAL</code> region
   */
  protected RegionAttributes getRegionAttributes() {
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.PRELOADED);
    factory.setEarlyAck(false);
    factory.setConcurrencyChecksEnabled(false);
    return factory.create();
  }

  public Properties getDistributedSystemProperties() {
    Properties p = new Properties();
    p.put(DistributionConfig.STATISTIC_SAMPLING_ENABLED_NAME, "true");
    p.put(DistributionConfig.LOG_LEVEL_NAME, "config");
    return p;
  }

  //////////////////////  Test Methods  //////////////////////

  /**
   * Tests the compatibility of creating certain kinds of subregions
   * of a local region.
   *
   * @see Region#createSubregion
   */
  public void testIncompatibleSubregions()
    throws CacheException, InterruptedException {

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    // Scope.GLOBAL is illegal if there is any other cache in the
    // distributed system that has the same region with
    // Scope.DISTRIBUTED_ACK.

    final String name = this.getUniqueName() + "-ACK";
    vm0.invoke(new SerializableRunnable("Create ACK Region") {
        public void run() {
          try {
            createRegion(name, "INCOMPATIBLE_ROOT", getRegionAttributes());

          } catch (CacheException ex) {
            fail("While creating ACK region", ex);
          }
        }
      });

    vm1.invoke(new SerializableRunnable("Create GLOBAL Region") {
        public void run() {
          try {
            AttributesFactory factory =
              new AttributesFactory(getRegionAttributes());
            factory.setScope(Scope.GLOBAL);
            try {
              createRegion(name, "INCOMPATIBLE_ROOT", factory.create());
              fail("Should have thrown an IllegalStateException");
            } catch (IllegalStateException ex) {
              // pass...
            }

          } catch (CacheException ex) {
            fail("While creating GLOBAL Region", ex);
          }
        }
      });
    vm1.invoke(new SerializableRunnable("Create NOACK Region") {
        public void run() {
          try {
            AttributesFactory factory =
              new AttributesFactory(getRegionAttributes());
            factory.setScope(Scope.DISTRIBUTED_NO_ACK);
            try {
              createRegion(name, "INCOMPATIBLE_ROOT", factory.create());
              fail("Should have thrown an IllegalStateException");
            } catch (IllegalStateException ex) {
              // pass...
            }

          } catch (CacheException ex) {
            fail("While creating NOACK Region", ex);
          }
        }
      });
  } 
  

  
}

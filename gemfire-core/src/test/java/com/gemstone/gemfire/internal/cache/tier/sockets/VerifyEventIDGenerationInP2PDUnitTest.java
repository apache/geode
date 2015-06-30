/*
 * ========================================================================= 
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved. 
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 * =========================================================================
 */
package com.gemstone.gemfire.internal.cache.tier.sockets;

import java.util.Properties;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.MirrorType;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.cache.EntryEventImpl;
import com.gemstone.gemfire.internal.cache.EventID;

import dunit.DistributedTestCase;
import dunit.Host;
import dunit.VM;

/**
 * 
 * To verify that new events get generated on the node by get operation for key
 * that is not present in the node's region.
 * Currently test is commented because of the bug.
 * 
 * @author Suyog Bhokare
 * 
 */

public class VerifyEventIDGenerationInP2PDUnitTest extends DistributedTestCase
{
  private static Cache cache = null;

  static VM vm0 = null;

  private static final String REGION_NAME = "VerifyEventIDGenerationInP2PDUnitTest_region";

  protected static EventID eventId;

  static boolean testEventIDResult = true;

  static boolean receiver = true;

  static boolean gotCallback = false;

  /* Constructor */

  public VerifyEventIDGenerationInP2PDUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception
  {
    super.setUp();
    final Host host = Host.getHost(0);
    vm0 = host.getVM(0);
    createServerCache();
    vm0
        .invoke(VerifyEventIDGenerationInP2PDUnitTest.class,
            "createServerCache");
    receiver = false;
  }

  public void _testEventIDGeneration() throws Exception
  {
    createEntry();
    vm0.invoke(VerifyEventIDGenerationInP2PDUnitTest.class, "get");
    Boolean pass = (Boolean)vm0.invoke(
        VerifyEventIDGenerationInP2PDUnitTest.class, "verifyResult");
    assertFalse(pass.booleanValue());
  }

  public void testDummy() throws Exception
  {
    
  }
  
  private void createCache(Properties props) throws Exception
  {
    DistributedSystem ds = getSystem(props);
    ds.disconnect();
    ds = getSystem(props);
    assertNotNull(ds);
    cache = CacheFactory.create(ds);
    assertNotNull(cache);
  }

  public static void setEventIDData(Object evID)
  {
    eventId = (EventID)evID;
  }

  public static void createServerCache() throws Exception
  {
    new VerifyEventIDGenerationInP2PDUnitTest("temp")
        .createCache(new Properties());
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setMirrorType(MirrorType.NONE);
    factory.setCacheListener(new CacheListenerAdapter() {

      public void afterCreate(EntryEvent event)
      {
        if (!receiver) {
          vm0.invoke(EventIDVerificationInP2PDUnitTest.class, "setEventIDData",
              new Object[] { ((EntryEventImpl)event).getEventId() });
        }
        else {
          testEventIDResult = ((EntryEventImpl)event).getEventId().equals(
              eventId);
        }
      }

    });

    RegionAttributes attrs = factory.create();
    cache.createRegion(REGION_NAME, attrs);

  }

  public static void createEntry()
  {
    try {
      Region r = cache.getRegion("/" + REGION_NAME);
      assertNotNull(r);

      if (!r.containsKey("key-1")) {
        r.create("key-1", "key-1");
      }
      // Verify that no invalidates occurred to this region
      assertEquals(r.getEntry("key-1").getValue(), "key-1");
    }
    catch (Exception ex) {
      fail("failed while createEntries()", ex);
    }
  }

  public static void get()
  {
    try {
      Region r = cache.getRegion("/" + REGION_NAME);
      assertNotNull(r);
      r.get("key-1");
    }
    catch (Exception ex) {
      fail("failed while r.put()", ex);
    }
  }

  public static Boolean verifyResult()
  {
    boolean temp = testEventIDResult;
    testEventIDResult = false;
    return new Boolean(temp);
  }

  public void tearDown2() throws Exception
  {
    super.tearDown2();
    closeCache();
    vm0.invoke(VerifyEventIDGenerationInP2PDUnitTest.class, "closeCache");
  }

  public static void closeCache()
  {
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
    }
  }
}

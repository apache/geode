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
package org.apache.geode.internal.cache.tier.sockets;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import java.util.Properties;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.MirrorType;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.ClientServerTest;

/**
 * To verify that new events get generated on the node by get operation for key that is not present
 * in the node's region. Currently test is commented because of the bug.
 */
@Category({ClientServerTest.class})
public class VerifyEventIDGenerationInP2PDUnitTest extends JUnit4DistributedTestCase {

  private static Cache cache = null;

  static VM vm0 = null;

  private static final String REGION_NAME = "VerifyEventIDGenerationInP2PDUnitTest_region";

  protected static EventID eventId;

  static boolean testEventIDResult = true;

  static boolean receiver = true;

  static boolean gotCallback = false;

  /* Constructor */

  public VerifyEventIDGenerationInP2PDUnitTest() {
    super();
  }

  @Override
  public final void postSetUp() throws Exception {
    final Host host = Host.getHost(0);
    vm0 = host.getVM(0);
    createServerCache();
    vm0.invoke(VerifyEventIDGenerationInP2PDUnitTest::createServerCache);
    receiver = false;
  }

  @Ignore("TODO")
  @Test
  public void testEventIDGeneration() throws Exception {
    createEntry();
    vm0.invoke(VerifyEventIDGenerationInP2PDUnitTest::get);
    Boolean pass = vm0.invoke(VerifyEventIDGenerationInP2PDUnitTest::verifyResult);
    assertFalse(pass.booleanValue());
  }

  private void createCache(Properties props) throws Exception {
    DistributedSystem ds = getSystem(props);
    ds.disconnect();
    ds = getSystem(props);
    assertNotNull(ds);
    cache = CacheFactory.create(ds);
    assertNotNull(cache);
  }

  public static void setEventIDData(Object evID) {
    eventId = (EventID) evID;
  }

  public static void createServerCache() throws Exception {
    new VerifyEventIDGenerationInP2PDUnitTest().createCache(new Properties());
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setMirrorType(MirrorType.NONE);
    factory.setCacheListener(new CacheListenerAdapter() {

      @Override
      public void afterCreate(EntryEvent event) {
        if (!receiver) {
          vm0.invoke(() -> VerifyEventIDGenerationInP2PDUnitTest
              .setEventIDData(((EntryEventImpl) event).getEventId()));
        } else {
          testEventIDResult = ((EntryEventImpl) event).getEventId().equals(eventId);
        }
      }

    });

    RegionAttributes attrs = factory.create();
    cache.createRegion(REGION_NAME, attrs);

  }

  public static void createEntry() {
    try {
      Region r = cache.getRegion(SEPARATOR + REGION_NAME);
      assertNotNull(r);

      if (!r.containsKey("key-1")) {
        r.create("key-1", "key-1");
      }
      // Verify that no invalidates occurred to this region
      assertEquals(r.getEntry("key-1").getValue(), "key-1");
    } catch (Exception ex) {
      Assert.fail("failed while createEntries()", ex);
    }
  }

  public static void get() {
    try {
      Region r = cache.getRegion(SEPARATOR + REGION_NAME);
      assertNotNull(r);
      r.get("key-1");
    } catch (Exception ex) {
      Assert.fail("failed while r.put()", ex);
    }
  }

  public static Boolean verifyResult() {
    boolean temp = testEventIDResult;
    testEventIDResult = false;
    return new Boolean(temp);
  }

  @Override
  public final void preTearDown() throws Exception {
    closeCache();
    vm0.invoke(VerifyEventIDGenerationInP2PDUnitTest::closeCache);
  }

  public static void closeCache() {
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
    }
  }
}

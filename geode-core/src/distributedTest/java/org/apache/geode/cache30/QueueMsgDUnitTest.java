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

import static org.apache.geode.cache.DataPolicy.NORMAL;
import static org.apache.geode.cache.InterestPolicy.ALL;
import static org.apache.geode.cache.Operation.CREATE;
import static org.apache.geode.cache.Operation.DESTROY;
import static org.apache.geode.cache.Operation.INVALIDATE;
import static org.apache.geode.cache.Operation.PUTALL_CREATE;
import static org.apache.geode.cache.Operation.REGION_CLEAR;
import static org.apache.geode.cache.Operation.REGION_CREATE;
import static org.apache.geode.cache.Operation.REGION_INVALIDATE;
import static org.apache.geode.cache.Operation.UPDATE;
import static org.apache.geode.cache.Scope.DISTRIBUTED_ACK;
import static org.apache.geode.distributed.ConfigurationProperties.ROLES;
import static org.apache.geode.test.dunit.Host.getHost;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import org.junit.Ignore;
import org.junit.Test;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.CacheEvent;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.InterestPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionEvent;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.SubscriptionAttributes;
import org.apache.geode.internal.cache.CachePerfStats;
import org.apache.geode.internal.cache.DistributedRegion;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.WaitCriterion;

/**
 * Test to make sure message queuing works.
 *
 * @since GemFire 5.0
 */

public class QueueMsgDUnitTest extends ReliabilityTestCase {

  /**
   * Make sure that cache operations are queued when a required role is missing
   */
  @Ignore("TODO: test is disabled")
  @Test
  public void testQueueWhenRoleMissing() throws Exception {
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(DISTRIBUTED_ACK);
    DistributedRegion r = (DistributedRegion) createRootRegion(factory.create());
    final CachePerfStats stats = r.getCachePerfStats();
    int queuedOps = stats.getReliableQueuedOps();
    r.create("createKey", "createValue", "createCBArg");
    r.invalidate("createKey", "invalidateCBArg");
    r.put("createKey", "putValue", "putCBArg");
    r.destroy("createKey", "destroyCBArg");
    assertEquals(queuedOps + 4, stats.getReliableQueuedOps());
    queuedOps = stats.getReliableQueuedOps();
    {
      Map m = new TreeMap();
      m.put("aKey", "aValue");
      m.put("bKey", "bValue");
      r.putAll(m);
    }
    assertEquals(queuedOps + 2, stats.getReliableQueuedOps());
    queuedOps = stats.getReliableQueuedOps();
    r.invalidateRegion("invalidateRegionCBArg");
    assertEquals(queuedOps + 1, stats.getReliableQueuedOps());
    queuedOps = stats.getReliableQueuedOps();
    r.clear();
    assertEquals(queuedOps + 1, stats.getReliableQueuedOps());
    queuedOps = stats.getReliableQueuedOps();
    // @todo darrel: try some other ops

    VM vm = getHost(0).getVM(0);
    // now create a system that fills this role since it does not create the
    // region our queue should not be flushed
    vm.invoke(new SerializableRunnable() {
      public void run() {
        Properties config = new Properties();
        config.setProperty(ROLES, "missing");
        getSystem(config);
      }
    });

    // we still should have everything queued since the region is not created
    assertEquals(queuedOps, stats.getReliableQueuedOps());

    // now create the region
    vm.invoke(new CacheSerializableRunnable("create root") {
      public void run2() throws CacheException {
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(DISTRIBUTED_ACK);
        factory.setDataPolicy(NORMAL);
        factory.setSubscriptionAttributes(new SubscriptionAttributes(ALL));
        TestCacheListener cl = new TestCacheListener() {
          public void afterCreate2(EntryEvent event) {}

          public void afterUpdate2(EntryEvent event) {}

          public void afterInvalidate2(EntryEvent event) {}

          public void afterDestroy2(EntryEvent event) {}
        };
        cl.enableEventHistory();
        factory.addCacheListener(cl);
        createRootRegion(factory.create());
      }
    });
    // after some amount of time we should see the queuedOps flushed
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        return stats.getReliableQueuedOps() == 0;
      }

      public String description() {
        return "waiting for reliableQueuedOps to become 0";
      }
    };
    GeodeAwaitility.await().untilAsserted(ev);

    // now check that the queued op was delivered
    vm.invoke(new CacheSerializableRunnable("check") {
      public void run2() throws CacheException {
        Region r = getRootRegion();
        assertEquals(null, r.getEntry("createKey"));
        // assertIndexDetailsEquals("putValue", r.getEntry("createKey").getValue());
        {
          int evIdx = 0;
          TestCacheListener cl = (TestCacheListener) r.getAttributes().getCacheListener();
          List events = cl.getEventHistory();
          {
            CacheEvent ce = (CacheEvent) events.get(evIdx++);
            assertEquals(REGION_CREATE, ce.getOperation());
          }
          {
            EntryEvent ee = (EntryEvent) events.get(evIdx++);
            assertEquals(CREATE, ee.getOperation());
            assertEquals("createKey", ee.getKey());
            assertEquals("createValue", ee.getNewValue());
            assertEquals(null, ee.getOldValue());
            assertEquals("createCBArg", ee.getCallbackArgument());
            assertEquals(true, ee.isOriginRemote());
          }
          {
            EntryEvent ee = (EntryEvent) events.get(evIdx++);
            assertEquals(INVALIDATE, ee.getOperation());
            assertEquals("createKey", ee.getKey());
            assertEquals(null, ee.getNewValue());
            assertEquals("createValue", ee.getOldValue());
            assertEquals("invalidateCBArg", ee.getCallbackArgument());
            assertEquals(true, ee.isOriginRemote());
          }
          {
            EntryEvent ee = (EntryEvent) events.get(evIdx++);
            assertEquals(UPDATE, ee.getOperation());
            assertEquals("createKey", ee.getKey());
            assertEquals("putValue", ee.getNewValue());
            assertEquals(null, ee.getOldValue());
            assertEquals("putCBArg", ee.getCallbackArgument());
            assertEquals(true, ee.isOriginRemote());
          }
          {
            EntryEvent ee = (EntryEvent) events.get(evIdx++);
            assertEquals(DESTROY, ee.getOperation());
            assertEquals("createKey", ee.getKey());
            assertEquals(null, ee.getNewValue());
            assertEquals("putValue", ee.getOldValue());
            assertEquals("destroyCBArg", ee.getCallbackArgument());
            assertEquals(true, ee.isOriginRemote());
          }
          {
            EntryEvent ee = (EntryEvent) events.get(evIdx++);
            assertEquals(PUTALL_CREATE, ee.getOperation());
            assertEquals("aKey", ee.getKey());
            assertEquals("aValue", ee.getNewValue());
            assertEquals(null, ee.getOldValue());
            assertEquals(null, ee.getCallbackArgument());
            assertEquals(true, ee.isOriginRemote());
          }
          {
            EntryEvent ee = (EntryEvent) events.get(evIdx++);
            assertEquals(PUTALL_CREATE, ee.getOperation());
            assertEquals("bKey", ee.getKey());
            assertEquals("bValue", ee.getNewValue());
            assertEquals(null, ee.getOldValue());
            assertEquals(null, ee.getCallbackArgument());
            assertEquals(true, ee.isOriginRemote());
          }
          {
            RegionEvent re = (RegionEvent) events.get(evIdx++);
            assertEquals(REGION_INVALIDATE, re.getOperation());
            assertEquals("invalidateRegionCBArg", re.getCallbackArgument());
            assertEquals(true, re.isOriginRemote());
          }
          {
            RegionEvent re = (RegionEvent) events.get(evIdx++);
            assertEquals(REGION_CLEAR, re.getOperation());
            assertEquals(null, re.getCallbackArgument());
            assertEquals(true, re.isOriginRemote());
          }

          assertEquals(evIdx, events.size());
        }
      }
    });
  }

  /**
   * Make sure a queued region does not allow non-queued subscribers
   */
  @Ignore("TODO: test is disabled")
  @Test
  public void testIllegalConfigQueueExists() throws Exception {
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    createRootRegion(factory.create());

    VM vm = Host.getHost(0).getVM(0);
    vm.invoke(new SerializableRunnable() {
      public void run() {
        Properties config = new Properties();
        config.setProperty(ROLES, "pubFirst");
        getSystem(config);
      }
    });

    // now create the region
    vm.invoke(new CacheSerializableRunnable("create root") {
      public void run2() throws CacheException {
        final String expectedExceptions = "does not allow queued messages";
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.DISTRIBUTED_ACK);
        // setting the following makes things legal
        factory.setDataPolicy(DataPolicy.NORMAL);
        factory.setSubscriptionAttributes(new SubscriptionAttributes(InterestPolicy.ALL));
        getCache().getLogger()
            .info("<ExpectedException action=add>" + expectedExceptions + "</ExpectedException>");
        try {
          createRootRegion(factory.create());
          fail("expected IllegalStateException");
        } catch (IllegalStateException expected) {
        } finally {
          getCache().getLogger().info(
              "<ExpectedException action=remove>" + expectedExceptions + "</ExpectedException>");
        }
      }
    });
  }

  /**
   * Make sure a subscriber that does not allow queued messages causes a queued publisher to fail
   * creation
   */
  @Ignore("TODO: test is disabled")
  @Test
  public void testIllegalConfigSubscriberExists() throws Exception {
    final String expectedExceptions = "does not allow queued messages";

    VM vm = Host.getHost(0).getVM(0);
    vm.invoke(new SerializableRunnable() {
      public void run() {
        Properties config = new Properties();
        config.setProperty(ROLES, "subFirst");
        getSystem(config);
      }
    });

    // now create the region
    vm.invoke(new CacheSerializableRunnable("create root") {
      public void run2() throws CacheException {
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.DISTRIBUTED_ACK);
        // setting the following makes things legal
        factory.setDataPolicy(DataPolicy.NORMAL);
        factory.setSubscriptionAttributes(new SubscriptionAttributes(InterestPolicy.ALL));
        createRootRegion(factory.create());
      }
    });

    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    getCache().getLogger()
        .info("<ExpectedException action=add>" + expectedExceptions + "</ExpectedException>");
    try {
      createRootRegion(factory.create());
      fail("expected IllegalStateException");
    } catch (IllegalStateException expected) {
    } finally {
      getCache().getLogger()
          .info("<ExpectedException action=remove>" + expectedExceptions + "</ExpectedException>");
    }
  }
}

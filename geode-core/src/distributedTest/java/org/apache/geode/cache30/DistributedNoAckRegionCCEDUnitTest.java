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

import static org.apache.geode.distributed.ConfigurationProperties.CONSERVE_SOCKETS;
import static org.apache.geode.distributed.ConfigurationProperties.DISTRIBUTED_SYSTEM_ID;
import static org.apache.geode.distributed.ConfigurationProperties.SOCKET_BUFFER_SIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Map;
import java.util.Properties;

import org.junit.Test;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.Wait;


public class DistributedNoAckRegionCCEDUnitTest extends DistributedNoAckRegionDUnitTest {

  static volatile boolean ListenerBlocking;

  @Override
  public Properties getDistributedSystemProperties() {
    Properties p = super.getDistributedSystemProperties();
    p.put(CONSERVE_SOCKETS, "false");
    if (distributedSystemID > 0) {
      p.put(DISTRIBUTED_SYSTEM_ID, "" + distributedSystemID);
    }
    p.put(SOCKET_BUFFER_SIZE, "" + 2000000);
    return p;
  }

  /**
   * Returns region attributes for a <code>GLOBAL</code> region
   */
  protected RegionAttributes getRegionAttributes() {
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_NO_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    factory.setConcurrencyChecksEnabled(true);
    return factory.create();
  }

  protected RegionAttributes getRegionAttributes(String type) {
    RegionAttributes ra = getCache().getRegionAttributes(type);
    if (ra == null) {
      throw new IllegalStateException("The region shortcut " + type + " has been removed.");
    }
    AttributesFactory factory = new AttributesFactory(ra);
    factory.setScope(Scope.DISTRIBUTED_NO_ACK);
    factory.setConcurrencyChecksEnabled(true);
    return factory.create();
  }

  @Override
  @Test
  public void testLocalDestroy() {
    // replicates don't allow local destroy
  }

  @Override
  @Test
  public void testEntryTtlLocalDestroy() {
    // replicates don't allow local destroy
  }

  @Test
  public void testClearWithManyEventsInFlight() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);

    // create replicated regions in VM 0 and 1, then perform concurrent ops
    // on the same key while creating the region in VM2. Afterward make
    // sure that all three regions are consistent

    final String name = this.getUniqueName() + "-CC";
    createRegionWithAttribute(vm0, name, false);
    createRegionWithAttribute(vm1, name, false);
    createRegionWithAttribute(vm2, name, false);
    createRegionWithAttribute(vm3, name, false);
    vm0.invoke(() -> DistributedNoAckRegionCCEDUnitTest.addBlockingListener());
    vm1.invoke(() -> DistributedNoAckRegionCCEDUnitTest.addBlockingListener());
    vm2.invoke(() -> DistributedNoAckRegionCCEDUnitTest.addBlockingListener());
    AsyncInvocation vm0Ops = vm0.invokeAsync(() -> DistributedNoAckRegionCCEDUnitTest.doManyOps());
    AsyncInvocation vm1Ops = vm1.invokeAsync(() -> DistributedNoAckRegionCCEDUnitTest.doManyOps());
    AsyncInvocation vm2Ops = vm2.invokeAsync(() -> DistributedNoAckRegionCCEDUnitTest.doManyOps());
    // pause to let a bunch of operations build up
    Wait.pause(5000);
    AsyncInvocation a0 = vm3.invokeAsync(() -> DistributedNoAckRegionCCEDUnitTest.clearRegion());
    vm0.invoke(() -> DistributedNoAckRegionCCEDUnitTest.unblockListener());
    vm1.invoke(() -> DistributedNoAckRegionCCEDUnitTest.unblockListener());
    vm2.invoke(() -> DistributedNoAckRegionCCEDUnitTest.unblockListener());
    waitForAsyncProcessing(a0, "");
    waitForAsyncProcessing(vm0Ops, "");
    waitForAsyncProcessing(vm1Ops, "");
    waitForAsyncProcessing(vm2Ops, "");

    Wait.pause(2000);// this test has with noack, thus we should wait before validating entries
    // check consistency of the regions
    Map r0Contents = vm0.invoke(() -> getCCRegionContents());
    Map r1Contents = vm1.invoke(() -> getCCRegionContents());
    Map r2Contents = vm2.invoke(() -> getCCRegionContents());
    Map r3Contents = vm3.invoke(() -> getCCRegionContents());

    for (int i = 0; i < 10; i++) {
      String key = "cckey" + i;
      assertEquals("region contents are not consistent", r0Contents.get(key), r1Contents.get(key));
      assertEquals("region contents are not consistent", r1Contents.get(key), r2Contents.get(key));
      assertEquals("region contents are not consistent", r2Contents.get(key), r3Contents.get(key));
      for (int subi = 1; subi < 3; subi++) {
        String subkey = key + "-" + subi;
        assertEquals("region contents are not consistent", r0Contents.get(subkey),
            r1Contents.get(subkey));
        assertEquals("region contents are not consistent", r1Contents.get(subkey),
            r2Contents.get(subkey));
        assertEquals("region contents are not consistent", r2Contents.get(subkey),
            r3Contents.get(subkey));
      }
    }
  }

  static void addBlockingListener() {
    ListenerBlocking = true;
    CCRegion.getAttributesMutator().addCacheListener(new CacheListenerAdapter() {
      public void afterCreate(EntryEvent event) {
        onEvent(event);
      }

      private void onEvent(EntryEvent event) {
        boolean blocked = false;
        if (event.isOriginRemote()) {
          synchronized (this) {
            while (ListenerBlocking) {
              LogWriterUtils.getLogWriter()
                  .info("blocking cache operations for " + event.getDistributedMember());
              blocked = true;
              try {
                wait();
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LogWriterUtils.getLogWriter().info("blocking cache listener interrupted");
                return;
              }
            }
          }
          if (blocked) {
            LogWriterUtils.getLogWriter()
                .info("allowing cache operations for " + event.getDistributedMember());
          }
        }
      }

      @Override
      public void close() {
        LogWriterUtils.getLogWriter().info("closing blocking listener");
        ListenerBlocking = false;
        synchronized (this) {
          notifyAll();
        }
      }

      @Override
      public void afterUpdate(EntryEvent event) {
        onEvent(event);
      }

      @Override
      public void afterInvalidate(EntryEvent event) {
        onEvent(event);
      }

      @Override
      public void afterDestroy(EntryEvent event) {
        onEvent(event);
      }
    });
  }

  static void doManyOps() {
    // do not include putAll, which requires an Ack to detect failures
    doOpsLoopNoFlush(5000, false, false);
  }

  static void unblockListener() {
    CacheListener listener = CCRegion.getCacheListener();
    ListenerBlocking = false;
    synchronized (listener) {
      listener.notifyAll();
    }
  }

  static void clearRegion() {
    CCRegion.clear();
  }

  /**
   * This test creates a server cache in vm0 and a peer cache in vm1. It then tests to see if GII
   * transferred tombstones to vm1 like it's supposed to. A client cache is created in vm2 and the
   * same sort of check is performed for register-interest.
   */

  @Test
  public void testGIISendsTombstones() {
    versionTestGIISendsTombstones();
  }


  protected void do_version_recovery_if_necessary(final VM vm0, final VM vm1, final VM vm2,
      final Object[] params) {
    // do nothing here
  }

  /**
   * This tests the concurrency versioning system to ensure that event conflation happens correctly
   * and that the statistic is being updated properly
   */
  @Test
  public void testConcurrentEvents() throws Exception {
    versionTestConcurrentEvents();
  }


  @Test
  public void testClearWithConcurrentEvents() {
    // need to figure out how to flush clear() ops for verification steps
  }

  @Test
  public void testClearWithConcurrentEventsAsync() {
    // need to figure out how to flush clear() ops for verification steps
  }

  @Test
  public void testClearOnNonReplicateWithConcurrentEvents() {
    // need to figure out how to flush clear() ops for verification steps
  }


  @Test
  public void testTombstones() {
    versionTestTombstones();
  }



  @Test
  public void testOneHopKnownIssues() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3); // this VM, but treat as a remote for uniformity

    // create an empty region in vm0 and replicated regions in VM 1 and 3,
    // then perform concurrent ops
    // on the same key while creating the region in VM2. Afterward make
    // sure that all three regions are consistent

    final String name = this.getUniqueName() + "-CC";
    SerializableRunnable createRegion = new SerializableRunnable("Create Region") {
      public void run() {
        try {
          final RegionFactory f;
          int vmNumber = VM.getCurrentVMNum();
          switch (vmNumber) {
            case 0:
              f = getCache().createRegionFactory(
                  getRegionAttributes(RegionShortcut.REPLICATE_PROXY.toString()));
              break;
            case 1:
              f = getCache()
                  .createRegionFactory(getRegionAttributes(RegionShortcut.REPLICATE.toString()));
              f.setDataPolicy(DataPolicy.NORMAL);
              break;
            default:
              f = getCache().createRegionFactory(getRegionAttributes());
              break;
          }
          CCRegion = (LocalRegion) f.create(name);
        } catch (CacheException ex) {
          Assert.fail("While creating region", ex);
        }
      }
    };

    vm0.invoke(createRegion); // empty
    vm1.invoke(createRegion); // normal
    vm2.invoke(createRegion); // replicate

    // case 1: entry already invalid on vm2 (replicate) is invalidated by vm0 (empty)
    final String invalidationKey = "invalidationKey";
    final String destroyKey = "destroyKey";
    SerializableRunnable test =
        new SerializableRunnable("case 1: second invalidation not applied or distributed") {
          public void run() {
            CCRegion.put(invalidationKey, "initialValue");

            int invalidationCount = CCRegion.getCachePerfStats().getInvalidates();
            CCRegion.invalidate(invalidationKey);
            CCRegion.invalidate(invalidationKey);
            assertEquals(invalidationCount + 1, CCRegion.getCachePerfStats().getInvalidates());

            // also test destroy() while we're at it. It should throw an exception
            int destroyCount = CCRegion.getCachePerfStats().getDestroys();
            CCRegion.destroy(invalidationKey);
            try {
              CCRegion.destroy(invalidationKey);
              fail("expected an EntryNotFoundException");
            } catch (EntryNotFoundException e) {
              // expected
            }
            assertEquals(destroyCount + 1, CCRegion.getCachePerfStats().getDestroys());
          }
        };
    vm0.invoke(test);

    // now do the same with the datapolicy=normal region
    test.setName("case 2: second invalidation not applied or distributed");
    vm1.invoke(test);
  }

  /**
   * This tests the concurrency versioning system to ensure that event conflation happens correctly
   * and that the statistic is being updated properly
   */
  @Test
  public void testConcurrentEventsOnEmptyRegion() {
    versionTestConcurrentEventsOnEmptyRegion();
  }

  /**
   * This tests the concurrency versioning system to ensure that event conflation happens correctly
   * and that the statistic is being updated properly
   */
  @Test
  public void testConcurrentEventsOnNonReplicatedRegion() {
    versionTestConcurrentEventsOnNonReplicatedRegion();
  }


  @Test
  public void testGetAllWithVersions() {
    versionTestGetAllWithVersions();
  }
}

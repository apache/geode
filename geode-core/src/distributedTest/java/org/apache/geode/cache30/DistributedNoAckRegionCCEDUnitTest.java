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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

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
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.Wait;


public class DistributedNoAckRegionCCEDUnitTest extends DistributedNoAckRegionDUnitTest {

  private static volatile boolean ListenerBlocking;

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
  @Override
  public <K, V> RegionAttributes<K, V> getRegionAttributes() {
    AttributesFactory<K, V> factory = new AttributesFactory<>();
    factory.setScope(Scope.DISTRIBUTED_NO_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    factory.setConcurrencyChecksEnabled(true);
    return factory.create();
  }

  @Override
  protected <K, V> RegionAttributes<K, V> getRegionAttributes(String type) {
    RegionAttributes<K, V> ra = getCache().getRegionAttributes(type);
    if (ra == null) {
      throw new IllegalStateException("The region shortcut " + type + " has been removed.");
    }
    AttributesFactory<K, V> factory = new AttributesFactory<>(ra);
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
    VM vm0 = VM.getVM(0);
    VM vm1 = VM.getVM(1);
    VM vm2 = VM.getVM(2);
    VM vm3 = VM.getVM(3);

    // create replicated regions in VM 0 and 1, then perform concurrent ops
    // on the same key while creating the region in VM2. Afterward make
    // sure that all three regions are consistent

    final String name = this.getUniqueName() + "-CC";
    createRegionWithAttribute(vm0, name, false);
    createRegionWithAttribute(vm1, name, false);
    createRegionWithAttribute(vm2, name, false);
    createRegionWithAttribute(vm3, name, false);
    vm0.invoke(DistributedNoAckRegionCCEDUnitTest::addBlockingListener);
    vm1.invoke(DistributedNoAckRegionCCEDUnitTest::addBlockingListener);
    vm2.invoke(DistributedNoAckRegionCCEDUnitTest::addBlockingListener);
    AsyncInvocation vm0Ops = vm0.invokeAsync(DistributedNoAckRegionCCEDUnitTest::doManyOps);
    AsyncInvocation vm1Ops = vm1.invokeAsync(DistributedNoAckRegionCCEDUnitTest::doManyOps);
    AsyncInvocation vm2Ops = vm2.invokeAsync(DistributedNoAckRegionCCEDUnitTest::doManyOps);
    // pause to let a bunch of operations build up
    Wait.pause(5000);
    AsyncInvocation a0 = vm3.invokeAsync(DistributedNoAckRegionCCEDUnitTest::clearRegion);
    vm0.invoke(DistributedNoAckRegionCCEDUnitTest::unblockListener);
    vm1.invoke(DistributedNoAckRegionCCEDUnitTest::unblockListener);
    vm2.invoke(DistributedNoAckRegionCCEDUnitTest::unblockListener);
    waitForAsyncProcessing(a0, "");
    waitForAsyncProcessing(vm0Ops, "");
    waitForAsyncProcessing(vm1Ops, "");
    waitForAsyncProcessing(vm2Ops, "");

    Wait.pause(2000);// this test has with noack, thus we should wait before validating entries
    // check consistency of the regions
    Map r0Contents = vm0.invoke(MultiVMRegionTestCase::getCCRegionContents);
    Map r1Contents = vm1.invoke(MultiVMRegionTestCase::getCCRegionContents);
    Map r2Contents = vm2.invoke(MultiVMRegionTestCase::getCCRegionContents);
    Map r3Contents = vm3.invoke(MultiVMRegionTestCase::getCCRegionContents);

    for (int i = 0; i < 10; i++) {
      String key = "cckey" + i;
      assertThat(r0Contents.get(key)).withFailMessage("region contents are not consistent")
          .isEqualTo(r1Contents.get(key));
      assertThat(r1Contents.get(key)).withFailMessage("region contents are not consistent")
          .isEqualTo(r2Contents.get(key));
      assertThat(r2Contents.get(key)).withFailMessage("region contents are not consistent")
          .isEqualTo(r3Contents.get(key));
      for (int subi = 1; subi < 3; subi++) {
        String subkey = key + "-" + subi;
        assertThat(r0Contents.get(subkey)).withFailMessage("region contents are not consistent")
            .isEqualTo(r1Contents.get(subkey));
        assertThat(r1Contents.get(subkey)).withFailMessage("region contents are not consistent")
            .isEqualTo(r2Contents.get(subkey));
        assertThat(r2Contents.get(subkey)).withFailMessage("region contents are not consistent")
            .isEqualTo(r3Contents.get(subkey));
      }
    }
  }

  private static void addBlockingListener() {
    ListenerBlocking = true;
    CCRegion.getAttributesMutator().addCacheListener(new CacheListenerAdapter() {
      @Override
      public void afterCreate(EntryEvent event) {
        onEvent(event);
      }

      private void onEvent(EntryEvent event) {
        boolean blocked = false;
        if (event.isOriginRemote()) {
          synchronized (this) {
            while (ListenerBlocking) {
              logger
                  .info("blocking cache operations for " + event.getDistributedMember());
              blocked = true;
              try {
                wait();
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.info("blocking cache listener interrupted");
                return;
              }
            }
          }
          if (blocked) {
            logger.info("allowing cache operations for " + event.getDistributedMember());
          }
        }
      }

      @Override
      public void close() {
        logger.info("closing blocking listener");
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

  private static void doManyOps() {
    // do not include putAll, which requires an Ack to detect failures
    doOpsLoopNoFlush(false, false);
  }

  private static void unblockListener() {
    CacheListener listener = CCRegion.getCacheListener();
    ListenerBlocking = false;
    synchronized (listener) {
      listener.notifyAll();
    }
  }

  private static void clearRegion() {
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
    VM vm0 = VM.getVM(0);
    VM vm1 = VM.getVM(1);
    VM vm2 = VM.getVM(2);

    assertThat(vm0).isNotNull();
    assertThat(vm1).isNotNull();
    assertThat(vm2).isNotNull();
    // create an empty region in vm0 and replicated regions in VM 1 and 3,
    // then perform concurrent ops
    // on the same key while creating the region in VM2. Afterward make
    // sure that all three regions are consistent

    final String name = this.getUniqueName() + "-CC";

    assertThat(vm0.invoke("Create Region", () -> {
      try {
        final RegionFactory f = getCache().createRegionFactory(
            getRegionAttributes(RegionShortcut.REPLICATE_PROXY.toString()));

        CCRegion = (LocalRegion) f.create(name);
        assertThat(CCRegion).isNotNull();
      } catch (CacheException ex) {
        fail("While creating region", ex);
      }
      return true;
    })).isTrue(); // empty

    assertThat(vm1.invoke("Create Region", () -> {
      try {
        final RegionFactory f = getCache()
            .createRegionFactory(getRegionAttributes(RegionShortcut.REPLICATE.toString()));
        f.setDataPolicy(DataPolicy.NORMAL);

        CCRegion = (LocalRegion) f.create(name);
        assertThat(CCRegion).isNotNull();
      } catch (CacheException ex) {
        fail("While creating region", ex);
      }
      return true;
    })).isTrue(); // normal

    assertThat(vm2.invoke("Create Region", () -> {
      try {
        final RegionFactory f = getCache().createRegionFactory(getRegionAttributes());
        CCRegion = (LocalRegion) f.create(name);
        assertThat(CCRegion).isNotNull();
      } catch (CacheException ex) {
        fail("While creating region", ex);
      }
      return true;
    })).isTrue(); // replicate

    // case 1: entry already invalid on vm2 (replicate) is invalidated by vm0 (empty)
    final String invalidationKey = "invalidationKey";
    SerializableRunnable test =
        new SerializableRunnable() {
          @Override
          public void run() {
            CCRegion.put(invalidationKey, "initialValue");

            long invalidationCount = CCRegion.getCachePerfStats().getInvalidates();
            CCRegion.invalidate(invalidationKey);
            CCRegion.invalidate(invalidationKey);
            assertThat(invalidationCount + 1)
                .isEqualTo(CCRegion.getCachePerfStats().getInvalidates());

            // also test destroy() while we're at it. It should throw an exception
            long destroyCount = CCRegion.getCachePerfStats().getDestroys();
            CCRegion.destroy(invalidationKey);
            try {
              CCRegion.destroy(invalidationKey);
              fail("expected an EntryNotFoundException");
            } catch (EntryNotFoundException e) {
              // expected
            }
            assertThat(destroyCount + 1).isEqualTo(CCRegion.getCachePerfStats().getDestroys());
          }
        };
    vm0.invoke("case 1: second invalidation not applied or distributed", test);

    // now do the same with the datapolicy=normal region
    vm1.invoke("case 2: second invalidation not applied or distributed", test);
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

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
package org.apache.geode.pdx;

import static org.apache.geode.internal.Assert.assertTrue;
import static org.apache.geode.internal.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheEvent;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.TransactionEvent;
import org.apache.geode.cache.TransactionListener;
import org.apache.geode.cache.TransactionWriter;
import org.apache.geode.cache.TransactionWriterException;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.DistributionMessageObserver;
import org.apache.geode.internal.cache.DistributedCacheOperation;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.pdx.internal.EnumInfo;
import org.apache.geode.pdx.internal.PeerTypeRegistration;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.DUnitBlackboard;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.SerializationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Category({DistributedTest.class, SerializationTest.class})
public class PdxSerializableDUnitTest extends JUnit4CacheTestCase {

  private static final SimpleClass ANOBJECT = new SimpleClass(57, (byte) 3);
  private static final String TEST_REGION_NAME = "testSimplePdx";

  @Test
  public void testSimplePut() {
    Host host = Host.getHost(0);
    VM vm1 = host.getVM(0);
    VM vm2 = host.getVM(1);
    VM vm3 = host.getVM(2);

    SerializableCallable createRegion = new SerializableCallable() {
      public Object call() throws Exception {
        AttributesFactory af = new AttributesFactory();
        af.setScope(Scope.DISTRIBUTED_ACK);
        af.setDataPolicy(DataPolicy.REPLICATE);
        createRootRegion(TEST_REGION_NAME, af.create());
        return null;
      }
    };

    vm1.invoke(createRegion);
    vm2.invoke(createRegion);
    vm3.invoke(createRegion);

    vm1.invoke(() -> doSimplePut(false));
    vm2.invoke(() -> verifySimplePut());

    vm3.invoke(new SerializableCallable("check for PDX") {

      public Object call() throws Exception {
        assertNotNull(getRootRegion(PeerTypeRegistration.REGION_NAME));
        return null;
      }
    });
  }

  private void createPR() {
    getCache().createRegionFactory(RegionShortcut.PARTITION)
        .setPartitionAttributes(new PartitionAttributesFactory<Integer, Object>().create())
        .create(TEST_REGION_NAME);
  }

  @Test
  public void testSimplePutOnPRWithTx() {
    Host host = Host.getHost(0);
    VM vm1 = host.getVM(0);
    VM vm2 = host.getVM(1);
    VM vm3 = host.getVM(2);

    vm1.invoke(() -> createPR());
    vm2.invoke(() -> createPR());
    vm3.invoke(() -> createPR());
    vm1.invoke(() -> doSimplePut(true));

    vm2.invoke(() -> verifySimplePut());

    vm3.invoke(() -> {
      assertNotNull(getRootRegion(PeerTypeRegistration.REGION_NAME));
    });
  }

  @Test
  public void testTransactionCallbacksNotInvoked() {
    Host host = Host.getHost(0);
    VM vm1 = host.getVM(0);
    VM vm2 = host.getVM(1);

    SerializableCallable createRegionAndAddPoisonedListener = new SerializableCallable() {
      public Object call() throws Exception {
        AttributesFactory af = new AttributesFactory();
        af.setScope(Scope.DISTRIBUTED_ACK);
        af.setDataPolicy(DataPolicy.REPLICATE);
        createRootRegion(TEST_REGION_NAME, af.create());
        addPoisonedTransactionListeners();
        return null;
      }
    };

    vm1.invoke(createRegionAndAddPoisonedListener);
    vm2.invoke(createRegionAndAddPoisonedListener);
    vm1.invoke(() -> doSimplePut(true));
    vm2.invoke(() -> verifySimplePut());
  }

  @Test
  public void testPersistenceDefaultDiskStore() throws Throwable {

    SerializableCallable createRegion = new SerializableCallable() {
      public Object call() throws Exception {
        // Make sure the type registry is persistent
        CacheFactory cf = new CacheFactory();
        cf.setPdxPersistent(true);
        getCache(cf);
        AttributesFactory af = new AttributesFactory();
        af.setScope(Scope.DISTRIBUTED_ACK);
        af.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
        createRootRegion(TEST_REGION_NAME, af.create());
        return null;
      }
    };

    persistenceTest(createRegion);
  }

  @Test
  public void testPersistenceExplicitDiskStore() throws Throwable {
    SerializableCallable createRegion = new SerializableCallable() {
      public Object call() throws Exception {
        // Make sure the type registry is persistent
        CacheFactory cf = new CacheFactory();
        cf.setPdxPersistent(true);
        cf.setPdxDiskStore("store1");
        Cache cache = getCache(cf);
        cache.createDiskStoreFactory().setMaxOplogSize(1).setDiskDirs(getDiskDirs())
            .create("store1");
        AttributesFactory af = new AttributesFactory();
        af.setScope(Scope.DISTRIBUTED_ACK);
        af.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
        af.setDiskStoreName("store1");
        createRootRegion(TEST_REGION_NAME, af.create());
        return null;
      }
    };
    persistenceTest(createRegion);
  }


  private void persistenceTest(SerializableCallable createRegion) throws Throwable {
    Host host = Host.getHost(0);
    VM vm1 = host.getVM(0);
    VM vm2 = host.getVM(1);
    VM vm3 = host.getVM(2);
    vm1.invoke(createRegion);
    vm2.invoke(createRegion);

    vm1.invoke(() -> doSimplePut(false));

    final SerializableCallable checkForObject = new SerializableCallable() {
      public Object call() throws Exception {
        Region r = getRootRegion(TEST_REGION_NAME);
        assertEquals(ANOBJECT, r.get(1));
        // Ok, now the type registry should exist
        assertNotNull(getRootRegion(PeerTypeRegistration.REGION_NAME));
        return null;
      }
    };

    vm2.invoke(checkForObject);

    SerializableCallable closeCache = new SerializableCallable() {
      public Object call() throws Exception {
        closeCache();
        return null;
      }
    };


    // Close the cache in both VMs
    vm1.invoke(closeCache);
    vm2.invoke(closeCache);


    // Now recreate the region, recoverying from disk
    AsyncInvocation future1 = vm1.invokeAsync(createRegion);
    AsyncInvocation future2 = vm2.invokeAsync(createRegion);

    future1.getResult();
    future2.getResult();

    // Make sure we can still find and deserialize the result.
    vm1.invoke(checkForObject);
    vm2.invoke(checkForObject);

    // Make sure a late comer can still create the type registry.
    vm3.invoke(createRegion);

    vm3.invoke(checkForObject);
  }


  @Test
  public void testVmWaitsForPdxType() throws Throwable {
    VM vm0 = Host.getHost(0).getVM(0);
    VM vm1 = Host.getHost(0).getVM(1);
    getBlackboard().initBlackboard();
    final Properties properties = getDistributedSystemProperties();
    properties.put("conserve-sockets", "false");

    // steps:
    // 1 create two caches and define a PdxType
    // 2 install a block in VM1 that delays receipt of new PDX types
    // 3 update the value of the PdxInstance in VM0 using a new Enum type
    // 4 get the value in VM0
    // The result should be that step 4 hangs unless the bug is fixed

    vm0.invoke("create cache", () -> {
      Cache cache = getCache(properties);
      Region region = cache.createRegionFactory(RegionShortcut.REPLICATE).create("testRegion");
      region.put("TestObject", new TestPdxObject("aString", 1, 1.0, TestPdxObject.AnEnum.ONE));
    });

    vm1.invoke("create cache and region", () -> {
      Cache cache = getCache(properties);
      // note that initial image transfer in testRegion will cause the object to be serialized in
      // vm0
      // and populate the PdxRegion in this vm
      cache.createRegionFactory(RegionShortcut.REPLICATE).create("testRegion");

      // this message observer will ensure that a new PDX registration doesn't occur
      final DUnitBlackboard bb = getBlackboard();
      DistributionMessageObserver.setInstance(new DistributionMessageObserver() {
        @Override
        public void beforeProcessMessage(DistributionManager dm, DistributionMessage msg) {
          if (msg instanceof DistributedCacheOperation.CacheOperationMessage) {
            try {
              DistributedCacheOperation.CacheOperationMessage cmsg =
                  (DistributedCacheOperation.CacheOperationMessage) msg;
              String path = cmsg.getRegionPath();
              if (path.equals(PeerTypeRegistration.REGION_FULL_PATH)) {
                System.out
                    .println("message observer found a PDX update message and is stalling: " + msg);
                try {
                  bb.signalGate("listenerWasInvoked");
                  bb.waitForGate("pdxObjectGetStarting", 3, TimeUnit.SECONDS);
                  // let the get() commence and block
                  Thread.sleep(30000);
                  System.out.println("message observer after sleep ");
                } catch (InterruptedException e) {
                  System.out.println("message observer is done stalling1 ");
                  bb.setMailbox("listenerProblem", e);
                } catch (TimeoutException e) {
                  System.out.println("message observer is done stalling2");
                  bb.setMailbox("listenerProblem", e);
                } finally {
                  System.out.println("message observer is done stalling");
                }
              }
            } catch (Exception e) {
              e.printStackTrace();
            }
          }
        }
      });
    });

    AsyncInvocation async0 = vm0.invokeAsync("propagate value with new pdx enum type", () -> {
      Cache cache = getCache(properties);
      final Region pdxRegion = cache.getRegion(PeerTypeRegistration.REGION_FULL_PATH);
      final DUnitBlackboard bb = getBlackboard();
      // now we register a new Id for our enum in a different thread. This will
      // block in vm1 due to its message observer
      Thread t = new Thread("PdxSerializableDUnitTest async thread") {
        public void run() {
          bb.signalGate("asyncThreadReady");
          // pdxRegion.put(new EnumId(0x3010101), new EnumInfo(TestPdxObject.AnEnum.TWO));
          ((GemFireCacheImpl) cache).getPdxRegistry().addRemoteEnum(0x3010101,
              new EnumInfo(TestPdxObject.AnEnum.TWO));
        }
      };
      t.setDaemon(true);
      t.start();
      try {
        bb.waitForGate("asyncThreadReady", 20, TimeUnit.SECONDS);
      } catch (TimeoutException e) {
        fail("timed out");
      }
      // reserialization will use the new Enumeration PDX type
      Region region = cache.getRegion("testRegion");
      bb.waitForGate("listenerWasInvoked", 20, TimeUnit.SECONDS);
      region.put("TestObject", new TestPdxObject("TestObject'", 2, 2.0, TestPdxObject.AnEnum.TWO));
      System.out.println("TestObject added put");
      bb.signalGate("pdxObjectPut");
    });

    // vm0 has sent a new TestObject but vm1 does not have the enum type needed to
    // deserialize it.
    AsyncInvocation async1 = vm1.invokeAsync("try to read object w/o enum type", () -> {
      DUnitBlackboard bb = getBlackboard();
      bb.waitForGate("pdxObjectPut", 10, TimeUnit.SECONDS);
      Region region = getCache(properties).getRegion("testRegion");
      bb.signalGate("pdxObjectGetStarting");
      Object testObject = region.get("TestObject");
      System.out.println("found " + testObject);
    });


    DUnitBlackboard bb = getBlackboard();

    try {
      async0.join(20000);
      async1.join(10000);
      if (async0.exceptionOccurred()) {
        throw async0.getException();
      }
      if (async1.exceptionOccurred()) {
        throw async1.getException();
      }
      assertTrue(bb.isGateSignaled("listenerWasInvoked"));
      /*
       * Throwable throwable = (Throwable)bb.getMailbox("listenerProblem"); if (throwable != null) {
       * RuntimeException rte = new RuntimeException("message observer had a problem", throwable);
       * throw rte; }
       */
    } finally {
      bb.signalGate("pdxObjectGetStarting");
      bb.signalGate("pdxObjectPut");
      bb.initBlackboard();
    }
  }

  public static class TestPdxObject implements org.apache.geode.pdx.PdxSerializable {

    public String stringVar;
    public int intVar;
    public double floatVar;

    public static enum AnEnum {
      ONE, TWO
    }

    ;

    public AnEnum enumVar;

    public TestPdxObject() {}

    public TestPdxObject(String stringVar, int intVar, double floatVar, AnEnum enumVar) {
      this.stringVar = stringVar;
      this.intVar = intVar;
      this.floatVar = floatVar;
      this.enumVar = enumVar;
    }

    @Override
    public void toData(final PdxWriter writer) {
      writer.writeString("stringVar", stringVar).writeInt("intVar", intVar)
          .writeDouble("floatVar", floatVar).writeObject("enumVar", this.enumVar);
    }

    @Override
    public void fromData(final PdxReader reader) {
      stringVar = reader.readString("stringVar");
      intVar = reader.readInt("intVar");
      floatVar = reader.readDouble("floatVar");
      enumVar = (AnEnum) reader.readObject("enumVar");
    }

    @Override
    public String toString() {
      return "TestPdxObject [stringVar=" + stringVar + ", intVar=" + intVar + ", floatVar="
          + floatVar + ", enumVar=" + enumVar + "]";
    }
  }

  /**
   * add a listener and writer that will throw an exception when invoked if events are for internal
   * regions
   */
  public void addPoisonedTransactionListeners() {
    MyTestTransactionListener listener = new MyTestTransactionListener();
    getCache().getCacheTransactionManager().addListener(listener);
    getCache().getCacheTransactionManager().setWriter(listener);
  }


  private void doSimplePut(boolean useTransaction) {
    // Check to make sure the type region is not yet created
    Region r = getRootRegion(TEST_REGION_NAME);
    if (useTransaction) {
      getCache().getCacheTransactionManager().begin();
    }
    try {
      r.put(1, ANOBJECT);
    } finally {
      if (useTransaction) {
        getCache().getCacheTransactionManager().commit();
      }
    }
    // Ok, now the type registry should exist
    assertNotNull(getRootRegion(PeerTypeRegistration.REGION_NAME));
  }

  private void verifySimplePut() {
    // Ok, now the type registry should exist
    assertNotNull(getRootRegion(PeerTypeRegistration.REGION_NAME));
    Region r = getRootRegion(TEST_REGION_NAME);
    assertEquals(ANOBJECT, r.get(1));
  }

  static private class MyTestTransactionListener implements TransactionWriter, TransactionListener {
    private MyTestTransactionListener() {

    }

    private void checkEvent(TransactionEvent event) {
      List<CacheEvent<?, ?>> events = event.getEvents();
      System.out.println("MyTestTransactionListener.checkEvent: events are " + events);
      for (CacheEvent<?, ?> cacheEvent : events) {
        if (((LocalRegion) cacheEvent.getRegion()).isPdxTypesRegion()) {
          throw new UnsupportedOperationException("found internal event: " + cacheEvent + " region="
              + cacheEvent.getRegion().getName());
        }
      }
    }

    @Override
    public void beforeCommit(TransactionEvent event) throws TransactionWriterException {
      checkEvent(event);
    }

    @Override
    public void close() {}

    @Override
    public void afterCommit(TransactionEvent event) {
      checkEvent(event);
    }

    @Override
    public void afterFailedCommit(TransactionEvent event) {
      checkEvent(event);
    }

    @Override
    public void afterRollback(TransactionEvent event) {
      checkEvent(event);
    }
  }
}

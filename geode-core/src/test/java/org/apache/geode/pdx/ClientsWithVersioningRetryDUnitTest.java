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
package org.apache.geode.pdx;

import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.DistributionMessageObserver;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.DistributedPutAllOperation;
import org.apache.geode.internal.cache.DistributedRegion;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.RegionEntry;
import org.apache.geode.internal.cache.tier.sockets.BaseCommand;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.tier.sockets.command.Put70;
import org.apache.geode.internal.cache.versions.VMVersionTag;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

@Category(DistributedTest.class)
public class ClientsWithVersioningRetryDUnitTest extends JUnit4CacheTestCase {

  // list of expected exceptions to remove in tearDown2()
  static List<IgnoredException> expectedExceptions = new LinkedList<IgnoredException>();

  @Override
  public final void postSetUp() throws Exception {
    Invoke.invokeInEveryVM(new SerializableRunnable() {
      @Override
      public void run() {
        //Disable endpoint shuffling, so that the client will always connect
        //to the first server we give it.
        System.setProperty(DistributionConfig.GEMFIRE_PREFIX + "bridge.disableShufflingOfEndpoints", "true");
      }
      
    });
  }
  
  @Override
  public final void postTearDownCacheTestCase() throws Exception {
    Invoke.invokeInEveryVM(new SerializableRunnable() {
      @Override      
      public void run() {
        System.setProperty(DistributionConfig.GEMFIRE_PREFIX + "bridge.disableShufflingOfEndpoints", "false");
      }
    });
    for (IgnoredException ex: expectedExceptions) {
      ex.remove();
    }
  }

  /**
   * Test that we can successfully retry a distributed put all and get
   * the version information.
   * second failure in bug 44951
   */
  @Test
  public void testRetryPut() {
    Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    final VM vm1 = host.getVM(1);
    

    createServerRegion(vm0, RegionShortcut.REPLICATE);
    createServerRegion(vm1, RegionShortcut.REPLICATE);

    // create an event tag in vm0 and then replay that event in vm1 
    final DistributedMember memberID = (DistributedMember)vm0.invoke(new SerializableCallable("get id") {
      public Object call() {
        return ((DistributedRegion)getCache().getRegion("region")).getDistributionManager().getDistributionManagerId();
      }
    });
    vm0.invoke(new SerializableCallable("create entry with fake event ID") {
      @Override
      public Object call() {
        DistributedRegion dr = (DistributedRegion)getCache().getRegion("region");
        VersionTag tag = new VMVersionTag();
        tag.setMemberID(dr.getVersionMember());
        tag.setRegionVersion(123);
        tag.setEntryVersion(9);
        tag.setVersionTimeStamp(System.currentTimeMillis());
        EventID eventID = new EventID(new byte[0], 1, 0);
        EntryEventImpl event = EntryEventImpl.create(dr, Operation.CREATE, "TestObject", "TestValue", null,
            false, memberID, true, eventID);
        event.setVersionTag(tag);
        event.setContext(new ClientProxyMembershipID(memberID));
        dr.recordEvent(event);
        event.release();
        return memberID;
      }
    });
    vm1.invoke(new SerializableRunnable("recover event tag in vm1 from vm0") {
      @Override
      public void run() {
        DistributedRegion dr = (DistributedRegion)getCache().getRegion("region");
        EventID eventID = new EventID(new byte[0], 1, 0);
        EntryEventImpl event = EntryEventImpl.create(dr, Operation.CREATE, "TestObject", "TestValue", null,
            false, memberID, true, eventID);
        try {
          event.setContext(new ClientProxyMembershipID(memberID));
          boolean recovered = ((BaseCommand)Put70.getCommand()).recoverVersionTagForRetriedOperation(event);
          assertTrue("Expected to recover the version for this event ID", recovered);
          assertEquals("Expected the region version to be 123", 123, event.getVersionTag().getRegionVersion());
        } finally {
          event.release();
        }
      }
    });
    // bug #48205 - a retried op in PR nodes not owning the primary bucket
    // may already have a version assigned to it in another backup bucket
    vm1.invoke(new SerializableRunnable("recover posdup event tag in vm1 event tracker from vm0") {
      @Override
      public void run() {
        DistributedRegion dr = (DistributedRegion)getCache().getRegion("region");
        EventID eventID = new EventID(new byte[0], 1, 0);
        EntryEventImpl event = EntryEventImpl.create(dr, Operation.CREATE, "TestObject", "TestValue", null,
            false, memberID, true, eventID);
        event.setPossibleDuplicate(true);
        try {
          dr.hasSeenEvent(event);
          assertTrue("Expected to recover the version for the event ID", event.getVersionTag() != null);
        } finally {
          event.release();
        }
      }
    });
  }


  /**
   * Test that we can successfully retry a distributed put all and get
   * the version information.
   * bug #45059
   */
  @Test
  public void testRetryPutAll() {
    Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    final VM vm1 = host.getVM(1);
    final VM vm2 = host.getVM(2);
    final VM vm3 = host.getVM(3);
    

    createServerRegion(vm0, RegionShortcut.PARTITION_REDUNDANT_PERSISTENT);
    vm0.invoke(new SerializableRunnable() {
      
      @Override
      public void run() {
        //Make sure the bucket 0 is primary in this member.
        Region region = getCache().getRegion("region");
        region.put(0, "value");
        
        //Add a listener to close vm1 when we send a distributed put all operation
        //this will cause a retry after we have applied the original put all to
        //the cache, causing a retry
        DistributionMessageObserver.setInstance(new DistributionMessageObserver() {

          @Override
          public void beforeSendMessage(DistributionManager dm,
              DistributionMessage msg) {
            if(msg instanceof DistributedPutAllOperation.PutAllMessage) {
              DistributionMessageObserver.setInstance(null);
              disconnectFromDS(vm1);
            }
          }
        });
        
      }
    });
    
    int port1 = createServerRegion(vm1, RegionShortcut.PARTITION_REDUNDANT_PERSISTENT);
    int port2 = createServerRegion(vm2, RegionShortcut.PARTITION_REDUNDANT_PERSISTENT);    
    createClientRegion(vm3, port1, port2);
    
    
    
    //This will be a put all to bucket 0
    //Here's the expected sequence
    //client->vm1 (accessor0)
    //vm1->vm0
    //vm0 will kill vm1
    //vm0->vm2
    //client will retry the putall
    vm3.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region region = getCache().getRegion("region");
        Map map = new HashMap();
        map.put(0, "a");
        map.put(113, "b");
        region.putAll(map);
        RegionEntry entry = ((LocalRegion)region).getRegionEntry(0);
        assertNotNull(entry);
        assertNotNull(entry.getVersionStamp());
        assertEquals(2, entry.getVersionStamp().getEntryVersion());
        return null;
      }
    });
    
    //Verify the observer was triggered
    vm0.invoke(new SerializableRunnable() {
      
      @Override
      public void run() {
        //if the observer was triggered, it would have cleared itself
        assertNull(DistributionMessageObserver.getInstance());
      }
    });

    //Make sure vm1 did in fact shut down
    vm1.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
        assertTrue(cache == null || cache.isClosed());
      }
    });
  }

  /**
   * Test that we can successfully retry a distributed putAll on an accessor
   * and get the version information.
   * bug #48205
   */
  @Test
  public void testRetryPutAllInAccessor() {
    Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    final VM vm1 = host.getVM(1);
    final VM vm2 = host.getVM(2);
    final VM vm3 = host.getVM(3);
    
    LogWriterUtils.getLogWriter().info("creating region in vm0");
    createRegionInPeer(vm0, RegionShortcut.PARTITION_REDUNDANT_PERSISTENT);
    
    vm0.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        //Make sure the bucket 0 is primary in this member.
        Region region = getCache().getRegion("region");
        region.put(0, "value");
      }
    });
    
    LogWriterUtils.getLogWriter().info("creating region in vm1");
    createRegionInPeer(vm1, RegionShortcut.PARTITION_REDUNDANT_PERSISTENT);
    LogWriterUtils.getLogWriter().info("creating region in vm2");
    createRegionInPeer(vm2, RegionShortcut.PARTITION_REDUNDANT_PERSISTENT);    
    LogWriterUtils.getLogWriter().info("creating region in vm3");
    createRegionInPeer(vm3, RegionShortcut.PARTITION_PROXY);
    
    expectedExceptions.add(IgnoredException.addIgnoredException("RuntimeException", vm2));
    vm2.invoke(new SerializableRunnable("install message listener to ignore update") {
      public void run() {
        //Add a listener to close vm2 when we send a distributed put all operation
        //this will cause a retry after we have applied the original put all to
        //the cache, causing a retry
        DistributionMessageObserver.setInstance(new DistributionMessageObserver() {

          @Override
          public void beforeProcessMessage(DistributionManager dm,
              DistributionMessage msg) {
            if(msg instanceof DistributedPutAllOperation.PutAllMessage) {
              DistributionMessageObserver.setInstance(null);
              Wait.pause(5000); // give vm1 time to process the message that we're ignoring
              disconnectFromDS(vm0);
              // no reply will be sent to vm0 due to this exception, but that's okay
              // because vm0 has been shut down
              throw new RuntimeException("test code is ignoring message: " + msg);
            }
          }
        });
        
      }
    });
    
    //This will be a put all to bucket 0
    //Here's the expected sequence
    //accessor->vm0 (primary)
    //vm0->vm1, vm2
    //vm2 will ignore the message & kill vm0
    //accessor->vm2 or vm1
    // version tag is recovered and put in the event & cache
    vm3.invoke(new SerializableCallable("perform putAll in accessor") {
      public Object call() throws Exception {
        Region region = getCache().getRegion("region");
        Map map = new HashMap();
        map.put(0, "a");
        map.put(113, "b");
        region.putAll(map);
        return null;
      }
    });
    

    // verify that the version is correct
    vm1.invoke(new SerializableRunnable("verify vm1") {
      
      @Override
      public void run() {
        //if the observer was triggered, it would have cleared itself
        assertNull(DistributionMessageObserver.getInstance());

        Region region = getCache().getRegion("region");
        VersionTag tag = ((LocalRegion)region).getVersionTag(0);
        assertEquals(2, tag.getEntryVersion());
      }
    });


    //Verify the observer was triggered and the version is correct
    vm2.invoke(new SerializableRunnable("verify vm2") {
      
      @Override
      public void run() {
        //if the observer was triggered, it would have cleared itself
        assertNull(DistributionMessageObserver.getInstance());

        Region region = getCache().getRegion("region");
        VersionTag tag = ((LocalRegion)region).getVersionTag(0);
        assertEquals(2, tag.getEntryVersion());
      }
    });

    //Make sure vm1 did in fact shut down
    vm0.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
        assertTrue(cache == null || cache.isClosed());
      }
    });
  }

  private void disconnectFromDS(VM vm) {
    vm.invoke(new SerializableCallable("disconnecting vm " + vm) {
      public Object call() throws Exception {
        disconnectFromDS();
        return null;
      }
    });
  }
  
  


  private int createServerRegion(VM vm, final RegionShortcut shortcut) {
    SerializableCallable createRegion = new SerializableCallable("create server region") {
      public Object call() throws Exception {
        RegionFactory<Object, Object> rf = getCache().createRegionFactory(shortcut);
        if (!shortcut.equals(RegionShortcut.REPLICATE)) { 
          rf.setPartitionAttributes(new PartitionAttributesFactory().setRedundantCopies(2).create());
        }
        rf.create("region");

        CacheServer server = getCache().addCacheServer();
        int port = AvailablePortHelper.getRandomAvailableTCPPort();
        server.setPort(port);
        server.start();
        return port;
      }
    };

    return (Integer) vm.invoke(createRegion);
  }
  
  private void createRegionInPeer(VM vm, final RegionShortcut shortcut) {
    SerializableCallable createRegion = new SerializableCallable("create peer region") {
      public Object call() throws Exception {
        RegionFactory<Object, Object> rf = getCache().createRegionFactory(shortcut);
        if (!shortcut.equals(RegionShortcut.REPLICATE)) { 
          rf.setPartitionAttributes(new PartitionAttributesFactory().setRedundantCopies(2).create());
        }
        rf.create("region");
        return null;
      }
    };
    vm.invoke(createRegion);
  }
  
  public Properties getDistributedSystemProperties() {
    Properties p = super.getDistributedSystemProperties();
    p.put(CONSERVE_SOCKETS, "false");
    return p;
  }
  
  private int createServerRegionWithPersistence(VM vm,
      final boolean persistentPdxRegistry) {
    SerializableCallable createRegion = new SerializableCallable() {
      public Object call() throws Exception {
        CacheFactory cf = new CacheFactory();
        if(persistentPdxRegistry) {
          cf.setPdxPersistent(true)
          .setPdxDiskStore("store");
        }
//      
        Cache cache = getCache(cf);
        cache.createDiskStoreFactory()
          .setDiskDirs(getDiskDirs())
          .create("store");
        
        AttributesFactory af = new AttributesFactory();
        af.setScope(Scope.DISTRIBUTED_ACK);
        af.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
        af.setDiskStoreName("store");
        createRootRegion("testSimplePdx", af.create());

        CacheServer server = getCache().addCacheServer();
        int port = AvailablePortHelper.getRandomAvailableTCPPort();
        server.setPort(port);
        server.start();
        return port;
      }
    };

    return (Integer) vm.invoke(createRegion);
  }
  
  private int createServerAccessor(VM vm) {
    SerializableCallable createRegion = new SerializableCallable() {
      public Object call() throws Exception {
        AttributesFactory af = new AttributesFactory();
        af.setScope(Scope.DISTRIBUTED_ACK);
        af.setDataPolicy(DataPolicy.EMPTY);
        createRootRegion("testSimplePdx", af.create());

        CacheServer server = getCache().addCacheServer();
        int port = AvailablePortHelper.getRandomAvailableTCPPort();
        server.setPort(port);
        server.start();
        return port;
      }
    };

    return (Integer) vm.invoke(createRegion);
  }
  
  
  private void createClientRegion(final VM vm, final int port1, final int port2) {
    createClientRegion(vm, port1, port2, false);
  }

  private void createClientRegion(final VM vm, final int port1, final int port2, 
      final boolean threadLocalConnections) {
    SerializableCallable createRegion = new SerializableCallable("create client region in " + vm) {
      public Object call() throws Exception {
        ClientCacheFactory cf = new ClientCacheFactory();
        cf.addPoolServer(NetworkUtils.getServerHostName(vm.getHost()), port1);
        cf.addPoolServer(NetworkUtils.getServerHostName(vm.getHost()), port2);
        cf.setPoolPRSingleHopEnabled(false);
        cf.setPoolThreadLocalConnections(threadLocalConnections);
        cf.setPoolReadTimeout(10 * 60 * 1000);
        ClientCache cache = getClientCache(cf);
        cache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY)
        .create("region");
        return null;
      }
    };
    vm.invoke(createRegion);
  }
}

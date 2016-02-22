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
package com.gemstone.gemfire.internal.cache;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.InterestResultPolicy;
import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.client.ClientRegionShortcut;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.cache.tier.InterestType;
import com.gemstone.gemfire.test.dunit.Assert;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.SerializableCallableIF;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * This tests the fix for bug #43407 under a variety of configurations and
 * also tests that tombstones are treated in a similar manner.  The ticket
 * complains that a client that does a get(K) does not end up with the entry
 * in its cache if K is invalid on the server.
 * @author bruces
 *
 */
public class ClientServerInvalidAndDestroyedEntryDUnitTest extends CacheTestCase {
  
  public ClientServerInvalidAndDestroyedEntryDUnitTest(String name) {
    super(name);
  }
  
  public void setUp() throws Exception {
    super.setUp();
    disconnectAllFromDS();
  }
  
  public void testClientGetsInvalidEntry() throws Exception {
    final String regionName = getUniqueName()+"Region";
    doTestClientGetsInvalidEntry(regionName, false, false);
  }
  
  public void testClientGetsInvalidEntryPR() throws Exception {
    final String regionName = getUniqueName()+"Region";
    doTestClientGetsInvalidEntry(regionName, true, false);
  }

  public void testClientGetsTombstone() throws Exception {
    final String regionName = getUniqueName()+"Region";
    doTestClientGetsTombstone(regionName, false, false);
  }
  
  public void testClientGetsTombstonePR() throws Exception {
    final String regionName = getUniqueName()+"Region";
    doTestClientGetsTombstone(regionName, true, false);
  }
  

  
  // same tests but with transactions...
  
  

  public void testClientGetsInvalidEntryTX() throws Exception {
    final String regionName = getUniqueName()+"Region";
    doTestClientGetsInvalidEntry(regionName, false, true);
  }
  
  public void testClientGetsInvalidEntryPRTX() throws Exception {
    final String regionName = getUniqueName()+"Region";
    doTestClientGetsInvalidEntry(regionName, true, true);
  }

  public void testClientGetsTombstoneTX() throws Exception {
    final String regionName = getUniqueName()+"Region";
    doTestClientGetsTombstone(regionName, false, true);
  }

  public void testClientGetsTombstonePRTX() throws Exception {
    final String regionName = getUniqueName()+"Region";
    doTestClientGetsTombstone(regionName, true, true);
  }
  
  
  // tests for bug #46780, tombstones left in client after RI
  
  public void testRegisterInterestRemovesOldEntry() throws Exception {
    final String regionName = getUniqueName()+"Region";
    doTestRegisterInterestRemovesOldEntry(regionName, false);
  }
  
  public void testRegisterInterestRemovesOldEntryPR() throws Exception {
    final String regionName = getUniqueName()+"Region";
    doTestRegisterInterestRemovesOldEntry(regionName, true);
  }

  /* this method creates a server cache and is used by all of the tests in this class */
  private SerializableCallableIF getCreateServerCallable(final String regionName, final boolean usePR) {
    return new SerializableCallable("create server and entries") {
      public Object call() {
        Cache cache = getCache();
        List<CacheServer> servers = cache.getCacheServers();
        CacheServer server;
        if (servers.size() > 0) {
          server = servers.get(0);
        } else {
          server = cache.addCacheServer();
          int port = AvailablePortHelper.getRandomAvailableTCPPort();
          server.setPort(port);
          server.setHostnameForClients("localhost");
          try {
            server.start();
          }
          catch (IOException e) {
            Assert.fail("Failed to start server ", e);
          }
        }
        if (usePR) {
          RegionFactory factory = cache.createRegionFactory(RegionShortcut.PARTITION);
          PartitionAttributesFactory pf = new PartitionAttributesFactory();
          pf.setTotalNumBuckets(2);
          factory.setPartitionAttributes(pf.create());
          factory.create(regionName);
        } else {
          cache.createRegionFactory(RegionShortcut.REPLICATE).create(regionName);
        }
        return server.getPort();
      }
    };
  }
  

  /**
   * Bug #43407 - when a client does a get(k) and the entry is invalid in the server
   * we want the client to end up with an entry that is invalid.
   */
  private void doTestClientGetsInvalidEntry(final String regionName, final boolean usePR,
      boolean useTX) throws Exception {
    VM vm1 = Host.getHost(0).getVM(1);
    VM vm2 = Host.getHost(0).getVM(2);
    
    // here are the keys that will be used to validate behavior.  Keys must be
    // colocated if using both a partitioned region in the server and transactions
    // in the client.  All of these keys hash to bucket 0 in a two-bucket PR
    // except Object11 and IDoNotExist1
    final String notAffectedKey = "Object1";
    final String nonexistantKey = (usePR && useTX)? "IDoNotExist2" : "IDoNotExist1";
    final String key1 = "Object10";
    final String key2 = (usePR && useTX) ? "Object12" : "Object11";

    SerializableCallableIF createServer = getCreateServerCallable(regionName, usePR);
    int serverPort = (Integer)vm1.invoke(createServer);
    vm2.invoke(createServer);
    vm1.invoke(new SerializableRunnable("populate server and create invalid entry") {
      public void run() {
        Region myRegion =  getCache().getRegion(regionName);
        for (int i=1; i<=20; i++) {
          myRegion.put("Object"+i, "Value"+i);
        }
        myRegion.invalidate(key1);
        myRegion.invalidate(key2);
      }
    });
    com.gemstone.gemfire.test.dunit.LogWriterUtils.getLogWriter().info("creating client cache");
    ClientCache c = new ClientCacheFactory()
                    .addPoolServer("localhost", serverPort)
                    .set(DistributionConfig.LOG_LEVEL_NAME, LogWriterUtils.getDUnitLogLevel())
                    .create();
    Region myRegion = c.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY).create(regionName);;
    if (useTX) {
      c.getCacheTransactionManager().begin();
    }
    
    // get of a valid entry should work
    assertNotNull(myRegion.get(notAffectedKey));
    
    // get of an invalid entry should return null and create the entry in an invalid state
    com.gemstone.gemfire.test.dunit.LogWriterUtils.getLogWriter().info("getting "+key1+" - should reach this cache and be INVALID");
    assertNull(myRegion.get(key1));
    assertTrue(myRegion.containsKey(key1));
    
    // since this might be a PR we also check the next key to force PR Get messaging
    assertNull(myRegion.get(key2));
    assertTrue(myRegion.containsKey(key2));
    
    // now try a key that doesn't exist anywhere
    assertNull(myRegion.get(nonexistantKey));
    assertFalse(myRegion.containsKey(nonexistantKey));

    if (useTX) {
      c.getCacheTransactionManager().commit();
      
      // test that the commit correctly created the entries in the region
      assertNotNull(myRegion.get(notAffectedKey));
      assertNull(myRegion.get(key1));
      assertTrue(myRegion.containsKey(key1));
      assertNull(myRegion.get(key2));
      assertTrue(myRegion.containsKey(key2));
    }

    myRegion.localDestroy(notAffectedKey);
    myRegion.localDestroy(key1);
    myRegion.localDestroy(key2);
    
    if (useTX) {
      c.getCacheTransactionManager().begin();
    }

    // check that getAll returns invalidated entries
    List keys = new LinkedList();
    keys.add(notAffectedKey); keys.add(key1); keys.add(key2);
    Map result = myRegion.getAll(keys);
    
    assertNotNull(result.get(notAffectedKey));
    assertNull(result.get(key1));
    assertNull(result.get(key2));
    assertTrue(result.containsKey(key1));
    assertTrue(result.containsKey(key2));
    assertTrue(myRegion.containsKey(key1));
    assertTrue(myRegion.containsKey(key2));
    
    if (useTX) {
      c.getCacheTransactionManager().commit();
      // test that the commit correctly created the entries in the region
      assertNotNull(myRegion.get(notAffectedKey));
      assertNull(myRegion.get(key1));
      assertTrue(myRegion.containsKey(key1));
      assertNull(myRegion.get(key2));
      assertTrue(myRegion.containsKey(key2));
    }
    
    // test that a listener is not invoked when there is already an invalidated
    // entry in the client cache
    UpdateListener listener = new UpdateListener();
    listener.log = com.gemstone.gemfire.test.dunit.LogWriterUtils.getLogWriter();
    myRegion.getAttributesMutator().addCacheListener(listener);
    myRegion.get(key1);
    assertEquals("expected no cache listener invocations",
        0, listener.updateCount, listener.updateCount);
    
    myRegion.localDestroy(notAffectedKey);
    myRegion.getAll(keys);
    assertTrue("expected to find " + notAffectedKey, myRegion.containsKey(notAffectedKey));
    assertEquals("expected only one listener invocation for " + notAffectedKey, 1, listener.updateCount);
  }
  
  
  
  /**
   * Similar to bug #43407 but not reported in a ticket, we want a client that
   * does a get() on a destroyed entry to end up with a tombstone for that entry.
   * This was already the case but there were no unit tests covering this for
   * different server configurations and with/without transactions. 
   */
  private void doTestClientGetsTombstone(final String regionName, final boolean usePR,
      boolean useTX) throws Exception {
    VM vm1 = Host.getHost(0).getVM(1);
    VM vm2 = Host.getHost(0).getVM(2);
    
    // here are the keys that will be used to validate behavior.  Keys must be
    // colocated if using both a partitioned region in the server and transactions
    // in the client.  All of these keys hash to bucket 0 in a two-bucket PR
    // except Object11 and IDoNotExist1
    final String notAffectedKey = "Object1";
    final String nonexistantKey = (usePR && useTX)? "IDoNotExist2" : "IDoNotExist1";
    final String key1 = "Object10";
    final String key2 = (usePR && useTX) ? "Object12" : "Object11";

    SerializableCallableIF createServer = getCreateServerCallable(regionName, usePR);
    int serverPort = (Integer)vm1.invoke(createServer);
    vm2.invoke(createServer);
    vm1.invoke(new SerializableRunnable("populate server and create invalid entry") {
      public void run() {
        Region myRegion =  getCache().getRegion(regionName);
        for (int i=1; i<=20; i++) {
          myRegion.put("Object"+i, "Value"+i);
        }
        myRegion.destroy(key1);
        myRegion.destroy(key2);
      }
    });
    com.gemstone.gemfire.test.dunit.LogWriterUtils.getLogWriter().info("creating client cache");
    ClientCache c = new ClientCacheFactory()
                    .addPoolServer("localhost", serverPort)
                    .set(DistributionConfig.LOG_LEVEL_NAME, LogWriterUtils.getDUnitLogLevel())
                    .create();
    Region myRegion = c.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY).create(regionName);;
    if (useTX) {
      c.getCacheTransactionManager().begin();
    }
    // get of a valid entry should work
    assertNotNull(myRegion.get(notAffectedKey));
    // get of an invalid entry should return null and create the entry in an invalid state
    com.gemstone.gemfire.test.dunit.LogWriterUtils.getLogWriter().info("getting "+key1+" - should reach this cache and be a TOMBSTONE");
    assertNull(myRegion.get(key1));
    assertFalse(myRegion.containsKey(key1));
    RegionEntry entry;
    if (!useTX) {
      entry = ((LocalRegion)myRegion).getRegionEntry(key1);
      assertNotNull(entry); // it should be there
      assertTrue(entry.isTombstone()); // it should be a destroyed entry with Token.TOMBSTONE
    }
    
    // since this might be a PR we also check the next key to force PR Get messaging
    assertNull(myRegion.get(key2));
    assertFalse(myRegion.containsKey(key2));
    if (!useTX) {
      entry = ((LocalRegion)myRegion).getRegionEntry(key2);
      assertNotNull(entry); // it should be there
      assertTrue(entry.isTombstone()); // it should be a destroyed entry with Token.TOMBSTONE
    }

    
    // now try a key that doesn't exist anywhere
    assertNull(myRegion.get(nonexistantKey));
    assertFalse(myRegion.containsKey(nonexistantKey));

    if (useTX) {
      c.getCacheTransactionManager().commit();
      
      // test that the commit correctly created the entries in the region
      assertNotNull(myRegion.get(notAffectedKey));
      assertNull(myRegion.get(key1));
      assertFalse(myRegion.containsKey(key1));
      entry = ((LocalRegion)myRegion).getRegionEntry(key1);
      assertNotNull(entry); // it should be there
      assertTrue(entry.isTombstone()); // it should be a destroyed entry with Token.TOMBSTONE

      assertNull(myRegion.get(key2));
      assertFalse(myRegion.containsKey(key2));
      entry = ((LocalRegion)myRegion).getRegionEntry(key2);
      assertNotNull(entry); // it should be there
      assertTrue(entry.isTombstone()); // it should be a destroyed entry with Token.TOMBSTONE
    }

    myRegion.localDestroy(notAffectedKey);
    
    if (useTX) {
      c.getCacheTransactionManager().begin();
    }

    // check that getAll returns invalidated entries
    List keys = new LinkedList();
    keys.add(notAffectedKey); keys.add(key1); keys.add(key2);
    Map result = myRegion.getAll(keys);
    
    com.gemstone.gemfire.test.dunit.LogWriterUtils.getLogWriter().info("result of getAll = " + result);
    assertNotNull(result.get(notAffectedKey));
    assertNull(result.get(key1));
    assertNull(result.get(key2));
    assertFalse(myRegion.containsKey(key1));
    assertFalse(myRegion.containsKey(key2));
    if (!useTX) {
      entry = ((LocalRegion)myRegion).getRegionEntry(key1);
      assertNotNull(entry); // it should be there
      assertTrue(entry.isTombstone()); // it should be a destroyed entry with Token.TOMBSTONE
     
      entry = ((LocalRegion)myRegion).getRegionEntry(key2);
      assertNotNull(entry); // it should be there
      assertTrue(entry.isTombstone()); // it should be a destroyed entry with Token.TOMBSTONE
    } else { // useTX
      c.getCacheTransactionManager().commit();
      // test that the commit correctly created the entries in the region
      assertNotNull(myRegion.get(notAffectedKey));
      
      assertNull(myRegion.get(key1));
      assertFalse(myRegion.containsKey(key1));
      entry = ((LocalRegion)myRegion).getRegionEntry(key1);
      assertNotNull(entry); // it should be there
      assertTrue(entry.isTombstone()); // it should be a destroyed entry with Token.TOMBSTONE

      assertNull(myRegion.get(key2));
      assertFalse(myRegion.containsKey(key2));
      entry = ((LocalRegion)myRegion).getRegionEntry(key2);
      assertNotNull(entry); // it should be there
      assertTrue(entry.isTombstone()); // it should be a destroyed entry with Token.TOMBSTONE
    }
    
  }
  
  
  private void doTestRegisterInterestRemovesOldEntry(final String regionName, final boolean usePR) throws Exception {
    VM vm1 = Host.getHost(0).getVM(1);
    VM vm2 = Host.getHost(0).getVM(2);
    
    // here are the keys that will be used to validate behavior.  Keys must be
    // colocated if using both a partitioned region in the server and transactions
    // in the client.  All of these keys hash to bucket 0 in a two-bucket PR
    // except Object11 and IDoNotExist1
    final String key10 = "Object10";
    final String interestPattern = "Object.*";

    SerializableCallableIF createServer = getCreateServerCallable(regionName, usePR);
    int serverPort = (Integer)vm1.invoke(createServer);
    vm2.invoke(createServer);
    vm1.invoke(new SerializableRunnable("populate server") {
      public void run() {
        Region myRegion =  getCache().getRegion(regionName);
        for (int i=1; i<=20; i++) {
          myRegion.put("Object"+i, "Value"+i);
        }
      }
    });
    com.gemstone.gemfire.test.dunit.LogWriterUtils.getLogWriter().info("creating client cache");
    ClientCache c = new ClientCacheFactory()
                    .addPoolServer("localhost", serverPort)
                    .set(DistributionConfig.LOG_LEVEL_NAME, LogWriterUtils.getDUnitLogLevel())
                    .setPoolSubscriptionEnabled(true)
                    .create();
    
    Region myRegion = c.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY).create(regionName);;

    myRegion.registerInterestRegex(interestPattern);
    
    // make sure key1 is in the client because we're going to mess with it
    assertNotNull(myRegion.get(key10));

    // remove the entry for key1 on the servers and then simulate interest recovery
    // to show that the entry for key1 is no longer there in the client when recovery
    // finishes
    SerializableRunnable destroyKey10 = new SerializableRunnable("locally destroy " + key10 + " in the servers") {
      public void run() {
        Region myRegion = getCache().getRegion(regionName);
        EntryEventImpl event = ((LocalRegion)myRegion).generateEvictDestroyEvent(key10);
        event.setOperation(Operation.LOCAL_DESTROY);
        if (usePR) {
          BucketRegion bucket = ((PartitionedRegion)myRegion).getBucketRegion(key10);
          if (bucket != null) {
            event.setRegion(bucket);
            com.gemstone.gemfire.test.dunit.LogWriterUtils.getLogWriter().info("performing local destroy in " + bucket + " ccEnabled="+bucket.concurrencyChecksEnabled + " rvv="+bucket.getVersionVector());
            bucket.concurrencyChecksEnabled = false; // turn off cc so entry is removed
            bucket.mapDestroy(event, false, false, null);
            bucket.concurrencyChecksEnabled = true;
          }
        } else {
          ((LocalRegion)myRegion).concurrencyChecksEnabled = false; // turn off cc so entry is removed
          ((LocalRegion)myRegion).mapDestroy(event, false, false, null);
          ((LocalRegion)myRegion).concurrencyChecksEnabled = true;
        }
      }
    };
    
    vm1.invoke(destroyKey10);
    vm2.invoke(destroyKey10);
    
    myRegion.getCache().getLogger().info("clearing keys of interest");
    ((LocalRegion)myRegion).clearKeysOfInterest(interestPattern,
        InterestType.REGULAR_EXPRESSION, InterestResultPolicy.KEYS_VALUES);
    myRegion.getCache().getLogger().info("done clearing keys of interest");

    assertTrue("expected region to be empty but it has " + myRegion.size() + " entries",
        myRegion.size() == 0);
    
    RegionEntry entry;
    entry = ((LocalRegion)myRegion).getRegionEntry(key10);
    assertNull(entry); // it should have been removed

    // now register interest.  At the end, finishRegisterInterest should clear
    // out the entry for key1 because it was stored in image-state as a
    // destroyed RI entry in clearKeysOfInterest
    myRegion.registerInterestRegex(interestPattern);

    entry = ((LocalRegion)myRegion).getRegionEntry(key10);
    assertNull(entry); // it should not be there
  }

  static class UpdateListener extends CacheListenerAdapter {
    int updateCount;
    LogWriter log;
    
    @Override
    public void afterUpdate(EntryEvent event) {
//      log.info("UpdateListener.afterUpdate invoked for " + event, new Exception("stack trace"));
      this.updateCount++;
    }
    @Override
    public void afterCreate(EntryEvent event) {
//      log.info("UpdateListener.afterCreate invoked for " + event, new Exception("stack trace"));
      this.updateCount++;
    }
  }

}

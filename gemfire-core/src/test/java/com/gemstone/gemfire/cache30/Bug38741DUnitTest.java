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
package com.gemstone.gemfire.cache30;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.gemstone.gemfire.CopyHelper;
import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.InterestResultPolicy;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.NanoTimer;
import com.gemstone.gemfire.internal.cache.CacheServerImpl;
import com.gemstone.gemfire.internal.cache.BucketRegion;
import com.gemstone.gemfire.internal.cache.BucketRegion.RawValue;
import com.gemstone.gemfire.internal.cache.CachedDeserializable;
import com.gemstone.gemfire.internal.cache.EnumListenerEvent;
import com.gemstone.gemfire.internal.cache.EventID;
import com.gemstone.gemfire.internal.cache.KeyInfo;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegionDataStore.BucketVisitor;
import com.gemstone.gemfire.internal.cache.ha.HARegionQueue;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheClientProxy;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientProxyMembershipID;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientUpdateMessageImpl;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.VM;

/**
 *
 * @author Mitch Thomas
 * @since bugfix5.7
 */
public class Bug38741DUnitTest extends ClientServerTestCase {
  private static final long serialVersionUID = 1L;

  protected RegionAttributes getRegionAttributes() {
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    return factory.create();
  }

  public Bug38741DUnitTest(String name) {
    super(name);
  }

  /**
   * Test that CopyOnRead doesn't cause {@link HARegionQueue#peek()} to create a copy,
   * assuming that creating copies performs a serialize and de-serialize operation.
   * @throws Exception when there is a failure
   * @since bugfix5.7
   */
  public void testCopyOnReadWithBridgeServer() throws Exception {
    final Host h = Host.getHost(0);
    final VM client = h.getVM(2);
    final VM server = h.getVM(3);
    final String rName = getUniqueName();
    final int ports[] = createUniquePorts(1);
    final String k1 = "k1";
    final String k2 = "k2";
    final String k3 = "k3";

    createBridgeServer(server, rName, ports[0]);
    // Put an instance of SerializationCounter to assert copy-on-read behavior
    // when notifyBySubscription is true
    server.invoke(new CacheSerializableRunnable("Enable copy on read and assert server copy behavior") {
      public void run2() throws CacheException {
        final LocalRegion r = (LocalRegion) getRootRegion(rName);

        // Using a key that counts serialization, the test captures
        // any serialization of the key when it is a member of another object,
        // specifically in this case ClientUpdateMessageImpl which is assume to be
        // the value of a HARegion
        SerializationCountingKey key = new SerializationCountingKey(k1);
        byte[] val = new byte[1];
        byte valIsObj = 0x01;
        Integer cb = new Integer(0);
        ClientProxyMembershipID cpmi = null;
        EventID eid = null;
        ClientUpdateMessageImpl cui =
          new ClientUpdateMessageImpl(EnumListenerEvent.AFTER_CREATE, r, key,
              val, valIsObj, cb, cpmi, eid);
        ClientUpdateMessageImpl cuiCopy = (ClientUpdateMessageImpl) CopyHelper.copy(cui);
        assertSame(key, cui.getKeyOfInterest());
        assertEquals(1, key.count.get());
        key = (SerializationCountingKey) cuiCopy.getKeyOfInterest();
        assertEquals(cui.getKeyOfInterest(), cuiCopy.getKeyOfInterest());
        assertEquals(1, key.count.get());


        SerializationCountingKey ks1 = new SerializationCountingKey(k1);
        { // Make sure nothing (HARegion) has serialized/de-serialized this instance
          SerializationCountingValue sc = new SerializationCountingValue();
          r.put(ks1, sc);
          assertEquals(0, sc.count.get());
          assertEquals(0, ks1.count.get());
        }

        { // No copy should be made upon get (assert standard no copy behavior)
          SerializationCountingValue sc = (SerializationCountingValue) r.get(ks1);
          assertEquals(0, sc.count.get());
          assertEquals(0, ks1.count.get());
        }

        // enable copy on read
        getCache().setCopyOnRead(true);

        { // Assert standard copy on read behavior
          SerializationCountingValue sc = (SerializationCountingValue) r.get(ks1);
          assertEquals(1, sc.count.get());
          assertEquals(0, ks1.count.get());
        }

        { // Put another counter with copy-on-read true
          // Again check that nothing (HARegion) has performed serialization
          SerializationCountingValue sc = new SerializationCountingValue();
          SerializationCountingKey ks3 = new SerializationCountingKey(k3);
          r.put(ks3, sc);
          assertEquals(0, sc.count.get());
          assertEquals(0, ks3.count.get());
        }
      }
    });

    // Setup a client which subscribes to the server region, registers (aka pulls)
    // interest in keys which creates an assumed HARegionQueue on the server
    // (in the event that the above code didn't already create a HARegion)
    final String serverHostName = getServerHostName(server.getHost());
    client.invoke(new CacheSerializableRunnable("Assert server copy behavior from client") {
      public void run2() throws CacheException {
        getCache();
        
        AttributesFactory factory = new AttributesFactory();
        ClientServerTestCase.configureConnectionPool(factory, serverHostName, ports, true,-1,1,null);
        factory.setScope(Scope.LOCAL);
        Region r = createRootRegion(rName, factory.create());
        SerializationCountingKey ks1 = new SerializationCountingKey(k1);
        SerializationCountingKey ks3 = new SerializationCountingKey(k3);
        r.registerInterest(ks1, InterestResultPolicy.KEYS_VALUES);
        r.registerInterest(new SerializationCountingKey(k2), InterestResultPolicy.KEYS_VALUES); // entry shouldn't exist yet
        r.registerInterest(ks3, InterestResultPolicy.KEYS_VALUES);

        { // Once for the get on the server, once to send the value to this client
          SerializationCountingValue sc = (SerializationCountingValue) r.get(ks1);
          assertEquals(2, sc.count.get());
        }

        { // Once to send the value to this client
          SerializationCountingValue sc = (SerializationCountingValue) r.get(ks3);
          assertEquals(1, sc.count.get());
        }
      }
    });

    // Put an instance of SerializationCounter to assert copy-on-read behavior
    // once a client has registered interest
    server.invoke(new CacheSerializableRunnable("Assert copy behavior after client is setup") {
      public void run2() throws CacheException {
        Region r = getRootRegion(rName);
        CacheServerImpl bsi = (CacheServerImpl)
          getCache().getCacheServers().iterator().next();
        Collection cp = bsi.getAcceptor().getCacheClientNotifier().getClientProxies();
        // Should only be one because only one client is connected
        assertEquals(1, cp.size());
        final CacheClientProxy ccp = (CacheClientProxy) cp.iterator().next();
        // Wait for messages to drain to capture a stable "processed message count"
        WaitCriterion ev = new WaitCriterion() {
          public boolean done() {
            return ccp.getHARegionQueue().size() == 0;
          }
          public String description() {
            return "region queue never became empty";
          }
        };
        DistributedTestCase.waitForCriterion(ev, 60 * 1000, 200, true);
        
        // Capture the current processed message count to know
        // when the next message has been serialized
        final int currMesgCount = ccp.getStatistics().getMessagesProcessed();

        SerializationCountingKey ks2 = new SerializationCountingKey(k2);
        SerializationCountingValue sc = new SerializationCountingValue();
        // Update a key upon which the client has expressed interest,
        // expect it to send an update message to the client
        r.put(ks2, sc);

        // Wait to know that the data has been at least serialized (possibly sent)
        ev = new WaitCriterion() {
          public boolean done() {
            return ccp.getStatistics().getMessagesProcessed() != currMesgCount;
          }
          public String description() {
            return null;
          }
        };
        DistributedTestCase.waitForCriterion(ev, 60 * 1000, 200, true);
        
        // assert one serialization to send value to interested client
        // more than one implies copy-on-read behavior (bad)
        assertEquals(1, sc.count.get());
        assertEquals(1, ks2.count.get());
      }
    });

    // Double-check the serialization count in the event that the previous check
    // missed the copy due to race conditions
    client.invoke(new CacheSerializableRunnable("Assert copy behavior from client after update") {
      public void run2() throws CacheException {
        Region r = getRootRegion(rName);
        { // Once to send the value to this client via the updater thread

          SerializationCountingKey ks2 = new SerializationCountingKey(k2);
          // Wait for the update to arrive on to the Cache Client Updater
          long start = NanoTimer.getTime();
          final int maxSecs = 30;
          while(!r.containsKey(ks2)) {
            pause(100);
            if ((NanoTimer.getTime() - start) > TimeUnit.SECONDS.toNanos(maxSecs)) {
              fail("Waited over " + maxSecs + "s");
            }
          }
          
          SerializationCountingValue sc = (SerializationCountingValue) r.getEntry(ks2).getValue();
          assertEquals(1, sc.count.get());
        }
      }
    });
  }

  /**
   * Test to ensure that a PartitionedRegion doesn't make more than the
   * expected number of copies when copy-on-read is set to true
   * @throws Exception
   */
  public void testPartitionedRegionAndCopyOnRead() throws Exception {
    final Host h = Host.getHost(0);
    final VM accessor = h.getVM(2);
    final VM datastore = h.getVM(3);
    final String rName = getUniqueName();
    final String k1 = "k1";

    datastore.invoke(new CacheSerializableRunnable("Create PR DataStore") {
      public void run2() throws CacheException {
        AttributesFactory factory = new AttributesFactory();
        factory.setPartitionAttributes(new PartitionAttributesFactory().setRedundantCopies(0).create());
        createRootRegion(rName, factory.create());
      }
    });

    accessor.invoke(new CacheSerializableRunnable("Create PR Accessor and put new value") {
      public void run2() throws CacheException {
        AttributesFactory factory = new AttributesFactory();
        factory.setPartitionAttributes(new PartitionAttributesFactory().setLocalMaxMemory(0).setRedundantCopies(0).create());
        Region r = createRootRegion(rName, factory.create());
        SerializationCountingValue val = new SerializationCountingValue();
        r.put(k1, val);
        // First put to a bucket will serialize once to determine the size of the value
        // to know how much extra space the new bucket with the new entry will consume
        // and serialize again to send the bytes
        assertEquals(2, val.count.get());
        // A put to an already created bucket should only be serialized once
        val = new SerializationCountingValue();
        r.put(k1, val);
        assertEquals(1, val.count.get());
      }
    });

    datastore.invoke(new CacheSerializableRunnable("assert datastore entry serialization count") {
      public void run2() throws CacheException {
        PartitionedRegion pr = (PartitionedRegion) getRootRegion(rName);
        // Visit the one bucket (since there is only one value in the entire PR)
        // to directly copy the entry bytes and assert the serialization count.
        // All this extra work is to assure the serialization count does not increase
        // (by de-serializing the value stored in the map, which would then have to be
        // re-serialized).
        pr.getDataStore().visitBuckets(new BucketVisitor() {
          public void visit(Integer bucketId, Region r) {
            BucketRegion br = (BucketRegion) r;
            try {
              KeyInfo keyInfo = new KeyInfo(k1, null, bucketId);
              RawValue rv = br.getSerialized(keyInfo, false, false, null, null, false, false);
              Object val = rv.getRawValue();
              assertTrue(val instanceof CachedDeserializable);
              CachedDeserializable cd = (CachedDeserializable)val;
              SerializationCountingValue scv = (SerializationCountingValue)cd.getDeserializedForReading();
              assertEquals(1, scv.count.get());
            } catch (IOException fail) {
              fail("Unexpected IOException", fail);
            }
          }
        });
      }
    });

    accessor.invoke(new CacheSerializableRunnable("assert accessor entry serialization count") {
      public void run2() throws CacheException {
        Region r = getRootRegion(rName);
        SerializationCountingValue v1 = (SerializationCountingValue)r.get(k1);
        // The counter was incremented once to send the data to the datastore
        assertEquals(1, v1.count.get());
        getCache().setCopyOnRead(true);
        // Once to send the data to the datastore, no need to do a serialization
        // when we make copy since it is serialized from datastore to us.
        SerializationCountingValue v2 = (SerializationCountingValue)r.get(k1);
        assertEquals(1, v2.count.get());
        assertTrue(v1 != v2);
      }
    });

    datastore.invoke(new CacheSerializableRunnable("assert value serialization") {
      public void run2() throws CacheException {
        Region r = getRootRegion(rName);
        SerializationCountingValue v1 = (SerializationCountingValue)r.get(k1);
        // Once to send the value from the accessor to the data store
        assertEquals(1, v1.count.get());
        getCache().setCopyOnRead(true);
        // Once to send the value from the accessor to the data store
        // once to make a local copy
        SerializationCountingValue v2 = (SerializationCountingValue)r.get(k1);
        assertEquals(2, v2.count.get());
        assertTrue(v1 != v2);
      }
    });
  }

  public Properties getDistributedSystemProperties() {
    Properties props = new Properties();
    props.setProperty(DistributionConfig.DELTA_PROPAGATION_PROP_NAME, "false");
    return props;
  }

  public static class SerializationCountingValue implements DataSerializable {
    private static final long serialVersionUID = 1L;
    public final AtomicInteger count = new AtomicInteger();
    public SerializationCountingValue() {
    }

    public void fromData(DataInput in) throws IOException,
        ClassNotFoundException {
      count.set(in.readInt());
    }
    public void toData(DataOutput out) throws IOException {
      out.writeInt(count.addAndGet(1));
      //GemFireCacheImpl.getInstance().getLogger().info("DEBUG "+this, new RuntimeException("STACK"));
    }
    public String toString() {
      return getClass().getName() + "@" + System.identityHashCode(this) + "; count=" + count;
    }
  }
  public static class SerializationCountingKey extends SerializationCountingValue {
    private static final long serialVersionUID = 1L;
    private String k;
    public SerializationCountingKey(String k) {
      this.k = k;
    }
    public SerializationCountingKey() {super();}
    public void fromData(DataInput in) throws IOException,
        ClassNotFoundException {
      super.fromData(in);
      k = DataSerializer.readString(in);
    }
    public void toData(DataOutput out) throws IOException {
      super.toData(out);
      DataSerializer.writeString(k, out);
    }
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      }
      if (obj instanceof SerializationCountingKey) {
        SerializationCountingKey other = (SerializationCountingKey) obj;
        return k.equals(other.k);
      }
      return false;
    }
    public int hashCode() {
      return k.hashCode();
    }
    public String toString() {
      return super.toString() + "; k=" + k;
    }
  }
}

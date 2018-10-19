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
package org.apache.geode.internal.cache;

import static org.apache.geode.distributed.ConfigurationProperties.DELTA_PROPAGATION;
import static org.apache.geode.internal.lang.SystemPropertyHelper.EARLY_ENTRY_EVENT_SERIALIZATION;
import static org.apache.geode.internal.lang.SystemPropertyHelper.GEODE_PREFIX;
import static org.apache.geode.test.dunit.Assert.assertEquals;
import static org.apache.geode.test.dunit.Assert.assertSame;
import static org.apache.geode.test.dunit.Assert.assertTrue;
import static org.apache.geode.test.dunit.Assert.fail;
import static org.apache.geode.test.dunit.Host.getHost;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import org.apache.geode.CopyHelper;
import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.InterestResultPolicy;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.cache30.ClientServerTestCase;
import org.apache.geode.internal.NanoTimer;
import org.apache.geode.internal.cache.BucketRegion.RawValue;
import org.apache.geode.internal.cache.PartitionedRegionDataStore.BucketVisitor;
import org.apache.geode.internal.cache.ha.HARegionQueue;
import org.apache.geode.internal.cache.tier.sockets.CacheClientProxy;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.tier.sockets.ClientUpdateMessageImpl;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.dunit.rules.DistributedRestoreSystemProperties;
import org.apache.geode.test.junit.categories.ClientServerTest;

/**
 * Client side deserialization should not throw EOFException when copy-on-read is true.
 *
 * <p>
 * TRAC #38741: EOFException during deserialize on client update with copy-on-read=true
 *
 * @since GemFire bugfix5.7
 */
@Category({ClientServerTest.class})
@RunWith(JUnitParamsRunner.class)
@SuppressWarnings("serial")
public class ClientDeserializationCopyOnReadRegressionTest extends ClientServerTestCase {

  private String k1 = "k1";
  private String k2 = "k2";
  private String k3 = "k3";

  private VM client;
  private VM server;
  private String rName;
  private int ports[];

  @Rule
  public DistributedRestoreSystemProperties restoreSystemProperties =
      new DistributedRestoreSystemProperties();

  @Before
  public void setUp() {
    client = getHost(0).getVM(2);
    server = getHost(0).getVM(3);
    rName = getUniqueName();
    ports = createUniquePorts(1);
  }

  @Override
  public Properties getDistributedSystemProperties() {
    Properties props = new Properties();
    props.setProperty(DELTA_PROPAGATION, "false");
    return props;
  }

  @Override
  protected RegionAttributes getRegionAttributes() {
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    return factory.create();
  }

  /**
   * Test that CopyOnRead doesn't cause {@link HARegionQueue#peek()} to create a copy, assuming that
   * creating copies performs a serialize and de-serialize operation.
   */
  @Test
  @Parameters({"true", "false"})
  @TestCaseName("{method}({params})")
  public void testCopyOnReadWithBridgeServer(final boolean serializeEarlyEnabled) throws Exception {
    System.setProperty(GEODE_PREFIX + EARLY_ENTRY_EVENT_SERIALIZATION, "true");
    Invoke.invokeInEveryVM(
        () -> System.setProperty(GEODE_PREFIX + EARLY_ENTRY_EVENT_SERIALIZATION, "true"));

    createBridgeServer(server, rName, ports[0]);
    // Put an instance of SerializationCounter to assert copy-on-read behavior
    // when notifyBySubscription is true
    server.invoke(
        new CacheSerializableRunnable("Enable copy on read and assert server copy behavior") {
          @Override
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
            ClientUpdateMessageImpl cui = new ClientUpdateMessageImpl(
                EnumListenerEvent.AFTER_CREATE, r, key, val, valIsObj, cb, cpmi, eid);
            ClientUpdateMessageImpl cuiCopy = (ClientUpdateMessageImpl) CopyHelper.copy(cui);
            assertSame(key, cui.getKeyOfInterest());
            assertEquals(1, key.count.get());
            key = (SerializationCountingKey) cuiCopy.getKeyOfInterest();
            assertEquals(cui.getKeyOfInterest(), cuiCopy.getKeyOfInterest());
            assertEquals(1, key.count.get());

            SerializationCountingKey ks1 = new SerializationCountingKey(k1);
            { // AbstractRegionMap basicPut now serializes newValue in EntryEventImpl
              // which can be used for delivering client update message later
              SerializationCountingValue sc = new SerializationCountingValue();
              r.put(ks1, sc);
              assertEquals(1, sc.count.get());
              assertEquals(0, ks1.count.get());
            }

            { // No copy should be made upon get (assert standard no copy behavior)
              SerializationCountingValue sc = (SerializationCountingValue) r.get(ks1);
              assertEquals(1, sc.count.get());
              assertEquals(0, ks1.count.get());
            }

            // enable copy on read
            getCache().setCopyOnRead(true);

            { // Assert standard copy on read behavior and basicPut in AbstractRegionMap
              SerializationCountingValue sc = (SerializationCountingValue) r.get(ks1);
              assertEquals(2, sc.count.get());
              assertEquals(0, ks1.count.get());
            }

            { // Put another counter with copy-on-read true
              // AbstractRegionMap basicPut now serializes newValue
              SerializationCountingValue sc = new SerializationCountingValue();
              SerializationCountingKey ks3 = new SerializationCountingKey(k3);
              r.put(ks3, sc);
              assertEquals(1, sc.count.get());
              assertEquals(0, ks3.count.get());
            }
          }
        });

    // Setup a client which subscribes to the server region, registers (aka pulls)
    // interest in keys which creates an assumed HARegionQueue on the server
    // (in the event that the above code didn't already create a HARegion)
    final String serverHostName = NetworkUtils.getServerHostName(server.getHost());
    client.invoke(new CacheSerializableRunnable("Assert server copy behavior from client") {
      @Override
      public void run2() throws CacheException {
        getCache();

        AttributesFactory factory = new AttributesFactory();
        ClientServerTestCase.configureConnectionPool(factory, serverHostName, ports, true, -1, 1,
            null);
        factory.setScope(Scope.LOCAL);
        Region r = createRootRegion(rName, factory.create());
        SerializationCountingKey ks1 = new SerializationCountingKey(k1);
        SerializationCountingKey ks3 = new SerializationCountingKey(k3);
        // original two serializations on server and one serialization for register interest
        r.registerInterest(ks1, InterestResultPolicy.KEYS_VALUES);
        // entry shouldn't exist yet
        r.registerInterest(new SerializationCountingKey(k2), InterestResultPolicy.KEYS_VALUES);
        // original one serializations on server and one serialization for register interest
        r.registerInterest(ks3, InterestResultPolicy.KEYS_VALUES);

        { // get from local cache.
          // original two serializations on server and one for previous register interest
          SerializationCountingValue sc = (SerializationCountingValue) r.get(ks1);
          assertEquals(3, sc.count.get());
        }

        { // get from local cache,
          // one from register interest key and one original put into AbstractRegionMap
          SerializationCountingValue sc = (SerializationCountingValue) r.get(ks3);
          assertEquals(2, sc.count.get());
        }
      }
    });

    // Put an instance of SerializationCounter to assert copy-on-read behavior
    // once a client has registered interest
    server.invoke(new CacheSerializableRunnable("Assert copy behavior after client is setup") {
      @Override
      public void run2() throws CacheException {
        Region r = getRootRegion(rName);
        CacheServerImpl bsi = (CacheServerImpl) getCache().getCacheServers().iterator().next();
        Collection cp = bsi.getAcceptor().getCacheClientNotifier().getClientProxies();
        // Should only be one because only one client is connected
        assertEquals(1, cp.size());
        final CacheClientProxy ccp = (CacheClientProxy) cp.iterator().next();
        // Wait for messages to drain to capture a stable "processed message count"
        WaitCriterion ev = new WaitCriterion() {
          @Override
          public boolean done() {
            return ccp.getHARegionQueue().size() == 0;
          }

          @Override
          public String description() {
            return "region queue never became empty";
          }
        };
        GeodeAwaitility.await().untilAsserted(ev);

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
          @Override
          public boolean done() {
            return ccp.getStatistics().getMessagesProcessed() != currMesgCount;
          }

          @Override
          public String description() {
            return null;
          }
        };
        GeodeAwaitility.await().untilAsserted(ev);

        // assert one serialization to send value to interested client
        // more than one implies copy-on-read behavior (bad)
        assertEquals(1, sc.count.get());
        assertEquals(1, ks2.count.get());
      }
    });

    // Double-check the serialization count in the event that the previous check
    // missed the copy due to race conditions
    client.invoke(new CacheSerializableRunnable("Assert copy behavior from client after update") {
      @Override
      public void run2() throws CacheException {
        Region r = getRootRegion(rName);
        { // Once to send the value to this client via the updater thread

          SerializationCountingKey ks2 = new SerializationCountingKey(k2);
          // Wait for the update to arrive on to the Cache Client Updater
          long start = NanoTimer.getTime();
          final int maxSecs = 30;
          while (!r.containsKey(ks2)) {
            Wait.pause(100);
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
   * Test to ensure that a PartitionedRegion doesn't make more than the expected number of copies
   * when copy-on-read is set to true
   */
  @Test
  public void testPartitionedRegionAndCopyOnRead() throws Exception {
    final Host h = getHost(0);
    final VM accessor = h.getVM(2);
    final VM datastore = h.getVM(3);
    final String rName = getUniqueName();
    final String k1 = "k1";

    datastore.invoke(new CacheSerializableRunnable("Create PR DataStore") {
      @Override
      public void run2() throws CacheException {
        AttributesFactory factory = new AttributesFactory();
        factory.setPartitionAttributes(
            new PartitionAttributesFactory().setRedundantCopies(0).create());
        createRootRegion(rName, factory.create());
      }
    });

    accessor.invoke(new CacheSerializableRunnable("Create PR Accessor and put new value") {
      @Override
      public void run2() throws CacheException {
        AttributesFactory factory = new AttributesFactory();
        factory.setPartitionAttributes(
            new PartitionAttributesFactory().setLocalMaxMemory(0).setRedundantCopies(0).create());
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
      @Override
      public void run2() throws CacheException {
        PartitionedRegion pr = (PartitionedRegion) getRootRegion(rName);
        // Visit the one bucket (since there is only one value in the entire PR)
        // to directly copy the entry bytes and assert the serialization count.
        // All this extra work is to assure the serialization count does not increase
        // (by de-serializing the value stored in the map, which would then have to be
        // re-serialized).
        pr.getDataStore().visitBuckets(new BucketVisitor() {
          @Override
          public void visit(Integer bucketId, Region r) {
            BucketRegion br = (BucketRegion) r;
            try {
              KeyInfo keyInfo = new KeyInfo(k1, null, bucketId);
              RawValue rv = br.getSerialized(keyInfo, false, false, null, null, false);
              Object val = rv.getRawValue();
              assertTrue(val instanceof CachedDeserializable);
              CachedDeserializable cd = (CachedDeserializable) val;
              SerializationCountingValue scv =
                  (SerializationCountingValue) cd.getDeserializedForReading();
              assertEquals(1, scv.count.get());
            } catch (IOException fail) {
              Assert.fail("Unexpected IOException", fail);
            }
          }
        });
      }
    });

    accessor.invoke(new CacheSerializableRunnable("assert accessor entry serialization count") {
      @Override
      public void run2() throws CacheException {
        Region r = getRootRegion(rName);
        SerializationCountingValue v1 = (SerializationCountingValue) r.get(k1);
        // The counter was incremented once to send the data to the datastore
        assertEquals(1, v1.count.get());
        getCache().setCopyOnRead(true);
        // Once to send the data to the datastore, no need to do a serialization
        // when we make copy since it is serialized from datastore to us.
        SerializationCountingValue v2 = (SerializationCountingValue) r.get(k1);
        assertEquals(1, v2.count.get());
        assertTrue(v1 != v2);
      }
    });

    datastore.invoke(new CacheSerializableRunnable("assert value serialization") {
      @Override
      public void run2() throws CacheException {
        Region r = getRootRegion(rName);
        SerializationCountingValue v1 = (SerializationCountingValue) r.get(k1);
        // Once to send the value from the accessor to the data store
        assertEquals(1, v1.count.get());
        getCache().setCopyOnRead(true);
        // Once to send the value from the accessor to the data store
        // once to make a local copy
        SerializationCountingValue v2 = (SerializationCountingValue) r.get(k1);
        assertEquals(2, v2.count.get());
        assertTrue(v1 != v2);
      }
    });
  }

  private static class SerializationCountingValue implements DataSerializable {
    public final AtomicInteger count = new AtomicInteger();

    public SerializationCountingValue() {
      // nothing
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      count.set(in.readInt());
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      out.writeInt(count.addAndGet(1));
    }

    @Override
    public String toString() {
      return getClass().getName() + "@" + System.identityHashCode(this) + "; count=" + count;
    }
  }

  private static class SerializationCountingKey extends SerializationCountingValue {
    private String k;

    public SerializationCountingKey(String k) {
      this.k = k;
    }

    public SerializationCountingKey() {
      super();
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      super.fromData(in);
      k = DataSerializer.readString(in);
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      super.toData(out);
      DataSerializer.writeString(k, out);
    }

    @Override
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

    @Override
    public int hashCode() {
      return k.hashCode();
    }

    @Override
    public String toString() {
      return super.toString() + "; k=" + k;
    }
  }
}

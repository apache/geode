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
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import org.apache.geode.CopyHelper;
import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.InterestResultPolicy;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache30.ClientServerTestCase;
import org.apache.geode.internal.cache.BucketRegion.RawValue;
import org.apache.geode.internal.cache.ha.HARegionQueue;
import org.apache.geode.internal.cache.tier.sockets.CacheClientProxy;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.tier.sockets.ClientUpdateMessageImpl;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.DistributedRestoreSystemProperties;
import org.apache.geode.test.junit.categories.ClientServerTest;
import org.apache.geode.test.junit.runners.GeodeParamsRunner;

/**
 * Client side deserialization should not throw EOFException when copy-on-read is true.
 *
 * <p>
 * TRAC #38741: EOFException during deserialize on client update with copy-on-read=true
 *
 * @since GemFire bugfix5.7
 */
@Category({ClientServerTest.class})
@RunWith(GeodeParamsRunner.class)
@SuppressWarnings("serial")
public class ClientDeserializationCopyOnReadRegressionTest extends ClientServerTestCase {

  private String k1 = "k1";
  private String k2 = "k2";
  private String k3 = "k3";

  private VM client;
  private VM server;
  private String rName;
  private int[] ports;

  @Rule
  public DistributedRestoreSystemProperties restoreSystemProperties =
      new DistributedRestoreSystemProperties();

  @Before
  public void setUp() {
    client = VM.getVM(2);
    server = VM.getVM(3);
    rName = getUniqueName();
    ports = createUniquePorts();
  }

  @Override
  public Properties getDistributedSystemProperties() {
    Properties props = new Properties();
    props.setProperty(DELTA_PROPAGATION, "false");
    return props;
  }

  @Override
  protected <K, V> RegionAttributes<K, V> getRegionAttributes() {
    AttributesFactory<K, V> factory = new AttributesFactory<>();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    return factory.create();
  }

  /**
   * Test that CopyOnRead doesn't cause {@link HARegionQueue#peek()} to create a copy, assuming that
   * creating copies performs a serialize and de-serialize operation.
   */
  @Test

  public void testCopyOnReadWithBridgeServer() {
    System.setProperty(GEODE_PREFIX + EARLY_ENTRY_EVENT_SERIALIZATION, "true");
    Invoke.invokeInEveryVM(
        () -> System.setProperty(GEODE_PREFIX + EARLY_ENTRY_EVENT_SERIALIZATION, "true"));

    createBridgeServer(server, rName, ports[0]);
    // Put an instance of SerializationCounter to assert copy-on-read behavior
    // when notifyBySubscription is true
    server.invoke("Enable copy on read and assert server copy behavior", () -> {
      final Region<Object, Object> rootRegion = getRootRegion(rName);

      // Using a key that counts serialization, the test captures
      // any serialization of the key when it is a member of another object,
      // specifically in this case ClientUpdateMessageImpl which is assume to be
      // the value of a HARegion
      SerializationCountingKey key = new SerializationCountingKey(k1);
      byte[] val = new byte[1];
      byte valIsObj = 0x01;
      Integer cb = 0;
      ClientProxyMembershipID cpmi = null;
      EventID eid = null;
      ClientUpdateMessageImpl cui = new ClientUpdateMessageImpl(
          EnumListenerEvent.AFTER_CREATE, (LocalRegion) rootRegion, key, val, valIsObj, cb, cpmi,
          eid);
      ClientUpdateMessageImpl cuiCopy = CopyHelper.copy(cui);
      assertThat(cui.getKeyOfInterest()).isSameAs(key);
      assertThat(key.count.get()).isEqualTo(1);
      key = (SerializationCountingKey) cuiCopy.getKeyOfInterest();
      assertThat(cuiCopy.getKeyOfInterest()).isEqualTo(cui.getKeyOfInterest());
      assertThat(key.count.get()).isEqualTo(1);

      SerializationCountingKey ks1 = new SerializationCountingKey(k1);
      // AbstractRegionMap basicPut now serializes newValue in EntryEventImpl
      // which can be used for delivering client update message later
      SerializationCountingValue sc = new SerializationCountingValue();
      rootRegion.put(ks1, sc);
      assertThat(sc.count.get()).isEqualTo(1);
      assertThat(ks1.count.get()).isEqualTo(0);


      // No copy should be made upon get (assert standard no copy behavior)
      sc = (SerializationCountingValue) rootRegion.get(ks1);
      assertThat(sc.count.get()).isEqualTo(1);
      assertThat(ks1.count.get()).isEqualTo(0);


      // enable copy on read
      getCache().setCopyOnRead(true);

      // Assert standard copy on read behavior and basicPut in AbstractRegionMap
      sc = (SerializationCountingValue) rootRegion.get(ks1);
      assertThat(sc.count.get()).isEqualTo(2);
      assertThat(ks1.count.get()).isEqualTo(0);
      // Put another counter with copy-on-read true
      // AbstractRegionMap basicPut now serializes newValue
      sc = new SerializationCountingValue();
      SerializationCountingKey ks3 = new SerializationCountingKey(k3);
      rootRegion.put(ks3, sc);
      assertThat(sc.count.get()).isEqualTo(1);
      assertThat(ks3.count.get()).isEqualTo(0);
    });

    // Setup a client which subscribes to the server region, registers (aka pulls)
    // interest in keys which creates an assumed HARegionQueue on the server
    // (in the event that the above code didn't already create a HARegion)
    final String serverHostName = NetworkUtils.getServerHostName();
    client.invoke("Assert server copy behavior from client", () -> {
      getCache();

      RegionFactory<Object, Object> factory = getCache().createRegionFactory();
      ClientServerTestCase.configureConnectionPool(factory, serverHostName, ports, true, -1, 1,
          null);
      factory.setScope(Scope.LOCAL);
      Region<Object, Object> rootRegion = createRootRegion(rName, factory);
      SerializationCountingKey ks1 = new SerializationCountingKey(k1);
      SerializationCountingKey ks3 = new SerializationCountingKey(k3);
      // original two serializations on server and one serialization for register interest
      rootRegion.registerInterest(ks1, InterestResultPolicy.KEYS_VALUES);
      // entry shouldn't exist yet
      rootRegion.registerInterest(new SerializationCountingKey(k2),
          InterestResultPolicy.KEYS_VALUES);
      // original one serializations on server and one serialization for register interest
      rootRegion.registerInterest(ks3, InterestResultPolicy.KEYS_VALUES);

      // get from local cache.
      // original two serializations on server and one for previous register interest
      SerializationCountingValue sc = (SerializationCountingValue) rootRegion.get(ks1);
      assertThat(sc.count.get()).isEqualTo(3);

      sc = (SerializationCountingValue) rootRegion.get(ks3);
      assertThat(sc.count.get()).isEqualTo(2);
    });

    // Put an instance of SerializationCounter to assert copy-on-read behavior
    // once a client has registered interest
    server.invoke("Assert copy behavior after client is setup", () -> {
      Region<Object, Object> rootRegion = getRootRegion(rName);
      CacheServerImpl bsi = (CacheServerImpl) getCache().getCacheServers().iterator().next();
      Collection cp = bsi.getAcceptor().getCacheClientNotifier().getClientProxies();
      // Should only be one because only one client is connected
      assertThat(cp.size()).isEqualTo(1);
      final CacheClientProxy ccp = (CacheClientProxy) cp.iterator().next();
      // Wait for messages to drain to capture a stable "processed message count"
      await("region queue never became empty")
          .until(() -> ccp.getHARegionQueue().size() == 0);

      // Capture the current processed message count to know
      // when the next message has been serialized
      final int currMesgCount = ccp.getStatistics().getMessagesProcessed();

      SerializationCountingKey ks2 = new SerializationCountingKey(k2);
      SerializationCountingValue sc = new SerializationCountingValue();
      // Update a key upon which the client has expressed interest,
      // expect it to send an update message to the client
      rootRegion.put(ks2, sc);

      // Wait to know that the data has been at least serialized (possibly sent)
      await()
          .until(() -> ccp.getStatistics().getMessagesProcessed() != currMesgCount);

      // assert one serialization to send value to interested client
      // more than one implies copy-on-read behavior (bad)
      assertThat(sc.count.get()).isEqualTo(1);
      assertThat(ks2.count.get()).isEqualTo(1);
    });

    // Double-check the serialization count in the event that the previous check
    // missed the copy due to race conditions
    client.invoke("Assert copy behavior from client after update", () -> {
      Region rootRegion = getRootRegion(rName);
      { // Once to send the value to this client via the updater thread

        SerializationCountingKey ks2 = new SerializationCountingKey(k2);
        // Wait for the update to arrive on to the Cache Client Updater
        final int maxSecs = 30;
        await("Waited over " + maxSecs + "s").timeout(maxSecs, TimeUnit.SECONDS)
            .until(() -> rootRegion.containsKey(ks2));

        SerializationCountingValue sc =
            (SerializationCountingValue) rootRegion.getEntry(ks2).getValue();
        assertThat(sc.count.get()).isEqualTo(1);
      }
    });
  }

  /**
   * Test to ensure that a PartitionedRegion doesn't make more than the expected number of copies
   * when copy-on-read is set to true
   */
  @Test
  public void testPartitionedRegionAndCopyOnRead() {
    final VM accessor = VM.getVM(2);
    final VM datastore = VM.getVM(3);
    final String rName = getUniqueName();
    final String k1 = "k1";

    datastore.invoke("Create PR DataStore", () -> {
      RegionFactory<Object, Object> factory = getCache().createRegionFactory();
      factory.setPartitionAttributes(
          new PartitionAttributesFactory().setRedundantCopies(0).create());
      createRootRegion(rName, factory);
    });

    accessor.invoke("Create PR Accessor and put new value", () -> {
      RegionFactory<Object, Object> factory = getCache().createRegionFactory();
      factory.setPartitionAttributes(
          new PartitionAttributesFactory().setLocalMaxMemory(0).setRedundantCopies(0).create());
      Region<Object, Object> rootRegion = createRootRegion(rName, factory);
      SerializationCountingValue val = new SerializationCountingValue();
      rootRegion.put(k1, val);
      // First put to a bucket will serialize once to determine the size of the value
      // to know how much extra space the new bucket with the new entry will consume
      // and serialize again to send the bytes
      assertThat(val.count.get()).isEqualTo(2);
      // A put to an already created bucket should only be serialized once
      val = new SerializationCountingValue();
      rootRegion.put(k1, val);
      assertThat(val.count.get()).isEqualTo(1);
    });

    datastore.invoke("assert datastore entry serialization count", () -> {
      PartitionedRegion pr = (PartitionedRegion) getRootRegion(rName);
      // Visit the one bucket (since there is only one value in the entire PR)
      // to directly copy the entry bytes and assert the serialization count.
      // All this extra work is to assure the serialization count does not increase
      // (by de-serializing the value stored in the map, which would then have to be
      // re-serialized).
      pr.getDataStore().visitBuckets((bucketId, r) -> {
        BucketRegion br = (BucketRegion) r;
        KeyInfo keyInfo = new KeyInfo(k1, null, bucketId);
        RawValue rv = null;
        try {
          rv = br.getSerialized(keyInfo, false, false, null, null, false);
        } catch (IOException e) {
          fail("Unexpected IOException", e);
        }
        Object val = rv.getRawValue();
        assertThat(val).isInstanceOf(CachedDeserializable.class);
        CachedDeserializable cd = (CachedDeserializable) val;
        SerializationCountingValue scv =
            (SerializationCountingValue) cd.getDeserializedForReading();
        assertThat(scv.count.get()).isEqualTo(1);
      });
    });

    accessor.invoke("assert accessor entry serialization count", () -> {
      Region<Object, Object> rootRegion = getRootRegion(rName);
      SerializationCountingValue value1 = (SerializationCountingValue) rootRegion.get(k1);
      // The counter was incremented once to send the data to the datastore
      assertThat(value1.count.get()).isEqualTo(1);
      getCache().setCopyOnRead(true);
      // Once to send the data to the datastore, no need to do a serialization
      // when we make copy since it is serialized from datastore to us.
      SerializationCountingValue value2 = (SerializationCountingValue) rootRegion.get(k1);
      assertThat(value2.count.get()).isEqualTo(1);
      assertThat(value2).isNotEqualTo(value1);
    });

    datastore.invoke("assert value serialization", () -> {
      Region rootRegion = getRootRegion(rName);
      SerializationCountingValue value1 = (SerializationCountingValue) rootRegion.get(k1);
      // Once to send the value from the accessor to the data store
      assertThat(value1.count.get()).isEqualTo(1);
      getCache().setCopyOnRead(true);
      // Once to send the value from the accessor to the data store
      // once to make a local copy
      SerializationCountingValue value2 = (SerializationCountingValue) rootRegion.get(k1);
      assertThat(value2.count.get()).isEqualTo(2);
      assertThat(value2).isNotEqualTo(value1);
    });
  }

  private static class SerializationCountingValue implements DataSerializable {
    public final AtomicInteger count = new AtomicInteger();

    @SuppressWarnings("WeakerAccess")
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

    @SuppressWarnings("WeakerAccess")
    public SerializationCountingKey(String k) {
      this.k = k;
    }

    @SuppressWarnings("unused")
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

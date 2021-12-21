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

import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPort;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import org.junit.Test;

import org.apache.geode.DataSerializable;
import org.apache.geode.Delta;
import org.apache.geode.InvalidDeltaException;
import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.CommitConflictException;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.InterestPolicy;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.SubscriptionAttributes;
import org.apache.geode.cache.UnsupportedOperationInTransactionException;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.internal.cache.execute.CustomerIDPartitionResolver;
import org.apache.geode.internal.cache.execute.data.CustId;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;


public class TransactionsWithDeltaDUnitTest extends JUnit4CacheTestCase {

  private static final String D_REFERENCE = "ref";
  private static final String CUSTOMER = "Customer";
  private static final String ORDER = "Order";

  public TransactionsWithDeltaDUnitTest() {
    super();
  }

  private Integer createRegionOnServer(VM vm, final boolean startServer, final boolean accessor) {
    return (Integer) vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        createRegion(accessor, 0, null);
        if (startServer) {
          int port = getRandomAvailableTCPPort();
          CacheServer s = getCache().addCacheServer();
          s.setPort(port);
          s.start();
          return port;
        }
        return 0;
      }
    });
  }

  private void createRegion(boolean accessor, int redundantCopies, InterestPolicy interestPolicy) {
    AttributesFactory af = new AttributesFactory();
    af.setScope(Scope.DISTRIBUTED_ACK);
    af.setDataPolicy(DataPolicy.REPLICATE);
    af.setCloningEnabled(true);
    getCache().createRegion(D_REFERENCE, af.create());
    af = new AttributesFactory();
    af.setCloningEnabled(true);
    if (interestPolicy != null) {
      af.setSubscriptionAttributes(new SubscriptionAttributes(interestPolicy));
    }
    af.setPartitionAttributes(new PartitionAttributesFactory<>()
        .setTotalNumBuckets(4).setLocalMaxMemory(accessor ? 0 : 1)
        .setPartitionResolver(new CustomerIDPartitionResolver("resolver1"))
        .setRedundantCopies(redundantCopies).create());
    getCache().createRegion(CUSTOMER, af.create());
    af.setPartitionAttributes(new PartitionAttributesFactory<>().setTotalNumBuckets(4)
        .setLocalMaxMemory(accessor ? 0 : 1)
        .setPartitionResolver(new CustomerIDPartitionResolver("resolver2"))
        .setRedundantCopies(redundantCopies).setColocatedWith(CUSTOMER).create());
    getCache().createRegion(ORDER, af.create());
  }

  private void createClientRegion(VM vm, final int port, final boolean isEmpty, final boolean ri) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        ClientCacheFactory ccf = new ClientCacheFactory();
        ccf.addPoolServer("localhost"/* getServerHostName(Host.getHost(0)) */, port);
        ccf.setPoolSubscriptionEnabled(false);
        ccf.set(LOG_LEVEL, LogWriterUtils.getDUnitLogLevel());
        ClientCache cCache = getClientCache(ccf);
        ClientRegionFactory<Integer, String> crf = cCache.createClientRegionFactory(
            isEmpty ? ClientRegionShortcut.PROXY : ClientRegionShortcut.CACHING_PROXY);
        Region<Integer, String> r = crf.create(D_REFERENCE);
        Region<Integer, String> customer = crf.create(CUSTOMER);
        Region<Integer, String> order = crf.create(ORDER);
        if (ri) {
          r.registerInterestRegex(".*");
          customer.registerInterestRegex(".*");
          order.registerInterestRegex(".*");
        }
        return null;
      }
    });
  }

  static class Customer implements Delta, DataSerializable {
    private int id;
    private String name;
    private boolean idChanged;
    private boolean nameChanged;
    private boolean fromDeltaCalled;
    private boolean toDeltaCalled;

    public Customer() {}

    public Customer(int id, String name) {
      this.id = id;
      this.name = name;
    }

    public void setId(int id) {
      idChanged = true;
      this.id = id;
    }

    public void setName(String name) {
      nameChanged = true;
      this.name = name;
    }

    public int getId() {
      return id;
    }

    public String getName() {
      return name;
    }

    @Override
    public void fromDelta(DataInput in) throws IOException, InvalidDeltaException {
      if (in.readBoolean()) {
        id = in.readInt();
      }
      if (in.readBoolean()) {
        name = in.readUTF();
      }
      fromDeltaCalled = true;
    }

    @Override
    public boolean hasDelta() {
      return idChanged || nameChanged;
    }

    @Override
    public void toDelta(DataOutput out) throws IOException {
      out.writeBoolean(idChanged);
      if (idChanged) {
        out.writeInt(id);
      }
      out.writeBoolean(nameChanged);
      if (nameChanged) {
        out.writeUTF(name);
      }
      toDeltaCalled = true;
    }

    @Override
    public String toString() {
      return " id:" + id + " name:" + name;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj instanceof Customer) {
        Customer other = (Customer) obj;
        return id == other.id && name.equals(other.name);
      }
      return false;
    }

    @Override
    public int hashCode() {
      return id + name.hashCode();
    }

    public boolean isFromDeltaCalled() {
      boolean retVal = fromDeltaCalled;
      fromDeltaCalled = false;
      return retVal;
    }

    public boolean isToDeltaCalled() {
      boolean retVal = toDeltaCalled;
      toDeltaCalled = false;
      return retVal;
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      out.writeInt(id);
      out.writeUTF(name);
      out.writeBoolean(idChanged);
      out.writeBoolean(nameChanged);
      out.writeBoolean(fromDeltaCalled);
      out.writeBoolean(toDeltaCalled);
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      id = in.readInt();
      name = in.readUTF();
      idChanged = in.readBoolean();
      nameChanged = in.readBoolean();
      fromDeltaCalled = in.readBoolean();
      toDeltaCalled = in.readBoolean();
    }
  }

  @Test
  public void testTxWithCloning() {
    Host host = Host.getHost(0);
    VM vm1 = host.getVM(0);
    VM vm2 = host.getVM(1);
    final String regionName = getUniqueName();

    SerializableCallable createRegion = new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        AttributesFactory af = new AttributesFactory();
        af.setDataPolicy(DataPolicy.REPLICATE);
        af.setScope(Scope.DISTRIBUTED_ACK);
        af.setCloningEnabled(true);
        getCache().createRegion(regionName, af.create());
        return null;
      }
    };

    vm1.invoke(createRegion);
    vm2.invoke(createRegion);
    final String key = "cust1";

    vm1.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        TXManagerImpl mgr = getGemfireCache().getTxManager();
        Region r = getCache().getRegion(regionName);
        Customer cust = new Customer(1, "cust1");
        r.put(key, cust);
        mgr.begin();
        cust.setName("");
        r.put(key, cust);
        return null;
      }
    });

    vm2.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Region<String, Customer> r = getCache().getRegion(regionName);
        Customer c = r.get(key);
        c.setName("cust1updated");
        r.put(key, c);
        return null;
      }
    });

    vm1.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        TXManagerImpl mgr = getGemfireCache().getTxManager();
        Region r = getCache().getRegion(regionName);
        assertNotNull(mgr.getTXState());
        try {
          mgr.commit();
          fail("expected CommitConflict not thrown");
        } catch (CommitConflictException e) {
          // expected
        }
        return null;
      }
    });
  }

  @Test
  public void testExceptionThrown() {
    Host host = Host.getHost(0);
    VM vm1 = host.getVM(0);
    VM vm2 = host.getVM(1);
    final String regionName = getUniqueName();

    SerializableCallable createRegion = new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        AttributesFactory af = new AttributesFactory();
        af.setDataPolicy(DataPolicy.REPLICATE);
        af.setScope(Scope.DISTRIBUTED_ACK);
        final RegionAttributes attr = af.create();
        getCache().createRegion(regionName, attr);
        return null;
      }
    };

    vm1.invoke(createRegion);
    vm2.invoke(createRegion);
    final String key = "cust1";

    vm1.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        TXManagerImpl mgr = getGemfireCache().getTxManager();
        Region r = getCache().getRegion(regionName);
        Customer cust = new Customer(1, "cust1");
        r.put(key, cust);
        mgr.begin();
        cust.setName("");
        try {
          r.put(key, cust);
          fail("exception not thrown");
        } catch (UnsupportedOperationInTransactionException expected) {
        }
        mgr.rollback();
        return null;
      }
    });
  }

  @Test
  public void testClientServerDelta() {
    Host host = Host.getHost(0);
    VM server = host.getVM(0);
    VM client = host.getVM(1);
    int port = createRegionOnServer(server, true, false);
    createClientRegion(client, port, false, false);

    server.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Region<CustId, Customer> pr = getCache().getRegion(CUSTOMER);
        CustId cust1 = new CustId(1);
        pr.put(cust1, new Customer(1, "name1"));
        Iterator<CustId> it = pr.keySet().iterator();
        while (it.hasNext()) {
          LogWriterUtils.getLogWriter().info("SWAP:iterator1:" + pr.get(it.next()));
        }
        Customer c = pr.get(cust1);
        assertNotNull(c);
        return null;
      }
    });

    client.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Region<CustId, Customer> pr = getCache().getRegion(CUSTOMER);
        TXManagerImpl mgr = getGemfireCache().getTxManager();
        CustId cust1 = new CustId(1);
        // pr.put(cust1, new Customer(1, "name1"));
        // pr.create(cust1, new Customer(1, "name1"));
        mgr.begin();
        Customer c = pr.get(cust1);
        c.setName("updatedName");
        LogWriterUtils.getLogWriter().info("SWAP:doingPut");
        pr.put(cust1, c);
        LogWriterUtils.getLogWriter().info("SWAP:getfromtx:" + pr.get(cust1));
        LogWriterUtils.getLogWriter().info("SWAP:doingCommit");
        assertEquals("updatedName", pr.get(cust1).getName());
        TXStateProxy tx = mgr.pauseTransaction();
        assertEquals("name1", pr.get(cust1).getName());
        mgr.unpauseTransaction(tx);
        mgr.commit();
        assertTrue(c.isToDeltaCalled());
        assertEquals(c, pr.get(cust1));
        return null;
      }
    });
    server.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Region<CustId, Customer> pr = getCache().getRegion(CUSTOMER);
        CustId cust1 = new CustId(1);
        Customer c = pr.get(cust1);
        assertTrue(c.isFromDeltaCalled());
        return null;
      }
    });
  }
}

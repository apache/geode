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
/**
 * 
 */
package com.gemstone.gemfire.internal.cache;

import com.gemstone.gemfire.Delta;
import com.gemstone.gemfire.InvalidDeltaException;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.CacheTransactionManager;
import com.gemstone.gemfire.cache.CommitConflictException;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.InterestPolicy;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.SubscriptionAttributes;
import com.gemstone.gemfire.cache.UnsupportedOperationInTransactionException;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.client.ClientRegionFactory;
import com.gemstone.gemfire.cache.client.ClientRegionShortcut;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.cache.execute.CustomerIDPartitionResolver;
import com.gemstone.gemfire.internal.cache.execute.data.CustId;
import com.gemstone.gemfire.internal.cache.execute.data.Order;
import com.gemstone.gemfire.internal.cache.execute.data.OrderId;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.VM;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;

/**
 * @author sbawaska
 *
 */
public class TransactionsWithDeltaDUnitTest extends CacheTestCase {

  private static final String D_REFERENCE = "ref";
  private static final String CUSTOMER = "Customer";
  private static final String ORDER = "Order";

  /**
   * @param name
   */
  public TransactionsWithDeltaDUnitTest(String name) {
    super(name);
  }

  private Integer createRegionOnServer(VM vm, final boolean startServer, final boolean accessor) {
    return (Integer)vm.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        createRegion(accessor, 0, null);
        if (startServer) {
          int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
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
    getCache().createRegion(D_REFERENCE,af.create());
    af = new AttributesFactory();
    af.setCloningEnabled(true);
    if (interestPolicy != null) {
      af.setSubscriptionAttributes(new SubscriptionAttributes(interestPolicy));
    }
    af.setPartitionAttributes(new PartitionAttributesFactory<CustId, Customer>()
        .setTotalNumBuckets(4).setLocalMaxMemory(accessor ? 0 : 1)
        .setPartitionResolver(new CustomerIDPartitionResolver("resolver1"))
        .setRedundantCopies(redundantCopies).create());
    getCache().createRegion(CUSTOMER, af.create());
    af.setPartitionAttributes(new PartitionAttributesFactory<OrderId, Order>()
        .setTotalNumBuckets(4).setLocalMaxMemory(accessor ? 0 : 1)
        .setPartitionResolver(new CustomerIDPartitionResolver("resolver2"))
        .setRedundantCopies(redundantCopies).setColocatedWith(CUSTOMER).create());
    getCache().createRegion(ORDER, af.create());
  }
  private void createClientRegion(VM vm, final int port, final boolean isEmpty, final boolean ri) {
    vm.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        ClientCacheFactory ccf = new ClientCacheFactory();
        ccf.addPoolServer("localhost"/*getServerHostName(Host.getHost(0))*/, port);
        ccf.setPoolSubscriptionEnabled(false);
        ccf.set("log-level", LogWriterUtils.getDUnitLogLevel());
        ClientCache cCache = getClientCache(ccf);
        ClientRegionFactory<Integer, String> crf = cCache
            .createClientRegionFactory(isEmpty ? ClientRegionShortcut.PROXY
                : ClientRegionShortcut.CACHING_PROXY);
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
  
  static class Customer implements Delta, Serializable {
    private int id;
    private String name;
    private boolean idChanged;
    private boolean nameChanged;
    private boolean fromDeltaCalled;
    private boolean toDeltaCalled;

    public Customer(int id, String name) {
      this.id = id;
      this.name = name;
    }
    public void setId(int id) {
      this.idChanged = true;
      this.id = id;
    }
    public void setName(String name) {
      this.nameChanged = true;
      this.name = name;
    }
    public int getId() {
      return id;
    }
    public String getName() {
      return name;
    }
    public void fromDelta(DataInput in) throws IOException,
        InvalidDeltaException {
      if (in.readBoolean()) {
        id = in.readInt();
      }
      if (in.readBoolean()) {
        name = in.readUTF();
      }
      fromDeltaCalled = true;
    }
    public boolean hasDelta() {
      return this.idChanged || this.nameChanged;
    }

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
      return " id:"+id+" name:"+name;
    }
    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj instanceof Customer) {
        Customer other = (Customer) obj;
        return this.id == other.id && this.name.equals(other.name);
      }
      return false;
    }
    @Override
    public int hashCode() {
      return this.id + this.name.hashCode();
    }
    public boolean isFromDeltaCalled() {
      boolean retVal = this.fromDeltaCalled;
      this.fromDeltaCalled = false;
      return retVal;
    }
    public boolean isToDeltaCalled() {
      boolean retVal = this.toDeltaCalled;
      this.toDeltaCalled = false;
      return retVal;
    }
  }
  
  public void testTxWithCloning() {
    AttributesFactory af = new AttributesFactory();
    af.setDataPolicy(DataPolicy.REPLICATE);
    af.setScope(Scope.DISTRIBUTED_ACK);
    af.setCloningEnabled(true);
    basicTest(af.create());
  }
  
  public void testExceptionThrown() {
    AttributesFactory af = new AttributesFactory();
    af.setDataPolicy(DataPolicy.REPLICATE);
    af.setScope(Scope.DISTRIBUTED_ACK);
    final RegionAttributes attr = af.create();
    Host host = Host.getHost(0);
    VM vm1 = host.getVM(0);
    VM vm2 = host.getVM(1);
    final String regionName = getUniqueName();
    
    SerializableCallable createRegion  = new SerializableCallable() {
      public Object call() throws Exception {
        getCache().createRegion(regionName, attr);
        return null;
      }
    };
    
    vm1.invoke(createRegion);
    vm2.invoke(createRegion);
    final String key = "cust1";
    
    vm1.invoke(new SerializableCallable() {
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
  
  private void basicTest(final RegionAttributes regionAttr) {
    Host host = Host.getHost(0);
    VM vm1 = host.getVM(0);
    VM vm2 = host.getVM(1);
    final String regionName = getUniqueName();
    
    SerializableCallable createRegion  = new SerializableCallable() {
      public Object call() throws Exception {
        getCache().createRegion(regionName, regionAttr);
        return null;
      }
    };
    
    vm1.invoke(createRegion);
    vm2.invoke(createRegion);
    final String key = "cust1";
    
    vm1.invoke(new SerializableCallable() {
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
      public Object call() throws Exception {
        Region<String, Customer> r = getCache().getRegion(regionName);
        Customer c = r.get(key);
        c.setName("cust1updated");
        r.put(key, c);
        return null;
      }
    });
    
    vm1.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        TXManagerImpl mgr = getGemfireCache().getTxManager();
        Region r = getCache().getRegion(regionName);
        assertNotNull(mgr.getTXState());
        try {
          mgr.commit();
          fail("expected CommitConflict not thrown");
        } catch (CommitConflictException e) {
          //expected
        }
        return null;
      }
    });
  }
  
  public void testClientServerDelta() {
    Host host = Host.getHost(0);
    VM server = host.getVM(0);
    VM client = host.getVM(1);
    int port = createRegionOnServer(server, true, false);
    createClientRegion(client, port, false, false);
    
    server.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region<CustId, Customer> pr = getCache().getRegion(CUSTOMER);
        CustId cust1 = new CustId(1);
        pr.put(cust1, new Customer(1, "name1"));
        Iterator<CustId> it = pr.keySet().iterator();
        while (it.hasNext()) {
          LogWriterUtils.getLogWriter().info("SWAP:iterator1:"+pr.get(it.next()));
        }
        Customer c = pr.get(cust1);
        assertNotNull(c);
        return null;
      }
    });
    
    client.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region<CustId, Customer> pr = getCache().getRegion(CUSTOMER);
        TXManagerImpl mgr = getGemfireCache().getTxManager();
        CustId cust1 = new CustId(1);
        //pr.put(cust1, new Customer(1, "name1"));
//        pr.create(cust1, new Customer(1, "name1"));
        mgr.begin();
        Customer c = pr.get(cust1);
        c.setName("updatedName");
        LogWriterUtils.getLogWriter().info("SWAP:doingPut");
        pr.put(cust1, c);
        LogWriterUtils.getLogWriter().info("SWAP:getfromtx:"+pr.get(cust1));
        LogWriterUtils.getLogWriter().info("SWAP:doingCommit");
        assertEquals("updatedName", pr.get(cust1).getName());
        TXStateProxy tx = mgr.internalSuspend();
        assertEquals("name1", pr.get(cust1).getName());
        mgr.resume(tx);
        mgr.commit();
        assertTrue(c.isToDeltaCalled());
        assertEquals(c, pr.get(cust1));
        return null;
      }
    });
    server.invoke(new SerializableCallable() {
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

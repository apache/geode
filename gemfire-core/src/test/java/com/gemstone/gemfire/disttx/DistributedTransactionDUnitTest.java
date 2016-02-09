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
package com.gemstone.gemfire.disttx;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheTransactionManager;
import com.gemstone.gemfire.cache.CommitConflictException;
import com.gemstone.gemfire.cache.CommitIncompleteException;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.DiskStore;
import com.gemstone.gemfire.cache.InterestPolicy;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.SubscriptionAttributes;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.ReplyException;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.cache.CacheServerImpl;
import com.gemstone.gemfire.internal.cache.BucketRegion;
import com.gemstone.gemfire.internal.cache.DistTXState;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.RegionEntry;
import com.gemstone.gemfire.internal.cache.TXManagerImpl;
import com.gemstone.gemfire.internal.cache.TXState;
import com.gemstone.gemfire.internal.cache.TXStateInterface;
import com.gemstone.gemfire.internal.cache.TXStateProxyImpl;
import com.gemstone.gemfire.internal.cache.execute.CustomerIDPartitionResolver;
import com.gemstone.gemfire.internal.cache.execute.data.CustId;
import com.gemstone.gemfire.internal.cache.execute.data.Customer;
import com.gemstone.gemfire.internal.cache.execute.data.Order;
import com.gemstone.gemfire.internal.cache.execute.data.OrderId;
import com.gemstone.gemfire.test.dunit.Assert;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.Invoke;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * Port of GemFireXD's corresponding test for distributed transactions
 * 
 * @author sjigyasu
 *
 */
@SuppressWarnings("deprecation")
public class DistributedTransactionDUnitTest extends CacheTestCase {
  final protected String CUSTOMER_PR = "customerPRRegion";
  final protected String ORDER_PR = "orderPRRegion";
  final protected String D_REFERENCE = "distrReference";
  final protected String PERSISTENT_CUSTOMER_PR = "persistentCustomerPRRegion";

  final protected String CUSTOMER_RR = "customerRRRegion";
  
  @Override
  public void setUp() throws Exception{
    super.setUp();
    Invoke.invokeInEveryVM(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        System.setProperty("gemfire.sync-commits", "true");
        return null;
      }
    });
    
//    this.invokeInEveryVM(new SerializableCallable() {
//      @Override
//      public Object call() throws Exception {
//        System.setProperty("gemfire.log-level", "fine");
//        return null;
//      }
//    });

    Invoke.invokeInEveryVM(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        //System.setProperty("gemfire.ALLOW_PERSISTENT_TRANSACTIONS", "true");
        TXManagerImpl.ALLOW_PERSISTENT_TRANSACTIONS = true;
        return null;
      }
    }); 
  }
  
  @Override
  protected final void preTearDownCacheTestCase() throws Exception {
    Invoke.invokeInEveryVM(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        System.setProperty("gemfire.sync-commits", "false");
        return null;
      }
    });
    Invoke.invokeInEveryVM(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        //System.setProperty("gemfire.ALLOW_PERSISTENT_TRANSACTIONS", "false");
        TXManagerImpl.ALLOW_PERSISTENT_TRANSACTIONS = false;
        return null;
      }
    }); 
  }
  
  public DistributedTransactionDUnitTest(String name) {
    super(name);
  }

  public Object execute(VM vm, SerializableCallable c) {
    return vm.invoke(c);
  }
  
  public void execute(VM[] vms, SerializableCallable c) {
    for (VM vm : vms) {
      execute(vm, c);
    }
  }
  
  public int startServer(VM vm) {
    return (Integer) vm.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
        CacheServer s = getCache().addCacheServer();
        s.setPort(port);
        ((CacheServerImpl) s).setTransactionTimeToLive(10);
        s.start();
        return port;
      }
    });
  }

  protected boolean getConcurrencyChecksEnabled() {
    return true;
  }

  void createRR(boolean isEmpty) {
    AttributesFactory af = new AttributesFactory();
    af.setScope(Scope.DISTRIBUTED_ACK);
    if (!isEmpty) {
      af.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    } else {
      af.setDataPolicy(DataPolicy.EMPTY); //for accessor
    }
    af.setConcurrencyChecksEnabled(getConcurrencyChecksEnabled());
    getCache().createRegion(CUSTOMER_RR, af.create());
  }
  
  void createPR(boolean accessor, int redundantCopies,
      InterestPolicy interestPolicy) {
    AttributesFactory af = new AttributesFactory();
    af.setConcurrencyChecksEnabled(getConcurrencyChecksEnabled());
    if (interestPolicy != null) {
      af.setSubscriptionAttributes(new SubscriptionAttributes(interestPolicy));
    }
    af.setPartitionAttributes(new PartitionAttributesFactory<CustId, Customer>()
        .setTotalNumBuckets(4).setLocalMaxMemory(accessor ? 0 : 1)
        .setPartitionResolver(new CustomerIDPartitionResolver("resolver1"))
        .setRedundantCopies(redundantCopies).create());
    getCache().createRegion(CUSTOMER_PR, af.create());
    
  }
  
  @Override
  public Properties getDistributedSystemProperties() {
    Properties props = super.getDistributedSystemProperties();
    //props.put("distributed-transactions", "true");
//    props.setProperty(DistributionConfig.LOG_LEVEL_NAME, "fine");
    return props;
  }

  
  void createRegions(boolean accessor, int redundantCopies,
      InterestPolicy interestPolicy) {
    AttributesFactory af = new AttributesFactory();
    af.setScope(Scope.DISTRIBUTED_ACK);
    af.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    //af.setConcurrencyChecksEnabled(getConcurrencyChecksEnabled());
    getCache().createRegion(D_REFERENCE, af.create());
    
    af = new AttributesFactory();
    //af.setConcurrencyChecksEnabled(getConcurrencyChecksEnabled());
    af.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
    if (interestPolicy != null) {
      af.setSubscriptionAttributes(new SubscriptionAttributes(interestPolicy));
    }
    af.setPartitionAttributes(new PartitionAttributesFactory<CustId, Customer>()
        .setTotalNumBuckets(4).setLocalMaxMemory(accessor ? 0 : 1)
        .setPartitionResolver(new CustomerIDPartitionResolver("resolver1"))
        .setRedundantCopies(redundantCopies).create());
    getCache().createRegion(CUSTOMER_PR, af.create());
    af.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
    af.setPartitionAttributes(new PartitionAttributesFactory<OrderId, Order>()
        .setTotalNumBuckets(4).setLocalMaxMemory(accessor ? 0 : 1)
        .setPartitionResolver(new CustomerIDPartitionResolver("resolver2"))
        .setRedundantCopies(redundantCopies).setColocatedWith(CUSTOMER_PR)
        .create());
    getCache().createRegion(ORDER_PR, af.create());
  }

  public void createRegions(VM[] vms) {
    for (VM vm : vms) {
      vm.invoke(new SerializableCallable() {
        @Override
        public Object call() throws Exception {
          createRegions(false, 1, null);
          return null;
        }
        
      });
    }
  }
  public void createRegions(final VM vm, final boolean accessor,
      final int redundantCopies) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        createRegions(accessor, redundantCopies, null);
        return null;
      }
    });
  }
  
  public void createRR(VM[] vms) {
    for (VM vm : vms) {
      vm.invoke(new SerializableCallable() {
        @Override
        public Object call() throws Exception {
          createRR(false);
          return null;
        }
      });
    }
  }
  
  public void createRRonAccessor(VM accessor) {
    accessor.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        createRR(true);
        return null;
      }
    });
  }

  public void createPR(VM[] vms) {
    for (VM vm : vms) {
      vm.invoke(new SerializableCallable() {
        @Override
        public Object call() throws Exception {
          createPR(false, 0, null);
          return null;
        }
      });
    }
  }
  
  public void createPRwithRedundanyCopies(VM[] vms, final int redundency) {
    for (VM vm : vms) {
      vm.invoke(new SerializableCallable() {
        @Override
        public Object call() throws Exception {
          createPR(false, redundency, null);
          return null;
        }
      });
    }
  }
  
  public void createPRonAccessor(VM accessor, final int redundency) {
    accessor.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        createPR(true, redundency, null);
        return null;
      }
    });
  }
  
  public void createPersistentPR(VM[] vms) {
    for (VM vm: vms) {
      vm.invoke(new SerializableCallable() {
        @Override
        public Object call() throws Exception {
          createPersistentPR();
          return null;
        }
      });
    }
  }
  
  public void createPersistentPR() {
    getCache().createRegion(PERSISTENT_CUSTOMER_PR, getPersistentPRAttributes(1, -1, getCache(), 113, true));
  }
  protected RegionAttributes getPersistentPRAttributes(final int redundancy, final int recoveryDelay,
      Cache cache, int numBuckets, boolean synchronous) {
        DiskStore ds = cache.findDiskStore("disk");
        if(ds == null) {
          ds = cache.createDiskStoreFactory()
          .setDiskDirs(getDiskDirs()).create("disk");
        }
        AttributesFactory af = new AttributesFactory();
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(redundancy);
        paf.setRecoveryDelay(recoveryDelay);
        paf.setTotalNumBuckets(numBuckets);
        paf.setLocalMaxMemory(500);
        af.setPartitionAttributes(paf.create());
        af.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
        af.setDiskStoreName("disk");
        af.setDiskSynchronous(synchronous);
        RegionAttributes attr = af.create();
        return attr;
      }

  
  void populateData() {
    Region custRegion = getCache().getRegion(CUSTOMER_PR);
    Region orderRegion = getCache().getRegion(ORDER_PR);
    Region refRegion = getCache().getRegion(D_REFERENCE);
    for (int i = 0; i < 5; i++) {
      CustId custId = new CustId(i);
      Customer customer = new Customer("customer" + i, "address" + i);
      OrderId orderId = new OrderId(i, custId);
      Order order = new Order("order" + i);
      custRegion.put(custId, customer);
      orderRegion.put(orderId, order);
      refRegion.put(custId, customer);
    }
  }

  void populateRR() {
    Region custRegion = getCache().getRegion(CUSTOMER_RR);
    for (int i = 0; i < 5; i++) {
      CustId custId = new CustId(i);
      Customer customer = new Customer("customer" + i, "address" + i);
      custRegion.put(custId, customer);
    }
  }

  public void populateData(final VM vm) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        populateData();
        return null;
      }
    });
  }

  public void populateRR(final VM vm) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        populateRR();
        return null;
      }
    });
  }
  
  

  
  /**
   * From GemFireXD: testTransactionalInsertOnReplicatedTable
   * 
   * @throws Exception
   */
  public void testTransactionalPutOnReplicatedRegion() throws Exception {
    Host host = Host.getHost(0);
    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);
    VM server3 = host.getVM(2);

    createRR(new VM[]{server1, server2, server3});
    execute(server1, new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        CacheTransactionManager mgr = getGemfireCache().getTxManager();
        mgr.setDistributed(true);
        mgr.begin();
        mgr.commit();
        mgr.begin();
        
        Region<CustId, Customer> custRegion = getCache().getRegion(CUSTOMER_RR);
        CustId custId = new CustId(1);
        Customer expectedCustomer = custRegion.get(custId);
        assertNull(expectedCustomer);
        
        // Perform a put
        CustId custIdOne = new CustId(1);
        Customer customerOne = new Customer("name1", "addr1");
        custRegion.put(custIdOne, customerOne);
        
        // Rollback the transaction
        mgr.rollback();
        

        mgr.begin();
        // Verify that the entry is rolled back
        expectedCustomer = custRegion.get(custId);
        assertNull(expectedCustomer);
        
        // Perform two more puts and a commit
        CustId custIdTwo = new CustId(2);
        Customer customerTwo = new Customer("name2", "addr2");
        CustId custIdThree = new CustId(3);
        Customer customerThree = new Customer("name3", "addr3");
        custRegion.put(custIdTwo, customerTwo);
        custRegion.put(custIdThree, customerThree);
        
        mgr.commit();
        mgr.begin();
        
        // Verify data
        assertEquals(2, custRegion.size());
        assertTrue(custRegion.containsKey(custIdTwo));
        assertTrue(custRegion.containsKey(custIdThree));
        assertEquals(customerTwo, custRegion.get(custIdTwo));
        assertEquals(customerThree, custRegion.get(custIdThree));
        
        // Perform one more put but don't commit
        custRegion.put(custIdOne, customerOne);
        
        // Verify data
        assertEquals(3, custRegion.size());
        assertTrue(custRegion.containsKey(custIdOne));
        assertEquals(customerOne, custRegion.get(custIdOne));

        mgr.commit();
        
        return null;
      }
      
    });
  }
  
  public void testTransactionalPutOnPartitionedRegion() throws Exception {
    Host host = Host.getHost(0);
    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);
    VM server3 = host.getVM(2);

    createPR(new VM[]{server1, server2, server3});
    execute(server1, new SerializableCallable() {

      @Override
      public Object call() throws Exception {
        CacheTransactionManager mgr = getGemfireCache().getTxManager();
        mgr.setDistributed(true);
        LogWriterI18n logger = getGemfireCache().getLoggerI18n();
        
        mgr.begin();
        logger.fine("TEST:Commit-1");
        mgr.commit();
       
        mgr.begin(); 
        Region<CustId, Customer> custRegion = getCache().getRegion(CUSTOMER_PR);
        CustId custId = new CustId(1);
        Customer expectedCustomer = custRegion.get(custId);
        assertNull(expectedCustomer);
        
        // Perform a put
        CustId custIdOne = new CustId(1);
        Customer customerOne = new Customer("name1", "addr1");
        logger.fine("TEST:Put-1");
        custRegion.put(custIdOne, customerOne);
        
        // Rollback the transaction
        logger.fine("TEST:Rollback-1");
        mgr.rollback();
        
        mgr.begin();
        // Verify that the entry is rolled back
        expectedCustomer = custRegion.get(custId);
        assertNull(expectedCustomer);
        
        // Perform two more puts and a commit
        CustId custIdTwo = new CustId(2);
        Customer customerTwo = new Customer("name2", "addr2");
        CustId custIdThree = new CustId(3);
        Customer customerThree = new Customer("name3", "addr3");
        logger.fine("TEST:Put-2");
        custRegion.put(custIdTwo, customerTwo);
        logger.fine("TEST:Put-3");
        custRegion.put(custIdThree, customerThree);
        
        logger.fine("TEST:Commit-2");
        mgr.commit();
        mgr.begin();
        
        // Verify data
        assertEquals(2, custRegion.size());
        assertTrue(custRegion.containsKey(custIdTwo));
        assertTrue(custRegion.containsKey(custIdThree));
        assertEquals(customerTwo, custRegion.get(custIdTwo));
        assertEquals(customerThree, custRegion.get(custIdThree));
        
        // Perform one more put but don't commit
        logger.fine("TEST:Put-4");
        Customer customerOneMod = new Customer("name1", "addr11");

        custRegion.put(custIdOne, customerOneMod);
        
        // Verify data
        assertEquals(3, custRegion.size());
        assertTrue(custRegion.containsKey(custIdOne));
        assertEquals(customerOneMod, custRegion.get(custIdOne));
        logger.fine("TEST:Commit-3");
        mgr.commit();
        
        return null;
      }
      
    });
  }

  @SuppressWarnings("serial")
  public void testCommitOnPartitionedAndReplicatedRegions() throws Exception {
    Host host = Host.getHost(0);
    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);
    VM server3 = host.getVM(2);
    createPR(new VM[]{server1, server2, server3});
    createRR(new VM[]{server1, server2, server3});
    execute(server1, new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        CacheTransactionManager mgr = getGemfireCache().getTxManager();
        mgr.setDistributed(true);
        mgr.begin();
        Region<CustId, Customer> rrRegion = getCache().getRegion(CUSTOMER_RR);
        Region<CustId, Customer> prRegion = getCache().getRegion(CUSTOMER_PR);

        CustId custIdOne = new CustId(1);
        Customer customerOne = new Customer("name1", "addr1");
        prRegion.put(custIdOne, customerOne);
        rrRegion.put(custIdOne, customerOne);

        CustId custIdTwo = new CustId(2);
        Customer customerTwo = new Customer("name2", "addr2");
        rrRegion.put(custIdTwo, customerTwo);

        mgr.commit();
        
        // Verify
        assertEquals(2, rrRegion.size());
        assertTrue(rrRegion.containsKey(custIdOne));
        assertTrue(rrRegion.containsKey(custIdTwo));
        assertEquals(customerOne, rrRegion.get(custIdOne));
        assertEquals(customerTwo, rrRegion.get(custIdTwo));
        
        assertEquals(1, prRegion.size());
        assertTrue(prRegion.containsKey(custIdOne));
        assertEquals(customerOne, rrRegion.get(custIdOne));
        
        return null;
      }
      
    });
  }
  
  public void testGetIsolated() throws Exception {
    Host host = Host.getHost(0);
    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);
    VM server3 = host.getVM(2);
    createPR(new VM[]{server1, server2, server3});
    createRR(new VM[]{server1, server2, server3});
    execute(server1, new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        CacheTransactionManager mgr = getGemfireCache().getTxManager();
        mgr.setDistributed(true);
        mgr.begin();
        mgr.commit();
        mgr.begin(); 
        Region<CustId, Customer> custPR = getCache().getRegion(CUSTOMER_PR);
        CustId custIdOne = new CustId(1);
        Customer customerOne = new Customer("name1", "addr1");
        custPR.put(custIdOne, customerOne);

        Region<CustId, Customer> replicatedRegion = getCache().getRegion(CUSTOMER_RR);
        replicatedRegion.put(custIdOne, customerOne);
        CustId custIdTwo = new CustId(2);
        Customer customerTwo = new Customer("name2", "addr2");
        replicatedRegion.put(custIdTwo, customerTwo);
        
        // Verify before commit
        assertEquals(2, replicatedRegion.size());
        assertEquals(customerOne, replicatedRegion.get(custIdOne));
        assertEquals(customerTwo, replicatedRegion.get(custIdTwo));
        
        // Perform commit
        mgr.commit();
        
        // Verify after commit
        assertEquals(2, replicatedRegion.size());
        assertEquals(customerOne, replicatedRegion.get(custIdOne));
        assertEquals(customerTwo, replicatedRegion.get(custIdTwo));
        
        return null;
      }
    });
  }
  
  public void testCommitAndRollback() throws Exception {
    Host host = Host.getHost(0);
    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);
    VM server3 = host.getVM(2);
    createPR(new VM[]{server1, server2, server3});
    execute(server1, new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        CacheTransactionManager mgr = getGemfireCache().getTxManager();
        mgr.setDistributed(true);
        //mgr.begin();
        Region<CustId, Customer> custPR = getCache().getRegion(CUSTOMER_PR);
        for (int i = 0; i < 1000; i++) {
          CustId custId = new CustId(i);
          Customer customer = new Customer("name"+i, "addr"+i);
          mgr.begin();
          custPR.put(custId, customer);
          if (i % 2 == 0) {
            mgr.commit();
          } else {
            mgr.rollback();
          }
        }
        // Verify number of puts
        assertEquals(500, custPR.size());
        Set<Region.Entry<?,?>> entries = custPR.entrySet(false);
        assertEquals(500, entries.size());
        
        return null;
      }
    });
  }
  
  /*
   * [sjigyasu] This adapation of test from GemFireXD allows the difference in 
   * the way GemFire and GemFireXD handle server groups.
   * We create 2 partitioned regions one on each server and have a third node
   * as accessor and fire transactional operations on it.
   */
  public void testNonColocatedPutByPartitioning() {
    Host host = Host.getHost(0);
    VM server1 = host.getVM(0); // datastore
    VM server2 = host.getVM(1); // datastore
    VM server3 = host.getVM(2); // accessor
    
    final String CUSTOMER_PR1 = "CUSTOMER_PR1";
    final String CUSTOMER_PR2 = "CUSTOMER_PR2";
    
    // Create CUSTOMER_PR1 on server1
    execute(server1, new SerializableCallable(){
      @Override
      public Object call() throws Exception {
        AttributesFactory af = new AttributesFactory();
        af.setConcurrencyChecksEnabled(getConcurrencyChecksEnabled());
        af.setPartitionAttributes(new PartitionAttributesFactory<CustId, Customer>()
            .setTotalNumBuckets(4).setLocalMaxMemory(1)
            .setPartitionResolver(new CustomerIDPartitionResolver("resolver1"))
            .setRedundantCopies(0).create());
        getCache().createRegion(CUSTOMER_PR1, af.create());
        return null;
      }
    });
    
    // Create CUSTOMER_PR2 on server2
    execute(server2, new SerializableCallable(){
      @Override
      public Object call() throws Exception {
        AttributesFactory af = new AttributesFactory();
        af.setConcurrencyChecksEnabled(getConcurrencyChecksEnabled());
        af.setPartitionAttributes(new PartitionAttributesFactory<CustId, Customer>()
            .setTotalNumBuckets(4).setLocalMaxMemory(1)
            .setPartitionResolver(new CustomerIDPartitionResolver("resolver2"))
            .setRedundantCopies(0).create());
        getCache().createRegion(CUSTOMER_PR2, af.create());
        return null;
      }
    });
    
    // Create both the regions on server3 (accessor)
    execute(server3, new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        AttributesFactory af = new AttributesFactory();
        af.setConcurrencyChecksEnabled(getConcurrencyChecksEnabled());
        af.setPartitionAttributes(new PartitionAttributesFactory<CustId, Customer>()
            .setTotalNumBuckets(4).setLocalMaxMemory(0) // since this is an accessor
            .setPartitionResolver(new CustomerIDPartitionResolver("resolver1"))
            .setRedundantCopies(0).create());
        getCache().createRegion(CUSTOMER_PR1, af.create());
        
        return null;
      }
    });
    execute(server3, new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        AttributesFactory af = new AttributesFactory();
        af.setConcurrencyChecksEnabled(getConcurrencyChecksEnabled());
        af.setPartitionAttributes(new PartitionAttributesFactory<CustId, Customer>()
            .setTotalNumBuckets(4).setLocalMaxMemory(0) // since this is an accessor
            .setPartitionResolver(new CustomerIDPartitionResolver("resolver2"))
            .setRedundantCopies(0).create());
        getCache().createRegion(CUSTOMER_PR2, af.create());
        return null;
      }
    });

    // Now perform tx ops on accessor
    execute(server3, new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        CacheTransactionManager mgr = getGemfireCache().getTxManager();
        mgr.setDistributed(true);
        mgr.begin();
        
        Region<CustId, Customer> custPR1 = getCache().getRegion(CUSTOMER_PR1);
        CustId custIdOne = new CustId(1);
        Customer customerOne = new Customer("name1", "addr1");
        custPR1.put(custIdOne, customerOne);
        
        Region<CustId, Customer> custPR2 = getCache().getRegion(CUSTOMER_PR2);
        custPR2.put(custIdOne, customerOne);
        
        mgr.commit();
        
        // Verify
        assertEquals(1, custPR1.size());
        assertEquals(1, custPR2.size());
        return null;
      }
    });
    
    // Verify on one of the servers
    execute(server1, new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Region<CustId, Customer> custPR1 = getCache().getRegion(CUSTOMER_PR1);
        assertEquals(1, custPR1.size());
        CustId custIdOne = new CustId(1);
        Customer customerOne = new Customer("name1", "addr1");
        assertEquals(customerOne, custPR1.get(custIdOne));
        return null;
      }
    });
  }
  
  public void testTransactionalKeyBasedUpdates() throws Exception {
    Host host = Host.getHost(0);
    VM server1 = host.getVM(0); 
    VM server2 = host.getVM(1); 
    VM server3 = host.getVM(2);
    createPR(new VM[]{server1, server2, server3});
    
    execute(server1, new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        CacheTransactionManager mgr = getGemfireCache().getTxManager();
        mgr.setDistributed(true);
        //mgr.begin();
        LogWriterI18n logger = getGemfireCache().getLoggerI18n();
        
        Region<CustId, Customer> custPR = getCache().getRegion(CUSTOMER_PR);
        for (int i = 1; i <= 2; i++) {
          mgr.begin();
          logger.fine("TEST:PUT-"+i);
          custPR.put(new CustId(i), new Customer("name"+i, "addr"+i));
          logger.fine("TEST:COMMIT-"+i);
          mgr.commit();
        }
        
        // Updates
        for (int i = 1; i <= 2; i++) {
          CustId custId = new CustId(i);
          Customer customer = custPR.get(custId);
          assertNotNull(customer);
          mgr.begin();
          logger.fine("TEST:UPDATE-"+i);
          custPR.put(custId, new Customer("name"+i*2, "addr"+i*2));
          logger.fine("TEST:UPDATED-"+i + "=" + custId + "," + custPR.get(custId));
          logger.fine("TEST:UPDATE COMMIT-"+i);
          mgr.commit();
          logger.fine("TEST:POSTCOMMIT-"+i + "=" + custId + "," + custPR.get(custId));
        }
        // Verify
        for (int i = 1; i <=2; i++) {
          CustId custId = new CustId(i);
          Customer customer = custPR.get(custId);
          assertNotNull(customer);
          logger.fine("TEST:VERIFYING-"+i);
          assertEquals(new Customer("name"+i*2, "addr"+i*2), customer);
        }
        
        return null;
      }
    });
  }

  public void testTransactionalKeyBasedDestroys_PR() throws Exception {
    Host host = Host.getHost(0);
    VM server1 = host.getVM(0); 
    VM server2 = host.getVM(1); 
    VM server3 = host.getVM(2); 
    createPR(new VM[]{server1, server2, server3});
    
    execute(server1, new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        CacheTransactionManager mgr = getGemfireCache().getTxManager();
        mgr.setDistributed(true);
        //mgr.begin();
        
        Region<CustId, Customer> custPR = getCache().getRegion(CUSTOMER_PR);
        for (int i = 1; i <= 1000; i++) {
          mgr.begin();
          custPR.put(new CustId(i), new Customer("name"+i, "addr"+i));
          mgr.commit();
        }
        
        // Destroys
        for (int i = 1; i <= 100; i++) {
          CustId custId = new CustId(i);
          mgr.begin();
          Object customerRemoved = custPR.remove(custId);
          // Removing this assertion since in case of distributed destroys the 
          // value will not be returned.
          //assertNotNull(customerRemoved);
          mgr.commit();
        }
        
        // Verify
        for (int i = 1; i <=100; i++) {
          CustId custId = new CustId(1);
          Customer customer = custPR.get(custId);
          assertNull(customer);
        }
        
        assertEquals(900, custPR.size());
        return null;
      }
    });
  }

  public void testTransactionalKeyBasedDestroys_RR() throws Exception {
    Host host = Host.getHost(0);
    VM server1 = host.getVM(0); 
    VM server2 = host.getVM(1); 
    VM server3 = host.getVM(2); 
    createRR(new VM[]{server1, server2, server3});
    
    execute(server1, new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        CacheTransactionManager mgr = getGemfireCache().getTxManager();
        mgr.setDistributed(true);
        //mgr.begin();
        
        Region<CustId, Customer> custRR = getCache().getRegion(CUSTOMER_RR);
        for (int i = 1; i <= 1000; i++) {
          mgr.begin();
          custRR.put(new CustId(i), new Customer("name"+i, "addr"+i));
          mgr.commit();
        }
        
        // Destroys
        for (int i = 1; i <= 100; i++) {
          CustId custId = new CustId(i);
          mgr.begin();
          Object customerRemoved = custRR.remove(custId);
          assertNotNull(customerRemoved);
          mgr.commit();
        }
        
        // Verify
        for (int i = 1; i <=100; i++) {
          CustId custId = new CustId(1);
          Customer customer = custRR.get(custId);
          assertNull(customer);
        }
        
        assertEquals(900, custRR.size());
        return null;
      }
    });
  }
  
  public void testTransactionalUpdates() throws Exception {
    Host host = Host.getHost(0);
    VM server1 = host.getVM(0); 
    VM server2 = host.getVM(1); 
    VM server3 = host.getVM(2); 

    createPR(new VM[]{server2, server3});
    execute(server1, new SerializableCallable(){
      @Override
      public Object call() throws Exception {
        CacheTransactionManager mgr = getGemfireCache().getTxManager();
        mgr.setDistributed(true);
        mgr.begin();
        createPR(false, 0, null);
        
        Region<CustId, Customer> custPR = getCache().getRegion(CUSTOMER_PR);
        for (int i = 1; i <= 200; i++) {
          custPR.put(new CustId(i), new Customer("name"+i, "addr"+i));
        }
        
        assertEquals(200, custPR.size());
        
        mgr.rollback();
        //mgr.commit();
        mgr.begin();
        assertEquals(0, custPR.size());
        
        mgr.commit();
        mgr.begin();

        for (int i = 1; i <= 200; i++) {
          custPR.put(new CustId(i), new Customer("name"+i, "addr"+i));
        }

        mgr.commit();
        //mgr.begin();
        for (int i = 1; i <= 200; i++) {
          mgr.begin();
          custPR.put(new CustId(i), new Customer("name"+i*2, "addr"+i*2));
          mgr.commit();
        }
        mgr.begin();
        mgr.rollback();
        
        assertEquals(200, custPR.size());
        
        for (int i = 1; i <= 200; i++) {
          assertEquals(new Customer("name"+i*2, "addr"+i*2), custPR.get(new CustId(i)));
        }
        
        return null;
      }
      
    });
  }

  public void testPutAllWithTransactions() throws Exception {
    Host host = Host.getHost(0);
    VM server1 = host.getVM(0); 
    VM server2 = host.getVM(1); 
    VM server3 = host.getVM(2);
    
    createRegions(new VM[]{server1, server2, server3});

    execute(new VM[]{server1, server2, server3}, new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        AttributesFactory af = new AttributesFactory();
        af.setConcurrencyChecksEnabled(getConcurrencyChecksEnabled());
        af.setPartitionAttributes(new PartitionAttributesFactory<CustId, Customer>()
            .setTotalNumBuckets(4).setLocalMaxMemory(1)
            .setPartitionResolver(new CustomerIDPartitionResolver("resolver1"))
            .setRedundantCopies(0).create());
        getCache().createRegion("NONCOLOCATED_PR", af.create());
        return null;
      }
    });
    
    execute(server1, new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Region custRegion = getCache().getRegion(CUSTOMER_PR);
        Region orderRegion = getCache().getRegion(ORDER_PR);
        
        Map custMap = new HashMap();
        Map orderMap = new HashMap();
        for (int i = 0; i < 5; i++) {
          CustId custId = new CustId(i);
          Customer customer = new Customer("customer" + i, "address" + i);
          OrderId orderId = new OrderId(i, custId);
          Order order = new Order("order" + i);
          custMap.put(custId, customer);
          orderMap.put(orderId, order);
        }

        CacheTransactionManager mgr = getGemfireCache().getTxManager();
        mgr.setDistributed(true);
        mgr.begin();
        custRegion.putAll(custMap);
        orderRegion.putAll(orderMap);
        mgr.commit();
        
        mgr.begin(); 
        assertEquals(5, custRegion.size());
        assertEquals(5, orderRegion.size());

        custMap = new HashMap();
        orderMap = new HashMap();
        for (int i = 5; i < 10; i++) {
          CustId custId = new CustId(i);
          Customer customer = new Customer("customer" + i, "address" + i);
          OrderId orderId = new OrderId(i, custId);
          Order order = new Order("order" + i);
          custMap.put(custId, customer);
          orderMap.put(orderId, order);
        }
        custRegion.putAll(custMap);
        orderRegion.putAll(orderMap);
        mgr.rollback();

        mgr.begin();        
        assertEquals(5, custRegion.size());
        assertEquals(5, orderRegion.size());

        custRegion.putAll(custMap);
        orderRegion.putAll(orderMap);

        assertEquals(10, custRegion.size());
        assertEquals(10, orderRegion.size());

        // Verify operations involving non colocated PR
        Map map = new HashMap();
        custMap.clear();
        for (int i = 10; i < 15; i++) {
          CustId custId = new CustId(i);
          Customer customer = new Customer("customer" + i, "address" + i);
          OrderId orderId = new OrderId(i, custId);
          Order order = new Order("order" + i);
          custMap.put(custId, customer);
          map.put(custId, customer);
        }
        custRegion.putAll(custMap);
        Region nonColocatedRegion = getCache().getRegion("NONCOLOCATED_PR");
        nonColocatedRegion.putAll(orderMap);


        mgr.commit();
        mgr.begin();
        assertEquals(15, custRegion.size());
        assertEquals(5, nonColocatedRegion.size());
        
        custMap.clear();
        map.clear();
        for (int i = 15; i < 20; i++) {
          CustId custId = new CustId(i);
          Customer customer = new Customer("customer" + i, "address" + i);
          OrderId orderId = new OrderId(i, custId);
          Order order = new Order("order" + i);
          custMap.put(custId, customer);
          map.put(custId, customer);
        }
        custRegion.putAll(custMap);
        nonColocatedRegion.putAll(orderMap);
         
        mgr.rollback();
        assertEquals(15, custRegion.size());
        assertEquals(5, nonColocatedRegion.size());
        
        return null;
      }
    });
  }
  
  public void testRemoveAllWithTransactions() throws Exception {
    Host host = Host.getHost(0);
    VM server1 = host.getVM(0); 
    VM server2 = host.getVM(1); 
    VM server3 = host.getVM(2);
    
    createRegions(new VM[]{server1, server2, server3});
    
    execute(server1, new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Region custRegion = getCache().getRegion(CUSTOMER_PR);
        Region orderRegion = getCache().getRegion(ORDER_PR);
        
        Map custMap = new HashMap();
        Map orderMap = new HashMap();
        for (int i = 0; i < 15; i++) {
          CustId custId = new CustId(i);
          Customer customer = new Customer("customer" + i, "address" + i);
          OrderId orderId = new OrderId(i, custId);
          Order order = new Order("order" + i);
          custMap.put(custId, customer);
          orderMap.put(orderId, order);
        }
  
        CacheTransactionManager mgr = getGemfireCache().getTxManager();
        mgr.setDistributed(true);
        mgr.begin();
        custRegion.putAll(custMap);
        orderRegion.putAll(orderMap);
        mgr.commit();
        
        mgr.begin(); 
        assertEquals(15, custRegion.size());
        assertEquals(15, orderRegion.size());
  
        custMap = new HashMap();
        orderMap = new HashMap();
        for (int i = 5; i < 10; i++) {
          CustId custId = new CustId(i);
          Customer customer = new Customer("customer" + i, "address" + i);
          OrderId orderId = new OrderId(i, custId);
          Order order = new Order("order" + i);
          custMap.put(custId, customer);
          orderMap.put(orderId, order);
        }
        custRegion.removeAll(custMap.keySet());
        orderRegion.removeAll(orderMap.keySet());
        mgr.rollback();
  
        mgr.begin();        
        assertEquals(15, custRegion.size());
        assertEquals(15, orderRegion.size());
  
        custRegion.removeAll(custMap.keySet());
        orderRegion.removeAll(orderMap.keySet());
  
        assertEquals(10, custRegion.size());
        assertEquals(10, orderRegion.size());
        mgr.commit();
        
        assertEquals(10, custRegion.size());
        assertEquals(10, orderRegion.size());
        
        return null;
      }
    });
  }

  public void testTxWithSingleDataStore() throws Exception {
    Host host = Host.getHost(0);
    VM server1 = host.getVM(0); // datastore
    
    final String CUSTOMER_PR1 = "CUSTOMER_PR1";
    final String CUSTOMER_PR2 = "CUSTOMER_PR2";
    
    // Create CUSTOMER_PR1 on server1
    execute(server1, new SerializableCallable(){
      @Override
      public Object call() throws Exception {
        AttributesFactory af = new AttributesFactory();
        af.setConcurrencyChecksEnabled(getConcurrencyChecksEnabled());
        af.setPartitionAttributes(new PartitionAttributesFactory<CustId, Customer>()
            .setTotalNumBuckets(4).setLocalMaxMemory(1)
            .setPartitionResolver(new CustomerIDPartitionResolver("resolver1"))
            .setRedundantCopies(0).create());
        getCache().createRegion(CUSTOMER_PR1, af.create());
        return null;
      }
    });
    
    // Create CUSTOMER_PR2 on server2
    execute(server1, new SerializableCallable(){
      @Override
      public Object call() throws Exception {
        AttributesFactory af = new AttributesFactory();
        af.setConcurrencyChecksEnabled(getConcurrencyChecksEnabled());
        af.setPartitionAttributes(new PartitionAttributesFactory<CustId, Customer>()
            .setTotalNumBuckets(4).setLocalMaxMemory(1)
            .setPartitionResolver(new CustomerIDPartitionResolver("resolver2"))
            .setRedundantCopies(0).create());
        getCache().createRegion(CUSTOMER_PR2, af.create());
        return null;
      }
    });
    
    // Now perform tx ops on accessor
    execute(server1, new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        CacheTransactionManager mgr = getGemfireCache().getTxManager();
        mgr.setDistributed(true);
        mgr.begin();
        
        Region<CustId, Customer> custPR1 = getCache().getRegion(CUSTOMER_PR1);
        CustId custIdOne = new CustId(1);
        Customer customerOne = new Customer("name1", "addr1");
        custPR1.put(custIdOne, customerOne);
        
        Region<CustId, Customer> custPR2 = getCache().getRegion(CUSTOMER_PR2);
        custPR2.put(custIdOne, customerOne);
        
        mgr.commit();
        
        // Verify
        assertEquals(1, custPR1.size());
        assertEquals(1, custPR2.size());
        return null;
      }
    });
    
    execute(server1, new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Region<CustId, Customer> custPR1 = getCache().getRegion(CUSTOMER_PR1);
        assertEquals(1, custPR1.size());
        CustId custIdOne = new CustId(1);
        Customer customerOne = new Customer("name1", "addr1");
        assertEquals(customerOne, custPR1.get(custIdOne));
        return null;
      }
    });
  }
  
  public void testMultipleOpsOnSameKeyInTx() throws Exception {
    Host host = Host.getHost(0);
    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);
    VM server3 = host.getVM(2);

    createPR(new VM[]{server1, server2, server3});
    execute(server1, new SerializableCallable() {

      @Override
      public Object call() throws Exception {
        CacheTransactionManager mgr = getGemfireCache().getTxManager();
        mgr.setDistributed(true);
        mgr.begin();
        mgr.commit();
       
        mgr.begin(); 
        Region<CustId, Customer> custRegion = getCache().getRegion(CUSTOMER_PR);
        CustId custId = new CustId(1);
        Customer expectedCustomer = custRegion.get(custId);
        assertNull(expectedCustomer);
        
        // Perform a put
        CustId custIdOne = new CustId(1);
        Customer customerOne = new Customer("name1", "addr1");
        custRegion.put(custIdOne, customerOne);
        
        // Rollback the transaction
        mgr.rollback();
        
        mgr.begin();
        // Verify that the entry is rolled back
        expectedCustomer = custRegion.get(custId);
        assertNull(expectedCustomer);
        
        // Add more data
        CustId custIdTwo = new CustId(2);
        Customer customerTwo = new Customer("name2", "addr2");
        CustId custIdThree = new CustId(3);
        Customer customerThree = new Customer("name3", "addr3");
        custRegion.put(custIdTwo, customerTwo);
        custRegion.put(custIdThree, customerThree);
        mgr.commit();

        mgr.begin();
        // Verify data
        assertEquals(2, custRegion.size());
        assertTrue(custRegion.containsKey(custIdTwo));
        assertTrue(custRegion.containsKey(custIdThree));
        assertEquals(customerTwo, custRegion.get(custIdTwo));
        assertEquals(customerThree, custRegion.get(custIdThree));
        
        // Update the values for the same keys multiple times
        custRegion.put(custIdOne, new Customer("name1_mod1", "addr1_mod1"));
        custRegion.put(custIdTwo,  new Customer("name2_mod1", "addr2_mod1"));
        custRegion.put(custIdOne, new Customer("name1_mod2", "addr1_mod2"));
        custRegion.put(custIdOne, new Customer("name1_mod3", "addr1_mod3"));
        custRegion.put(custIdTwo,  new Customer("name2_mod2", "addr2_mod2"));

        assertEquals(3, custRegion.size());
        mgr.commit();
        
        assertEquals(3, custRegion.size());
        Customer c = custRegion.get(custIdOne);
        return null;
      }
    });
    
  }
  

  /*
   * Test to reproduce a scenario where:
   * 1. On primary, the tx op is applied first followed by non-tx
   * 2. On secondary, non-tx op is applied first followed by tx.
   */
  public void DISABLED_testConcurrentTXAndNonTXOperations() throws Exception {
    Host host = Host.getHost(0);
    final VM server1 = host.getVM(0);
    final VM server2 = host.getVM(1);
    
    createPersistentPR(new VM[]{server1});

    execute(server1, new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Region<CustId, Customer> prRegion = getCache().getRegion(
            PERSISTENT_CUSTOMER_PR);

        CustId custIdOne = new CustId(1);
        Customer customerOne = new Customer("name1", "addr1");
        prRegion.put(custIdOne, customerOne);
        
        BucketRegion br = ((PartitionedRegion) prRegion)
            .getBucketRegion(custIdOne);
        
        String primaryMember = br.getBucketAdvisor().getPrimary().toString();
        getGemfireCache().getLoggerI18n().fine("TEST:PRIMARY:" + primaryMember);
        
        String memberId = getGemfireCache().getDistributedSystem().getMemberId();
        getGemfireCache().getLoggerI18n().fine("TEST:MEMBERID:"+memberId);
        
        return null;
      }
    });    
    
    createPersistentPR(new VM[]{server2});
    
    Boolean isPrimary = (Boolean)execute(server1, new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Region<CustId, Customer> prRegion = getCache().getRegion(
            PERSISTENT_CUSTOMER_PR);
        CustId custIdOne = new CustId(1);
        BucketRegion br = ((PartitionedRegion) prRegion)
            .getBucketRegion(custIdOne);
        
        String primaryMember = br.getBucketAdvisor().getPrimary().toString();
        getGemfireCache().getLoggerI18n().fine("TEST:PRIMARY:" + primaryMember);
        
        String memberId = getGemfireCache().getDistributedSystem().getMemberId();
        getGemfireCache().getLoggerI18n().fine("TEST:MEMBERID:"+memberId);

        return memberId.equals(primaryMember);
      }
    });
    
    final VM primary = isPrimary.booleanValue() ? server1 : server2;
    final VM secondary = !isPrimary.booleanValue() ? server1 : server2;
    
    System.out.println("TEST:SERVER-1:VM-"+server1.getPid());
    System.out.println("TEST:SERVER-2:VM-"+server2.getPid());
    System.out.println("TEST:PRIMARY=VM-"+primary.getPid());
    System.out.println("TEST:SECONDARY=VM-"+secondary.getPid());
    
    class WaitRelease implements Runnable {
      CountDownLatch cdl;
      String op;
      public WaitRelease(CountDownLatch cdl, String member) {
        this.cdl = cdl;
      }
      @Override
      public void run() {
        try {
          GemFireCacheImpl.getExisting().getLoggerI18n().fine("TEST:TX WAITING - " + op);
          cdl.await();  
          GemFireCacheImpl.getExisting().getLoggerI18n().fine("TEST:TX END WAITING");
        } catch (InterruptedException e) {
        }
      }
      public void release() {
        GemFireCacheImpl.getExisting().getLoggerI18n().fine("TEST:TX COUNTDOWN - " + op);
        cdl.countDown();
      }
    }

    // Install TX hook
    SerializableCallable txHook = new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        CountDownLatch cdl = new CountDownLatch(1);
        GemFireCacheImpl.internalBeforeApplyChanges = new WaitRelease(cdl, "TX OP");
        return null;
      }
    };
    
    execute(secondary, txHook);


    // Install non-TX hook
    SerializableCallable nontxHook = new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        CountDownLatch cdl = new CountDownLatch(1);
        GemFireCacheImpl.internalBeforeNonTXBasicPut = new WaitRelease(cdl, "NON TX OP");
        return null;
      }
    };
    
    // Install the wait-release hook on the secondary
    execute(secondary, nontxHook);

    
    // Start a tx operation on primary
        
    execute(primary, new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        // The reason this is run in a separate thread instead of controller thread
        // is that this is going to block because the secondary is going to wait.
        new Thread() {
          public void run() {
            CacheTransactionManager mgr = getGemfireCache().getTxManager();
            mgr.setDistributed(true);
            getGemfireCache().getLoggerI18n().fine(
                "TEST:DISTTX=" + mgr.isDistributed());
            mgr.begin();
            Region<CustId, Customer> prRegion = getCache().getRegion(
                PERSISTENT_CUSTOMER_PR);

            CustId custIdOne = new CustId(1);
            Customer customerOne = new Customer("name1_tx", "addr1");
            getGemfireCache().getLoggerI18n().fine("TEST:TX UPDATE");
            prRegion.put(custIdOne, customerOne);
            getGemfireCache().getLoggerI18n().fine("TEST:TX COMMIT");
            mgr.commit();
          }
        }.start();
        return null;
      }
    });    
    
    // Let the TX op be applied on primary first
    Thread.currentThread().sleep(200);
    
    // Perform a non-tx op on the same key on primary
    execute(primary, new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Region<CustId, Customer> prRegion = getCache().getRegion(PERSISTENT_CUSTOMER_PR);

        CustId custIdOne = new CustId(1);
        Customer customerOne = new Customer("name1_nontx", "addr1");
        getGemfireCache().getLoggerI18n().fine("TEST:TX NONTXUPDATE");
        prRegion.put(custIdOne, customerOne);
        return null;
      }
    });
   
    
    // Wait for a few milliseconds
    Thread.currentThread().sleep(200);
    
    // Release the waiting non-tx op first, on secondary 
    execute(secondary, new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Runnable r = GemFireCacheImpl.internalBeforeNonTXBasicPut;
        assert(r != null && r instanceof WaitRelease);
        WaitRelease e = (WaitRelease)r;
        e.release();
        return null;
      }
    });
    
    // Now release the waiting commit on secondary
    execute(secondary, new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Runnable r = GemFireCacheImpl.internalBeforeApplyChanges;
        assert(r != null && r instanceof WaitRelease);
        WaitRelease e = (WaitRelease)r;
        e.release();
        return null;
      }
    });
    
    // Verify region and entry versions on primary and secondary
    SerializableCallable verifyPrimary = new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Region<CustId, Customer> prRegion = getCache().getRegion(PERSISTENT_CUSTOMER_PR);
        
        CustId custId = new CustId(1);
        Customer customer = prRegion.get(custId);
        
        BucketRegion br = ((PartitionedRegion)prRegion).getBucketRegion(custId);
        RegionEntry re = br.getRegionEntry(custId);
        
        getGemfireCache().getLoggerI18n().fine("TEST:TX PRIMARY CUSTOMER="+customer);
        
        getGemfireCache().getLoggerI18n().fine("TEST:TX PRIMARY REGION VERSION="+re.getVersionStamp().getRegionVersion());
        getGemfireCache().getLoggerI18n().fine("TEST:TX PRIMARY ENTRY VERSION="+re.getVersionStamp().getEntryVersion());
        return null;
      }
    };
    execute(primary, verifyPrimary);
    SerializableCallable verifySecondary = new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Region<CustId, Customer> prRegion = getCache().getRegion(PERSISTENT_CUSTOMER_PR);
        
        CustId custId = new CustId(1);
        Customer customer = prRegion.get(custId);
        
        BucketRegion br = ((PartitionedRegion)prRegion).getBucketRegion(custId);
        RegionEntry re = br.getRegionEntry(custId);
        
        getGemfireCache().getLoggerI18n().fine("TEST:TX SECONDARY CUSTOMER="+customer);
        
        getGemfireCache().getLoggerI18n().fine("TEST:TX SECONDARY REGION VERSION="+re.getVersionStamp().getRegionVersion());
        getGemfireCache().getLoggerI18n().fine("TEST:TX SECONDARY ENTRY VERSION="+re.getVersionStamp().getEntryVersion());
        return null;
      }
    };
    
    execute(secondary, verifySecondary);
  }
  
  public void testBasicDistributedTX() throws Exception {
    Host host = Host.getHost(0);
    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);
    
    createPersistentPR(new VM[]{server1, server2});
    execute(server2, new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        
        CacheTransactionManager mgr = getGemfireCache().getTxManager();
        mgr.setDistributed(true);
        getGemfireCache().getLoggerI18n().fine("TEST:DISTTX=" + mgr.isDistributed());
        getGemfireCache().getLoggerI18n().fine("TEST:TX BEGIN");
        mgr.begin();
        Region<CustId, Customer> prRegion = getCache().getRegion(PERSISTENT_CUSTOMER_PR);

        CustId custIdOne = new CustId(1);
        Customer customerOne = new Customer("name1", "addr1");
        getGemfireCache().getLoggerI18n().fine("TEST:TX PUT 1");
        prRegion.put(custIdOne, customerOne);

        CustId custIdTwo = new CustId(2);
        Customer customerTwo = new Customer("name2", "addr2");
        getGemfireCache().getLoggerI18n().fine("TEST:TX PUT 2");
        prRegion.put(custIdTwo, customerTwo);

        getGemfireCache().getLoggerI18n().fine("TEST:TX COMMIT");
        mgr.commit();
        return null;
        }
    });

  }

  public void testRegionAndEntryVersionsPR() throws Exception {
    Host host = Host.getHost(0);
    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);
    
    createPersistentPR(new VM[]{server1, server2});
    execute(server2, new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        CacheTransactionManager mgr = getGemfireCache().getTxManager();
        mgr.setDistributed(true);
        getGemfireCache().getLoggerI18n().fine(
            "TEST:DISTTX=" + mgr.isDistributed());
        getGemfireCache().getLoggerI18n().fine("TEST:TX BEGIN");
        mgr.begin();

        Region<CustId, Customer> prRegion = getCache().getRegion(
            PERSISTENT_CUSTOMER_PR);

        CustId custIdOne = new CustId(1);
        Customer customerOne = new Customer("name1", "addr1");
        getGemfireCache().getLoggerI18n().fine("TEST:TX PUT 1");
        prRegion.put(custIdOne, customerOne);

        BucketRegion br = ((PartitionedRegion) prRegion)
            .getBucketRegion(custIdOne);

        assertEquals(0L, br.getVersionVector().getCurrentVersion());
        getGemfireCache().getLoggerI18n().fine("TEST:TX COMMIT 1");
        mgr.commit();

        // Verify region version on the region
        assertEquals(1L, br.getVersionVector().getCurrentVersion());

        RegionEntry re = br.getRegionEntry(custIdOne);
        getGemfireCache().getLoggerI18n().fine(
            "TEST:VERSION-STAMP:" + re.getVersionStamp());

        // Verify region version on the region entry
        assertEquals(1L, re.getVersionStamp().getRegionVersion());

        // Verify entry version
        assertEquals(1, re.getVersionStamp().getEntryVersion());

        mgr.begin();
        prRegion.put(custIdOne, new Customer("name1_1", "addr1"));

        getGemfireCache().getLoggerI18n().fine("TEST:TX COMMIT 2");

        assertEquals(1L, br.getVersionVector().getCurrentVersion());
        mgr.commit();

        // Verify region version on the region
        assertEquals(2L, br.getVersionVector().getCurrentVersion());

        re = br.getRegionEntry(custIdOne);
        getGemfireCache().getLoggerI18n().fine(
            "TEST:VERSION-STAMP:" + re.getVersionStamp());

        // Verify region version on the region entry
        assertEquals(2L, re.getVersionStamp().getRegionVersion());

        // Verify entry version
        assertEquals(2, re.getVersionStamp().getEntryVersion());
        return null;
      }

    });
    
    execute(server1, new SerializableCallable() {

      @Override
      public Object call() throws Exception {
        Region<CustId, Customer> prRegion = getCache().getRegion(PERSISTENT_CUSTOMER_PR);
        CustId custIdOne = new CustId(1);
        BucketRegion br = ((PartitionedRegion)prRegion).getBucketRegion(custIdOne);
        
        // Verify region version on the region
        assertEquals(2L, br.getVersionVector().getCurrentVersion());
        
        // Verify region version ont the region entry
        RegionEntry re = br.getRegionEntry(custIdOne);
        assertEquals(2L, re.getVersionStamp().getRegionVersion());
        
        // Verify entry version 
        assertEquals(2, re.getVersionStamp().getEntryVersion());
        return null;
      }
      
    });

  }

  public void testRegionAndEntryVersionsRR() throws Exception {
    Host host = Host.getHost(0);
    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);
    
    createRR(new VM[]{server1, server2});
    execute(server2, new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        CacheTransactionManager mgr = getGemfireCache().getTxManager();
        mgr.setDistributed(true);
        getGemfireCache().getLoggerI18n().fine("TEST:DISTTX=" + mgr.isDistributed());
        getGemfireCache().getLoggerI18n().fine("TEST:TX BEGIN");
        mgr.begin();

        Region<CustId, Customer> region = getCache().getRegion(
            CUSTOMER_RR);

        CustId custIdOne = new CustId(1);
        Customer customerOne = new Customer("name1", "addr1");
        getGemfireCache().getLoggerI18n().fine("TEST:TX PUT 1");
        region.put(custIdOne, customerOne);

        LocalRegion lr = (LocalRegion) region;

        assertEquals(0L, lr.getVersionVector().getCurrentVersion());
        getGemfireCache().getLoggerI18n().fine("TEST:TX COMMIT 1");
        mgr.commit();

        // Verify region version on the region
        assertEquals(1L, lr.getVersionVector().getCurrentVersion());

        RegionEntry re = lr.getRegionEntry(custIdOne);
        getGemfireCache().getLoggerI18n().fine(
            "TEST:VERSION-STAMP:" + re.getVersionStamp());

        // Verify region version on the region entry
        assertEquals(1L, re.getVersionStamp().getRegionVersion());

        // Verify entry version
        assertEquals(1, re.getVersionStamp().getEntryVersion());

        mgr.begin();
        region.put(custIdOne, new Customer("name1_1", "addr1"));

        getGemfireCache().getLoggerI18n().fine("TEST:TX COMMIT 2");

        assertEquals(1L, lr.getVersionVector().getCurrentVersion());
        mgr.commit();

        // Verify region version on the region
        assertEquals(2L, lr.getVersionVector().getCurrentVersion());

        re = lr.getRegionEntry(custIdOne);
        getGemfireCache().getLoggerI18n().fine(
            "TEST:VERSION-STAMP:" + re.getVersionStamp());

        // Verify region version on the region entry
        assertEquals(2L, re.getVersionStamp().getRegionVersion());

        // Verify entry version
        assertEquals(2, re.getVersionStamp().getEntryVersion());
        return null;
      }

    });
    
    execute(server1, new SerializableCallable() {

      @Override
      public Object call() throws Exception {
        Region<CustId, Customer> region = getCache().getRegion(CUSTOMER_RR);
        CustId custIdOne = new CustId(1);
        
        LocalRegion lr = (LocalRegion)region;
        
        // Verify region version on the region
        assertEquals(2L, lr.getVersionVector().getCurrentVersion());
        
        // Verify region version ont the region entry
        RegionEntry re = lr.getRegionEntry(custIdOne);
        assertEquals(2L, re.getVersionStamp().getRegionVersion());
        
        // Verify entry version 
        assertEquals(2, re.getVersionStamp().getEntryVersion());
        return null;
      }
      
    });

  }
  
  
  public void testTxWorksWithNewNodeJoining() throws Exception {
    Host host = Host.getHost(0);
    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);
    VM server3 = host.getVM(2);
    
    createPR(new VM[]{server1, server2});

    class Ops extends SerializableCallable {
      private boolean flag = false; 
      public void setWaitFlag(boolean value) {
        flag = value;
      }
      @Override
      public Object call() throws Exception {
        
        CacheTransactionManager mgr = getGemfireCache().getTxManager();
        mgr.setDistributed(true);
        mgr.begin();
        Region<CustId, Customer> prRegion = getCache().getRegion(CUSTOMER_PR);

        CustId custIdOne = new CustId(1);
        Customer customerOne = new Customer("name1", "addr1");
        prRegion.put(custIdOne, customerOne);


        // Install the hook at this point
        TestObserver o = TestObserver.getInstance();
        o.setFlag(true);
        
        while (o.getFlag()) {
          Thread.currentThread().sleep(1000);
        }
        
        mgr.commit();
        
        // Verify
        assertEquals(1, prRegion.size());
        assertTrue(prRegion.containsKey(custIdOne));

        return null;
      }
    }
    server1.invokeAsync(Ops.class, "call");
    
    // Now create cache on the third server and let it join the distributed system.
    createPR(new VM[]{server3});
    
    // Let the original thread move on by signalling the flag
    
    execute(server1, new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        TestObserver o = TestObserver.getInstance();
        o.setFlag(false);
        return null;
      }
    });
  }

  public static class TestObserver {
    private static final TestObserver singleInstance = new TestObserver();
    private static boolean flag = false;
    public static TestObserver getInstance() {
      return singleInstance;
    }
    public static void setFlag(boolean val) {
      flag = val;
    }
    public boolean getFlag() {
      return flag;
    }
  }
  
  
  private class TxOps_Conflicts extends SerializableCallable {
    
    final String regionName;
    public TxOps_Conflicts(String regionName) {
      this.regionName = regionName;
    }
    
    @Override
    public Object call() throws Exception {
      CacheTransactionManager mgr = getGemfireCache().getTxManager();
      mgr.setDistributed(true);
      mgr.begin();

      // Perform a put
      Region<CustId, Customer> custRegion = getCache().getRegion(this.regionName);
      
      CustId custIdOne = new CustId(1);
      Customer customerOne = new Customer("name1", "addr1");
      CustId custIdTwo = new CustId(2);
      Customer customerTwo = new Customer("name2", "addr2");
      CustId custIdThree = new CustId(3);
      Customer customerThree = new Customer("name3", "addr3");
      custRegion.put(custIdOne, customerOne);
      custRegion.put(custIdTwo, customerTwo);
      custRegion.put(custIdThree, customerThree);
      
      // spawn a new thread modify and custIdOne in another tx
      // so that outer thread fails
      final class TxThread extends Thread {
        public void run() {
          CacheTransactionManager mgr = getGemfireCache().getTxManager();
          mgr.setDistributed(true);
          mgr.begin();
          CustId custIdOne = new CustId(1);
          Customer customerOne = new Customer("name1", "addr11");
          Region<CustId, Customer> custRegion = getCache().getRegion(regionName);
          custRegion.put(custIdOne, customerOne);
          mgr.commit();
        }
      }
      
      TxThread txThread = new TxThread(); 
      txThread.start();
      txThread.join(); //let the tx commit
      
      try {
        mgr.commit();
        fail("this test should have failed with CommitConflictException");
        // [DISTTX] TODO after conflict detection either  
        // CommitIncompleteException or CommitConflictException is thrown. 
        // Should it always be CommitConflictException?
      } catch (CommitIncompleteException cie) {
      } 
      catch (CommitConflictException ce) {
      }
      
      //verify data
      assertEquals(new Customer("name1", "addr11"), custRegion.get(custIdOne));
      assertEquals(null, custRegion.get(custIdTwo));
      assertEquals(null, custRegion.get(custIdThree));
      
      //clearing the region
      custRegion.remove(custIdOne);
      return null;
    }
  }
  
  
  /*
   * Start two concurrent transactions that put same entries. Make sure that
   * conflict is detected at the commit time.
   */
  public void testCommitConflicts_PR() throws Exception {
    Host host = Host.getHost(0);
    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);
    VM server3 = host.getVM(2);
    VM accessor = host.getVM(3);

    createPRwithRedundanyCopies(new VM[] { server1, server2, server3 }, 1);
    createPRonAccessor(accessor, 1);
    
    server1.invoke(new TxOps_Conflicts(CUSTOMER_PR));
    
    //test thru accessor as well
    accessor.invoke(new TxOps_Conflicts(CUSTOMER_PR));
  }
  
  /*
   * Start two concurrent transactions that put same entries. Make sure that
   * conflict is detected at the commit time.
   */
  public void testCommitConflicts_RR() throws Exception {
    Host host = Host.getHost(0);
    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);
    VM server3 = host.getVM(2);
    VM accessor = host.getVM(3);

    createRR(new VM[] { server1, server2, server3 });
    createRRonAccessor(accessor);
    
    server1.invoke(new TxOps_Conflicts(CUSTOMER_RR));
    
    //test thru accessor as well
    accessor.invoke(new TxOps_Conflicts(CUSTOMER_RR));
  }
  
  
  final class TxConflictRunnable implements Runnable {
    final String regionName;

    public TxConflictRunnable(String regionName) {
      this.regionName = regionName;
    }

    @Override
    public void run() {
      // spawn a new thread modify and custIdOne in another tx
      // so that outer thread fails
      final class TxThread extends Thread {
        public boolean gotConflict = false;
        public boolean gotOtherException = false;
        public Exception ex = new Exception();
        
        public void run() {
          LogWriterUtils.getLogWriter().info("Inside TxConflictRunnable.TxThread after aquiring locks");
          CacheTransactionManager mgr = getGemfireCache().getTxManager();
          mgr.setDistributed(true);
          mgr.begin();
          CustId custIdOne = new CustId(1);
          Customer customerOne = new Customer("name1", "addr11");
          Region<CustId, Customer> custRegion = getCache()
              .getRegion(regionName);
          custRegion.put(custIdOne, customerOne);
          try {
            mgr.commit();
          } catch (CommitConflictException ce) {
            gotConflict = true;
            LogWriterUtils.getLogWriter().info("Received exception ", ce);
          } catch (Exception e) {
            gotOtherException = true;
            LogWriterUtils.getLogWriter().info("Received exception ", e);
            ex.initCause(e);
          }
        }
      }

      TxThread txThread = new TxThread();
      txThread.start();
      try {
        txThread.join(); // let the tx commit
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      } 
      
      assertTrue("This test should fail with CommitConflictException",
          txThread.gotConflict);
      if (txThread.gotOtherException) {
        Assert.fail("Received unexpected exception ", txThread.ex);
      }
    }
  }
  
  private class TxOps_conflicts_after_locks_acquired extends SerializableCallable {
    
    final String regionName;
    public TxOps_conflicts_after_locks_acquired(String regionName) {
      this.regionName = regionName;
    }
    
    @Override
    public Object call() throws Exception {
      CacheTransactionManager mgr = getGemfireCache().getTxManager();
      mgr.setDistributed(true);
      mgr.begin();
      
      //set up a callback to be invoked after locks are acquired at commit time
      ((TXStateProxyImpl)((TXManagerImpl)mgr).getTXState()).forceLocalBootstrap();
      TXStateInterface txp = ((TXManagerImpl)mgr).getTXState();
      DistTXState tx = (DistTXState)((TXStateProxyImpl)txp).getRealDeal(null, null);
      tx.setAfterReservation(new TxConflictRunnable(this.regionName)); //callback

      // Perform a put
      Region<CustId, Customer> custRegion = getCache().getRegion(this.regionName);
      
      CustId custIdOne = new CustId(1);
      Customer customerOne = new Customer("name1", "addr1");
      CustId custIdTwo = new CustId(2);
      Customer customerTwo = new Customer("name2", "addr2");
      CustId custIdThree = new CustId(3);
      Customer customerThree = new Customer("name3", "addr3");
      CustId custIdFour = new CustId(4);
      Customer customerFour = new Customer("name4", "addr4");
      custRegion.put(custIdOne, customerOne);
      custRegion.put(custIdTwo, customerTwo);
      custRegion.put(custIdThree, customerThree);
      custRegion.put(custIdFour, customerFour);
      
      // will invoke the callback that spawns a new thread and another
      // transaction
      mgr.commit(); 

      //verify data
      assertEquals(new Customer("name1", "addr1"), custRegion.get(custIdOne));
      assertEquals(new Customer("name2", "addr2"), custRegion.get(custIdTwo));
      assertEquals(new Customer("name3", "addr3"), custRegion.get(custIdThree));
      assertEquals(new Customer("name4", "addr4"), custRegion.get(custIdFour));

      return null;
    }
  }
  
  /*
   * Start a transaction, at commit time after acquiring locks, start another
   * transaction in a new thread that modifies same entries as in the earlier
   * transaction. Make sure that conflict is detected
   */
  public void testCommitConflicts_PR_after_locks_acquired() throws Exception {
    Host host = Host.getHost(0);
    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);

//    createPRwithRedundanyCopies(new VM[] { server1, server2 }, 1);
    
    createPRwithRedundanyCopies(new VM[] { server1 }, 0);

    server1.invoke(new TxOps_conflicts_after_locks_acquired(CUSTOMER_PR));
  }
  
  /*
   * Start a transaction, at commit time after acquiring locks, start another
   * transaction in a new thread that modifies same entries as in the earlier
   * transaction. Make sure that conflict is detected
   */
  public void testCommitConflicts_RR_after_locks_acquired() throws Exception {
    Host host = Host.getHost(0);
    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);

    createRR(new VM[] { server1, server2 });

    server1.invoke(new TxOps_conflicts_after_locks_acquired(CUSTOMER_RR));
  }
  
  
  final class TxRunnable implements Runnable {
    final String regionName;

    public TxRunnable(String regionName) {
      this.regionName = regionName;
    }

    @Override
    public void run() {
      final class TxThread extends Thread {
        public boolean gotException = false;
        public Exception ex = new Exception();

        public void run() {
          LogWriterUtils.getLogWriter()
              .info("Inside TxRunnable.TxThread after aquiring locks");
          CacheTransactionManager mgr = getGemfireCache().getTxManager();
          mgr.setDistributed(true);
          mgr.begin();
          Region<CustId, Customer> custRegion = getCache()
              .getRegion(regionName);
          for (int i=11; i<=20; i++) {
            custRegion.put(new CustId(i), new Customer("name" + i, "addr" + i));
          }
          try {
            mgr.commit();
          } catch (Exception e) {
            gotException = true;
            LogWriterUtils.getLogWriter().info("Received exception ", e);
            ex.initCause(e);
          }
        }
      }

      TxThread txThread = new TxThread();
      txThread.start();
      try {
        txThread.join(); // let the tx commit
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      if (txThread.gotException) {
        Assert.fail("Received exception ", txThread.ex);
      }
    }
  }
  
private class TxOps_no_conflicts extends SerializableCallable {
    
    final String regionName;
    public TxOps_no_conflicts(String regionName) {
      this.regionName = regionName;
    }
    
    @Override
    public Object call() throws Exception {
      CacheTransactionManager mgr = getGemfireCache().getTxManager();
      mgr.setDistributed(true);
      mgr.begin();
      
      //set up a callback to be invoked after locks are acquired at commit time
      ((TXStateProxyImpl)((TXManagerImpl)mgr).getTXState()).forceLocalBootstrap();
      TXStateInterface txp = ((TXManagerImpl)mgr).getTXState();
      DistTXState tx = (DistTXState)((TXStateProxyImpl)txp).getRealDeal(null, null);
      tx.setAfterReservation(new TxRunnable(this.regionName)); //callback

      // Perform a put
      Region<CustId, Customer> custRegion = getCache().getRegion(this.regionName);
      
      CustId custIdOne = new CustId(1);
      Customer customerOne = new Customer("name1", "addr1");
      CustId custIdTwo = new CustId(2);
      Customer customerTwo = new Customer("name2", "addr2");
      CustId custIdThree = new CustId(3);
      Customer customerThree = new Customer("name3", "addr3");
      CustId custIdFour = new CustId(4);
      Customer customerFour = new Customer("name4", "addr4");
      custRegion.put(custIdOne, customerOne);
      custRegion.put(custIdTwo, customerTwo);
      custRegion.put(custIdThree, customerThree);
      custRegion.put(custIdFour, customerFour);
      
      // will invoke the callback that spawns a new thread and another
      // transaction that does puts of 10 entries
      mgr.commit(); 

      //verify data
      assertEquals(14, custRegion.size());

      return null;
    }
  }

  /*
   * Start a transaction, at commit time after acquiring locks, start another
   * transaction in a new thread that modifies different entries Make sure that
   * there is no conflict or exception.
   */
public void testCommitNoConflicts_PR() throws Exception {
  Host host = Host.getHost(0);
  VM server1 = host.getVM(0);
  VM server2 = host.getVM(1);

  createPRwithRedundanyCopies(new VM[] { server1, server2 }, 1);

  server1.invoke(new TxOps_no_conflicts(CUSTOMER_PR));
}

/*
 * Start a transaction, at commit time after acquiring locks, start another
 * transaction in a new thread that modifies different entries Make sure that
 * there is no conflict or exception.
 */
public void testCommitNoConflicts_RR() throws Exception {
  Host host = Host.getHost(0);
  VM server1 = host.getVM(0);
  VM server2 = host.getVM(1);

  createRR(new VM[] { server1, server2 });

  server1.invoke(new TxOps_no_conflicts(CUSTOMER_RR));
}

  
  
}

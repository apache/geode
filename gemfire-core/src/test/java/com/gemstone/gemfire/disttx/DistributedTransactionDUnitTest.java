package com.gemstone.gemfire.disttx;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheTransactionManager;
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
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.cache.BridgeServerImpl;
import com.gemstone.gemfire.internal.cache.BucketRegion;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.RegionEntry;
import com.gemstone.gemfire.internal.cache.TXManagerImpl;
import com.gemstone.gemfire.internal.cache.execute.CustomerIDPartitionResolver;
import com.gemstone.gemfire.internal.cache.execute.data.CustId;
import com.gemstone.gemfire.internal.cache.execute.data.Customer;
import com.gemstone.gemfire.internal.cache.execute.data.Order;
import com.gemstone.gemfire.internal.cache.execute.data.OrderId;
import com.gemstone.gemfire.test.junit.categories.DistributedTransactionsTest;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

import dunit.Host;
import dunit.SerializableCallable;
import dunit.VM;

/**
 * Port of GemFireXD's corresponding test for distributed transactions
 * 
 * @author sjigyasu
 *
 */
@SuppressWarnings("deprecation")
@Category(DistributedTransactionsTest.class)
public class DistributedTransactionDUnitTest extends CacheTestCase {
  final protected String CUSTOMER_PR = "customerPRRegion";
  final protected String ORDER_PR = "orderPRRegion";
  final protected String D_REFERENCE = "distrReference";
  final protected String PERSISTENT_CUSTOMER_PR = "persistentCustomerPRRegion";

  final protected String CUSTOMER_RR = "customerRRRegion";
  
  @Override
  public void setUp() throws Exception{
    super.setUp();
    this.invokeInEveryVM(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        System.setProperty("gemfire.sync-commits", "true");
        return null;
      }
    });
    
    this.invokeInEveryVM(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        //System.setProperty("gemfire.log-level", "fine");
        return null;
      }
    }); 

    this.invokeInEveryVM(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        //System.setProperty("gemfire.ALLOW_PERSISTENT_TRANSACTIONS", "true");
        TXManagerImpl.ALLOW_PERSISTENT_TRANSACTIONS = true;
        return null;
      }
    }); 
  }
  
  public void tearDown2() throws Exception {
    this.invokeInEveryVM(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        System.setProperty("gemfire.sync-commits", "false");
        return null;
      }
    });
    this.invokeInEveryVM(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        //System.setProperty("gemfire.ALLOW_PERSISTENT_TRANSACTIONS", "false");
        TXManagerImpl.ALLOW_PERSISTENT_TRANSACTIONS = false;
        return null;
      }
    }); 
    
    super.tearDown2();
  }
  
  public DistributedTransactionDUnitTest(String name) {
    super(name);
  }

  public void execute(VM vm, SerializableCallable c) {
    vm.invoke(c);
  }
  
  public void execute(VM[] vms, SerializableCallable c) {
    for (VM vm : vms) {
      execute(vm, c);
    }
  }

  protected String reduceLogging() {
    return "config";
  }

  
  public int startServer(VM vm) {
    return (Integer) vm.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
        CacheServer s = getCache().addCacheServer();
        s.setPort(port);
        ((BridgeServerImpl) s).setTransactionTimeToLive(10);
        s.start();
        return port;
      }
    });
  }

  protected boolean getConcurrencyChecksEnabled() {
    return true;
  }

  void createRR() {
    AttributesFactory af = new AttributesFactory();
    af.setScope(Scope.DISTRIBUTED_ACK);
    af.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
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
          createRR();
          return null;
        }
      });
    }
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
  
  
  private class TxOps_Conflicts extends SerializableCallable {
    private boolean gotConflict = false;
    @Override
    public Object call() throws Exception {
      CacheTransactionManager mgr = getGemfireCache().getTxManager();
      mgr.setDistributed(true);
      mgr.begin();

      // Perform a put
      Region<CustId, Customer> custRegion = getCache().getRegion(CUSTOMER_PR);
      
      CustId custIdOne = new CustId(1);
      Customer customerOne = new Customer("name1", "addr1");
      CustId custIdTwo = new CustId(2);
      Customer customerTwo = new Customer("name2", "addr2");
      CustId custIdThree = new CustId(3);
      Customer customerThree = new Customer("name3", "addr3");
      custRegion.put(custIdOne, customerOne);
      custRegion.put(custIdTwo, customerTwo);
      custRegion.put(custIdThree, customerThree);
      
      final class TxThread extends Thread {
        public boolean gotConflict = false;
        public void run() {
          CacheTransactionManager mgr = getGemfireCache().getTxManager();
          mgr.setDistributed(true);
          mgr.begin();
          CustId custIdOne = new CustId(1);
          Customer customerOne = new Customer("name1", "addr1");
          Region<CustId, Customer> custRegion = getCache().getRegion(CUSTOMER_PR);
          
          // This should give a commit conflict/
          // [sjigyasu] Not sure at this point what the exception will be.
          // TODO: Check for the correct exception when the merge is over.
          try {
            custRegion.put(custIdOne, customerOne);
          } catch (Exception e) {
            // Assuming there is conflict exception.
            gotConflict = true;
            mgr.rollback();
          }
        }
      }
      
      TxThread txThread = new TxThread();
      txThread.start();
      txThread.join();
      assertTrue(txThread.gotConflict);
      return null;
    }
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
  
  // [DISTTX] TODO
  public void DISABLED_testPutAllWithTransactions() throws Exception {
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
  
}
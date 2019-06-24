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
package org.apache.geode.disttx;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Properties;

import org.junit.Test;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.CacheTransactionManager;
import org.apache.geode.cache.CommitConflictException;
import org.apache.geode.cache.CommitIncompleteException;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.InterestPolicy;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.SubscriptionAttributes;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.cache.DistTXState;
import org.apache.geode.internal.cache.TXManagerImpl;
import org.apache.geode.internal.cache.TXStateInterface;
import org.apache.geode.internal.cache.TXStateProxyImpl;
import org.apache.geode.internal.cache.execute.CustomerIDPartitionResolver;
import org.apache.geode.internal.cache.execute.data.CustId;
import org.apache.geode.internal.cache.execute.data.Customer;
import org.apache.geode.internal.cache.execute.data.Order;
import org.apache.geode.internal.cache.execute.data.OrderId;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;

@SuppressWarnings("deprecation")

public class ReadConflictsTransactionDUnitTest extends JUnit4CacheTestCase {

  protected final String CUSTOMER_PR = "customerPRRegion";
  protected final String ORDER_PR = "orderPRRegion";
  protected final String D_REFERENCE = "distrReference";

  protected final String CUSTOMER_RR = "customerRRRegion";

  @Override
  public void preSetUp() throws Exception {
    Invoke.invokeInEveryVM(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        System.setProperty(DistributionConfig.GEMFIRE_PREFIX + "detectReadConflicts", "true");
        return null;
      }
    });
  }

  @Override
  public final void postSetUp() throws Exception {
    Invoke.invokeInEveryVM(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        System.setProperty(DistributionConfig.GEMFIRE_PREFIX + "sync-commits", "true");
        return null;
      }
    });

    Invoke.invokeInEveryVM(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        // System.setProperty("gemfire.ALLOW_PERSISTENT_TRANSACTIONS", "true");
        TXManagerImpl.ALLOW_PERSISTENT_TRANSACTIONS = true;
        return null;
      }
    });
  }

  @Override
  public final void preTearDownCacheTestCase() throws Exception {
    Invoke.invokeInEveryVM(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        System.setProperty(DistributionConfig.GEMFIRE_PREFIX + "sync-commits", "false");
        return null;
      }
    });
    Invoke.invokeInEveryVM(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        System.setProperty(DistributionConfig.GEMFIRE_PREFIX + "detectReadConflicts", "false");
        return null;
      }
    });
    Invoke.invokeInEveryVM(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        // System.setProperty("gemfire.ALLOW_PERSISTENT_TRANSACTIONS", "false");
        TXManagerImpl.ALLOW_PERSISTENT_TRANSACTIONS = false;
        return null;
      }
    });
  }

  public ReadConflictsTransactionDUnitTest() {
    super();
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
      @Override
      public Object call() throws Exception {
        int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
        CacheServer s = getCache().addCacheServer();
        s.setPort(port);
        s.start();
        TXManagerImpl txMgr = (TXManagerImpl) getCache().getCacheTransactionManager();
        txMgr.setTransactionTimeToLiveForTest(10);
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
      af.setDataPolicy(DataPolicy.EMPTY); // for accessor
    }
    af.setConcurrencyChecksEnabled(getConcurrencyChecksEnabled());
    getCache().createRegion(CUSTOMER_RR, af.create());
  }

  void createPR(boolean accessor, int redundantCopies, InterestPolicy interestPolicy) {
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
    return props;
  }


  void createRegions(boolean accessor, int redundantCopies, InterestPolicy interestPolicy) {
    AttributesFactory af = new AttributesFactory();
    af.setScope(Scope.DISTRIBUTED_ACK);
    af.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    // af.setConcurrencyChecksEnabled(getConcurrencyChecksEnabled());
    getCache().createRegion(D_REFERENCE, af.create());

    af = new AttributesFactory();
    // af.setConcurrencyChecksEnabled(getConcurrencyChecksEnabled());
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
    af.setPartitionAttributes(new PartitionAttributesFactory<OrderId, Order>().setTotalNumBuckets(4)
        .setLocalMaxMemory(accessor ? 0 : 1)
        .setPartitionResolver(new CustomerIDPartitionResolver("resolver2"))
        .setRedundantCopies(redundantCopies).setColocatedWith(CUSTOMER_PR).create());
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

  public void createRegions(final VM vm, final boolean accessor, final int redundantCopies) {
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


  private class TxOps_Conflicts extends SerializableCallable {

    final String regionName;

    public TxOps_Conflicts(String regionName) {
      this.regionName = regionName;
    }

    @Override
    public Object call() throws Exception {
      CacheTransactionManager mgr = getGemfireCache().getTxManager();

      // Perform a put
      Region<CustId, Customer> custRegion = getCache().getRegion(this.regionName);
      mgr.setDistributed(true);
      mgr.begin();

      CustId custIdOne = new CustId(1);
      Customer customerOne = new Customer("name1", "addr1");
      CustId custIdTwo = new CustId(2);
      Customer customerTwo = new Customer("name2", "addr2");
      CustId custIdThree = new CustId(3);
      Customer customerThree = new Customer("name3", "addr3");
      custRegion.put(custIdOne, customerOne);
      custRegion.put(custIdTwo, customerTwo);
      custRegion.put(custIdThree, customerThree);
      mgr.commit();

      mgr.begin();
      assertEquals(customerOne, custRegion.get(custIdOne));

      // spawn a new thread modify and custIdOne in another tx
      // so that outer thread fails
      class TxThread extends Thread {
        @Override
        public void run() {
          CacheTransactionManager mgr = getGemfireCache().getTxManager();
          mgr.setDistributed(true);
          mgr.begin();
          CustId custIdOne = new CustId(1);
          Customer customerOne = new Customer("name11", "addr11");
          Region<CustId, Customer> custRegion = getCache().getRegion(regionName);
          custRegion.put(custIdOne, customerOne);
          mgr.commit();
        }
      }

      TxThread txThread = new TxThread();
      txThread.start();
      txThread.join(); // let the tx commit

      try {
        mgr.commit();
        fail("this test should have failed with CommitConflictException");
        // [DISTTX] TODO after conflict detection either
        // CommitIncompleteException or CommitConflictException is thrown.
        // Should it always be CommitConflictException?
      } catch (CommitIncompleteException cie) {
      } catch (CommitConflictException ce) {
      }

      // verify data
      assertEquals(customerTwo, custRegion.get(custIdTwo));
      assertEquals(customerThree, custRegion.get(custIdThree));

      // clearing the region
      custRegion.remove(custIdOne);
      return null;
    }
  }

  /*
   * Start two concurrent transactions that put and get same entries. Make sure that conflict is
   * detected at
   * the commit time.
   */


  @Test
  public void testCommitConflicts_PR() throws Exception {
    Host host = Host.getHost(0);
    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);
    VM server3 = host.getVM(2);
    VM accessor = host.getVM(3);

    createPRwithRedundanyCopies(new VM[] {server1, server2, server3}, 1);
    createPRonAccessor(accessor, 1);

    server1.invoke(new TxOps_Conflicts(CUSTOMER_PR));

    // test thru accessor as well
    accessor.invoke(new TxOps_Conflicts(CUSTOMER_PR));
  }

  /*
   * Start two concurrent transactions that put same entries. Make sure that conflict is detected at
   * the commit time.
   */
  @Test
  public void testCommitConflicts_RR() throws Exception {
    Host host = Host.getHost(0);
    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);
    VM server3 = host.getVM(2);
    VM accessor = host.getVM(3);

    createRR(new VM[] {server1, server2, server3});
    createRRonAccessor(accessor);

    server1.invoke(new TxOps_Conflicts(CUSTOMER_RR));

    // test thru accessor as well
    accessor.invoke(new TxOps_Conflicts(CUSTOMER_RR));
  }


  class TxConflictRunnable implements Runnable {
    final String regionName;

    public TxConflictRunnable(String regionName) {
      this.regionName = regionName;
    }

    @Override
    public void run() {
      // spawn a new thread get custIdOne in another tx
      // so that outer thread fails
      class TxThread extends Thread {
        public boolean gotConflict = false;
        public boolean gotOtherException = false;
        public Exception ex = new Exception();

        @Override
        public void run() {
          LogWriterUtils.getLogWriter()
              .info("Inside TxConflictRunnable.TxThread after aquiring locks");
          CacheTransactionManager mgr = getGemfireCache().getTxManager();
          mgr.setDistributed(true);
          mgr.begin();
          CustId custIdTwo = new CustId(2);
          Customer customerOne = new Customer("name1", "addr1");

          Region<CustId, Customer> custRegion = getCache().getRegion(regionName);
          assertEquals(customerOne, custRegion.get(custIdTwo));
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

      assertTrue("This test should fail with CommitConflictException", txThread.gotConflict);
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

      // Perform a put
      Region<CustId, Customer> custRegion = getCache().getRegion(this.regionName);

      mgr.setDistributed(true);
      mgr.begin();
      CustId custIdOne = new CustId(1);
      Customer customerOne = new Customer("name1", "addr1");
      CustId custIdTwo = new CustId(2);
      Customer customerTwo = new Customer("name2", "addr2");
      CustId custIdThree = new CustId(3);
      Customer customerThree = new Customer("name3", "addr3");
      CustId custIdFour = new CustId(4);
      Customer customerFour = new Customer("name4", "addr4");
      custRegion.put(custIdTwo, customerOne);
      mgr.commit();

      mgr.begin();
      Customer customerOneA = new Customer("name1A", "addr1A");
      Customer customerTwoA = new Customer("name2A", "addr2A");
      Customer customerThreeA = new Customer("name3A", "addr3A");
      Customer customerFourA = new Customer("name4A", "addr4A");

      // set up a callback to be invoked after locks are acquired at commit time
      ((TXStateProxyImpl) ((TXManagerImpl) mgr).getTXState()).forceLocalBootstrap();
      TXStateInterface txp = ((TXManagerImpl) mgr).getTXState();
      DistTXState tx = (DistTXState) ((TXStateProxyImpl) txp).getRealDeal(null, null);
      tx.setAfterReservation(new TxConflictRunnable(this.regionName)); // callback

      custRegion.put(custIdOne, customerOneA);
      custRegion.put(custIdTwo, customerTwoA);
      custRegion.put(custIdThree, customerThreeA);
      custRegion.put(custIdFour, customerFourA);

      // will invoke the callback that spawns a new thread and another
      // transaction
      mgr.commit();

      // verify data
      assertEquals(new Customer("name1A", "addr1A"), custRegion.get(custIdOne));
      assertEquals(new Customer("name2A", "addr2A"), custRegion.get(custIdTwo));
      assertEquals(new Customer("name3A", "addr3A"), custRegion.get(custIdThree));
      assertEquals(new Customer("name4A", "addr4A"), custRegion.get(custIdFour));

      return null;
    }
  }


  /*
   * Start a transaction, at commit time after acquiring locks, start another transaction in a new
   * thread that modifies same entries as in the earlier transaction. Make sure that conflict is
   * detected
   */
  @Test
  public void testCommitConflicts_PR_after_locks_acquired() throws Exception {
    Host host = Host.getHost(0);
    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);

    // createPRwithRedundanyCopies(new VM[] { server1, server2 }, 1);

    createPRwithRedundanyCopies(new VM[] {server1}, 0);

    server1.invoke(new TxOps_conflicts_after_locks_acquired(CUSTOMER_PR));
  }

  /*
   * Start a transaction, at commit time after acquiring locks, start another transaction in a new
   * thread that modifies same entries as in the earlier transaction. Make sure that conflict is
   * detected
   */
  @Test
  public void testCommitConflicts_RR_after_locks_acquired() throws Exception {
    Host host = Host.getHost(0);
    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);

    createRR(new VM[] {server1, server2});

    server1.invoke(new TxOps_conflicts_after_locks_acquired(CUSTOMER_RR));
  }


}

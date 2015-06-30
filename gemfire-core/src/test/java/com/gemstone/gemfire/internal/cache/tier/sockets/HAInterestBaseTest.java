/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.tier.sockets;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.InterestResultPolicy;
import com.gemstone.gemfire.cache.MirrorType;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.client.internal.Connection;
import com.gemstone.gemfire.cache.client.internal.PoolImpl;
import com.gemstone.gemfire.cache.client.internal.RegisterInterestTracker;
import com.gemstone.gemfire.cache.client.internal.ServerRegionProxy;
import com.gemstone.gemfire.cache.util.BridgeServer;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.ServerLocation;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.cache.BridgeObserverAdapter;
import com.gemstone.gemfire.internal.cache.BridgeObserverHolder;
import com.gemstone.gemfire.internal.cache.BridgeServerImpl;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.tier.InterestType;

import dunit.DistributedTestCase;
import dunit.Host;
import dunit.VM;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 * Tests Interest Registration Functionality
 *
 */
public class HAInterestBaseTest extends DistributedTestCase
{
  private static int MAX_WAIT = 60000;
  protected static Cache cache = null;
  protected static PoolImpl pool = null ;
  protected static Connection conn = null ;

  protected static  int PORT1 ;
  protected static  int PORT2 ;
  protected static  int PORT3 ;

  protected static final String k1 = "k1";
  protected static final String k2 = "k2";

  protected static final String client_k1 = "client-k1";
  protected static final String client_k2 = "client-k2";

  protected static final String server_k1 = "server-k1";
  protected static final String server_k2 = "server-k2";

  protected static final String server_k1_updated = "server_k1_updated";

  // To verify the beforeInterestRegistration is called or not
  protected static boolean isBeforeRegistrationCallbackCalled = false;

  // To verify the beforeInterestRecoeryCallbackCalled is called or not
  protected static boolean isBeforeInterestRecoveryCallbackCalled = false;

  //To verify the afterInterestRegistration is called or not
  protected static boolean isAfterRegistrationCallbackCalled = false;


  protected static final String REGION_NAME = "HAInterestBaseTest_region";

  protected static Host host = null ;

  protected static VM server1 = null;
  protected static VM server2 = null;
  protected static VM server3 = null;
  protected volatile static boolean exceptionOccured = false;

  /** constructor */
  public HAInterestBaseTest(String name) {
    super(name);
  }

  public void setUp() throws Exception
  {
    super.setUp();
    host = Host.getHost(0);
    server1 = host.getVM(0);
    server2 = host.getVM(1);
    server3 = host.getVM(2);
    CacheServerTestUtil.disableShufflingOfEndpoints();
    // start servers first
    PORT1 =  ((Integer) server1.invoke(HAInterestBaseTest.class, "createServerCache")).intValue();
    PORT2 =  ((Integer) server2.invoke(HAInterestBaseTest.class, "createServerCache")).intValue();
    PORT3 =  ((Integer) server3.invoke(HAInterestBaseTest.class, "createServerCache")).intValue();
    exceptionOccured = false;

  }


  /**
   * Return the current primary waiting for a primary to exist.
   * @since 5.7
   */
  public static VM getPrimaryVM() {
    return getPrimaryVM(null);
  }

  /**
   * Return the current primary waiting for a primary to exist and for it
   * not to be the oldPrimary (if oldPrimary is NOT null).
   * @since 5.7
   */
  public static VM getPrimaryVM(final VM oldPrimary) {
    WaitCriterion wc = new WaitCriterion() {
      String excuse;
      public boolean done() {
        int primaryPort = pool.getPrimaryPort();
        if (primaryPort == -1) {
          return false;
        }
        // we have a primary
        VM currentPrimary = getServerVM(primaryPort);
        if (currentPrimary != oldPrimary) {
          return true;
        }
        return false;
      }
      public String description() {
        return excuse;
      }
    };
    DistributedTestCase.waitForCriterion(wc, MAX_WAIT, 1000, true);
    
    int primaryPort = pool.getPrimaryPort();
    assertTrue(primaryPort != -1);
    VM currentPrimary = getServerVM(primaryPort);
    assertTrue(currentPrimary != oldPrimary);
    return currentPrimary;
  }
  
  public static VM getBackupVM() {
    return getBackupVM(null);
  }
  public static VM getBackupVM(VM stoppedBackup) {
    VM currentPrimary = getPrimaryVM(null);
    if (currentPrimary != server2 && server2 != stoppedBackup) {
      return server2;
    } else if (currentPrimary != server3 && server3 != stoppedBackup) {
      return server3;
    } else if (currentPrimary != server1 && server1 != stoppedBackup) {
      return server1;
    } else {
      fail("expected currentPrimary " + currentPrimary + " to be "
           + server1 + ", or "
           + server2 + ", or "
           + server3);
      return null;
    }
  }

  /**
   * Given a server vm (server1, server2, or server3) return its port.
   * @since 5.7
   */
  public static int getServerPort(VM vm) {
    if (vm == server1) {
      return PORT1;
    } else if (vm == server2) {
      return PORT2;
    } else if (vm == server3) {
      return PORT3;
    } else {
      fail("expected vm " + vm + " to be "
           + server1
           + ", or "
           + server2
           + ", or "
           + server3);
      return -1;
    }
  }
  /**
   * Given a server port (PORT1, PORT2, or PORT3) return its vm.
   * @since 5.7
   */
  public static VM getServerVM(int port) {
    if (port == PORT1) {
      return server1;
    } else if (port == PORT2) {
      return server2;
    } else if (port == PORT3) {
      return server3;
    } else {
      fail("expected port " + port + " to be "
           + PORT1
           + ", or "
           + PORT2
           + ", or "
           + PORT3);
      return null;
    }
  }
  
 public static void verifyRefreshedEntriesFromServer()
  {
    final Region r1 = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    assertNotNull(r1);
    
    WaitCriterion wc = new WaitCriterion() {
      String excuse;
      public boolean done() {
        Region.Entry re = r1.getEntry(k1);
        if (re == null) return false;
        Object val = re.getValue();
        return client_k1.equals(val);
      }
      public String description() {
        return excuse;
      }
    };
    DistributedTestCase.waitForCriterion(wc, MAX_WAIT, 1000, true);

    wc = new WaitCriterion() {
      String excuse;
      public boolean done() {
        Region.Entry re = r1.getEntry(k2);
        if (re == null) return false;
        Object val = re.getValue();
        return client_k2.equals(val);
      }
      public String description() {
        return excuse;
      }
    };
    DistributedTestCase.waitForCriterion(wc, MAX_WAIT, 1000, true);

    // assertEquals(client_k1, r1.getEntry(k1).getValue());
    // assertEquals(client_k2 ,r1.getEntry(k2).getValue());
  }
 public static void verifyDeadAndLiveServers(final int expectedDeadServers, 
     final int expectedLiveServers)
{
   WaitCriterion wc = new WaitCriterion() {
     String description;
     public boolean done() {
       return pool.getConnectedServerCount() == expectedLiveServers;  
     }
     public String description() {
       return description;
     }
   };
   DistributedTestCase.waitForCriterion(wc, MAX_WAIT, 1000, true);
   
//   while (proxy.getDeadServers().size() != expectedDeadServers) { // wait until condition is met
//     assertTrue("Waited over " + maxWaitTime + "for dead servers to become : " + expectedDeadServers  + " This issue can occur on Solaris as DSM thread get stuck in connectForServer() call, and hence not recovering any newly started server This may be beacuase of tcp_ip_abort_cinterval kernal level property on solaris which has 3 minutes as a default value",
//         (System.currentTimeMillis() - start) < maxWaitTime);
//     try {
//       Thread.yield();
//         synchronized(delayLock) {delayLock.wait(2000);}
//     }
//     catch (InterruptedException ie) {
//       fail("Interrupted while waiting ", ie);
//     }
//   }
//   start = System.currentTimeMillis();
}


 public static void putK1andK2()
  {
    Region r1 = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    assertNotNull(r1);
    try {
      r1.put(k1, server_k1);
      r1.put(k2, server_k2);
    }
    catch (Exception e) {
      fail("Test failed due to Exception in putK1andK2 ::" + e);
    }
  }

 public static void setBridgeObserverForBeforeInterestRecoveryFailure()
 {
   PoolImpl.BEFORE_RECOVER_INTEREST_CALLBACK_FLAG = true;
   BridgeObserverHolder.setInstance(new BridgeObserverAdapter() {
     public void beforeInterestRecovery()
     {
       synchronized (HAInterestBaseTest.class) {
        Thread t = new Thread (){
       public void run(){
         getBackupVM().invoke(HAInterestBaseTest.class, "startServer");
         getPrimaryVM().invoke(HAInterestBaseTest.class, "stopServer");
     }
     };
     t.start();
     try {
       DistributedTestCase.join(t, 30 * 1000, getLogWriter());
     }catch(Exception ignore) {
       exceptionOccured= true;
     }
         HAInterestBaseTest.isBeforeInterestRecoveryCallbackCalled = true;
         HAInterestBaseTest.class.notify();
         PoolImpl.BEFORE_RECOVER_INTEREST_CALLBACK_FLAG = false;
       }
     }
   });
 }


 public static void setBridgeObserverForBeforeInterestRecovery()
 {
   PoolImpl.BEFORE_RECOVER_INTEREST_CALLBACK_FLAG = true;
   BridgeObserverHolder.setInstance(new BridgeObserverAdapter() {
     public void beforeInterestRecovery()
     {
       synchronized (HAInterestBaseTest.class) {
         Thread t = new Thread (){
           public void run(){
               Region r1 = cache.getRegion(Region.SEPARATOR + REGION_NAME);
               assertNotNull(r1);
               try {
              r1.put(k1, server_k1_updated);
            } catch (Exception e) {
              e.printStackTrace();
              fail("Test Failed due to ..."+e);
            }
           }
         };
       t.start();

       HAInterestBaseTest.isBeforeInterestRecoveryCallbackCalled = true;
       HAInterestBaseTest.class.notify();
         PoolImpl.BEFORE_RECOVER_INTEREST_CALLBACK_FLAG = false;
       }
     }
   });
 }

 public static void waitForBeforeInterestRecoveryCallBack()
 {

   assertNotNull(cache);
   synchronized (HAInterestBaseTest.class) {
     while (!isBeforeInterestRecoveryCallbackCalled) {
       try {
         HAInterestBaseTest.class.wait();
       }
       catch (InterruptedException e) {
         fail("Test failed due to InterruptedException in waitForBeforeInterstRecovery()");
       }
     }
   }

 }

public static void setBridgeObserverForBeforeRegistration(final VM vm)
{
  PoolImpl.BEFORE_REGISTER_CALLBACK_FLAG = true;
  BridgeObserverHolder.setInstance(new BridgeObserverAdapter() {
    public void beforeInterestRegistration()
    {
      synchronized (HAInterestBaseTest.class) {
        vm.invoke(HAInterestBaseTest.class, "startServer");
        HAInterestBaseTest.isBeforeRegistrationCallbackCalled = true;
        HAInterestBaseTest.class.notify();
        PoolImpl.BEFORE_REGISTER_CALLBACK_FLAG = false;
      }
    }
  });
}

public static void waitForBeforeRegistrationCallback()
{

  assertNotNull(cache);
  synchronized (HAInterestBaseTest.class) {
    while (!isBeforeRegistrationCallbackCalled) {
      try {
        HAInterestBaseTest.class.wait();
      }
      catch (InterruptedException e) {
        fail("Test failed due to InterruptedException in waitForBeforeRegistrationCallback()");
      }
    }
  }
}

public static void setBridgeObserverForAfterRegistration(final VM vm)
{
  PoolImpl.AFTER_REGISTER_CALLBACK_FLAG = true;
  BridgeObserverHolder.setInstance(new BridgeObserverAdapter() {
    public void afterInterestRegistration()
    {
      synchronized (HAInterestBaseTest.class) {
        vm.invoke(HAInterestBaseTest.class, "startServer");
        HAInterestBaseTest.isAfterRegistrationCallbackCalled = true;
        HAInterestBaseTest.class.notify();
        PoolImpl.AFTER_REGISTER_CALLBACK_FLAG = false;
      }
    }
  });
}

public static void waitForAfterRegistrationCallback()
{

  assertNotNull(cache);
  if (!isAfterRegistrationCallbackCalled) {
    synchronized (HAInterestBaseTest.class) {
      while (!isAfterRegistrationCallbackCalled) {
        try {
          HAInterestBaseTest.class.wait();
        }
        catch (InterruptedException e) {
          fail("Test failed due to InterruptedException in waitForAfterRegistrationCallback()");
        }
      }
    }
  }

}



 public static void unSetBridgeObserverForRegistrationCallback()
{
  synchronized (HAInterestBaseTest.class) {
    PoolImpl.BEFORE_REGISTER_CALLBACK_FLAG = false;
    PoolImpl.AFTER_REGISTER_CALLBACK_FLAG = false ;
    HAInterestBaseTest.isBeforeRegistrationCallbackCalled = false;
    HAInterestBaseTest.isAfterRegistrationCallbackCalled = false ;
  }

}
    public static void verifyDispatcherIsAlive()
    {
    try {
      assertEquals("More than one BridgeServer", 1, cache.getBridgeServers()
          .size());
      WaitCriterion wc = new WaitCriterion() {
        String excuse;
        public boolean done() {
          return cache.getBridgeServers().size() == 1;
        }
        public String description() {
          return excuse;
        }
      };
      DistributedTestCase.waitForCriterion(wc, MAX_WAIT, 1000, true);
      
      BridgeServerImpl bs = (BridgeServerImpl)cache.getBridgeServers()
          .iterator().next();
      assertNotNull(bs);
      assertNotNull(bs.getAcceptor());
      assertNotNull(bs.getAcceptor().getCacheClientNotifier());
      final CacheClientNotifier ccn = bs.getAcceptor().getCacheClientNotifier();

      wc = new WaitCriterion() {
        String excuse;
        public boolean done() {
          return ccn.getClientProxies().size() > 0;
        }
        public String description() {
          return excuse;
        }
      };
      DistributedTestCase.waitForCriterion(wc, MAX_WAIT, 1000, true);

      wc = new WaitCriterion() {
        String excuse;
        Iterator iter_prox;
        CacheClientProxy proxy;
        public boolean done() {
          iter_prox = ccn.getClientProxies().iterator();
          if(iter_prox.hasNext()) {
            proxy = (CacheClientProxy)iter_prox.next();
            return proxy._messageDispatcher.isAlive();
          }
          else {
            return false;
          }         
        }
        public String description() {
          return excuse;
        }
      };
      DistributedTestCase.waitForCriterion(wc, MAX_WAIT, 1000, true);
    }
    catch (Exception ex) {
      fail("while setting verifyDispatcherIsAlive  " + ex);
    }
  }

    public static void verifyDispatcherIsNotAlive()
    {
    try {
      WaitCriterion wc = new WaitCriterion() {
        String excuse;
        public boolean done() {
          return cache.getBridgeServers().size() == 1;
        }
        public String description() {
          return excuse;
        }
      };
      DistributedTestCase.waitForCriterion(wc, MAX_WAIT, 1000, true);

      BridgeServerImpl bs = (BridgeServerImpl)cache.getBridgeServers()
          .iterator().next();
      assertNotNull(bs);
      assertNotNull(bs.getAcceptor());
      assertNotNull(bs.getAcceptor().getCacheClientNotifier());
      final CacheClientNotifier ccn = bs.getAcceptor().getCacheClientNotifier();
      wc = new WaitCriterion() {
        String excuse;
        public boolean done() {
          return ccn.getClientProxies().size() > 0;
        }
        public String description() {
          return excuse;
        }
      };
      DistributedTestCase.waitForCriterion(wc, MAX_WAIT, 1000, true);

      Iterator iter_prox = ccn.getClientProxies().iterator();
      if (iter_prox.hasNext()) {
        CacheClientProxy proxy = (CacheClientProxy)iter_prox.next();
        assertFalse("Dispatcher on secondary should not be alive",
            proxy._messageDispatcher.isAlive());
      }

    }
    catch (Exception ex) {
      fail("while setting verifyDispatcherIsNotAlive  " + ex);
    }
  }


    public static void createEntriesK1andK2OnServer()
    {
      try {
        Region r1 = cache.getRegion(Region.SEPARATOR+ REGION_NAME);
        assertNotNull(r1);
        if (!r1.containsKey(k1)) {
          r1.create(k1, server_k1);
        }
        if (!r1.containsKey(k2)) {
          r1.create(k2, server_k2);
        }
        assertEquals(r1.getEntry(k1).getValue(), server_k1);
        assertEquals(r1.getEntry(k2).getValue(), server_k2);
      }
      catch (Exception ex) {
        fail("failed while createEntries()", ex);
      }
    }

    public static void createEntriesK1andK2()
    {
      try {
        Region r1 = cache.getRegion(Region.SEPARATOR+ REGION_NAME);
        assertNotNull(r1);
        if (!r1.containsKey(k1)) {
          r1.create(k1, client_k1);
        }
        if (!r1.containsKey(k2)) {
          r1.create(k2, client_k2);
        }
        assertEquals(r1.getEntry(k1).getValue(), client_k1);
        assertEquals(r1.getEntry(k2).getValue(), client_k2);
      }
      catch (Exception ex) {
        fail("failed while createEntries()", ex);
      }
    }

    public static void createServerEntriesK1andK2()
    {
      try {
        Region r1 = cache.getRegion(Region.SEPARATOR+ REGION_NAME);
        assertNotNull(r1);
        if (!r1.containsKey(k1)) {
          r1.create(k1, server_k1);
        }
        if (!r1.containsKey(k2)) {
          r1.create(k2, server_k2);
        }
        assertEquals(r1.getEntry(k1).getValue(), server_k1);
        assertEquals(r1.getEntry(k2).getValue(), server_k2);
      }
      catch (Exception ex) {
        fail("failed while createEntries()", ex);
      }
    }

    public static void registerK1AndK2()
    {
      try {
        Region r = cache.getRegion(Region.SEPARATOR+REGION_NAME);
        assertNotNull(r);
        List list = new ArrayList();
        list.add(k1);
        list.add(k2);
        r.registerInterest(list, InterestResultPolicy.KEYS_VALUES);
      }
      catch (Exception ex) {
        ex.printStackTrace();
        fail("failed while region.registerK1AndK2()", ex);
      }
    }

    public static void reRegisterK1AndK2()
    {
      try {
        Region r = cache.getRegion(Region.SEPARATOR+REGION_NAME);
        assertNotNull(r);
        List list = new ArrayList();
        list.add(k1);
        list.add(k2);
        r.registerInterest(list);

      }
      catch (Exception ex) {
        fail("failed while region.reRegisterK1AndK2()", ex);
      }
    }

    public static void startServer()
    {
    try {
      Cache c = CacheFactory.getAnyInstance();
      assertEquals("More than one BridgeServer", 1, c.getBridgeServers().size());
      BridgeServerImpl bs = (BridgeServerImpl) c.getBridgeServers().iterator().next();
      assertNotNull(bs);
      bs.start();
    }
      catch (Exception ex) {
        fail("while startServer()  " + ex);
      }
    }

    public static void startServerAndPause()
    {
    try {
      Cache c = CacheFactory.getAnyInstance();
      assertEquals("More than one BridgeServer", 1, c.getBridgeServers().size());
      BridgeServerImpl bs = (BridgeServerImpl) c.getBridgeServers().iterator().next();
      assertNotNull(bs);
      bs.start();
      Thread.sleep(10000);
    }
      catch (Exception ex) {
        fail("while startServer()  " + ex);
      }
    }


    public static void stopServer()
    {
    try {
          assertEquals("More than one BridgeServer", 1, cache.getBridgeServers().size());
          BridgeServerImpl bs = (BridgeServerImpl) cache.getBridgeServers().iterator().next();
          assertNotNull(bs);
          bs.stop();
    }
      catch (Exception ex) {
        fail("while setting stopServer  " + ex);
      }
    }


    public static void stopPrimaryAndRegisterK1AndK2AndVerifyResponse()
    {
      try {
        LocalRegion r = (LocalRegion)cache.getRegion(Region.SEPARATOR+REGION_NAME);
        assertNotNull(r);
        ServerRegionProxy srp = new ServerRegionProxy(r);

        WaitCriterion wc = new WaitCriterion() {
          public boolean done() {
            return pool.getConnectedServerCount() == 3;
          }
          public String description() {
            return "connected server count never became 3";
          }
        };
        DistributedTestCase.waitForCriterion(wc, 30 * 1000, 1000, true);

        // close primaryEP
        getPrimaryVM().invoke(HAInterestBaseTest.class, "stopServer");
        List list = new ArrayList();
        list.add(k1);
        list.add(k2);
        List serverKeys = srp.registerInterest(list, InterestType.KEY,
          InterestResultPolicy.KEYS, false,
          r.getAttributes().getDataPolicy().ordinal);
        assertNotNull(serverKeys);
        List resultKeys =(List) serverKeys.get(0) ;
        assertEquals(2,resultKeys.size());
        assertTrue(resultKeys.contains(k1));
        assertTrue(resultKeys.contains(k2));

      }
      catch (Exception ex) {
        fail("failed while region.stopPrimaryAndRegisterK1AndK2AndVerifyResponse()", ex);
      }
    }

    public static void stopPrimaryAndUnregisterRegisterK1()
    {
      try {
        LocalRegion r = (LocalRegion)cache.getRegion(Region.SEPARATOR+REGION_NAME);
        assertNotNull(r);
        ServerRegionProxy srp = new ServerRegionProxy(r);

        WaitCriterion wc = new WaitCriterion() {
          public boolean done() {
            return pool.getConnectedServerCount() == 3;
          }
          public String description() {
            return "connected server count never became 3";
          }
        };
        DistributedTestCase.waitForCriterion(wc, 30 * 1000, 1000, true);

        // close primaryEP
        getPrimaryVM().invoke(HAInterestBaseTest.class, "stopServer");
        List list = new ArrayList();
        list.add(k1);
        srp.unregisterInterest(list, InterestType.KEY,false,false);
      }
      catch (Exception ex) {
        fail("failed while region.stopPrimaryAndUnregisterRegisterK1()", ex);
      }
    }

    public static void stopBothPrimaryAndSecondaryAndRegisterK1AndK2AndVerifyResponse()
    {
      try {
        LocalRegion r = (LocalRegion)cache.getRegion(Region.SEPARATOR+REGION_NAME);
        assertNotNull(r);
        ServerRegionProxy srp = new ServerRegionProxy(r);

        WaitCriterion wc = new WaitCriterion() {
          public boolean done() {
            return pool.getConnectedServerCount() == 3;
          }
          public String description() {
            return "connected server count never became 3";
          }
        };
        DistributedTestCase.waitForCriterion(wc, 30 * 1000, 1000, true);

        // close primaryEP
        VM backup = getBackupVM();
        getPrimaryVM().invoke(HAInterestBaseTest.class, "stopServer");
        //close secondary
        backup.invoke(HAInterestBaseTest.class, "stopServer");
        List list = new ArrayList();
        list.add(k1);
        list.add(k2);
        List serverKeys = srp.registerInterest(list, InterestType.KEY,
          InterestResultPolicy.KEYS, false,
          r.getAttributes().getDataPolicy().ordinal);

        assertNotNull(serverKeys);
        List resultKeys =(List) serverKeys.get(0) ;
        assertEquals(2,resultKeys.size());
        assertTrue(resultKeys.contains(k1));
        assertTrue(resultKeys.contains(k2));
       }
      catch (Exception ex) {
        fail("failed while region.stopBothPrimaryAndSecondaryAndRegisterK1AndK2AndVerifyResponse()", ex);
      }
    }
  /**
   * returns the secondary that was stopped
   */
    public static VM stopSecondaryAndRegisterK1AndK2AndVerifyResponse()
    {
      try {
        LocalRegion r = (LocalRegion)cache.getRegion(Region.SEPARATOR+REGION_NAME);
        assertNotNull(r);
        ServerRegionProxy srp = new ServerRegionProxy(r);

        WaitCriterion wc = new WaitCriterion() {
          public boolean done() {
            return pool.getConnectedServerCount() == 3;
          }
          public String description() {
            return "Never got three connected servers";
          }
        };
        DistributedTestCase.waitForCriterion(wc, 90 * 1000, 1000, true);
        
        // close secondary EP
        VM result = getBackupVM();
        result.invoke(HAInterestBaseTest.class, "stopServer");
        List list = new ArrayList();
        list.add(k1);
        list.add(k2);
        List serverKeys = srp.registerInterest(list, InterestType.KEY,
          InterestResultPolicy.KEYS,
          false, r.getAttributes().getDataPolicy().ordinal);

        assertNotNull(serverKeys);
        List resultKeys =(List) serverKeys.get(0) ;
        assertEquals(2,resultKeys.size());
        assertTrue(resultKeys.contains(k1));
        assertTrue(resultKeys.contains(k2));
        return result;
      }
      catch (Exception ex) {
        fail("failed while region.stopSecondaryAndRegisterK1AndK2AndVerifyResponse()", ex);
        return null;
      }
    }

  /**
   * returns the secondary that was stopped
   */
    public static VM stopSecondaryAndUNregisterK1()
    {
      try {
        LocalRegion r = (LocalRegion)cache.getRegion(Region.SEPARATOR+REGION_NAME);
        assertNotNull(r);
        ServerRegionProxy srp = new ServerRegionProxy(r);

        WaitCriterion wc = new WaitCriterion() {
          public boolean done() {
            return pool.getConnectedServerCount() == 3;
          }
          public String description() {
            return "connected server count never became 3";
          }
        };
        DistributedTestCase.waitForCriterion(wc, 30 * 1000, 1000, true);

        // close secondary EP
        VM result = getBackupVM();
        result.invoke(HAInterestBaseTest.class, "stopServer");
        List list = new ArrayList();
        list.add(k1);
        srp.unregisterInterest(list, InterestType.KEY,false,false);
        return result;
      }
      catch (Exception ex) {
        fail("failed while region.stopSecondaryAndUNregisterK1()", ex);
        return null;
      }
    }

    public static void registerK1AndK2OnPrimaryAndSecondaryAndVerifyResponse()
    {
      try {
        ServerLocation primary = pool.getPrimary();
        ServerLocation secondary = (ServerLocation)pool.getRedundants().get(0);
        LocalRegion r = (LocalRegion)cache.getRegion(Region.SEPARATOR+REGION_NAME);
        assertNotNull(r);
        ServerRegionProxy srp = new ServerRegionProxy(r);
        List list = new ArrayList();
        list.add(k1);
        list.add(k2);

        //Primary server
        List serverKeys1 = srp.registerInterestOn(primary, list,
          InterestType.KEY, InterestResultPolicy.KEYS, false, r.getAttributes()
              .getDataPolicy().ordinal);
        assertNotNull(serverKeys1);
        //expect serverKeys in response from primary
        List resultKeys =(List) serverKeys1.get(0) ;
        assertEquals(2,resultKeys.size());
        assertTrue(resultKeys.contains(k1));
        assertTrue(resultKeys.contains(k2));

        //Secondary server
        List serverKeys2 = srp.registerInterestOn(secondary, list,
          InterestType.KEY, InterestResultPolicy.KEYS, false, r.getAttributes()
              .getDataPolicy().ordinal);
        // if the list is null then it is empty
        if (serverKeys2 != null) {
          // no serverKeys in response from secondary
          assertTrue(serverKeys2.isEmpty());
        }


      }
      catch (Exception ex) {
        fail("failed while region.registerK1AndK2OnPrimaryAndSecondaryAndVerifyResponse()", ex);
      }
    }


      public static void verifyInterestRegistration()
      {
    try {
      WaitCriterion wc = new WaitCriterion() {
        String excuse;
        public boolean done() {
          return cache.getBridgeServers().size() == 1;
        }
        public String description() {
          return excuse;
        }
      };
      DistributedTestCase.waitForCriterion(wc, MAX_WAIT, 1000, true);

      BridgeServerImpl bs = (BridgeServerImpl)cache.getBridgeServers()
          .iterator().next();
      assertNotNull(bs);
      assertNotNull(bs.getAcceptor());
      assertNotNull(bs.getAcceptor().getCacheClientNotifier());
      final CacheClientNotifier ccn = bs.getAcceptor().getCacheClientNotifier();
      wc = new WaitCriterion() {
        String excuse;
        public boolean done() {
          return ccn.getClientProxies().size() > 0;
        }
        public String description() {
          return excuse;
        }
      };
      DistributedTestCase.waitForCriterion(wc, MAX_WAIT, 1000, true);

      Iterator iter_prox = ccn.getClientProxies().iterator();

      if (iter_prox.hasNext()) {
        final CacheClientProxy ccp =(CacheClientProxy)iter_prox.next();
        wc = new WaitCriterion() {
          String excuse;
          public boolean done() {
            Set keysMap = (Set)ccp.cils[RegisterInterestTracker.interestListIndex]
              .getProfile(Region.SEPARATOR + REGION_NAME)
              .getKeysOfInterestFor(ccp.getProxyID());
            return keysMap != null && keysMap.size() == 2;
          }
          public String description() {
            return excuse;
          }
        };
        DistributedTestCase.waitForCriterion(wc, MAX_WAIT, 1000, true);

        Set keysMap = (Set)ccp.cils[RegisterInterestTracker.interestListIndex]
          .getProfile( Region.SEPARATOR + REGION_NAME)
          .getKeysOfInterestFor(ccp.getProxyID());
        assertNotNull(keysMap);
        assertEquals(2, keysMap.size());
        assertTrue(keysMap.contains(k1));
        assertTrue(keysMap.contains(k2));
      }
    }
    catch (Exception ex) {
      fail("while setting verifyInterestRegistration  " + ex);
    }
  }

      public static void verifyInterestUNRegistration()
      {
    try {
      WaitCriterion wc = new WaitCriterion() {
        String excuse;
        public boolean done() {
          return cache.getBridgeServers().size() == 1;
        }
        public String description() {
          return excuse;
        }
      };
      DistributedTestCase.waitForCriterion(wc, MAX_WAIT, 1000, true);
      
      BridgeServerImpl bs = (BridgeServerImpl)cache.getBridgeServers()
          .iterator().next();
      assertNotNull(bs);
      assertNotNull(bs.getAcceptor());
      assertNotNull(bs.getAcceptor().getCacheClientNotifier());
      final CacheClientNotifier ccn = bs.getAcceptor().getCacheClientNotifier();
      wc = new WaitCriterion() {
        String excuse;
        public boolean done() {
          return ccn.getClientProxies().size() > 0;
        }
        public String description() {
          return excuse;
        }
      };
      DistributedTestCase.waitForCriterion(wc, MAX_WAIT, 1000, true);
      
      Iterator iter_prox = ccn.getClientProxies().iterator();
      if (iter_prox.hasNext()) {
        final CacheClientProxy ccp =(CacheClientProxy)iter_prox.next();
        wc = new WaitCriterion() {
          String excuse;
          public boolean done() {
            Set keysMap = (Set)ccp.cils[RegisterInterestTracker.interestListIndex]
              .getProfile(Region.SEPARATOR + REGION_NAME)
              .getKeysOfInterestFor(ccp.getProxyID());
            return keysMap != null;
          }
          public String description() {
            return excuse;
          }
        };
        DistributedTestCase.waitForCriterion(wc, MAX_WAIT, 1000, true);
        
        Set keysMap = (Set)ccp.cils[RegisterInterestTracker.interestListIndex]
          .getProfile(Region.SEPARATOR + REGION_NAME)
          .getKeysOfInterestFor(ccp.getProxyID());
        assertNotNull(keysMap);
        assertEquals(1, keysMap.size());
        assertFalse(keysMap.contains(k1));
        assertTrue(keysMap.contains(k2));
      }
    }
    catch (Exception ex) {
      fail("while setting verifyInterestUNRegistration  " + ex);
    }
  }

  private  void createCache(Properties props) throws Exception
  {
    DistributedSystem ds = getSystem(props);
    assertNotNull(ds);
    ds.disconnect();
    ds = getSystem(props);
    cache = CacheFactory.create(ds);
    assertNotNull(cache);
  }

  public static void createClientPoolCache(String testName,String host) throws Exception
  {
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "");
    new HAInterestBaseTest("temp").createCache(props);
    CacheServerTestUtil.disableShufflingOfEndpoints();
    PoolImpl p;
    try {
      p = (PoolImpl)PoolManager.createFactory()
        .addServer(host, PORT1)
        .addServer(host, PORT2)
        .addServer(host, PORT3)
        .setSubscriptionEnabled(true)
        .setSubscriptionRedundancy(-1)
        .setReadTimeout(1000)
        .setPingInterval(1000)
        // retryInterval should be more so that only registerInterste thread will initiate failover
        // .setRetryInterval(20000)
        .create("HAInterestBaseTestPool");
    } finally {
      CacheServerTestUtil.enableShufflingOfEndpoints();
    }
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    factory.setConcurrencyChecksEnabled(true);
    factory.setPoolName(p.getName());

    cache.createRegion(REGION_NAME, factory.create());
    pool = p;
    conn = pool.acquireConnection();
    assertNotNull(conn);
  }

  public static void createClientPoolCacheWithSmallRetryInterval(String testName,String host) throws Exception
  {
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "");
    new HAInterestBaseTest("temp").createCache(props);
    CacheServerTestUtil.disableShufflingOfEndpoints();
    PoolImpl p;
    try {
      p = (PoolImpl)PoolManager.createFactory()
        .addServer(host, PORT1)
        .addServer(host, PORT2)
        .setSubscriptionEnabled(true)
        .setSubscriptionRedundancy(-1)
        .setReadTimeout(1000)
        .setSocketBufferSize(32768)
        .setMinConnections(6)
        .setPingInterval(200)
        // .setRetryInterval(200)
        // retryAttempts 3
        .create("HAInterestBaseTestPool");
    } finally {
      CacheServerTestUtil.enableShufflingOfEndpoints();
    }
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    factory.setConcurrencyChecksEnabled(true);
    factory.setPoolName(p.getName());

    cache.createRegion(REGION_NAME, factory.create());

    pool = p;
    conn = pool.acquireConnection();
    assertNotNull(conn);
  }
  
  public static void createClientPoolCacheConnectionToSingleServer(String testName,String hostName) throws Exception
  {

    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "");
    new HAInterestBaseTest("temp").createCache(props);
    PoolImpl p = (PoolImpl)PoolManager.createFactory()
      .addServer(hostName, PORT1)
      .setSubscriptionEnabled(true)
      .setSubscriptionRedundancy(-1)
      .setReadTimeout(1000)
      // .setRetryInterval(20)
      .create("HAInterestBaseTestPool");
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    factory.setConcurrencyChecksEnabled(true);
    factory.setPoolName(p.getName());

    cache.createRegion(REGION_NAME, factory.create());

    pool = p;
    conn = pool.acquireConnection();
    assertNotNull(conn);
  }


  public static Integer createServerCache() throws Exception
  {
    new HAInterestBaseTest("temp").createCache(new Properties());
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setEnableBridgeConflation(true);
    factory.setMirrorType(MirrorType.KEYS_VALUES);
    factory.setConcurrencyChecksEnabled(true);
    cache.createRegion(REGION_NAME, factory.create());

    BridgeServer server = cache.addBridgeServer();
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET) ;
    server.setPort(port);
    server.setMaximumTimeBetweenPings(180000);
    // ensures updates to be sent instead of invalidations
    server.setNotifyBySubscription(true);
    server.start();
    return new Integer(server.getPort());

  }

  public static Integer createServerCacheWithLocalRegion() throws Exception
  {
    new HAInterestBaseTest("temp").createCache(new Properties());
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    factory.setConcurrencyChecksEnabled(true);
    RegionAttributes attrs = factory.create();
    cache.createRegion(REGION_NAME, attrs);

    BridgeServer server = cache.addBridgeServer();
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET) ;
    server.setPort(port);
    // ensures updates to be sent instead of invalidations
    server.setNotifyBySubscription(true);
    server.setMaximumTimeBetweenPings(180000);
    server.start();
    return new Integer(server.getPort());

  }



  public void tearDown2() throws Exception
  {
        super.tearDown2();
    // close the clients first
    closeCache();

    // then close the servers
    server1.invoke(HAInterestBaseTest.class, "closeCache");
    server2.invoke(HAInterestBaseTest.class, "closeCache");
    server3.invoke(HAInterestBaseTest.class, "closeCache");
    CacheServerTestUtil.resetDisableShufflingOfEndpointsFlag();
  }

  public static void closeCache()
  {
   PoolImpl.AFTER_REGISTER_CALLBACK_FLAG = false ;
   PoolImpl.BEFORE_PRIMARY_IDENTIFICATION_FROM_BACKUP_CALLBACK_FLAG = false ;
   PoolImpl.BEFORE_RECOVER_INTEREST_CALLBACK_FLAG = false ;
   PoolImpl.BEFORE_REGISTER_CALLBACK_FLAG = false ;
   HAInterestBaseTest.isAfterRegistrationCallbackCalled = false ;
   HAInterestBaseTest.isBeforeInterestRecoveryCallbackCalled = false ;
   HAInterestBaseTest.isBeforeRegistrationCallbackCalled = false ;
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
    }
  }
}



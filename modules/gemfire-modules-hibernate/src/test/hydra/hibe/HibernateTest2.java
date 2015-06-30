/*=========================================================================
 * Copyright (c) 2002-2014, Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package hibe;

import hydra.*;

import java.util.*;
import com.gemstone.gemfire.cache.*;

/**
 * A Hydra test interacts with Hiberate APIs.
 *
 * @see HibernatePrms
 *
 * @author lhughes
 * @since 6.5
 */
public class HibernateTest2 {
    
    /* The singleton instance of EventTest in this VM */
    static protected HibernateTest2 testInstance;
    
    protected boolean useTransactions;
    // cache whether this instance should perform all operations in one invocation
    // as a single transaction.
    protected boolean isSerialExecution;

    /**
     * STARTTASK for Hibernate (one time execution)
     */
    public synchronized static void HydraTask_startTask() {
       long counter = HibernateBB.getBB().getSharedCounters().incrementAndRead(HibernateBB.STARTTASK);
       Log.getLogWriter().info("invoked HydraTask_startTask(), STARTTASK counter = " + counter);
    }

    /**
     * Creates and {@linkplain #initialize initializes} the singleton
     * instance of <code>HibernateTest2</code> in this VM.
     */
    public synchronized static void HydraTask_initialize() {
       if (testInstance == null) {
           testInstance = new HibernateTest2();
           testInstance.initialize();
       }
    }
    
    /**
     * @see #HydraTask_initialize
     */
    protected void initialize() {
        String clientName = System.getProperty( "clientName" );
        useTransactions = HibernatePrms.useTransactions();
        isSerialExecution = TestConfig.tab().booleanAt(Prms.serialExecution, false);
        long counter = HibernateBB.getBB().getSharedCounters().incrementAndRead(HibernateBB.INITTASK);


        StringBuffer aStr = new StringBuffer();
        aStr.append("invoked initialize() in " + clientName + "\n");
        aStr.append("useTransactions = " + useTransactions + "\n");
        aStr.append("isSerialExecution = " + isSerialExecution + "\n");
        aStr.append("INITTASK counter = " + counter);

        Log.getLogWriter().info(aStr.toString());
    }

    /**
     * Initializes the test region in the peer VMs
     */
    public static void createPeerCache() {
       CacheHelper.createCache(ConfigPrms.getCacheConfig());
    }

    /**
     * Initializes the test region in the bridge server VM
     */
    public static void initBridgeServer() {
       // create cache from xml
       String cacheXmlFile = "$JTESTS/gemfirePlugins/server.xml";
       CacheHelper.createCacheFromXml(cacheXmlFile);
       //BridgeHelper.startBridgeServer(ConfigPrms.getBridgeConfig());
    }
    
    /**
     * TASK for hibernate test ... MasterController will continually
     * assign until totalTaskTimeSec.
     */
    public static synchronized void HydraTask_doOps() {
       testInstance.doOps();
    }

    private void doOps() {
       long counter = HibernateBB.getBB().getSharedCounters().incrementAndRead(HibernateBB.TASK);
       Log.getLogWriter().info("invoked doOps(), TASK counter = " + counter);
    }

    /** 
     * TASK for jpab (benchmark) test.  Simply wait for client 
     * to publish data.
     */
    public static synchronized void HydraTask_waitForJPAB() {
      MasterController.sleepForMs(10000);
      testInstance.displayRegions();
    }
    
    /**
     * CLOSETASK for validate regions/region sizes
     */
    public static synchronized void HydraTask_displayRegions() {
       testInstance.displayRegions();
    }

    private void displayRegions() {
       long counter = HibernateBB.getBB().getSharedCounters().incrementAndRead(HibernateBB.CLOSETASK);

       StringBuffer aStr = new StringBuffer();
       Set<Region<?,?>> rootRegions = CacheHelper.getCache().rootRegions();
       aStr.append("There are " + rootRegions.size() + " rootRegions:\n");

       for (Iterator i = rootRegions.iterator(); i.hasNext(); ) {
          Region r = (Region)i.next();
          aStr.append(r.getName() + " has " + r.entrySet().size() + " entries\n");
       }
       Log.getLogWriter().info(aStr.toString());
    }
    
    /**
     * CLOSETASK for hibernate test ... 
     */
    public static synchronized void HydraTask_closeTask() {
       testInstance.closeTask();
    }

    private void closeTask() {
       long counter = HibernateBB.getBB().getSharedCounters().incrementAndRead(HibernateBB.CLOSETASK);
       Log.getLogWriter().info("invoked closeTask(), CLOSETASK counter = " + counter);
    }
    
    /**
     * ENDTASK for hibernate test ... time to check BB counters!
     */
    public static synchronized void HydraTask_endTask() {
       testInstance = new HibernateTest2();
       testInstance.initialize();
       testInstance.endTask();
    }

    private void endTask() {
       HibernateBB.getBB().printSharedCounters();
    }
}

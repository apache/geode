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
import java.sql.*;
import java.io.*;
import com.gemstone.gemfire.*;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.internal.OSProcess;
import com.gemstone.gemfire.internal.cache.*;

import org.hibernate.FlushMode;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;

/**
 * A Hydra test interacts with Hiberate APIs.
 *
 * @see HibernatePrms
 *
 * @author lhughes
 * @since 6.5
 */
public class HibernateTest {
    
    /* The singleton instance of EventTest in this VM */
    static protected HibernateTest testInstance;
    
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
     * instance of <code>HibernateTest</code> in this VM.
     */
    public synchronized static void HydraTask_initialize() throws Exception {
       if (testInstance == null) {
           testInstance = new HibernateTest();
           testInstance.initialize();
       }
    }
    
    /**
     * @see #HydraTask_initialize
     */
    protected void initialize() throws Exception{
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
        Log.getLogWriter().info("Creating DB");
	createDatabase();
        Log.getLogWriter().info("Created DB");
    }
   
    /**
     * Initializes the test region in the peer VMs
     */
    public static void createPeerCache() {
       Cache c = CacheHelper.createCache(ConfigPrms.getCacheConfig());
       DiskStoreFactory dsf = c.createDiskStoreFactory();
       File f = new File("member"+RemoteTestModule.getMyVmid());
       f.mkdir();
       dsf.setDiskDirs(new File[] { f});
       dsf.create(DiskStoreFactory.DEFAULT_DISK_STORE_NAME);
    }
    
    /**
     * TASK for hibernate test ... MasterController will continually
     * assign until totalTaskTimeSec.
     */
    public static synchronized void HydraTask_doOps() {
       testInstance.doOps();
    }


static LogWriter log = null;

 public static void doNothing() throws Exception { }

 public static void testBasic() throws Exception {
    log = Log.getLogWriter();
    log.info("SWAP:creating session factory In hibernateTestCase");
    Session session = getSessionFactory().openSession();
    log.info("SWAP:session opened");
    // session.setFlushMode(FlushMode.COMMIT);
    session.beginTransaction();
    Event theEvent = new Event();
    theEvent.setTitle("title");
    theEvent.setDate(new java.util.Date());
    session.save(theEvent);
    Long id = theEvent.getId();
    session.getTransaction().commit();
    log.info("commit complete...doing load");
    session.beginTransaction();
    Event ev = (Event)session.load(Event.class, id);
    log.info("load complete: " + ev);
    log.info("SWAP");
    ev.setTitle("newTitle");
    session.save(ev);
    log.info("commit");
    session.getTransaction().commit();
    log.info("save complete " + ev);

    session.beginTransaction();
    ev = (Event)session.load(Event.class, id);
    log.info("load complete: " + ev);
    ev.setTitle("newTitle2");
    session.save(ev);
    log.info("commit");
    session.getTransaction().commit();
    log.info("save complete " + ev);

    ev = (Event)session.load(Event.class, id);
    log.info("second load " + ev);
    session.flush();
    session.disconnect();
    log.info("flush complete session:" + session);

    session = getSessionFactory().openSession();
    // ev = (Event) session.load(Event.class, id);
    ev = (Event)session.get(Event.class, id);
    log.info("third load " + ev);
    //printExistingDB();
    // System.in.read();
    // try direct data

  }
    private void doOps() {
       long counter = HibernateBB.getBB().getSharedCounters().incrementAndRead(HibernateBB.TASK);
       Log.getLogWriter().info("invoked doOps(), TASK counter = " + counter);
    }
    
    /**
     * CLOSETASK for hibernate test ... 
     */
    public static synchronized void HydraTask_closeTask() {
      if(testInstance!=null) {
       testInstance.closeTask();
      }
    }

    private void closeTask() {
       long counter = HibernateBB.getBB().getSharedCounters().incrementAndRead(HibernateBB.CLOSETASK);
       Log.getLogWriter().info("invoked closeTask(), CLOSETASK counter = " + counter);
    }
    
    /**
     * ENDTASK for hibernate test ... time to check BB counters!
     */
    public static synchronized void HydraTask_endTask() throws Exception {
       testInstance = new HibernateTest();
       testInstance.initialize();
       testInstance.endTask();
    }

    private void endTask() {
       HibernateBB.getBB().printSharedCounters();
    }

    private static SessionFactory getSessionFactory() throws Exception {
      Configuration cfg = new Configuration();
    cfg.setProperty("hibernate.dialect", "org.hibernate.dialect.DerbyDialect");
    cfg.setProperty("hibernate.connection.driver_class",
        "org.apache.derby.jdbc.EmbeddedDriver");
    cfg.setProperty("hibernate.connection.url", "jdbc:derby:sup;create=true");
    cfg.setProperty("hibernate.connection.pool_size", "1");
    cfg.setProperty("hibernate.connection.autocommit", "true");
    cfg.setProperty("hibernate.hbm2ddl.auto", "update");
    cfg.setProperty("hibernate.cache.region.factory_class",
        "com.gemstone.gemfire.modules.hibernate.GemFireRegionFactory");
    cfg.setProperty("hibernate.show_sql", "true");
    cfg.setProperty("hibernate.cache.use_query_cache", "true");
    cfg.setProperty("gemfire.locators", getLocatorString());
    cfg.setProperty("gemfire.mcast-port", "0");
    String strategy = HibernatePrms.getCachingStrategy();
    if(strategy!=null) {
      cfg.setProperty("gemfire.default-region-attributes-id",strategy);
    }
    //cfg.setProperty("gemfire.log-level", "fine");
    // cfg.setProperty("", "");
    cfg.addClass(Person.class);
    cfg.addClass(Event.class);
    return cfg.buildSessionFactory();
    }
    
    public static void validateQueryCacheRegion() throws Exception {
      Cache c = CacheHelper.getCache(); 
      Set regions = c.rootRegions();
      Region r = c.getRegion("/gemfire.hibernateQueryResults");
      if(r==null) {
        throw new Exception("query cache region not found!");
      }
    }
      
   public static void validateEventPersonRegions() throws Exception {
      validateEventPersonRegions(true);
    }
    public static void validateEventPersonRegionsOnPeers() throws Exception {
      validateEventPersonRegions(false);
    }
    private static void validateEventPersonRegions(boolean expectLocal) throws Exception { 
      Cache c = CacheHelper.getCache(); 
      Set regions = c.rootRegions();
      for (Object object : regions) {
        System.out.println("REGION BURGER:"+((Region)object).getFullPath());
      }
      
   
      Region r = c.getRegion("/__regionConfigurationMetadata");
      if(r==null) {
        throw new Exception ("Metadata region is null");
      }
      validateRegion("/hibe.Event", expectLocal);
      validateRegion("/hibe.Person", expectLocal);
      validateRegion("/hibe.Person.events", expectLocal);
      
    }

  private static void validateRegion(String regionName, boolean expectLocal) throws Exception {
    /*
     * REPLICATE,
     * REPLICATE_PERSISTENT,
     * REPLICATE_PROXY,
     * PARTITION,
     * PARTITION_PERSISTENT,
     * PARTITION_REDUNDANT, 
     * PARTITION_REDUNDANT_PERSISTENT, 
     * PARTITION_PROXY_REDUNDANT,
     * PARTITION_PROXY, 
     * LOCAL, 
     * LOCAL_PERSISTENT
     * 
     * HEAP_LRU
     * 
     */
    String strategy = HibernatePrms.getCachingStrategy();
    boolean isLocal = strategy.contains("LOCAL");
    Cache c = CacheHelper.getCache(); 
    Region r = c.getRegion(regionName);
    if(!isLocal && r == null) {
      throw new Exception(regionName+" region not found!");
    } else if (isLocal) {
      if (expectLocal && r == null) {
        throw new Exception("expected region:"+ regionName+" to be null");
      } else {
        return;
      }
    }
    boolean partition = false;
    boolean persistent = false;
    boolean overflow = false;
    boolean heapLru = false;
    boolean local = false;
    boolean redundant = false;
    
    
    System.out.println("VALIDATIN STRAT:"+strategy+" Regger:"+r.getClass());
    if(strategy.indexOf("PARTITION")>-1) {
      partition = true;
    }
    if(strategy.indexOf("PERSISTENT")>-1) {
      persistent = true;
    }
    
    
    if(strategy.indexOf("OVERFLOW")>-1) {
      overflow = true;
    }
    
    
    if(strategy.indexOf("HEAP")>-1) {
      heapLru = true;
    }

    if(strategy.indexOf("LOCAL")>-1) {
      local = true;
    }
    
    if(strategy.indexOf("REDUNDANT")>-1) {
      redundant = true;
    }
    
    
    
      RegionAttributes ra = r.getAttributes();
      if(ra.getPartitionAttributes()==null && partition) {
        throw new Exception("Supposed to be partition, but no partition attributes");
      } else if(!partition && ra.getPartitionAttributes()!=null) {
        throw new Exception("Supposed to be !partition but partition attributes exist");
      } else if(!partition) {
        if(local) {
          if(ra.getScope()!=Scope.LOCAL) {
            throw new Exception("Scope should have been LOCAL, but it is:"+ra.getScope());
          }
        } else if(ra.getScope()!=Scope.DISTRIBUTED_ACK) {
          throw new Exception("Scope should have been D_ACK, but it is:"+ra.getScope());
        }
      } else if(partition && redundant) {
        //ok we are chill and partitioned
        if(ra.getPartitionAttributes().getRedundantCopies()==0) {
          throw new Exception("Redundant copies should have been greater than 0");
        }
      }
      
      if(ra.getPersistBackup() && !persistent) {
        throw new Exception("Was supposed to be !persistent, but it is!");
      } else if(!ra.getPersistBackup() && persistent) {
        throw new Exception("Was supposed to be persistent, but it isn't!");
      }

      if(overflow) {
        EvictionAttributes ea = ra.getEvictionAttributes();
        if(ea.getAlgorithm()==EvictionAlgorithm.NONE || ea.getAction()!=EvictionAction.OVERFLOW_TO_DISK) {
          throw new Exception("Overflow should have been on, but wasn't");
        } 
      } else if(!heapLru) {
        EvictionAttributes ea = ra.getEvictionAttributes();
        if(ea.getAlgorithm()!=EvictionAlgorithm.NONE) {
          throw new Exception("EvictionAttributes should have been null");
        }
      }
      
      if(heapLru) {
        EvictionAttributes ea = ra.getEvictionAttributes();
        if(ea.getAlgorithm()==EvictionAlgorithm.NONE) {
          throw new Exception("Eviction should have been on, but wasn't");
        }
        EvictionAlgorithm eaa = ea.getAlgorithm();
        
        if(eaa==null || eaa!=EvictionAlgorithm.LRU_HEAP) {
          throw new Exception("heap lru should have been on, but wasn't");
        } 
      } else if(!overflow) {
        EvictionAttributes ea = ra.getEvictionAttributes();
        if(ea.getAlgorithm()!=EvictionAlgorithm.NONE) {
          throw new Exception("EvictionAttributes should have been null");
        }
      }
      
      
      
      
    }

  /**
   * Finds the locator endpoint for this VM from the shared {@link
   * DistributedSystemBlackboard} map, if it exists.  Caches the result.
   */
  private static synchronized String getLocatorString() {
      Integer vmid = new Integer(5);
      DistributedSystemHelper.Endpoint TheLocatorEndpoint = (DistributedSystemHelper.Endpoint)DistributedSystemBlackboard.getInstance()
                                    .getSharedMap().get(vmid);
    return TheLocatorEndpoint.getAddress()+"["+TheLocatorEndpoint.getPort()+"]";
  }

        private static void createDatabase() throws Exception {
                // Extract all the persistence unit properties:
                final Properties properties = new Properties();
 		properties.put("javax.persistence.jdbc.driver","org.apache.derby.jdbc.EmbeddedDriver");
 		properties.put("javax.persistence.jdbc.url","jdbc:derby:/export/monaco1/users/lhughes/jpa/jpab/temp/work/derby/jpab1379523664;create=true");
                // Load the JDBC driver:
                String driver = properties.getProperty("javax.persistence.jdbc.driver");
                if (driver == null) {
                        return; // cannot connect to the server
                }
                Class.forName(driver).newInstance();

                // Build the connection url:
                String url = properties.getProperty("javax.persistence.jdbc.url");
                int dbNamePos = url.lastIndexOf('/') + 1;
                int dbNameEndPos = url.lastIndexOf(';');
                String dbName = (dbNameEndPos < 0) ?
                        url.substring(dbNamePos) : url.substring(dbNamePos, dbNameEndPos);
                url = url.substring(0, dbNamePos - 1);
                url += "?user=" + properties.getProperty("javax.persistence.jdbc.user");
                url += "&password=" + properties.getProperty(
                        "javax.persistence.jdbc.password");

                // Try to create the database:
                try {
                        Connection con = DriverManager.getConnection(url);
                        Statement s = con.createStatement();
                        s.executeUpdate("CREATE DATABASE " + dbName);
                }
                catch (Exception e) {
                        // silently ignored - database may be created automatically
                }
        }
}

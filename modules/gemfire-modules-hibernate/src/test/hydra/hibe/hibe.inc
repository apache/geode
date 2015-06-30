hydra.Prms-testRequirement = "Starts a locator and p2p clients in a single DS";
hydra.Prms-testDescription = " TBD ";

INCLUDE $JTESTS/hydraconfig/hydraparams1.inc;
INCLUDE $JTESTS/hydraconfig/topology_p2p_2_locator.inc;

/**
 * Start the locators and connect them to distributed system.
 */
INITTASK     taskClass = hydra.DistributedSystemHelper taskMethod = createLocator
             threadGroups = locator;

INITTASK     taskClass = hydra.DistributedSystemHelper taskMethod = startLocatorAndDS
             threadGroups = locator;

INITTASK     taskClass   = hibe.HibernateTest taskMethod  = HydraTask_initialize
             threadGroups = hibernate
             ;

INITTASK     taskClass   = hibe.HibernateTest taskMethod  = createPeerCache
             threadGroups = locator,clients
             ;

TASK         taskClass   = hibe.HibernateTest taskMethod  = doNothing
	     threadGroups = clients,locator
	     maxTimesToRun = 1
             ;


CLOSETASK    taskClass   = hibe.HibernateTest taskMethod  = validateEventPersonRegions 
             threadGroups = hibernate
             ;
             
CLOSETASK    taskClass   = hibe.HibernateTest taskMethod  = validateEventPersonRegionsOnPeers 
             threadGroups = clients
             ;

CLOSETASK    taskClass   = hibe.HibernateTest taskMethod  = validateQueryCacheRegion 
             threadGroups = hibernate
             ;       
             
             
             
CLOSETASK    taskClass   = hibe.HibernateTest taskMethod  = HydraTask_closeTask 
             threadGroups = clients
             ;

CLOSETASK    taskClass   = hydra.DistributedSystemHelper taskMethod  = stopLocator
             threadGroups = locator;
             ;


hydra.Prms-totalTaskTimeSec = 600;
hydra.Prms-maxResultWaitSec = 180;

// test controls the locator (create/start/stop)
hydra.Prms-manageLocatorAgents = false; // turn off master-managed locators

// define a cache (no regions)
hydra.ConfigPrms-cacheConfig = gemfireCache;
hydra.CachePrms-names        = gemfireCache;

// all are peers in a single DS
hydra.GemFirePrms-distributedSystem = ds;

THREADGROUP locator
    totalThreads = fcn ${locatorHosts} * ${locatorVMsPerHost}
                                       * ${locatorThreadsPerVM}
                   ncf     
    clientNames  = fcn "hydra.TestConfigFcns.generateNames
                        (\"locator\", ${locatorHosts}, true)"
                   ncf;    
THREADGROUP clients
    totalThreads = fcn ${clientHosts} * ${clientVMsPerHost}
                                    * ${clientThreadsPerVM}
                   ncf     
    clientNames  = fcn "hydra.TestConfigFcns.generateNames
                        (\"client\", ${clientHosts}, true)"
                   ncf;

THREADGROUP hibernate
    totalThreads = fcn ${hibernateHosts} * ${hibernateVMsPerHost}
                                    * ${hibernateThreadsPerVM}
                   ncf
    clientNames  = fcn "hydra.TestConfigFcns.generateNames
                        (\"hibernate\", ${hibernateHosts}, true)"
                   ncf;




// Control logging in hydra controlled portion of test
//hydra.log.LogPrms-file_logLevel = fine;
hydra.GemFirePrms-logLevel = fine;
//hydra.VmPrms-extraVMArgs += "-DDistributionManager.VERBOSE=true";
//hydra.VmPrms-extraVMArgs += "-DDistributionManager.DEBUG_JAVAGROUPS=true";


// need same jars as the jpab (jpa benchmark) 
hydra.VmPrms-extraClassPaths += /export/java/users/java_share/jpa/jpab/lib/antlr-2.7.6.jar;
hydra.VmPrms-extraClassPaths += /export/java/users/java_share/jpa/jpab/lib/c3p0-0.9.1.jar;
hydra.VmPrms-extraClassPaths += /export/java/users/java_share/jpa/jpab/lib/cglib-2.2.jar;
hydra.VmPrms-extraClassPaths += /export/java/users/java_share/jpa/jpab/lib/commons-collections-3.1.jar;
hydra.VmPrms-extraClassPaths += /export/java/users/java_share/jpa/jpab/lib/dom4j-1.6.1.jar;
hydra.VmPrms-extraClassPaths += /export/java/users/java_share/jpa/jpab/lib/ehcache-core-2.2.0.jar;
hydra.VmPrms-extraClassPaths += /export/mclaren1/users/sbawaska/gemfireModules/gemfire-modules/target/gemfire-modules-2.1.jar;
hydra.VmPrms-extraClassPaths += /home/sbawaska/.m2/repository/org/hibernate/hibernate-core/3.5.0-Final/hibernate-core-3.5.0-Final.jar;
hydra.VmPrms-extraClassPaths += /export/java/users/java_share/jpa/jpab/lib/hibernate-index-annotation.jar;
hydra.VmPrms-extraClassPaths += /export/java/users/java_share/jpa/jpab/lib/hibernate-jpa-2.0-api-1.0.0.Final.jar;
hydra.VmPrms-extraClassPaths += /export/java/users/java_share/jpa/jpab/lib/infinispan-core-4.0.0.FINAL.jar;
hydra.VmPrms-extraClassPaths += /export/java/users/java_share/jpa/jpab/lib/javassist-3.9.0.GA.jar;
hydra.VmPrms-extraClassPaths += /export/java/users/java_share/jpa/jpab/lib/jbosscache-core-3.2.1.GA.jar;
hydra.VmPrms-extraClassPaths += /export/java/users/java_share/jpa/jpab/lib/jdo2-index.jar;
hydra.VmPrms-extraClassPaths += /export/java/users/java_share/jpa/jpab/lib/jpa2.jar;
hydra.VmPrms-extraClassPaths += /export/java/users/java_share/jpa/jpab/lib/jta-1.1.jar;
hydra.VmPrms-extraClassPaths += /export/java/users/java_share/jpa/jpab/lib/openjpa-index-annotation.jar;
hydra.VmPrms-extraClassPaths += /export/java/users/java_share/jpa/jpab/lib/oscache-2.1.jar;
hydra.VmPrms-extraClassPaths += /export/java/users/java_share/jpa/jpab/lib/proxool-0.8.3.jar;
hydra.VmPrms-extraClassPaths += /export/java/users/java_share/jpa/jpab/lib/slf4j-api-1.5.8.jar;
hydra.VmPrms-extraClassPaths += /export/java/users/java_share/jpa/jpab/lib/slf4j-jdk14-1.5.8.jar;
hydra.VmPrms-extraClassPaths += /export/java/users/java_share/jpa/jpab/lib/swarmcache-1.0RC2.jar;

// jpa test classes
hydra.VmPrms-extraClassPaths += /export/java/users/java_share/jpa/jpab/jpab.jar;

// hibernate jar from jpa/Hibernate/lib
hydra.VmPrms-extraClassPaths += /export/java/users/java_share/jpa/jpab/jpa/Hibernate/lib/hibernate3.jar;
hydra.VmPrms-extraClassPaths += /export/gcm/where/java/derby/derby-10.4.2.0/jars/insane/derby.jar;

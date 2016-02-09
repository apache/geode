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
package com.gemstone.gemfire.test.dunit;

import java.text.DecimalFormat;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.logging.log4j.Logger;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.admin.internal.AdminDistributedSystemImpl;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.HoplogConfig;
import com.gemstone.gemfire.cache.query.QueryTestUtils;
import com.gemstone.gemfire.cache.query.internal.QueryObserverHolder;
import com.gemstone.gemfire.cache30.ClientServerTestCase;
import com.gemstone.gemfire.cache30.GlobalLockingDUnitTest;
import com.gemstone.gemfire.cache30.MultiVMRegionTestCase;
import com.gemstone.gemfire.cache30.RegionTestCase;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.DistributionMessageObserver;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem.CreationStackGenerator;
import com.gemstone.gemfire.internal.SocketCreator;
import com.gemstone.gemfire.internal.admin.ClientStatsManager;
import com.gemstone.gemfire.internal.cache.DiskStoreObserver;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.HARegion;
import com.gemstone.gemfire.internal.cache.InitialImageOperation;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.tier.InternalClientMembership;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheServerTestUtil;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientProxyMembershipID;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheCreation;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.management.internal.cli.LogWrapper;
import com.gemstone.gemfire.test.dunit.standalone.DUnitLauncher;
import com.gemstone.gemfire.test.junit.categories.DistributedTest;

import junit.framework.TestCase;

/**
 * This class is the superclass of all distributed unit tests.
 * 
 * tests/hydra/JUnitTestTask is the main DUnit driver. It supports two 
 * additional public static methods if they are defined in the test case:
 * 
 * public static void caseSetUp() -- comparable to JUnit's BeforeClass annotation
 * 
 * public static void caseTearDown() -- comparable to JUnit's AfterClass annotation
 *
 * @author David Whitlock
 */
@Category(DistributedTest.class)
@SuppressWarnings("serial")
public abstract class DistributedTestCase extends TestCase implements java.io.Serializable {
  
  private static final Logger logger = LogService.getLogger();
  
  private static final Set<String> testHistory = new LinkedHashSet<String>();

  /** This VM's connection to the distributed system */
  public static InternalDistributedSystem system;
  private static Class lastSystemCreatedInTest;
  private static Properties lastSystemProperties;
  private static volatile String testMethodName;
  
  /** For formatting timing info */
  private static final DecimalFormat format = new DecimalFormat("###.###");

  public static boolean reconnect = false;

  public static final boolean logPerTest = Boolean.getBoolean("dunitLogPerTest");

  /**
   * Creates a new <code>DistributedTestCase</code> test with the
   * given name.
   */
  public DistributedTestCase(final String name) {
    super(name);
    DUnitLauncher.launchIfNeeded();
  }

  //---------------------------------------------------------------------------
  // methods for tests
  //---------------------------------------------------------------------------
  
  public final void setSystem(final Properties props, final DistributedSystem ds) { // TODO: override getDistributedSystemProperties and then delete
    system = (InternalDistributedSystem)ds;
    lastSystemProperties = props;
    lastSystemCreatedInTest = getClass(); // used to be getDeclaringClass()
  }
  
  /**
   * Returns this VM's connection to the distributed system.  If
   * necessary, the connection will be lazily created using the given
   * <code>Properties</code>.  Note that this method uses hydra's
   * configuration to determine the location of log files, etc.
   * Note: "final" was removed so that WANTestBase can override this method.
   * This was part of the xd offheap merge.
   *
   * see hydra.DistributedConnectionMgr#connect
   * @since 3.0
   */
  public /*final*/ InternalDistributedSystem getSystem(final Properties props) { // TODO: make final
    // Setting the default disk store name is now done in setUp
    if (system == null) {
      system = InternalDistributedSystem.getAnyInstance();
    }
    if (system == null || !system.isConnected()) {
      // Figure out our distributed system properties
      Properties p = DistributedTestUtils.getAllDistributedSystemProperties(props);
      lastSystemCreatedInTest = getClass(); // used to be getDeclaringClass()
      if (logPerTest) {
        String testMethod = getTestMethodName();
        String testName = lastSystemCreatedInTest.getName() + '-' + testMethod;
        String oldLogFile = p.getProperty(DistributionConfig.LOG_FILE_NAME);
        p.put(DistributionConfig.LOG_FILE_NAME, 
            oldLogFile.replace("system.log", testName+".log"));
        String oldStatFile = p.getProperty(DistributionConfig.STATISTIC_ARCHIVE_FILE_NAME);
        p.put(DistributionConfig.STATISTIC_ARCHIVE_FILE_NAME, 
            oldStatFile.replace("statArchive.gfs", testName+".gfs"));
      }
      system = (InternalDistributedSystem)DistributedSystem.connect(p);
      lastSystemProperties = p;
    } else {
      boolean needNewSystem = false;
      if(!getClass().equals(lastSystemCreatedInTest)) { // used to be getDeclaringClass()
        Properties newProps = DistributedTestUtils.getAllDistributedSystemProperties(props);
        needNewSystem = !newProps.equals(lastSystemProperties);
        if(needNewSystem) {
          LogWriterUtils.getLogWriter().info(
              "Test class has changed and the new DS properties are not an exact match. "
                  + "Forcing DS disconnect. Old props = "
                  + lastSystemProperties + "new props=" + newProps);
        }
      } else {
        Properties activeProps = system.getProperties();
        for (Iterator iter = props.entrySet().iterator();
        iter.hasNext(); ) {
          Map.Entry entry = (Map.Entry) iter.next();
          String key = (String) entry.getKey();
          String value = (String) entry.getValue();
          if (!value.equals(activeProps.getProperty(key))) {
            needNewSystem = true;
            LogWriterUtils.getLogWriter().info("Forcing DS disconnect. For property " + key
                                + " old value = " + activeProps.getProperty(key)
                                + " new value = " + value);
            break;
          }
        }
      }
      if(needNewSystem) {
        // the current system does not meet our needs to disconnect and
        // call recursively to get a new system.
        LogWriterUtils.getLogWriter().info("Disconnecting from current DS in order to make a new one");
        disconnectFromDS();
        getSystem(props);
      }
    }
    return system;
  }

  /**
   * Returns this VM's connection to the distributed system.  If
   * necessary, the connection will be lazily created using the
   * <code>Properties</code> returned by {@link
   * #getDistributedSystemProperties}.
   *
   * @see #getSystem(Properties)
   *
   * @since 3.0
   */
  public final InternalDistributedSystem getSystem() {
    return getSystem(getDistributedSystemProperties());
  }

  /**
   * Returns a loner distributed system that isn't connected to other
   * vms
   * 
   * @since 6.5
   */
  public final InternalDistributedSystem getLonerSystem() {
    Properties props = getDistributedSystemProperties();
    props.put(DistributionConfig.MCAST_PORT_NAME, "0");
    props.put(DistributionConfig.LOCATORS_NAME, "");
    return getSystem(props);
  }
  
  /**
   * Returns a loner distributed system in combination with enforceUniqueHost
   * and redundancyZone properties.
   * Added specifically to test scenario of defect #47181.
   */
  public final InternalDistributedSystem getLonerSystemWithEnforceUniqueHost() {
    Properties props = getDistributedSystemProperties();
    props.put(DistributionConfig.MCAST_PORT_NAME, "0");
    props.put(DistributionConfig.LOCATORS_NAME, "");
    props.put(DistributionConfig.ENFORCE_UNIQUE_HOST_NAME, "true");
    props.put(DistributionConfig.REDUNDANCY_ZONE_NAME, "zone1");
    return getSystem(props);
  }

  /**
   * Returns whether or this VM is connected to a {@link
   * DistributedSystem}.
   */
  public final boolean isConnectedToDS() {
    return system != null && system.isConnected();
  }

  /**
   * Returns a <code>Properties</code> object used to configure a
   * connection to a {@link
   * com.gemstone.gemfire.distributed.DistributedSystem}.
   * Unless overridden, this method will return an empty
   * <code>Properties</code> object.
   *
   * @since 3.0
   */
  public Properties getDistributedSystemProperties() {
    return new Properties();
  }

  public static void disconnectAllFromDS() {
    disconnectFromDS();
    Invoke.invokeInEveryVM(()->disconnectFromDS());
  }

  /**
   * Disconnects this VM from the distributed system
   */
  public static void disconnectFromDS() {
    setTestMethodName(null);
    GemFireCacheImpl.testCacheXml = null;
    if (system != null) {
      system.disconnect();
      system = null;
    }
    
    for (;;) {
      DistributedSystem ds = InternalDistributedSystem.getConnectedInstance();
      if (ds == null) {
        break;
      }
      try {
        ds.disconnect();
      } catch (Exception e) {
        // ignore
      }
    }
    
    AdminDistributedSystemImpl ads = AdminDistributedSystemImpl.getConnectedInstance();
    if (ads != null) {// && ads.isConnected()) {
      ads.disconnect();
    }
  }

  //---------------------------------------------------------------------------
  // name methods
  //---------------------------------------------------------------------------
  
  public static String getTestMethodName() {
    return testMethodName;
  }

  public static void setTestMethodName(final String testMethodName) { // TODO: delete
    DistributedTestCase.testMethodName = testMethodName;
  }
  
  /**
   * Returns a unique name for this test method.  It is based on the
   * name of the class as well as the name of the method.
   */
  public String getUniqueName() {
    return getClass().getSimpleName() + "_" + getName();
  }

  //---------------------------------------------------------------------------
  // setup methods
  //---------------------------------------------------------------------------
  
  /**
   * Sets up the DistributedTestCase.
   * <p>
   * Do not override this method. Override {@link #preSetUp()} with work that
   * needs to occur before setUp() or override {@link #postSetUp()} with work
   * that needs to occur after setUp().
   */
  @Override
  public void setUp() throws Exception {
    preSetUp();
    setUpDistributedTestCase();
    postSetUp();
  }
  
  /**
   * Sets up DistributedTest in controller and remote VMs. This includes the
   * defining the test name, setting the default disk store name, logging the 
   * test history, and capturing a creation stack for detecting the source of
   * incompatible DistributedSystem connections.
   * <p>
   * Do not override this method.
   */
  private final void setUpDistributedTestCase() {
    final String className = getClass().getCanonicalName();
    final String methodName = getName();
    
    logTestHistory();
    
    setUpVM(methodName, getDefaultDiskStoreName(0, -1, className, methodName));
    
    for (int hostIndex = 0; hostIndex < Host.getHostCount(); hostIndex++) {
      Host host = Host.getHost(hostIndex);
      for (int vmIndex = 0; vmIndex < host.getVMCount(); vmIndex++) {
        final String vmDefaultDiskStoreName = getDefaultDiskStoreName(hostIndex, vmIndex, className, methodName);
        host.getVM(vmIndex).invoke(()->setUpVM(methodName, vmDefaultDiskStoreName));
      }
    }
    
    logTestStart();
  }

  /**
   * <code>preSetUp()</code> is invoked before {@link #setUpDistributedTestCase()}.
   * <p>
   * Override this as needed. Default implementation is empty.
   */
  protected void preSetUp() throws Exception {
  }
  
  /**
   * <code>postSetUp()</code> is invoked after {@link #setUpDistributedTestCase()}.
   * <p>
   * Override this as needed. Default implementation is empty.
   */
  protected void postSetUp() throws Exception {
  }
  
  private static String getDefaultDiskStoreName(final int hostIndex, final int vmIndex, final String className, final String methodName) {
    return "DiskStore-" + String.valueOf(hostIndex) + "-" + String.valueOf(vmIndex) + "-" + className + "." + methodName; // used to be getDeclaringClass()
  }
  
  private static void setUpVM(final String methodName, final String defaultDiskStoreName) {
    setTestMethodName(methodName);
    GemFireCacheImpl.setDefaultDiskStoreName(defaultDiskStoreName);
    System.setProperty(HoplogConfig.ALLOW_LOCAL_HDFS_PROP, "true");    
    setUpCreationStackGenerator();
  }

  private void logTestStart() {
    System.out.println("\n\n[setup] START TEST " + getClass().getSimpleName()+"."+testMethodName+"\n\n");
  }
  
  private static void setUpCreationStackGenerator() {
    // the following is moved from InternalDistributedSystem to fix #51058
    InternalDistributedSystem.TEST_CREATION_STACK_GENERATOR.set(
    new CreationStackGenerator() {
      @Override
      public Throwable generateCreationStack(final DistributionConfig config) {
        final StringBuilder sb = new StringBuilder();
        final String[] validAttributeNames = config.getAttributeNames();
        for (int i = 0; i < validAttributeNames.length; i++) {
          final String attName = validAttributeNames[i];
          final Object actualAtt = config.getAttributeObject(attName);
          String actualAttStr = actualAtt.toString();
          sb.append("  ");
          sb.append(attName);
          sb.append("=\"");
          if (actualAtt.getClass().isArray()) {
            actualAttStr = InternalDistributedSystem.arrayToString(actualAtt);
          }
          sb.append(actualAttStr);
          sb.append("\"");
          sb.append("\n");
        }
        return new Throwable("Creating distributed system with the following configuration:\n" + sb.toString());
      }
    });
  }
  
  /**
   * Write a message to the log about what tests have ran previously. This
   * makes it easier to figure out if a previous test may have caused problems
   */
  private void logTestHistory() {
    String classname = getClass().getSimpleName();
    testHistory.add(classname);
    System.out.println("Previously run tests: " + testHistory);
  }

  //---------------------------------------------------------------------------
  // teardown methods
  //---------------------------------------------------------------------------

  /**
   * Tears down the DistributedTestCase.
   * <p>
   * Do not override this method. Override {@link #preTearDown()} with work that
   * needs to occur before tearDown() or override {@link #postTearDown()} with work
   * that needs to occur after tearDown().
   */
  @Override
  public final void tearDown() throws Exception {
    preTearDown();
    tearDownDistributedTestCase();
    postTearDown();
  }

  private final void tearDownDistributedTestCase() throws Exception {
    Invoke.invokeInEveryVM(()->tearDownCreationStackGenerator());
    if (logPerTest) {
      disconnectFromDS();
      Invoke.invokeInEveryVM(()->disconnectFromDS());
    }
    cleanupAllVms();
  }
  
  /**
   * <code>preTearDown()</code> is invoked before {@link #tearDownDistributedTestCase()}.
   * <p>
   * Override this as needed. Default implementation is empty.
   */
  protected void preTearDown() throws Exception {
  }
  
  /**
   * <code>postTearDown()</code> is invoked after {@link #tearDownDistributedTestCase()}.
   * <p>
   * Override this as needed. Default implementation is empty.
   */
  protected void postTearDown() throws Exception {
  }
  
  public static void cleanupAllVms() { // TODO: make private
    tearDownVM();
    Invoke.invokeInEveryVM(()->tearDownVM());
    Invoke.invokeInLocator(()->{
      DistributionMessageObserver.setInstance(null);
      DistributedTestUtils.unregisterInstantiatorsInThisVM();
    });
    DUnitLauncher.closeAndCheckForSuspects();
  }

  private static void tearDownVM() {
    closeCache();

    // keep alphabetized to detect duplicate lines
    CacheCreation.clearThreadLocals();
    CacheServerTestUtil.clearCacheReference();
    ClientProxyMembershipID.system = null;
    ClientServerTestCase.AUTO_LOAD_BALANCE = false;
    ClientStatsManager.cleanupForTests();
    DiskStoreObserver.setInstance(null);
    DistributedTestUtils.unregisterInstantiatorsInThisVM();
    DistributionMessageObserver.setInstance(null);
    GlobalLockingDUnitTest.region_testBug32356 = null;
    InitialImageOperation.slowImageProcessing = 0;
    InternalClientMembership.unregisterAllListeners();
    LogWrapper.close();
    MultiVMRegionTestCase.CCRegion = null;
    QueryObserverHolder.reset();
    QueryTestUtils.setCache(null);
    RegionTestCase.preSnapshotRegion = null;
    SocketCreator.resetHostNameCache();
    SocketCreator.resolve_dns = true;

    // clear system properties -- keep alphabetized
    System.clearProperty("gemfire.log-level");
    System.clearProperty(HoplogConfig.ALLOW_LOCAL_HDFS_PROP);    
    System.clearProperty("jgroups.resolve_dns");
    
    if (InternalDistributedSystem.systemAttemptingReconnect != null) {
      InternalDistributedSystem.systemAttemptingReconnect.stopReconnecting();
    }
    
    IgnoredException.removeAllExpectedExceptions();
  }

  private static void closeCache() {
    GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
    if (cache != null && !cache.isClosed()) {
      destroyRegions(cache);
      cache.close();
    }
  }
  
  protected static final void destroyRegions(final Cache cache) { // TODO: make private
    if (cache != null && !cache.isClosed()) {
      // try to destroy the root regions first so that we clean up any persistent files.
      for (Iterator itr = cache.rootRegions().iterator(); itr.hasNext();) {
        Region root = (Region)itr.next();
        String regionFullPath = root == null ? null : root.getFullPath();
        // for colocated regions you can't locally destroy a partitioned region.
        if(root.isDestroyed() || root instanceof HARegion || root instanceof PartitionedRegion) {
          continue;
        }
        try {
          root.localDestroyRegion("teardown");
        } catch (Throwable t) {
          logger.error("Failure during tearDown destroyRegions for " + regionFullPath, t);
        }
      }
    }
  }
  
  private static void tearDownCreationStackGenerator() {
    InternalDistributedSystem.TEST_CREATION_STACK_GENERATOR.set(InternalDistributedSystem.DEFAULT_CREATION_STACK_GENERATOR);
  }
}

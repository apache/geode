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

import java.io.File;
import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.net.UnknownHostException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.InternalGemFireError;
import com.gemstone.gemfire.LogWriter;
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
import com.gemstone.gemfire.distributed.internal.DistributionConfigImpl;
import com.gemstone.gemfire.distributed.internal.DistributionMessageObserver;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem.CreationStackGenerator;
import com.gemstone.gemfire.distributed.internal.membership.gms.MembershipManagerHelper;
import com.gemstone.gemfire.internal.InternalDataSerializer;
import com.gemstone.gemfire.internal.InternalInstantiator;
import com.gemstone.gemfire.internal.OSProcess;
import com.gemstone.gemfire.internal.SocketCreator;
import com.gemstone.gemfire.internal.admin.ClientStatsManager;
import com.gemstone.gemfire.internal.cache.DiskStoreObserver;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.HARegion;
import com.gemstone.gemfire.internal.cache.InitialImageOperation;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.tier.InternalClientMembership;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheServerTestUtil;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientProxyMembershipID;
import com.gemstone.gemfire.internal.cache.tier.sockets.DataSerializerPropogationDUnitTest;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheCreation;
import com.gemstone.gemfire.internal.logging.InternalLogWriter;
import com.gemstone.gemfire.internal.logging.LocalLogWriter;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.LogWriterFactory;
import com.gemstone.gemfire.internal.logging.LogWriterImpl;
import com.gemstone.gemfire.internal.logging.ManagerLogWriter;
import com.gemstone.gemfire.internal.logging.log4j.LogWriterLogger;
import com.gemstone.gemfire.internal.util.Callable;
import com.gemstone.gemfire.management.internal.cli.LogWrapper;
import com.jayway.awaitility.Awaitility;
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
  private static final LogWriterLogger oldLogger = LogWriterLogger.create(logger);
  private static final LinkedHashSet<String> testHistory = new LinkedHashSet<String>();

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
  
  private static void tearDownCreationStackGenerator() {
    InternalDistributedSystem.TEST_CREATION_STACK_GENERATOR.set(InternalDistributedSystem.DEFAULT_CREATION_STACK_GENERATOR);
  }
  
  /** This VM's connection to the distributed system */
  public static InternalDistributedSystem system;
  private static Class lastSystemCreatedInTest;
  private static Properties lastSystemProperties;
  public static volatile String testName;
  
  private static ConcurrentLinkedQueue<ExpectedException> expectedExceptions = new ConcurrentLinkedQueue<ExpectedException>();

  /** For formatting timing info */
  private static final DecimalFormat format = new DecimalFormat("###.###");

  public static boolean reconnect = false;

  public static final boolean logPerTest = Boolean.getBoolean("dunitLogPerTest");

  ///////////////////////  Utility Methods  ///////////////////////
  
  public void attachDebugger(VM vm, final String msg) {
    vm.invoke(new SerializableRunnable("Attach Debugger") {
      public void run() {
        com.gemstone.gemfire.internal.util.DebuggerSupport.
        waitForJavaDebugger(getSystem().getLogWriter().convertToLogWriterI18n(), msg);
      } 
    });
  }


  /**
   * Invokes a <code>SerializableRunnable</code> in every VM that
   * DUnit knows about.
   *
   * @see VM#invoke(Runnable)
   */
  public static void invokeInEveryVM(SerializableRunnable work) {
    for (int h = 0; h < Host.getHostCount(); h++) {
      Host host = Host.getHost(h);

      for (int v = 0; v < host.getVMCount(); v++) {
        VM vm = host.getVM(v);
        vm.invoke(work);
      }
    }
  }

  public static void invokeInLocator(SerializableRunnable work) {
    Host.getLocator().invoke(work);
  }
  
  /**
   * Invokes a <code>SerializableCallable</code> in every VM that
   * DUnit knows about.
   *
   * @return a Map of results, where the key is the VM and the value is the result
   * @see VM#invoke(Callable)
   */
  protected static Map invokeInEveryVM(SerializableCallable work) {
    HashMap ret = new HashMap();
    for (int h = 0; h < Host.getHostCount(); h++) {
      Host host = Host.getHost(h);
      for (int v = 0; v < host.getVMCount(); v++) {
        VM vm = host.getVM(v);
        ret.put(vm, vm.invoke(work));
      }
    }
    return ret;
  }

  /**
   * Invokes a method in every remote VM that DUnit knows about.
   *
   * @see VM#invoke(Class, String)
   */
  protected static void invokeInEveryVM(Class c, String method) {
    for (int h = 0; h < Host.getHostCount(); h++) {
      Host host = Host.getHost(h);

      for (int v = 0; v < host.getVMCount(); v++) {
        VM vm = host.getVM(v);
        vm.invoke(c, method);
      }
    }
  }

  /**
   * Invokes a method in every remote VM that DUnit knows about.
   *
   * @see VM#invoke(Class, String)
   */
  protected static void invokeInEveryVM(Class c, String method, Object[] methodArgs) {
    for (int h = 0; h < Host.getHostCount(); h++) {
      Host host = Host.getHost(h);

      for (int v = 0; v < host.getVMCount(); v++) {
        VM vm = host.getVM(v);
        vm.invoke(c, method, methodArgs);
      }
    }
  }
  
  /**
   * The number of milliseconds to try repeating validation code in the
   * event that AssertionFailedError is thrown.  For ACK scopes, no
   * repeat should be necessary.
   */
  protected long getRepeatTimeoutMs() {
    return 0;
  }
  
  protected void invokeRepeatingIfNecessary(VM vm, RepeatableRunnable task) {
    vm.invokeRepeatingIfNecessary(task, getRepeatTimeoutMs());
  }
  
  /**
   * Invokes a <code>SerializableRunnable</code> in every VM that
   * DUnit knows about.  If work.run() throws an assertion failure, 
   * its execution is repeated, until no assertion failure occurs or
   * repeatTimeout milliseconds have passed.
   *
   * @see VM#invoke(Runnable)
   */
  protected void invokeInEveryVMRepeatingIfNecessary(RepeatableRunnable work) {
    for (int h = 0; h < Host.getHostCount(); h++) {
      Host host = Host.getHost(h);

      for (int v = 0; v < host.getVMCount(); v++) {
        VM vm = host.getVM(v);
        vm.invokeRepeatingIfNecessary(work, getRepeatTimeoutMs());
      }
    }
  }

  /** Return the total number of VMs on all hosts */
  protected static int getVMCount() {
    int count = 0;
    for (int h = 0; h < Host.getHostCount(); h++) {
      Host host = Host.getHost(h);
      count += host.getVMCount();
    }
    return count;
  }


  /** print a stack dump for this vm
      @author bruce
      @since 5.0
   */
  public static void dumpStack() {
    com.gemstone.gemfire.internal.OSProcess.printStacks(0, false);
  }
  
  /** print a stack dump for the given vm
      @author bruce
      @since 5.0
   */
  public static void dumpStack(VM vm) {
    vm.invoke(com.gemstone.gemfire.test.dunit.DistributedTestCase.class, "dumpStack");
  }
  
  /** print stack dumps for all vms on the given host
      @author bruce
      @since 5.0
   */
  public static void dumpStack(Host host) {
    for (int v=0; v < host.getVMCount(); v++) {
      host.getVM(v).invoke(com.gemstone.gemfire.test.dunit.DistributedTestCase.class, "dumpStack");
    }
  }
  
  /** print stack dumps for all vms
      @author bruce
      @since 5.0
   */
  public static void dumpAllStacks() {
    for (int h=0; h < Host.getHostCount(); h++) {
      dumpStack(Host.getHost(h));
    }
  }


  public static String noteTiming(long operations, String operationUnit,
                                  long beginTime, long endTime,
                                  String timeUnit)
  {
    long delta = endTime - beginTime;
    StringBuffer sb = new StringBuffer();
    sb.append("  Performed ");
    sb.append(operations);
    sb.append(" ");
    sb.append(operationUnit);
    sb.append(" in ");
    sb.append(delta);
    sb.append(" ");
    sb.append(timeUnit);
    sb.append("\n");

    double ratio = ((double) operations) / ((double) delta);
    sb.append("    ");
    sb.append(format.format(ratio));
    sb.append(" ");
    sb.append(operationUnit);
    sb.append(" per ");
    sb.append(timeUnit);
    sb.append("\n");

    ratio = ((double) delta) / ((double) operations);
    sb.append("    ");
    sb.append(format.format(ratio));
    sb.append(" ");
    sb.append(timeUnit);
    sb.append(" per ");
    sb.append(operationUnit);
    sb.append("\n");

    return sb.toString();
  }

  /**
   * Creates a new LogWriter and adds it to the config properties. The config
   * can then be used to connect to DistributedSystem, thus providing early
   * access to the LogWriter before connecting. This call does not connect
   * to the DistributedSystem. It simply creates and returns the LogWriter
   * that will eventually be used by the DistributedSystem that connects using
   * config.
   * 
   * @param config the DistributedSystem config properties to add LogWriter to
   * @return early access to the DistributedSystem LogWriter
   */
  protected static LogWriter createLogWriter(Properties config) { // TODO:LOG:CONVERT: this is being used for ExpectedExceptions
    Properties nonDefault = config;
    if (nonDefault == null) {
      nonDefault = new Properties();
    }
    addHydraProperties(nonDefault);
    
    DistributionConfig dc = new DistributionConfigImpl(nonDefault);
    LogWriter logger = LogWriterFactory.createLogWriterLogger(
        false/*isLoner*/, false/*isSecurityLog*/, dc, 
        false);        
    
    // if config was non-null, then these will be added to it...
    nonDefault.put(DistributionConfig.LOG_WRITER_NAME, logger);
    
    return logger;
  }
  
  /**
   * Fetches the GemFireDescription for this test and adds its 
   * DistributedSystem properties to the provided props parameter.
   * 
   * @param config the properties to add hydra's test properties to
   */
  protected static void addHydraProperties(Properties config) {
    Properties p = DUnitEnv.get().getDistributedSystemProperties();
    for (Iterator iter = p.entrySet().iterator();
        iter.hasNext(); ) {
      Map.Entry entry = (Map.Entry) iter.next();
      String key = (String) entry.getKey();
      String value = (String) entry.getValue();
      if (config.getProperty(key) == null) {
        config.setProperty(key, value);
      }
    }
  }
  
  ////////////////////////  Constructors  ////////////////////////

  /**
   * Creates a new <code>DistributedTestCase</code> test with the
   * given name.
   */
  public DistributedTestCase(String name) {
    super(name);
    DUnitLauncher.launchIfNeeded();
  }

  ///////////////////////  Instance Methods  ///////////////////////


  protected Class getTestClass() {
    Class clazz = getClass();
    while (clazz.getDeclaringClass() != null) {
      clazz = clazz.getDeclaringClass();
    }
    return clazz;
  }
  
  
  /**
   * This finds the log level configured for the test run.  It should be used
   * when creating a new distributed system if you want to specify a log level.
   * @return the dunit log-level setting
   */
  public static String getDUnitLogLevel() {
    Properties p = DUnitEnv.get().getDistributedSystemProperties();
    String result = p.getProperty(DistributionConfig.LOG_LEVEL_NAME);
    if (result == null) {
      result = ManagerLogWriter.levelToString(DistributionConfig.DEFAULT_LOG_LEVEL);
    }
    return result;
  }

  public final static Properties getAllDistributedSystemProperties(Properties props) {
    Properties p = DUnitEnv.get().getDistributedSystemProperties();
    
    // our tests do not expect auto-reconnect to be on by default
    if (!p.contains(DistributionConfig.DISABLE_AUTO_RECONNECT_NAME)) {
      p.put(DistributionConfig.DISABLE_AUTO_RECONNECT_NAME, "true");
    }

    for (Iterator iter = props.entrySet().iterator();
    iter.hasNext(); ) {
      Map.Entry entry = (Map.Entry) iter.next();
      String key = (String) entry.getKey();
      Object value = entry.getValue();
      p.put(key, value);
    }
    return p;
  }

  public void setSystem(Properties props, DistributedSystem ds) {
    system = (InternalDistributedSystem)ds;
    lastSystemProperties = props;
    lastSystemCreatedInTest = getTestClass();
  }
  /**
   * Returns this VM's connection to the distributed system.  If
   * necessary, the connection will be lazily created using the given
   * <code>Properties</code>.  Note that this method uses hydra's
   * configuration to determine the location of log files, etc.
   * Note: "final" was removed so that WANTestBase can override this method.
   * This was part of the xd offheap merge.
   *
   * @see hydra.DistributedConnectionMgr#connect
   * @since 3.0
   */
  public /*final*/ InternalDistributedSystem getSystem(Properties props) {
    // Setting the default disk store name is now done in setUp
    if (system == null) {
      system = InternalDistributedSystem.getAnyInstance();
    }
    if (system == null || !system.isConnected()) {
      // Figure out our distributed system properties
      Properties p = getAllDistributedSystemProperties(props);
      lastSystemCreatedInTest = getTestClass();
      if (logPerTest) {
        String testMethod = getTestName();
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
      if(!getTestClass().equals(lastSystemCreatedInTest)) {
        Properties newProps = getAllDistributedSystemProperties(props);
        needNewSystem = !newProps.equals(lastSystemProperties);
        if(needNewSystem) {
          getLogWriter().info(
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
            getLogWriter().info("Forcing DS disconnect. For property " + key
                                + " old value = " + activeProps.getProperty(key)
                                + " new value = " + value);
            break;
          }
        }
      }
      if(needNewSystem) {
        // the current system does not meet our needs to disconnect and
        // call recursively to get a new system.
        getLogWriter().info("Disconnecting from current DS in order to make a new one");
        disconnectFromDS();
        getSystem(props);
      }
    }
    return system;
  }


  /**
   * Crash the cache in the given VM in such a way that it immediately stops communicating with
   * peers.  This forces the VM's membership manager to throw a ForcedDisconnectException by
   * forcibly terminating the JGroups protocol stack with a fake EXIT event.<p>
   * 
   * NOTE: if you use this method be sure that you clean up the VM before the end of your
   * test with disconnectFromDS() or disconnectAllFromDS().
   */
  public boolean crashDistributedSystem(VM vm) {
    return (Boolean)vm.invoke(new SerializableCallable("crash distributed system") {
      public Object call() throws Exception {
        DistributedSystem msys = InternalDistributedSystem.getAnyInstance();
        crashDistributedSystem(msys);
        return true;
      }
    });
  }
  
  /**
   * Crash the cache in the given VM in such a way that it immediately stops communicating with
   * peers.  This forces the VM's membership manager to throw a ForcedDisconnectException by
   * forcibly terminating the JGroups protocol stack with a fake EXIT event.<p>
   * 
   * NOTE: if you use this method be sure that you clean up the VM before the end of your
   * test with disconnectFromDS() or disconnectAllFromDS().
   */
  public void crashDistributedSystem(final DistributedSystem msys) {
    MembershipManagerHelper.crashDistributedSystem(msys);
    MembershipManagerHelper.inhibitForcedDisconnectLogging(false);
    WaitCriterion wc = new WaitCriterion() {
      public boolean done() {
        return !msys.isConnected();
      }
      public String description() {
        return "waiting for distributed system to finish disconnecting: " + msys;
      }
    };
//    try {
      waitForCriterion(wc, 10000, 1000, true);
//    } finally {
//      dumpMyThreads(getLogWriter());
//    }
  }

  private String getDefaultDiskStoreName() {
    String vmid = System.getProperty("vmid");
    return "DiskStore-"  + vmid + "-"+ getTestClass().getCanonicalName() + "." + getTestName();
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
    return getSystem(this.getDistributedSystemProperties());
  }

  /**
   * Returns a loner distributed system that isn't connected to other
   * vms
   * 
   * @since 6.5
   */
  public final InternalDistributedSystem getLonerSystem() {
    Properties props = this.getDistributedSystemProperties();
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
    Properties props = this.getDistributedSystemProperties();
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

  /**
   * Sets up the test (noop).
   */
  @Override
  public void setUp() throws Exception {
    logTestHistory();
    setUpCreationStackGenerator();
    testName = getName();
    System.setProperty(HoplogConfig.ALLOW_LOCAL_HDFS_PROP, "true");
    
    if (testName != null) {
      GemFireCacheImpl.setDefaultDiskStoreName(getDefaultDiskStoreName());
      String baseDefaultDiskStoreName = getTestClass().getCanonicalName() + "." + getTestName();
      for (int h = 0; h < Host.getHostCount(); h++) {
        Host host = Host.getHost(h);
        for (int v = 0; v < host.getVMCount(); v++) {
          VM vm = host.getVM(v);
          String vmDefaultDiskStoreName = "DiskStore-" + h + "-" + v + "-" + baseDefaultDiskStoreName;
          vm.invoke(DistributedTestCase.class, "perVMSetUp", new Object[] {testName, vmDefaultDiskStoreName});
        }
      }
    }
    System.out.println("\n\n[setup] START TEST " + getClass().getSimpleName()+"."+testName+"\n\n");
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

  public static void perVMSetUp(String name, String defaultDiskStoreName) {
    setTestName(name);
    GemFireCacheImpl.setDefaultDiskStoreName(defaultDiskStoreName);
    System.setProperty(HoplogConfig.ALLOW_LOCAL_HDFS_PROP, "true");    
  }
  public static void setTestName(String name) {
    testName = name;
  }
  
  public static String getTestName() {
    return testName;
  }

  /**
   * For logPerTest to work, we have to disconnect from the DS, but all
   * subclasses do not call super.tearDown(). To prevent this scenario
   * this method has been declared final. Subclasses must now override
   * {@link #tearDown2()} instead.
   * @throws Exception
   */
  @Override
  public final void tearDown() throws Exception {
    tearDownCreationStackGenerator();
    tearDown2();
    realTearDown();
    tearDownAfter();
  }

  /**
   * Tears down the test. This method is called by the final {@link #tearDown()} method and should be overridden to
   * perform actual test cleanup and release resources used by the test.  The tasks executed by this method are
   * performed before the DUnit test framework using Hydra cleans up the client VMs.
   * <p/>
   * @throws Exception if the tear down process and test cleanup fails.
   * @see #tearDown
   * @see #tearDownAfter()
   */
  // TODO rename this method to tearDownBefore and change the access modifier to protected!
  public void tearDown2() throws Exception {
  }

  protected void realTearDown() throws Exception {
    if (logPerTest) {
      disconnectFromDS();
      invokeInEveryVM(DistributedTestCase.class, "disconnectFromDS");
    }
    cleanupAllVms();
  }
  
  /**
   * Tears down the test.  Performs additional tear down tasks after the DUnit tests framework using Hydra cleans up
   * the client VMs.  This method is called by the final {@link #tearDown()} method and should be overridden to perform
   * post tear down activities.
   * <p/>
   * @throws Exception if the test tear down process fails.
   * @see #tearDown()
   * @see #tearDown2()
   */
  protected void tearDownAfter() throws Exception {
  }

  public static void cleanupAllVms()
  {
    cleanupThisVM();
    invokeInEveryVM(DistributedTestCase.class, "cleanupThisVM");
    invokeInLocator(new SerializableRunnable() {
      public void run() {
        DistributionMessageObserver.setInstance(null);
        unregisterInstantiatorsInThisVM();
      }
    });
    DUnitLauncher.closeAndCheckForSuspects();
  }


  private static void cleanupThisVM() {
    closeCache();
    
    SocketCreator.resolve_dns = true;
    CacheCreation.clearThreadLocals();
    System.getProperties().remove("gemfire.log-level");
    System.getProperties().remove("jgroups.resolve_dns");
    InitialImageOperation.slowImageProcessing = 0;
    DistributionMessageObserver.setInstance(null);
    QueryTestUtils.setCache(null);
    CacheServerTestUtil.clearCacheReference();
    RegionTestCase.preSnapshotRegion = null;
    GlobalLockingDUnitTest.region_testBug32356 = null;
    LogWrapper.close();
    ClientProxyMembershipID.system = null;
    MultiVMRegionTestCase.CCRegion = null;
    InternalClientMembership.unregisterAllListeners();
    ClientStatsManager.cleanupForTests();
    ClientServerTestCase.AUTO_LOAD_BALANCE = false;
    unregisterInstantiatorsInThisVM();
    DistributionMessageObserver.setInstance(null);
    QueryObserverHolder.reset();
    DiskStoreObserver.setInstance(null);
    System.getProperties().remove("gemfire.log-level");
    System.getProperties().remove("jgroups.resolve_dns");
    
    if (InternalDistributedSystem.systemAttemptingReconnect != null) {
      InternalDistributedSystem.systemAttemptingReconnect.stopReconnecting();
    }
    ExpectedException ex;
    while((ex = expectedExceptions.poll()) != null) {
      ex.remove();
    }
  }

  private static void closeCache() {
    GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
    if(cache != null && !cache.isClosed()) {
      destroyRegions(cache);
      cache.close();
    }
  }
  
  protected static final void destroyRegions(Cache cache)
      throws InternalGemFireError, Error, VirtualMachineError {
    if (cache != null && !cache.isClosed()) {
      //try to destroy the root regions first so that
      //we clean up any persistent files.
      for (Iterator itr = cache.rootRegions().iterator(); itr.hasNext();) {
        Region root = (Region)itr.next();
        //for colocated regions you can't locally destroy a partitioned
        //region.
        if(root.isDestroyed() || root instanceof HARegion || root instanceof PartitionedRegion) {
          continue;
        }
        try {
          root.localDestroyRegion("teardown");
        }
        catch (VirtualMachineError e) {
          SystemFailure.initiateFailure(e);
          throw e;
        }
        catch (Throwable t) {
          getLogWriter().error(t);
        }
      }
    }
  }
  
  
  public static void unregisterAllDataSerializersFromAllVms()
  {
    unregisterDataSerializerInThisVM();
    invokeInEveryVM(new SerializableRunnable() {
      public void run() {
        unregisterDataSerializerInThisVM();
      }
    });
    invokeInLocator(new SerializableRunnable() {
      public void run() {
        unregisterDataSerializerInThisVM();
      }
    });
  }

  public static void unregisterInstantiatorsInThisVM() {
    // unregister all the instantiators
    InternalInstantiator.reinitialize();
    assertEquals(0, InternalInstantiator.getInstantiators().length);
  }
  
  public static void unregisterDataSerializerInThisVM()
  {
    DataSerializerPropogationDUnitTest.successfullyLoadedTestDataSerializer = false;
    // unregister all the Dataserializers
    InternalDataSerializer.reinitialize();
    // ensure that all are unregistered
    assertEquals(0, InternalDataSerializer.getSerializers().length);
  }


  protected static void disconnectAllFromDS() {
    disconnectFromDS();
    invokeInEveryVM(DistributedTestCase.class,
                    "disconnectFromDS");
  }

  /**
   * Disconnects this VM from the distributed system
   */
  public static void disconnectFromDS() {
    testName = null;
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
      }
      catch (Exception e) {
        // ignore
      }
    }
    
    {
      AdminDistributedSystemImpl ads = 
          AdminDistributedSystemImpl.getConnectedInstance();
      if (ads != null) {// && ads.isConnected()) {
        ads.disconnect();
      }
    }
  }

  /**
   * Strip the package off and gives just the class name.
   * Needed because of Windows file name limits.
   */
  private String getShortClassName() {
    String result = this.getClass().getName();
    int idx = result.lastIndexOf('.');
    if (idx != -1) {
      result = result.substring(idx+1);
    }
    return result;
  }
  
  /** get the host name to use for a server cache in client/server dunit
   * testing
   * @param host
   * @return the host name
   */
  public static String getServerHostName(Host host) {
    return System.getProperty("gemfire.server-bind-address") != null?
        System.getProperty("gemfire.server-bind-address")
        : host.getHostName();
  }

  /** get the IP literal name for the current host, use this instead of  
   * "localhost" to avoid IPv6 name resolution bugs in the JDK/machine config.
   * @return an ip literal, this method honors java.net.preferIPvAddresses
   */
  public static String getIPLiteral() {
    try {
      return SocketCreator.getLocalHost().getHostAddress();
    } catch (UnknownHostException e) {
      throw new Error("problem determining host IP address", e);
    }
  }
 
 
  /**
   * Get the port that the standard dunit locator is listening on.
   * @return
   */
  public static int getDUnitLocatorPort() {
    return DUnitEnv.get().getLocatorPort();
  }
    
  
  /**
   * Returns a unique name for this test method.  It is based on the
   * name of the class as well as the name of the method.
   */
  public String getUniqueName() {
    return getShortClassName() + "_" + this.getName();
  }

  /**
   * Returns a <code>LogWriter</code> for logging information
   * @deprecated Use a static logger from the log4j2 LogService.getLogger instead.
   */
  @Deprecated
  public static InternalLogWriter getLogWriter() {
    return oldLogger;
  }

  /**
   * Helper method that causes this test to fail because of the given
   * exception.
   */
  public static void fail(String message, Throwable ex) {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw, true);
    pw.print(message);
    pw.print(": ");
    ex.printStackTrace(pw);
    fail(sw.toString());
  }

  // utility methods

  /** pause for a default interval */
  protected void pause() {
    pause(250);
  }

  /**
   * Use of this function indicates a place in the tests tree where t
   * he use of Thread.sleep() is
   * highly questionable.
   * <p>
   * Some places in the system, especially those that test expirations and other
   * timeouts, have a very good reason to call {@link Thread#sleep(long)}.  The
   * <em>other</em> places are marked by the use of this method.
   * 
   * @param ms
   */
  static public final void staticPause(int ms) {
//    getLogWriter().info("FIXME: Pausing for " + ms + " ms..."/*, new Exception()*/);
    final long target = System.currentTimeMillis() + ms;
    try {
      for (;;) {
        long msLeft = target - System.currentTimeMillis();
        if (msLeft <= 0) {
          break;
        }
        Thread.sleep(msLeft);
      }
    }
    catch (InterruptedException e) {
      fail("interrupted", e);
    }
    
  }
  
  /**
   * Blocks until the clock used for expiration moves forward.
   * @return the last time stamp observed
   */
  public static final long waitForExpiryClockToChange(LocalRegion lr) {
    return waitForExpiryClockToChange(lr, lr.cacheTimeMillis());
  }
  /**
   * Blocks until the clock used for expiration moves forward.
   * @param baseTime the timestamp that the clock must exceed
   * @return the last time stamp observed
   */
  public static final long waitForExpiryClockToChange(LocalRegion lr, final long baseTime) {
    long nowTime;
    do {
      Thread.yield();
      nowTime = lr.cacheTimeMillis();
    } while ((nowTime - baseTime) <= 0L);
    return nowTime;
  }
  
  /** pause for specified ms interval
   * Make sure system clock has advanced by the specified number of millis before
   * returning.
   */
  public static final void pause(int ms) {
    LogWriter log = getLogWriter();
    if (ms >= 1000 || log.fineEnabled()) { // check for fine but log at info
      getLogWriter().info("Pausing for " + ms + " ms..."/*, new Exception()*/);
    }
    final long target = System.currentTimeMillis() + ms;
    try {
      for (;;) {
        long msLeft = target - System.currentTimeMillis();
        if (msLeft <= 0) {
          break;
        }
        Thread.sleep(msLeft);
      }
    }
    catch (InterruptedException e) {
      fail("interrupted", e);
    }
  }
  
  public interface WaitCriterion {
    public boolean done();
    public String description();
  }
  
  public interface WaitCriterion2 extends WaitCriterion {
    /**
     * If this method returns true then quit waiting even if we are not done.
     * This allows a wait to fail early.
     */
    public boolean stopWaiting();
  }

  /**
   * If true, we randomize the amount of time we wait before polling a
   * {@link WaitCriterion}.
   */
  static private final boolean USE_JITTER = true;
  static private final Random jitter = new Random();
  
  /**
   * Return a jittered interval up to a maximum of <code>ms</code>
   * milliseconds, inclusive.
   * 
   * The result is bounded by 50 ms as a minimum and 5000 ms as a maximum.
   * 
   * @param ms total amount of time to wait
   * @return randomized interval we should wait
   */
  private static int jitterInterval(long ms) {
    final int minLegal = 50;
    final int maxLegal = 5000;
    if (ms <= minLegal) {
      return (int)ms; // Don't ever jitter anything below this.
    }

    int maxReturn = maxLegal;
    if (ms < maxLegal) {
      maxReturn = (int)ms;
    }

    return minLegal + jitter.nextInt(maxReturn - minLegal + 1);
  }
  
  /**
   * Wait until given criterion is met
   * @param ev criterion to wait on
   * @param ms total time to wait, in milliseconds
   * @param interval pause interval between waits
   * @param throwOnTimeout if false, don't generate an error
   * @deprecated Use {@link Awaitility} instead.
   */
  @Deprecated
  static public void waitForCriterion(WaitCriterion ev, long ms, 
      long interval, boolean throwOnTimeout) {
    long waitThisTime;
    if (USE_JITTER) {
      waitThisTime = jitterInterval(interval);
    }
    else {
      waitThisTime = interval;
    }
    final long tilt = System.currentTimeMillis() + ms;
    for (;;) {
//      getLogWriter().info("Testing to see if event has occurred: " + ev.description());
      if (ev.done()) {
        return; // success
      }
      if (ev instanceof WaitCriterion2) {
        WaitCriterion2 ev2 = (WaitCriterion2)ev;
        if (ev2.stopWaiting()) {
          if (throwOnTimeout) {
            fail("stopWaiting returned true: " + ev.description());
          }
          return;
        }
      }

      // Calculate time left
      long timeLeft = tilt - System.currentTimeMillis();
      if (timeLeft <= 0) {
        if (!throwOnTimeout) {
          return; // not an error, but we're done
        }
        fail("Event never occurred after " + ms + " ms: " + ev.description());
      }
      
      if (waitThisTime > timeLeft) {
        waitThisTime = timeLeft;
      }
      
      // Wait a little bit
      Thread.yield();
      try {
//        getLogWriter().info("waiting " + waitThisTime + "ms for " + ev.description());
        Thread.sleep(waitThisTime);
      } catch (InterruptedException e) {
        fail("interrupted");
      }
    }
  }

  /**
   * Wait on a mutex.  This is done in a loop in order to address the
   * "spurious wakeup" "feature" in Java.
   * @param ev condition to test
   * @param mutex object to lock and wait on
   * @param ms total amount of time to wait
   * @param interval interval to pause for the wait
   * @param throwOnTimeout if false, no error is thrown.
   */
  static public void waitMutex(WaitCriterion ev, Object mutex, long ms, 
      long interval, boolean throwOnTimeout) {
    final long tilt = System.currentTimeMillis() + ms;
    long waitThisTime;
    if (USE_JITTER) {
      waitThisTime = jitterInterval(interval);
    }
    else {
      waitThisTime = interval;
    }
    synchronized (mutex) {
      for (;;) {
        if (ev.done()) {
          break;
        }
        
        long timeLeft = tilt - System.currentTimeMillis();
        if (timeLeft <= 0) {
          if (!throwOnTimeout) {
            return; // not an error, but we're done
          }
          fail("Event never occurred after " + ms + " ms: " + ev.description());
        }
        
        if (waitThisTime > timeLeft) {
          waitThisTime = timeLeft;
        }
        
        try {
          mutex.wait(waitThisTime);
        } catch (InterruptedException e) {
          fail("interrupted");
        }
      } // for
    } // synchronized
  }

  /**
   * Wait for a thread to join
   * @param t thread to wait on
   * @param ms maximum time to wait
   * @throws AssertionFailure if the thread does not terminate
   */
  static public void join(Thread t, long ms, LogWriter logger) {
    final long tilt = System.currentTimeMillis() + ms;
    final long incrementalWait;
    if (USE_JITTER) {
      incrementalWait = jitterInterval(ms);
    }
    else {
      incrementalWait = ms; // wait entire time, no looping.
    }
    final long start = System.currentTimeMillis();
    for (;;) {
      // I really do *not* understand why this check is necessary
      // but it is, at least with JDK 1.6.  According to the source code
      // and the javadocs, one would think that join() would exit immediately
      // if the thread is dead.  However, I can tell you from experimentation
      // that this is not the case. :-(  djp 2008-12-08
      if (!t.isAlive()) {
        break;
      }
      try {
        t.join(incrementalWait);
      } catch (InterruptedException e) {
        fail("interrupted");
      }
      if (System.currentTimeMillis() >= tilt) {
        break;
      }
    } // for
    if (logger == null) {
      logger = new LocalLogWriter(LogWriterImpl.INFO_LEVEL, System.out);
    }
    if (t.isAlive()) {
      logger.info("HUNG THREAD");
      dumpStackTrace(t, t.getStackTrace(), logger);
      dumpMyThreads(logger);
      t.interrupt(); // We're in trouble!
      fail("Thread did not terminate after " + ms + " ms: " + t);
//      getLogWriter().warning("Thread did not terminate" 
//          /* , new Exception()*/
//          );
    }
    long elapsedMs = (System.currentTimeMillis() - start);
    if (elapsedMs > 0) {
      String msg = "Thread " + t + " took " 
        + elapsedMs
        + " ms to exit.";
      logger.info(msg);
    }
  }

  public static void dumpStackTrace(Thread t, StackTraceElement[] stack, LogWriter logger) {
    StringBuilder msg = new StringBuilder();
    msg.append("Thread=<")
      .append(t)
      .append("> stackDump:\n");
    for (int i=0; i < stack.length; i++) {
      msg.append("\t")
        .append(stack[i])
        .append("\n");
    }
    logger.info(msg.toString());
  }
  /**
   * Dump all thread stacks
   */
  public static void dumpMyThreads(LogWriter logger) {
    OSProcess.printStacks(0, false);
  }
  
  /**
   * A class that represents an currently logged expected exception, which
   * should be removed
   * 
   * @author Mitch Thomas
   * @since 5.7bugfix
   */
  public static class ExpectedException implements Serializable {
    private static final long serialVersionUID = 1L;

    final String ex;

    final transient VM v;

    public ExpectedException(String exception) {
      this.ex = exception;
      this.v = null;
    }

    ExpectedException(String exception, VM vm) {
      this.ex = exception;
      this.v = vm;
    }

    public String getRemoveString() {
      return "<ExpectedException action=remove>" + ex + "</ExpectedException>";
    }

    public String getAddString() {
      return "<ExpectedException action=add>" + ex + "</ExpectedException>";
    }

    public void remove() {
      SerializableRunnable removeRunnable = new SerializableRunnable(
          "removeExpectedExceptions") {
        public void run() {
          final String remove = getRemoveString();
          final InternalDistributedSystem sys = InternalDistributedSystem
              .getConnectedInstance();
          if (sys != null) {
            sys.getLogWriter().info(remove);
          }
          try {
            getLogWriter().info(remove);
          } catch (Exception noHydraLogger) {
          }

          logger.info(remove);
        }
      };

      if (this.v != null) {
        v.invoke(removeRunnable);
      }
      else {
        invokeInEveryVM(removeRunnable);
      }
      String s = getRemoveString();
      LogManager.getLogger(LogService.BASE_LOGGER_NAME).info(s);
      // log it locally
      final InternalDistributedSystem sys = InternalDistributedSystem
          .getConnectedInstance();
      if (sys != null) { // avoid creating a system
        sys.getLogWriter().info(s);
      }
      getLogWriter().info(s);
    }
  }

  /**
   * Log in all VMs, in both the test logger and the GemFire logger the
   * expected exception string to prevent grep logs from complaining. The
   * expected string is used by the GrepLogs utility and so can contain
   * regular expression characters.
   * 
   * If you do not remove the expected exception, it will be removed at the
   * end of your test case automatically.
   * 
   * @since 5.7bugfix
   * @param exception
   *          the exception string to expect
   * @return an ExpectedException instance for removal
   */
  public static ExpectedException addExpectedException(final String exception) {
    return addExpectedException(exception, null);
  }

  /**
   * Log in all VMs, in both the test logger and the GemFire logger the
   * expected exception string to prevent grep logs from complaining. The
   * expected string is used by the GrepLogs utility and so can contain
   * regular expression characters.
   * 
   * @since 5.7bugfix
   * @param exception
   *          the exception string to expect
   * @param v
   *          the VM on which to log the expected exception or null for all VMs
   * @return an ExpectedException instance for removal purposes
   */
  public static ExpectedException addExpectedException(final String exception,
      VM v) {
    final ExpectedException ret;
    if (v != null) {
      ret = new ExpectedException(exception, v);
    }
    else {
      ret = new ExpectedException(exception);
    }
    // define the add and remove expected exceptions
    final String add = ret.getAddString();
    SerializableRunnable addRunnable = new SerializableRunnable(
        "addExpectedExceptions") {
      public void run() {
        final InternalDistributedSystem sys = InternalDistributedSystem
            .getConnectedInstance();
        if (sys != null) {
          sys.getLogWriter().info(add);
        }
        try {
          getLogWriter().info(add);
        } catch (Exception noHydraLogger) {
        }
 
        logger.info(add);
      }
    };
    if (v != null) {
      v.invoke(addRunnable);
    }
    else {
      invokeInEveryVM(addRunnable);
    }
    
    LogManager.getLogger(LogService.BASE_LOGGER_NAME).info(add);
    // Log it locally too
    final InternalDistributedSystem sys = InternalDistributedSystem
        .getConnectedInstance();
    if (sys != null) { // avoid creating a cache
      sys.getLogWriter().info(add);
    }
    getLogWriter().info(add);
    expectedExceptions.add(ret);
    return ret;
  }

  /** 
   * delete locator state files.  Use this after getting a random port
   * to ensure that an old locator state file isn't picked up by the
   * new locator you're starting.
   * @param ports
   */
  public void deleteLocatorStateFile(int... ports) {
    for (int i=0; i<ports.length; i++) {
      File stateFile = new File("locator"+ports[i]+"view.dat");
      if (stateFile.exists()) {
        stateFile.delete();
      }
    }
  }
  
}

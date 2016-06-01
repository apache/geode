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
package com.gemstone.gemfire.test.dunit.internal;

import static com.gemstone.gemfire.distributed.DistributedSystemConfigProperties.*;

import com.gemstone.gemfire.admin.internal.AdminDistributedSystemImpl;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Region;
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
import com.gemstone.gemfire.internal.SocketCreator;
import com.gemstone.gemfire.internal.admin.ClientStatsManager;
import com.gemstone.gemfire.internal.cache.*;
import com.gemstone.gemfire.internal.cache.tier.InternalClientMembership;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheServerTestUtil;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientProxyMembershipID;
import com.gemstone.gemfire.internal.cache.tier.sockets.Message;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheCreation;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.management.internal.cli.LogWrapper;
import com.gemstone.gemfire.test.dunit.*;
import com.gemstone.gemfire.test.dunit.standalone.DUnitLauncher;
import com.gemstone.gemfire.test.junit.rules.serializable.SerializableTestName;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.*;

import static com.gemstone.gemfire.distributed.DistributedSystemConfigProperties.LOCATORS;
import static com.gemstone.gemfire.distributed.DistributedSystemConfigProperties.MCAST_PORT;
import static org.junit.Assert.assertNotNull;

/**
 * This class is the base class for all distributed tests using JUnit 4.
 */
public abstract class JUnit4DistributedTestCase implements DistributedTestFixture, Serializable {

  private static final Logger logger = LogService.getLogger();

  private static final Set<String> testHistory = new LinkedHashSet<String>();

  /** This VM's connection to the distributed system */
  private static InternalDistributedSystem system;
  private static Class lastSystemCreatedInTest;
  private static Properties lastSystemProperties;
  private static volatile String testMethodName;

  /** For formatting timing info */
  private static final DecimalFormat format = new DecimalFormat("###.###");

  private static boolean reconnect = false;

  private static final boolean logPerTest = Boolean.getBoolean("dunitLogPerTest");

  private final DistributedTestFixture distributedTestFixture;

  /**
   * Constructs a new distributed test. All JUnit 4 test classes need to have a
   * no-arg constructor.
   */
  public JUnit4DistributedTestCase() {
    this(null);
  }

  /**
   * This constructor should only be used by {@link JUnit3DistributedTestCase}.
   */
  protected JUnit4DistributedTestCase(final DistributedTestFixture distributedTestFixture) {
    if (distributedTestFixture == null) {
      this.distributedTestFixture = this;
    } else {
      this.distributedTestFixture = distributedTestFixture;
    }
  }

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @BeforeClass
  public static final void initializeDistributedTestCase() {
    DUnitLauncher.launchIfNeeded();
  }

  public final String getName() {
    if (this.distributedTestFixture != this) {
      return this.distributedTestFixture.getName();
    }
    return this.testName.getMethodName();
  }

  public final Class<? extends DistributedTestFixture> getTestClass() {
    return this.distributedTestFixture.getClass();
  }

  //---------------------------------------------------------------------------
  // methods for tests
  //---------------------------------------------------------------------------

  /**
   * @deprecated Please override {@link #getDistributedSystemProperties()} instead.
   */
  @Deprecated
  public final void setSystem(final Properties props, final DistributedSystem ds) { // TODO: override getDistributedSystemProperties and then delete
    system = (InternalDistributedSystem)ds;
    lastSystemProperties = props;
    lastSystemCreatedInTest = getTestClass(); // used to be getDeclaringClass()
  }

  /**
   * Returns this VM's connection to the distributed system.  If necessary, the
   * connection will be lazily created using the given {@code Properties}.
   *
   * <p>Do not override this method. Override {@link #getDistributedSystemProperties()}
   * instead.
   *
   * <p>Note: "final" was removed so that WANTestBase can override this method.
   * This was part of the xd offheap merge.
   *
   * @since GemFire 3.0
   */
  public final InternalDistributedSystem getSystem(final Properties props) {
    // Setting the default disk store name is now done in setUp
    if (system == null) {
      system = InternalDistributedSystem.getAnyInstance();
    }
    if (system == null || !system.isConnected()) {
      // Figure out our distributed system properties
      Properties p = DistributedTestUtils.getAllDistributedSystemProperties(props);
      lastSystemCreatedInTest = getTestClass(); // used to be getDeclaringClass()
      if (logPerTest) {
        String testMethod = getTestMethodName();
        String testName = lastSystemCreatedInTest.getName() + '-' + testMethod;
        String oldLogFile = p.getProperty(LOG_FILE);
        p.put(LOG_FILE,
                oldLogFile.replace("system.log", testName+".log"));
        String oldStatFile = p.getProperty(STATISTIC_ARCHIVE_FILE);
        p.put(STATISTIC_ARCHIVE_FILE,
                oldStatFile.replace("statArchive.gfs", testName+".gfs"));
      }
      system = (InternalDistributedSystem)DistributedSystem.connect(p);
      lastSystemProperties = p;
    } else {
      boolean needNewSystem = false;
      if(!getTestClass().equals(lastSystemCreatedInTest)) { // used to be getDeclaringClass()
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
   * Returns this VM's connection to the distributed system.  If necessary, the
   * connection will be lazily created using the {@code Properties} returned by
   * {@link #getDistributedSystemProperties()}.
   *
   * <p>Do not override this method. Override {@link #getDistributedSystemProperties()}
   * instead.
   *
   * @see #getSystem(Properties)
   *
   * @since GemFire 3.0
   */
  public final InternalDistributedSystem getSystem() {
    return getSystem(getDistributedSystemProperties());
  }

  public final InternalDistributedSystem basicGetSystem() {
    return system;
  }

  public final void nullSystem() { // TODO: delete
    system = null;
  }

  public static final InternalDistributedSystem getSystemStatic() {
    return system;
  }

  /**
   * Returns a loner distributed system that isn't connected to other vms.
   *
   * @since GemFire 6.5
   */
  public final InternalDistributedSystem getLonerSystem() {
    Properties props = getDistributedSystemProperties();
    props.put(MCAST_PORT, "0");
    props.put(LOCATORS, "");
    return getSystem(props);
  }

  /**
   * Returns whether or this VM is connected to a {@link DistributedSystem}.
   */
  public final boolean isConnectedToDS() {
    return system != null && system.isConnected();
  }

  /**
   * Returns a {@code Properties} object used to configure a connection to a
   * {@link DistributedSystem}. Unless overridden, this method will return an
   * empty {@code Properties} object.
   *
   * <p>Override this as needed. Default implementation returns empty {@code Properties}.
   *
   * @since GemFire 3.0
   */
  @Override
  public Properties getDistributedSystemProperties() {
    if (this.distributedTestFixture != this) {
      return this.distributedTestFixture.getDistributedSystemProperties();
    }
    return defaultGetDistributedSystemProperties();
  }

  final Properties defaultGetDistributedSystemProperties() {
    return new Properties();
  }

  public static final void disconnectAllFromDS() {
    disconnectFromDS();
    Invoke.invokeInEveryVM(()->disconnectFromDS());
  }

  /**
   * Disconnects this VM from the distributed system
   */
  public static final void disconnectFromDS() {
    //setTestMethodName(null);
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

  public static final String getTestMethodName() {
    return testMethodName;
  }

  private static final void setTestMethodName(final String testMethodName) {
    JUnit4DistributedTestCase.testMethodName = testMethodName;
  }

  /**
   * Returns a unique name for this test method.  It is based on the
   * name of the class as well as the name of the method.
   */
  public final String getUniqueName() {
    return getTestClass().getSimpleName() + "_" + getName();
  }

  //---------------------------------------------------------------------------
  // setup methods
  //---------------------------------------------------------------------------

  /**
   * Sets up the DistributedTestCase.
   *
   * <p>Do not override this method. Override {@link #preSetUp()} with work that
   * needs to occur before setUp() or override {@link #postSetUp()} with work
   * that needs to occur after setUp().
   */
  @Before
  public final void setUp() throws Exception {
    preSetUp();
    setUpDistributedTestCase();
    postSetUp();
  }

  /**
   * Sets up DistributedTest in controller and remote VMs. This includes the
   * defining the test name, setting the default disk store name, logging the
   * test history, and capturing a creation stack for detecting the source of
   * incompatible DistributedSystem connections.
   *
   * <p>Do not override this method.
   */
  private final void setUpDistributedTestCase() {
    final String className = getTestClass().getCanonicalName();
    final String methodName = getName();

    logTestHistory();

    setUpVM(methodName, getDefaultDiskStoreName(0, -1, className, methodName));

    for (int hostIndex = 0; hostIndex < Host.getHostCount(); hostIndex++) {
      Host host = Host.getHost(hostIndex);
      for (int vmIndex = 0; vmIndex < host.getVMCount(); vmIndex++) {
        final String vmDefaultDiskStoreName = getDefaultDiskStoreName(hostIndex, vmIndex, className, methodName);
        host.getVM(vmIndex).invoke("setupVM", ()->setUpVM(methodName, vmDefaultDiskStoreName));
      }
    }

    logTestStart();
  }

  /**
   * {@code preSetUp()} is invoked before {@link #setUpDistributedTestCase()}.
   *
   * <p>Override this as needed. Default implementation is empty.
   */
  @Override
  public void preSetUp() throws Exception {
    if (this.distributedTestFixture != this) {
      this.distributedTestFixture.preSetUp();
    }
  }

  /**
   * {@code postSetUp()} is invoked after {@link #setUpDistributedTestCase()}.
   *
   * <p>Override this as needed. Default implementation is empty.
   */
  @Override
  public void postSetUp() throws Exception {
    if (this.distributedTestFixture != this) {
      this.distributedTestFixture.postSetUp();
    }
  }

  private static final String getDefaultDiskStoreName(final int hostIndex, final int vmIndex, final String className, final String methodName) {
    return "DiskStore-" + String.valueOf(hostIndex) + "-" + String.valueOf(vmIndex) + "-" + className + "." + methodName; // used to be getDeclaringClass()
  }

  private static final void setUpVM(final String methodName, final String defaultDiskStoreName) {
    assertNotNull("methodName must not be null", methodName);
    assertNotNull("defaultDiskStoreName must not be null", defaultDiskStoreName);
    setTestMethodName(methodName);
    GemFireCacheImpl.setDefaultDiskStoreName(defaultDiskStoreName);
    setUpCreationStackGenerator();
  }

  private final void logTestStart() {
    System.out.println("\n\n[setup] START TEST " + getTestClass().getSimpleName()+"."+testMethodName+"\n\n");
  }

  private static final void setUpCreationStackGenerator() {
    // the following is moved from InternalDistributedSystem to fix #51058
    InternalDistributedSystem.TEST_CREATION_STACK_GENERATOR.set(
        new InternalDistributedSystem.CreationStackGenerator() {
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
  private final void logTestHistory() {
    String classname = getTestClass().getSimpleName();
    testHistory.add(classname);
    System.out.println("Previously run tests: " + testHistory);
  }

  //---------------------------------------------------------------------------
  // teardown methods
  //---------------------------------------------------------------------------

  /**
   * Tears down the DistributedTestCase.
   *
   * <p>Do not override this method. Override {@link #preTearDown()} with work that
   * needs to occur before tearDown() or override {@link #postTearDown()} with work
   * that needs to occur after tearDown().
   */
  @After
  public final void tearDown() throws Exception {
    try {
      preTearDownAssertions();
    } finally {
      preTearDown();
      tearDownDistributedTestCase();
      postTearDown();
    }
    postTearDownAssertions();
  }

  private final void tearDownDistributedTestCase() throws Exception {
    Invoke.invokeInEveryVM(()->tearDownCreationStackGenerator());
    if (logPerTest) {
      disconnectAllFromDS();
    }
    cleanupAllVms();
    if (!getDistributedSystemProperties().isEmpty()) {
      disconnectAllFromDS();
    }
  }

  /**
   * {@code preTearDown()} is invoked before {@link #tearDownDistributedTestCase()}.
   *
   * <p>Override this as needed. Default implementation is empty.
   */
  @Override
  public void preTearDown() throws Exception {
    if (this.distributedTestFixture != this) {
      this.distributedTestFixture.preTearDown();
    }
  }

  /**
   * {@code postTearDown()} is invoked after {@link #tearDownDistributedTestCase()}.
   *
   * <p>Override this as needed. Default implementation is empty.
   */
  @Override
  public void postTearDown() throws Exception {
    if (this.distributedTestFixture != this) {
      this.distributedTestFixture.postTearDown();
    }
  }

  @Override
  public void preTearDownAssertions() throws Exception {
    if (this.distributedTestFixture != this) {
      this.distributedTestFixture.preTearDownAssertions();
    }
  }

  @Override
  public void postTearDownAssertions() throws Exception {
    if (this.distributedTestFixture != this) {
      this.distributedTestFixture.postTearDownAssertions();
    }
  }

  private static final void cleanupAllVms() {
    tearDownVM();
    Invoke.invokeInEveryVM(()->tearDownVM());
    Invoke.invokeInLocator(()->{
      DistributionMessageObserver.setInstance(null);
      DistributedTestUtils.unregisterInstantiatorsInThisVM();
    });
    DUnitLauncher.closeAndCheckForSuspects();
  }

  private static final void tearDownVM() {
    closeCache();
    disconnectFromDS();
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
    Message.MAX_MESSAGE_SIZE = Message.DEFAULT_MAX_MESSAGE_SIZE;

    // clear system properties -- keep alphabetized
    System.clearProperty(DistributionConfig.GEMFIRE_PREFIX + "log-level");
    System.clearProperty("jgroups.resolve_dns");

    if (InternalDistributedSystem.systemAttemptingReconnect != null) {
      InternalDistributedSystem.systemAttemptingReconnect.stopReconnecting();
    }

    IgnoredException.removeAllExpectedExceptions();
  }

  private static final void closeCache() { // TODO: this should move to CacheTestCase
    GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
    if (cache != null && !cache.isClosed()) {
      destroyRegions(cache);
      cache.close();
    }
  }

  protected static final void destroyRegions(final Cache cache) { // TODO: this should move to CacheTestCase
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

  private static final void tearDownCreationStackGenerator() {
    InternalDistributedSystem.TEST_CREATION_STACK_GENERATOR.set(InternalDistributedSystem.DEFAULT_CREATION_STACK_GENERATOR);
  }
}

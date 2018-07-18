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
package org.apache.geode.test.dunit.internal;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.STATISTIC_ARCHIVE_FILE;
import static org.apache.geode.distributed.internal.InternalConfigurationPersistenceService.CLUSTER_CONFIG_DISK_DIR_PREFIX;
import static org.apache.geode.test.dunit.DistributedTestUtils.getAllDistributedSystemProperties;
import static org.apache.geode.test.dunit.DistributedTestUtils.unregisterInstantiatorsInThisVM;
import static org.apache.geode.test.dunit.Invoke.invokeInEveryVM;
import static org.apache.geode.test.dunit.Invoke.invokeInLocator;
import static org.apache.geode.test.dunit.LogWriterUtils.getLogWriter;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.Serializable;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionMessageObserver;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.HARegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.DUnitBlackboard;
import org.apache.geode.test.dunit.Disconnect;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.dunit.rules.DistributedTestRule;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

/**
 * This class is the base class for all distributed tests using JUnit 4.
 */
public abstract class JUnit4DistributedTestCase implements DistributedTestFixture, Serializable {
  private static final Logger logger = LogService.getLogger();

  private static final Set<String> testHistory = new LinkedHashSet<>();

  /** This VM's connection to the distributed system */
  protected static InternalDistributedSystem system;
  private static Class lastSystemCreatedInTest;
  private static Properties lastSystemProperties;
  private static volatile String testMethodName;

  private static DUnitBlackboard blackboard;

  private static final boolean logPerTest = Boolean.getBoolean("dunitLogPerTest");

  private final DistributedTestFixture distributedTestFixture;

  /**
   * Constructs a new distributed test. All JUnit 4 test classes need to have a no-arg constructor.
   */
  public JUnit4DistributedTestCase() {
    this(null);
  }

  /**
   * This constructor should only be used internally, not by tests.
   */
  protected JUnit4DistributedTestCase(final DistributedTestFixture distributedTestFixture) {
    if (distributedTestFixture == null) {
      this.distributedTestFixture = this;
    } else {
      this.distributedTestFixture = distributedTestFixture;
    }
  }

  @Rule
  public SerializableTestName testNameForDistributedTestCase = new SerializableTestName();

  @BeforeClass
  public static final void initializeDistributedTestCase() {
    DUnitLauncher.launchIfNeeded();
  }

  public static final void initializeBlackboard() {
    blackboard = new DUnitBlackboard();
  }

  protected static void deleteBACKUPDiskStoreFile(final File file) {
    if (file.getName().startsWith("BACKUPDiskStore-")
        || file.getName().startsWith(CLUSTER_CONFIG_DISK_DIR_PREFIX)) {
      FileUtils.deleteQuietly(file);
    }
  }

  public static final void cleanDiskDirs() {
    FileUtils.deleteQuietly(JUnit4CacheTestCase.getDiskDir());
    FileUtils.deleteQuietly(JUnit4CacheTestCase.getDiskDir());
    Arrays.stream(new File(".").listFiles()).forEach(file -> deleteBACKUPDiskStoreFile(file));
  }

  public final String getName() {
    if (this.distributedTestFixture != this) {
      return this.distributedTestFixture.getName();
    }
    return this.testNameForDistributedTestCase.getMethodName();
  }

  public final Class<? extends DistributedTestFixture> getTestClass() {
    return this.distributedTestFixture.getClass();
  }

  /**
   * @deprecated Please override {@link #getDistributedSystemProperties()} instead.
   */
  @Deprecated
  public final void setSystem(final Properties props, final DistributedSystem ds) {
    // TODO: override getDistributedSystemProperties and then delete
    system = (InternalDistributedSystem) ds;
    lastSystemProperties = props;
    lastSystemCreatedInTest = getTestClass(); // used to be getDeclaringClass()
  }

  /**
   * Returns this VM's connection to the distributed system. If necessary, the connection will be
   * lazily created using the given {@code Properties}.
   *
   * <p>
   * Do not override this method. Override {@link #getDistributedSystemProperties()} instead.
   *
   * <p>
   * Note: "final" was removed so that WANTestBase can override this method. This was part of the xd
   * offheap merge.
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
      Properties p = getAllDistributedSystemProperties(props);
      lastSystemCreatedInTest = getTestClass(); // used to be getDeclaringClass()
      if (logPerTest) {
        String testMethod = getTestMethodName();
        String testName = lastSystemCreatedInTest.getName() + '-' + testMethod;
        String oldLogFile = p.getProperty(LOG_FILE);
        p.put(LOG_FILE, oldLogFile.replace("system.log", testName + ".log"));
        String oldStatFile = p.getProperty(STATISTIC_ARCHIVE_FILE);
        p.put(STATISTIC_ARCHIVE_FILE, oldStatFile.replace("statArchive.gfs", testName + ".gfs"));
      }
      if (Version.CURRENT_ORDINAL < 75) {
        p.remove("validate-serializable-objects");
        p.remove("serializable-object-filter");
      }
      system = (InternalDistributedSystem) DistributedSystem.connect(p);
      lastSystemProperties = p;

    } else {
      boolean needNewSystem = false;
      if (!getTestClass().equals(lastSystemCreatedInTest)) { // used to be getDeclaringClass()
        Properties newProps = getAllDistributedSystemProperties(props);
        needNewSystem = !newProps.equals(lastSystemProperties);
        if (needNewSystem) {
          getLogWriter()
              .info("Test class has changed and the new DS properties are not an exact match. "
                  + "Forcing DS disconnect. Old props = " + lastSystemProperties + "new props="
                  + newProps);
        }

      } else {
        Properties activeProps = system.getConfig().toProperties();
        for (Entry<Object, Object> entry : props.entrySet()) {
          String key = (String) entry.getKey();
          if (key.startsWith("security-")) {
            continue;
          }
          String value = (String) entry.getValue();
          if (!value.equals(activeProps.getProperty(key))) {
            needNewSystem = true;
            getLogWriter().info("Forcing DS disconnect. For property " + key + " old value = "
                + activeProps.getProperty(key) + " new value = " + value);
            break;
          }
        }
        try {
          activeProps = system.getConfig().toSecurityProperties();
          for (Entry<Object, Object> entry : props.entrySet()) {
            String key = (String) entry.getKey();
            if (!key.startsWith("security-")) {
              continue;
            }
            String value = (String) entry.getValue();
            if (!value.equals(activeProps.getProperty(key))) {
              needNewSystem = true;
              getLogWriter().info("Forcing DS disconnect. For property " + key + " old value = "
                  + activeProps.getProperty(key) + " new value = " + value);
              break;
            }
          }
        } catch (NoSuchMethodError e) {
          if (Version.CURRENT_ORDINAL >= 85) {
            throw new IllegalStateException("missing method", e);
          }
        }
      }

      if (needNewSystem) {
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
   * Returns this VM's connection to the distributed system. If necessary, the connection will be
   * lazily created using the {@code Properties} returned by
   * {@link #getDistributedSystemProperties()}.
   *
   * <p>
   * Do not override this method. Override {@link #getDistributedSystemProperties()} instead.
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
   * {@link DistributedSystem}. Unless overridden, this method will return an empty
   * {@code Properties} object.
   *
   * <p>
   * Override this as needed. Default implementation returns empty {@code Properties}.
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
    Disconnect.disconnectAllFromDS();
  }

  /**
   * Disconnects this VM from the distributed system
   */
  public static final void disconnectFromDS() {
    if (system != null) {
      system.disconnect();
      system = null;
    }

    Disconnect.disconnectFromDS();
  }

  /**
   * Returns a DUnitBlackboard that can be used to pass data between VMs and synchronize actions.
   *
   * @return the blackboard
   */
  public DUnitBlackboard getBlackboard() {
    return blackboard;
  }

  public static final String getTestMethodName() {
    return testMethodName;
  }

  private static final void setTestMethodName(final String testMethodName) {
    JUnit4DistributedTestCase.testMethodName = testMethodName;
  }

  /**
   * Returns a unique name for this test method. It is based on the name of the class as well as the
   * name of the method.
   */
  public final String getUniqueName() {
    assertNotNull(getName());
    return getTestClass().getSimpleName() + "_" + getName();
  }

  /**
   * Sets up the DistributedTestCase.
   *
   * <p>
   * Do not override this method. Override {@link #preSetUp()} with work that needs to occur before
   * setUp() or override {@link #postSetUp()} with work that needs to occur after setUp().
   */
  @Before
  public final void setUpDistributedTestCase() throws Exception {
    preSetUp();
    doSetUpDistributedTestCase();
    postSetUp();
  }

  /**
   * Sets up DistributedTest in controller and remote VMs. This includes the defining the test name,
   * setting the default disk store name, logging the test history, and capturing a creation stack
   * for detecting the source of incompatible DistributedSystem connections.
   *
   * <p>
   * Do not override this method.
   */
  private final void doSetUpDistributedTestCase() {
    final String className = getTestClass().getCanonicalName();
    final String methodName = getName();

    logTestHistory();

    setUpVM(methodName, getDefaultDiskStoreName(0, -1, className, methodName));

    for (int hostIndex = 0; hostIndex < Host.getHostCount(); hostIndex++) {
      Host host = Host.getHost(hostIndex);
      for (int vmIndex = 0; vmIndex < host.getVMCount(); vmIndex++) {
        final String vmDefaultDiskStoreName =
            getDefaultDiskStoreName(hostIndex, vmIndex, className, methodName);
        host.getVM(vmIndex).invoke("setupVM", () -> setUpVM(methodName, vmDefaultDiskStoreName));
      }
    }

    logTestStart();
  }

  /**
   * {@code preSetUp()} is invoked before {@link #doSetUpDistributedTestCase()}.
   *
   * <p>
   * Override this as needed. Default implementation is empty.
   */
  @Override
  public void preSetUp() throws Exception {
    if (this.distributedTestFixture != this) {
      this.distributedTestFixture.preSetUp();
    }
  }

  /**
   * {@code postSetUp()} is invoked after {@link #doSetUpDistributedTestCase()}.
   *
   * <p>
   * Override this as needed. Default implementation is empty.
   */
  @Override
  public void postSetUp() throws Exception {
    if (this.distributedTestFixture != this) {
      this.distributedTestFixture.postSetUp();
    }
  }

  private static final String getDefaultDiskStoreName(final int hostIndex, final int vmIndex,
      final String className, final String methodName) {
    return "DiskStore-" + String.valueOf(hostIndex) + "-" + String.valueOf(vmIndex) + "-"
        + className + "." + methodName; // used to be getDeclaringClass()
  }

  private static final void setUpVM(final String methodName, final String defaultDiskStoreName) {
    assertNotNull("methodName must not be null", methodName);
    assertNotNull("defaultDiskStoreName must not be null", defaultDiskStoreName);
    setTestMethodName(methodName);
    GemFireCacheImpl.setDefaultDiskStoreName(defaultDiskStoreName);
    setUpCreationStackGenerator();
  }

  private final void logTestStart() {
    System.out.println(
        "\n\n[setup] START TEST " + getTestClass().getSimpleName() + "." + testMethodName + "\n\n");
  }

  private static final void setUpCreationStackGenerator() {
    // the following is moved from InternalDistributedSystem to fix #51058
    InternalDistributedSystem.TEST_CREATION_STACK_GENERATOR
        .set(new InternalDistributedSystem.CreationStackGenerator() {
          @Override
          public Throwable generateCreationStack(final DistributionConfig config) {
            StringBuilder sb = new StringBuilder();
            String[] validAttributeNames = config.getAttributeNames();
            for (String attName : validAttributeNames) {
              Object actualAtt = config.getAttributeObject(attName);
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
            return new Throwable(
                "Creating distributed system with the following configuration:\n" + sb.toString());
          }
        });
  }

  /**
   * Write a message to the log about what tests have ran previously. This makes it easier to figure
   * out if a previous test may have caused problems
   */
  private final void logTestHistory() {
    String classname = getTestClass().getSimpleName();
    testHistory.add(classname);
    System.out.println("Previously run tests: " + testHistory);
  }

  /**
   * Tears down the DistributedTestCase.
   *
   * <p>
   * Do not override this method. Override {@link #preTearDown()} with work that needs to occur
   * before tearDown() or override {@link #postTearDown()} with work that needs to occur after
   * tearDown().
   */
  @After
  public final void tearDownDistributedTestCase() throws Exception {
    try {
      try {
        preTearDownAssertions();
      } finally {
        preTearDown();
        doTearDownDistributedTestCase();
      }
    } finally {
      postTearDown();
      postTearDownAssertions();
    }
  }

  private final void doTearDownDistributedTestCase() throws Exception {
    invokeInEveryVM("tearDownCreationStackGenerator", () -> tearDownCreationStackGenerator());
    if (logPerTest) {
      disconnectAllFromDS();
    }
    cleanupAllVms();
    if (!getDistributedSystemProperties().isEmpty()) {
      disconnectAllFromDS();
    }
  }

  /**
   * {@code preTearDown()} is invoked before {@link #doTearDownDistributedTestCase()}.
   *
   * <p>
   * Override this as needed. Default implementation is empty.
   */
  @Override
  public void preTearDown() throws Exception {
    if (this.distributedTestFixture != this) {
      this.distributedTestFixture.preTearDown();
    }
  }

  /**
   * {@code postTearDown()} is invoked after {@link #doTearDownDistributedTestCase()}.
   *
   * <p>
   * Override this as needed. Default implementation is empty.
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

  public static final void cleanupAllVms() {
    tearDownVM();
    invokeInEveryVM("tearDownVM", () -> tearDownVM());
    invokeInLocator(() -> {
      DistributionMessageObserver.setInstance(null);
      unregisterInstantiatorsInThisVM();
    });
    DUnitLauncher.closeAndCheckForSuspects();
  }

  private static final void tearDownVM() {
    closeCache();
    DistributedTestRule.TearDown.tearDownInVM();
    cleanDiskDirs();
  }

  // TODO: this should move to CacheTestCase
  private static final void closeCache() {
    GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
    if (cache != null && !cache.isClosed()) {
      destroyRegions(cache);
      cache.close();
    }
  }

  // TODO: this should move to CacheTestCase
  protected static final void destroyRegions(final Cache cache) {
    if (cache != null && !cache.isClosed()) {
      // try to destroy the root regions first so that we clean up any persistent files.
      for (Region<?, ?> root : cache.rootRegions()) {
        String regionFullPath = root == null ? null : root.getFullPath();
        // for colocated regions you can't locally destroy a partitioned region.
        if (root.isDestroyed() || root instanceof HARegion || root instanceof PartitionedRegion) {
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
    InternalDistributedSystem.TEST_CREATION_STACK_GENERATOR
        .set(InternalDistributedSystem.DEFAULT_CREATION_STACK_GENERATOR);
  }
}

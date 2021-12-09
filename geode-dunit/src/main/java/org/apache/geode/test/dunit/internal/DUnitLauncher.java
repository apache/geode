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

import static org.apache.geode.distributed.ConfigurationProperties.DISABLE_AUTO_RECONNECT;
import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_CLUSTER_CONFIGURATION;
import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_MANAGEMENT_REST_SERVICE;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.USE_CLUSTER_CONFIGURATION;
import static org.apache.geode.distributed.ConfigurationProperties.VALIDATE_SERIALIZABLE_OBJECTS;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPort;
import static org.apache.geode.util.internal.GeodeGlossary.GEMFIRE_PREFIX;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.FileAppender;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.junit.Assert;

import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.distributed.internal.membership.gms.membership.GMSJoinLeave;
import org.apache.geode.logging.internal.spi.LoggingProvider;
import org.apache.geode.test.dunit.DUnitEnv;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.greplogs.ExpectedStrings;
import org.apache.geode.test.greplogs.LogConsumer;

/**
 * A class to build a fake test configuration and launch some DUnit VMS.
 *
 * For use within eclipse. This class completely skips hydra and just starts some vms directly,
 * creating a fake test configuration
 *
 * Also, it's a good idea to set your working directory, because the test code a lot of files that
 * it leaves around.
 */
public class DUnitLauncher {

  /**
   * change this to use a different log level in unit tests
   */
  public static final String logLevel = System.getProperty(LOG_LEVEL, "info");

  public static final String LOG4J = System.getProperty("log4j.configurationFile");

  /**
   * change this to have dunit/vmX directories deleted and recreated when processes are launched
   */
  public static final boolean MAKE_NEW_WORKING_DIRS =
      Boolean.getBoolean("makeNewWorkingDirsOnBounce");

  static int locatorPort;

  /**
   * Number of VMs to use during initialization.
   */
  public static int NUM_VMS = 4;

  /**
   * VM ID for the VM to use for the debugger.
   */
  public static final int DEBUGGING_VM_NUM = -1;

  /**
   * VM ID for the VM to use for the locator.
   */
  static final int LOCATOR_VM_NUM = -2;

  static final long STARTUP_TIMEOUT = 120 * 1000;
  static final String STARTUP_TIMEOUT_MESSAGE =
      "VMs did not start up within " + STARTUP_TIMEOUT / 1000 + " seconds";

  private static final String SUSPECT_FILENAME_PREFIX = "dunit_suspect";

  public static final String DUNIT_DIR = "dunit";
  public static final String WORKSPACE_DIR_PARAM = "WORKSPACE_DIR";
  public static final boolean LOCATOR_LOG_TO_DISK = Boolean.getBoolean("locatorLogToDisk");

  static final String MASTER_PARAM = "DUNIT_MASTER";

  public static final String REMOTE_STUB_PORT_PARAM = "DUnitLauncher.REMOTE_STUB_PORT";
  public static final String RMI_PORT_PARAM = GEMFIRE_PREFIX + "DUnitLauncher.RMI_PORT";
  public static final String RMI_HOST_PARAM = GEMFIRE_PREFIX + "DUnitLauncher.RMI_HOST";
  public static final String VM_NUM_PARAM = GEMFIRE_PREFIX + "DUnitLauncher.VM_NUM";
  public static final String VM_VERSION_PARAM = GEMFIRE_PREFIX + "DUnitLauncher.VM_VERSION";

  private static final String LAUNCHED_PROPERTY = GEMFIRE_PREFIX + "DUnitLauncher.LAUNCHED";

  private static final VMEventNotifier vmEventNotifier = new VMEventNotifier();

  private static Master master;

  private DUnitLauncher() {}

  private static boolean isHydra() {
    try {
      // TODO - this is hacky way to test for a hydra environment - see
      // if there is registered test configuration object.
      Class<?> clazz = Class.forName("hydra.TestConfig");
      Method getInstance = clazz.getMethod("getInstance");
      getInstance.invoke(null);
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  /**
   * Launch DUnit. If the unit test was launched through the hydra framework, leave the test alone.
   */
  public static void launchIfNeeded() {
    launchIfNeeded(true);
  }

  public static void launchIfNeeded(boolean launchLocator) {
    if (System.getProperties().contains(VM_NUM_PARAM)) {
      // we're a dunit child vm, do nothing.
      return;
    }

    if (!isHydra() && !isLaunched()) {
      try {
        launch(launchLocator);
      } catch (Exception e) {
        throw new RuntimeException("Unable to launch dunit VMs", e);
      }
    }

    Host.setAllVMsToCurrentVersion();
  }

  /**
   * Launch DUnit. If the unit test was launched through the hydra framework, leave the test alone.
   */
  public static void launchIfNeeded(int vmCount) {
    NUM_VMS = vmCount;
    launchIfNeeded();
  }

  /**
   * Launch DUnit if not already launched.
   *
   * @param vmCount specified number of VMs
   * @param launchLocator determines if default dunit locator should be launched
   */
  public static void launchIfNeeded(int vmCount, boolean launchLocator) {
    NUM_VMS = vmCount;
    launchIfNeeded(launchLocator);
  }

  /**
   * Test it see if the eclise dunit environment is launched.
   */
  public static boolean isLaunched() {
    return Boolean.getBoolean(LAUNCHED_PROPERTY);
  }

  public static String getLocatorString() {
    return "localhost[" + locatorPort + "]";
  }

  private static void launch(boolean launchLocator) throws AlreadyBoundException, IOException,
      InterruptedException, NotBoundException {

    deleteDunitSuspectFiles();

    // create an RMI registry and add an object to share our tests config
    int namingPort = getRandomAvailableTCPPort();
    Registry registry = LocateRegistry.createRegistry(namingPort);
    System.setProperty(RMI_PORT_PARAM, "" + namingPort);

    JUnit4DistributedTestCase.initializeBlackboard();

    final ProcessManager processManager = new ProcessManager(namingPort, registry);
    master = new Master(registry, processManager);
    registry.bind(MASTER_PARAM, master);

    // inhibit banners to make logs smaller
    System.setProperty(InternalLocator.INHIBIT_DM_BANNER, "true");

    // restrict membership ports to be outside of AvailablePort's range
    System.setProperty(DistributionConfig.RESTRICT_MEMBERSHIP_PORT_RANGE, "true");

    Runtime.getRuntime().addShutdownHook(new Thread(processManager::killVMs));

    if (launchLocator) {
      // Create a VM for the locator
      processManager.launchVM(LOCATOR_VM_NUM);

      // wait for the VM to start up
      if (!processManager.waitForVMs(STARTUP_TIMEOUT)) {
        throw new RuntimeException(STARTUP_TIMEOUT_MESSAGE);
      }

      locatorPort = startLocator(registry);
    }

    init(master);

    // Launch an initial set of VMs
    for (int i = 0; i < NUM_VMS; i++) {
      processManager.launchVM(i);
    }

    // wait for the VMS to start up
    if (!processManager.waitForVMs(STARTUP_TIMEOUT)) {
      throw new RuntimeException(STARTUP_TIMEOUT_MESSAGE);
    }

    // populate the Host class with our stubs. The tests use this host class
    DUnitHost host =
        new DUnitHost(InetAddress.getLocalHost().getCanonicalHostName(), processManager,
            vmEventNotifier);
    host.init(NUM_VMS, launchLocator);
  }

  public static Properties getDistributedSystemProperties() {
    Properties p = new Properties();
    p.setProperty(LOCATORS, getLocatorString());
    p.setProperty(MCAST_PORT, "0");
    p.setProperty(ENABLE_CLUSTER_CONFIGURATION, "false");
    p.setProperty(USE_CLUSTER_CONFIGURATION, "false");
    p.setProperty(VALIDATE_SERIALIZABLE_OBJECTS, "true");
    p.setProperty(LOG_LEVEL, logLevel);
    return p;
  }

  /**
   * Add an appender to Log4j which sends all INFO+ messages to a separate file which will be used
   * later to scan for suspect strings. The pattern of the messages conforms to the original log
   * format so that hydra will be able to parse them.
   */
  private static void addSuspectFileAppender(final String workspaceDir) {
    final String suspectFilename = createDunitSuspectFile(DUnitEnv.get().getId(), workspaceDir)
        .getAbsolutePath();

    Object mainLogger = LogManager.getLogger(LoggingProvider.MAIN_LOGGER_NAME);
    if (!(mainLogger instanceof org.apache.logging.log4j.core.Logger)) {
      System.err.format(
          "Unable to configure suspect file appender - cannot retrieve LoggerContext from type: %s\n",
          mainLogger.getClass().getName());
      return;
    }

    final LoggerContext appenderContext =
        ((org.apache.logging.log4j.core.Logger) mainLogger).getContext();

    final PatternLayout layout = PatternLayout.createLayout(
        "[%level{lowerCase=true} %date{yyyy/MM/dd HH:mm:ss.SSS z} <%thread> tid=%tid] %message%n%throwable%n",
        null, null, null, Charset.defaultCharset(), true, false, "", "");

    final FileAppender fileAppender = FileAppender.createAppender(suspectFilename, "true", "false",
        DUnitLauncher.class.getName(), "true", "false", "false", "0", layout, null, null, null,
        appenderContext.getConfiguration());
    fileAppender.start();

    LoggerConfig loggerConfig =
        appenderContext.getConfiguration().getLoggerConfig(LoggingProvider.MAIN_LOGGER_NAME);
    loggerConfig.addAppender(fileAppender, Level.INFO, null);
  }

  private static int startLocator(Registry registry) throws IOException, NotBoundException {
    RemoteDUnitVMIF remote = (RemoteDUnitVMIF) registry.lookup("vm" + LOCATOR_VM_NUM);

    int port = getRandomAvailableTCPPort();
    final File locatorLogFile =
        LOCATOR_LOG_TO_DISK ? new File("locator-" + port + ".log") : new File("");

    MethodInvokerResult result = remote.executeMethodOnObject(new SerializableCallable() {
      @Override
      public Object call() throws IOException {
        Properties p = getDistributedSystemProperties();
        // I never want this locator to end up starting a jmx manager
        // since it is part of the unit test framework
        p.setProperty(JMX_MANAGER, "false");
        p.setProperty(ENABLE_MANAGEMENT_REST_SERVICE, "false");
        // Disable the shared configuration on this locator.
        // Shared configuration tests create their own locator
        p.setProperty(ENABLE_CLUSTER_CONFIGURATION, "false");
        // Tell the locator it's the first in the system for
        // faster boot-up
        System.setProperty(GMSJoinLeave.BYPASS_DISCOVERY_PROPERTY, "true");
        // disable auto-reconnect - tests fly by so fast that it will never be
        // able to do so successfully anyway
        p.setProperty(DISABLE_AUTO_RECONNECT, "true");

        try {
          Locator.startLocatorAndDS(port, locatorLogFile, p);
          locatorPort = port;
        } finally {
          System.getProperties().remove(GMSJoinLeave.BYPASS_DISCOVERY_PROPERTY);
        }

        return locatorPort;
      }
    }, "call");
    if (result.getException() != null) {
      RuntimeException ex = new RuntimeException("Failed to start locator", result.getException());
      ex.printStackTrace();
      throw ex;
    }
    return (Integer) result.getResult();
  }

  public static void init(MasterRemote master) {
    DUnitEnv.set(new StandAloneDUnitEnv(master));
    addSuspectFileAppender(getWorkspaceDir());

    // Free off heap memory when disconnecting from the distributed system
    System.setProperty(GEMFIRE_PREFIX + "free-off-heap-memory", "true");

    // indicate that this CM is controlled by the eclipse dunit.
    System.setProperty(LAUNCHED_PROPERTY, "true");
  }

  private static List<File> getDunitSuspectFiles() {
    File[] suspectFiles = getDunitSuspectsDir()
        .listFiles((dir, name) -> name.startsWith(SUSPECT_FILENAME_PREFIX));

    return Arrays.asList(suspectFiles);
  }

  private static File getDunitSuspectsDir() {
    return Paths.get(getWorkspaceDir()).toFile();
  }

  private static void deleteDunitSuspectFiles() {
    getDunitSuspectFiles().forEach(File::delete);
  }

  private static File createDunitSuspectFile(int vmId, String workingDir) {
    String suffix;

    switch (vmId) {
      case -2:
        suffix = "locator";
        break;
      case -1:
        suffix = "local";
        break;
      default:
        suffix = "vm" + vmId;
    }

    File dunitSuspect = new File(getDunitSuspectsDir(),
        getSuspectFileName(suffix));
    dunitSuspect.deleteOnExit();

    return dunitSuspect;
  }

  private static String getWorkspaceDir() {
    String workspaceDir = System.getProperty(DUnitLauncher.WORKSPACE_DIR_PARAM);
    workspaceDir = workspaceDir == null ? new File(".").getAbsolutePath() : workspaceDir;

    return workspaceDir;
  }

  public static void closeAndCheckForSuspects(int vmIndex) {
    String suffix = "vm" + vmIndex;
    String fileName = getSuspectFileName(suffix);
    File[] suspectFiles = getDunitSuspectsDir()
        .listFiles((dir, name) -> name.startsWith(fileName));
    closeAndCheckForSuspects(Arrays.asList(suspectFiles));
  }

  private static String getSuspectFileName(String suffix) {
    return String.format("%s-%s.log", SUSPECT_FILENAME_PREFIX, suffix);
  }

  public static void closeAndCheckForSuspects(List<File> suspectFiles) {
    StringBuilder suspectStringCollector = new StringBuilder();
    for (File suspect : suspectFiles) {
      checkSuspectFile(suspect, suspectStringCollector);
    }

    if (suspectStringCollector.length() != 0) {
      System.err.println("Suspicious strings were written to the log during this run.\n"
          + "Fix the strings or use IgnoredException.addIgnoredException to ignore.\n"
          + suspectStringCollector);

      Assert.fail("Suspicious strings were written to the log during this run.\n"
          + "Fix the strings or use IgnoredException.addIgnoredException to ignore.\n"
          + suspectStringCollector);
    }
  }

  public static void closeAndCheckForSuspects() {
    if (!isLaunched()) {
      return;
    }
    List<File> suspectFiles = getDunitSuspectFiles();
    if (suspectFiles.isEmpty()) {
      throw new IllegalStateException("No dunit suspect log files found in '"
          + getDunitSuspectsDir().getAbsolutePath()
          + "' - perhaps a rule that is cleaning up before suspect processing has already run.");
    }
    closeAndCheckForSuspects(suspectFiles);
  }

  private static void checkSuspectFile(File suspectFile, StringBuilder suspectStringCollector) {
    final List<Pattern> expectedStrings = ExpectedStrings.create("dunit");
    final LogConsumer logConsumer = new LogConsumer(true, expectedStrings,
        suspectFile.getName(), 5);

    BufferedReader buffReader;
    FileChannel fileChannel;
    try {
      fileChannel = new FileOutputStream(suspectFile, true).getChannel();
      buffReader = new BufferedReader(new FileReader(suspectFile));
    } catch (FileNotFoundException e) {
      throw new RuntimeException(e);
    }

    try {
      String line;
      try {
        while ((line = buffReader.readLine()) != null) {
          final String suspectString = logConsumer.consume(line);
          if (suspectString != null) {
            suspectStringCollector.append(suspectString);
          }
        }
      } catch (IOException e) {
        System.err.println("Could not read the suspect string output file: " + e);
      }

      try {
        fileChannel.truncate(0);
      } catch (IOException e) {
        System.err.println("Could not truncate the suspect string output file: " + e);
      }

    } finally {
      try {
        buffReader.close();
        fileChannel.close();
      } catch (IOException e) {
        System.err.println("Could not close the suspect string output file: " + e);
      }
    }
  }

}

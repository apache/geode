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
package org.apache.geode.internal.cache;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPorts;
import static org.apache.geode.internal.cache.xmlcache.CacheXml.GEODE_NAMESPACE;
import static org.apache.geode.internal.cache.xmlcache.CacheXml.LATEST_SCHEMA_LOCATION;
import static org.apache.geode.internal.process.ProcessUtils.isProcessAlive;
import static org.apache.geode.test.dunit.Disconnect.disconnectAllFromDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.Assume.assumeFalse;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.rmi.AlreadyBoundException;
import java.rmi.NoSuchObjectException;
import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import org.apache.geode.InternalGemFireError;
import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.distributed.Locator;
import org.apache.geode.internal.PureJavaMode;
import org.apache.geode.internal.cache.control.InternalResourceManager;
import org.apache.geode.internal.cache.control.InternalResourceManager.ResourceObserverAdapter;
import org.apache.geode.internal.cache.xmlcache.CacheXml;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.junit.categories.FlakyTest;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.geode.test.process.ProcessWrapper;

/**
 * Tests the CacheServerLauncher. Extracted/renamed from CacheServerLauncherDUnitTest.
 *
 * @since GemFire 6.0
 */
@Category(IntegrationTest.class)
public class DeprecatedCacheServerLauncherIntegrationTest {
  private static final Logger logger = LogService.getLogger();

  private static final String CLASSNAME =
      DeprecatedCacheServerLauncherIntegrationTest.class.getSimpleName();
  private static final String CONTROLLER_NAMING_PORT_PROP = CLASSNAME + ".controllerNamingPort";
  private static final String CACHESERVER_NAMING_PORT_PROP = CLASSNAME + ".cacheServerNamingPort";
  private static final String REBALANCE_STATUS_BINDING = CLASSNAME + ".REBALANCE_STATUS_BINDING";
  private static final String FAIL_SAFE_BINDING = CLASSNAME + ".FAIL_SAFE_BINDING";

  private String classpath;

  private int serverPort;

  private int controllerNamingPort;
  private int cacheServerNamingPort;
  private int commandPort;
  private int xmlPort;

  private File directory;
  private File logFile;
  private File cacheXmlFile;

  private String directoryPath;
  private String logFileName;
  private String cacheXmlFileName;

  private Registry registry;
  private RebalanceStatus status;

  private ProcessWrapper processWrapper;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public TestName testName = new TestName();

  @Before
  public void setUp() throws Exception {
    this.classpath = System.getProperty("java.class.path");
    assertThat(this.classpath).isNotEmpty();

    this.directory = this.temporaryFolder.getRoot();
    this.directoryPath = this.directory.getAbsolutePath();
    this.logFileName = this.testName.getMethodName() + ".log";
    this.cacheXmlFileName = this.testName.getMethodName() + ".xml";

    this.cacheXmlFile = new File(this.directory, this.cacheXmlFileName);
    this.logFile = new File(this.directory, this.logFileName);

    int[] tcpPorts = getRandomAvailableTCPPorts(5);
    this.serverPort = tcpPorts[0];
    this.controllerNamingPort = tcpPorts[1];
    this.cacheServerNamingPort = tcpPorts[2];
    this.commandPort = tcpPorts[3];
    this.xmlPort = tcpPorts[4];
  }

  @After
  public void tearDown() throws Exception {
    disconnectAllFromDS();

    invokeFailSafe();

    unexportObject(this.status);
    unexportObject(this.registry);
    destroy(this.processWrapper);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    if (Locator.hasLocator()) {
      Locator.getLocator().stop();
    }
  }

  @Test
  public void testStartStatusStop() throws Exception {
    createCacheXml(this.directory, this.cacheXmlFileName, this.serverPort);

    execAndValidate("CacheServer pid: \\d+ status: running", "start",
        "-J-D" + CONTROLLER_NAMING_PORT_PROP + "=" + this.controllerNamingPort,
        "-J-D" + CACHESERVER_NAMING_PORT_PROP + "=" + this.cacheServerNamingPort,
        "-J-Xmx" + Runtime.getRuntime().maxMemory(), "-J-Dgemfire.use-cluster-configuration=false",
        "-J-Dgemfire.locators=\"\"", "log-file=" + this.logFileName,
        "cache-xml-file=" + this.cacheXmlFileName, "-dir=" + this.directoryPath,
        "-classpath=" + this.classpath);

    execAndValidate("CacheServer pid: \\d+ status: running", "status",
        "-dir=" + this.directoryPath);

    execAndValidate(".*The CacheServer has stopped\\.", "stop", "-dir=" + this.directoryPath);
  }

  @Test
  public void testStartWithExistingCacheServerDotSerFileCheckStatusAndStop() throws Exception {
    File cacheServerDotSerFile = createCacheServerSerFile(this.directory);

    createCacheXml(this.directory, this.cacheXmlFileName, this.serverPort, 0);

    execAndValidate("CacheServer pid: \\d+ status: running", "start",
        "cache-xml-file=" + this.cacheXmlFile.getAbsolutePath(),
        "log-file=" + this.logFile.getAbsolutePath(), "-J-Dgemfire.use-cluster-configuration=false",
        "-J-Dgemfire.locators=\"\"", "-server-port=" + this.serverPort,
        "-dir=" + this.directory.getAbsolutePath(), "-classpath=" + this.classpath);

    execAndValidate("CacheServer pid: \\d+ status: running", "status",
        "-dir=" + this.directory.getAbsolutePath());

    execAndValidate(".*The CacheServer has stopped\\.", "stop",
        "-dir=" + this.directory.getAbsolutePath());

    await().atMost(2, MINUTES).until(() -> assertThat(cacheServerDotSerFile).doesNotExist());
  }

  @Ignore("This test needs to be reworked")
  @Test
  public void testCacheServerTerminatingAbnormally() throws Exception {
    assumeFalse(PureJavaMode.isPure());

    createCacheXml(this.directory, this.cacheXmlFileName, this.serverPort, 0);

    this.processWrapper = new ProcessWrapper.Builder().mainClass(CacheServerLauncher.class)
        .mainArguments(new String[] {"start", "cache-xml-file=" + this.cacheXmlFileName,
            "log-file=" + this.logFileName, "log-level=info",
            "-J-Dgemfire.use-cluster-configuration=false", "-J-Dgemfire.locators=\"\"",
            "-server-port=" + this.serverPort, "-dir=" + this.directoryPath,
            "-classpath=" + this.classpath})
        .build();

    this.processWrapper.execute();
    this.processWrapper.waitForOutputToMatch("CacheServer pid: \\d+ status: running");
    this.processWrapper.waitFor();
    String processOutput = this.processWrapper.getOutput();

    Matcher matcher = Pattern.compile("\\d+").matcher(processOutput);
    assertThat(matcher.find()).isTrue();
    assertThat(matcher.find()).isTrue();
    String pidString = matcher.group();
    int pid = Integer.parseInt(pidString);

    assertThat(isProcessAlive(pid)).isTrue();

    // now, we will forcefully kill the CacheServer process
    invokeFailSafe();

    await().atMost(2, MINUTES)
        .until(() -> assertThat(this.processWrapper.getProcess().isAlive()).isFalse());

    await().atMost(2, MINUTES).until(() -> assertThat(isProcessAlive(pid)).isFalse());

    File dotCacheServerDotSerFile = new File(this.directory, ".cacheserver.ser");

    // assert that the .cacheserver.ser file remains...
    assertThat(dotCacheServerDotSerFile).exists();

    await().atMost(2, MINUTES)
        .until(() -> execAndWaitForOutputToMatch("CacheServer pid: " + pid + " status: stopped",
            "status", "-dir=" + this.directory.getName()));

    assertThat(this.processWrapper.getOutput()).isNull();

    execAndValidate("The CacheServer has stopped.", "stop", "-dir=" + this.directory.getName());

    execAndValidate("CacheServer pid: 0 status: stopped", "status",
        "-dir=" + this.directory.getName());

    assertThat(dotCacheServerDotSerFile).doesNotExist();
  }

  @Test
  public void testRebalance() throws Exception {
    this.registry = LocateRegistry.createRegistry(this.controllerNamingPort);
    this.status = new RebalanceStatus();
    this.registry.bind(REBALANCE_STATUS_BINDING, this.status);

    createCacheXml(this.directory, this.cacheXmlFileName, this.serverPort);

    execAndValidate("CacheServer pid: \\d+ status: running", "start",
        "-J-D" + CONTROLLER_NAMING_PORT_PROP + "=" + this.controllerNamingPort,
        "-J-D" + CACHESERVER_NAMING_PORT_PROP + "=" + this.cacheServerNamingPort,
        "-J-Xmx" + Runtime.getRuntime().maxMemory(), "-J-Dgemfire.use-cluster-configuration=false",
        "-J-Dgemfire.locators=\"\"", "log-file=" + this.logFileName,
        "cache-xml-file=" + this.cacheXmlFileName, "-dir=" + this.directoryPath,
        "-classpath=" + this.classpath, "-rebalance");

    await().atMost(2, MINUTES).until(() -> assertThat(this.status.isStarted()).isTrue());
    await().atMost(2, MINUTES).until(() -> assertThat(this.status.isFinished()).isTrue());

    execAndValidate("CacheServer pid: \\d+ status: running", "status", "-dir=" + this.directory);

    execAndValidate(".*The CacheServer has stopped\\.", "stop", "-dir=" + this.directory);
  }

  @Test
  @Category(FlakyTest.class) // GEODE-3939
  public void testCreateBuckets() throws Exception {
    this.registry = LocateRegistry.createRegistry(this.controllerNamingPort);
    this.status = new RebalanceStatus();
    this.registry.bind(REBALANCE_STATUS_BINDING, this.status);

    createCacheXml(this.directory, this.cacheXmlFileName, this.serverPort);

    execAndValidate("CacheServer pid: \\d+ status: running", "start",
        "-J-D" + CONTROLLER_NAMING_PORT_PROP + "=" + this.controllerNamingPort,
        "-J-D" + CACHESERVER_NAMING_PORT_PROP + "=" + this.cacheServerNamingPort,
        "-J-D" + CacheServerLauncher.ASSIGN_BUCKETS + "=true",
        "-J-Xmx" + Runtime.getRuntime().maxMemory(), "-J-Dgemfire.use-cluster-configuration=false",
        "-J-Dgemfire.locators=\"\"", "log-file=" + this.logFileName,
        "cache-xml-file=" + this.cacheXmlFileName, "-dir=" + this.directoryPath,
        "-classpath=" + this.classpath, "-rebalance");

    await().atMost(2, MINUTES).until(() -> assertThat(this.status.isStarted()).isTrue());
    await().atMost(2, MINUTES).until(() -> assertThat(this.status.isFinished()).isTrue());

    execAndValidate("CacheServer pid: \\d+ status: running", "status", "-dir=" + this.directory);

    execAndValidate(".*The CacheServer has stopped\\.", "stop", "-dir=" + this.directory);
  }

  @Test
  public void testWithoutServerPort() throws Exception {
    createCacheXml(this.directory, this.cacheXmlFileName, this.xmlPort, 1);

    execAndValidate("CacheServer pid: \\d+ status: running", "start",
        "-J-D" + CONTROLLER_NAMING_PORT_PROP + "=" + this.controllerNamingPort,
        "-J-D" + CACHESERVER_NAMING_PORT_PROP + "=" + this.cacheServerNamingPort,
        "-J-Xmx" + Runtime.getRuntime().maxMemory(), "-J-Dgemfire.use-cluster-configuration=false",
        "-J-Dgemfire.locators=\"\"", "log-file=" + this.logFileName,
        "cache-xml-file=" + this.cacheXmlFileName, "-dir=" + this.directoryPath,
        "-classpath=" + this.classpath);

    execAndValidate("CacheServer pid: \\d+ status: running", "status", "-dir=" + this.directory);

    ClientCache cache = new ClientCacheFactory().create();
    ClientRegionFactory<Integer, Integer> regionFactory =
        cache.createClientRegionFactory(ClientRegionShortcut.PROXY);
    Pool pool = PoolManager.createFactory().addServer("localhost", this.xmlPort).create("cslPool");
    regionFactory.setPoolName(pool.getName());
    Region<Integer, Integer> region = regionFactory.create("rgn");
    List<InetSocketAddress> servers = pool.getServers();

    assertThat(servers).hasSize(1);
    assertThat(servers.iterator().next().getPort()).isEqualTo(this.xmlPort);

    region.put(1, 1); // put should be successful

    execAndValidate("The CacheServer has stopped\\.", "stop", "-dir=" + this.directory);
  }

  @Test
  public void testServerPortOneCacheServer() throws Exception {
    createCacheXml(this.directory, this.cacheXmlFileName, this.xmlPort, 1);

    execAndValidate("CacheServer pid: \\d+ status: running", "start",
        "-J-D" + CONTROLLER_NAMING_PORT_PROP + "=" + this.controllerNamingPort,
        "-J-D" + CACHESERVER_NAMING_PORT_PROP + "=" + this.cacheServerNamingPort,
        "-J-Xmx" + Runtime.getRuntime().maxMemory(), "-J-Dgemfire.use-cluster-configuration=false",
        "-J-Dgemfire.locators=\"\"", "log-file=" + this.logFileName,
        "cache-xml-file=" + this.cacheXmlFileName, "-dir=" + this.directoryPath,
        "-classpath=" + this.classpath, "-server-port=" + this.commandPort);

    execAndValidate("CacheServer pid: \\d+ status: running", "status", "-dir=" + this.directory);

    ClientCache cache = new ClientCacheFactory().create();

    ClientRegionFactory<Integer, Integer> regionFactory =
        cache.createClientRegionFactory(ClientRegionShortcut.PROXY);
    Pool pool =
        PoolManager.createFactory().addServer("localhost", this.commandPort).create("cslPool");
    regionFactory.setPoolName(pool.getName());
    Region<Integer, Integer> region = regionFactory.create("rgn");
    List<InetSocketAddress> servers = pool.getServers();

    assertThat(servers).hasSize(1);
    assertThat(servers.iterator().next().getPort()).isEqualTo(this.commandPort);

    region.put(1, 1); // put should be successful

    execAndValidate("The CacheServer has stopped\\.", "stop", "-dir=" + this.directory);
  }

  @Test
  public void testServerPortNoCacheServer() throws Exception {
    createCacheXml(this.directory, this.cacheXmlFileName, 0, 0);

    execAndValidate("CacheServer pid: \\d+ status: running", "start",
        "-J-D" + CONTROLLER_NAMING_PORT_PROP + "=" + this.controllerNamingPort,
        "-J-D" + CACHESERVER_NAMING_PORT_PROP + "=" + this.cacheServerNamingPort,
        "-J-Xmx" + Runtime.getRuntime().maxMemory(), "-J-Dgemfire.use-cluster-configuration=false",
        "-J-Dgemfire.locators=\"\"", "log-file=" + this.logFileName,
        "cache-xml-file=" + this.cacheXmlFileName, "-dir=" + this.directoryPath,
        "-classpath=" + this.classpath, "-server-port=" + this.commandPort,
        "-server-bind-address=" + InetAddress.getLocalHost().getHostName());

    execAndValidate("CacheServer pid: \\d+ status: running", "status", "-dir=" + this.directory);

    ClientCache cache = new ClientCacheFactory().create();
    ClientRegionFactory<Integer, Integer> regionFactory =
        cache.createClientRegionFactory(ClientRegionShortcut.PROXY);
    Pool pool = PoolManager.createFactory()
        .addServer(InetAddress.getLocalHost().getHostName(), this.commandPort).create("cslPool");
    regionFactory.setPoolName(pool.getName());
    Region<Integer, Integer> region = regionFactory.create("rgn");
    List<InetSocketAddress> servers = pool.getServers();

    assertThat(servers).hasSize(1);
    assertThat(servers.iterator().next().getPort()).isEqualTo(this.commandPort);

    region.put(1, 1); // put should be successful

    execAndValidate("The CacheServer has stopped\\.", "stop", "-dir=" + this.directory);
  }

  private void unexportObject(final Remote object) {
    if (object == null) {
      return;
    }
    try {
      UnicastRemoteObject.unexportObject(object, true);
    } catch (NoSuchObjectException ignore) {
    }
  }

  private void destroy(final ProcessWrapper processWrapper) {
    if (processWrapper == null) {
      return;
    }
    processWrapper.destroy();
  }

  private File createCacheServerSerFile(final File directory) throws Exception {
    File cacheServerSerFile = new File(directory, ".cacheserver.ser");
    assertThat(cacheServerSerFile.createNewFile()).isTrue();
    return cacheServerSerFile;
  }

  private void invokeFailSafe() {
    try {
      Registry registry = LocateRegistry.getRegistry(this.cacheServerNamingPort);
      FailSafeRemote failSafe = (FailSafeRemote) registry.lookup(FAIL_SAFE_BINDING);
      failSafe.kill();
    } catch (RemoteException | NotBoundException ignore) {
      // cacheserver was probably stopped already
    }
  }

  private void execAndValidate(final String regex, final String... args)
      throws InterruptedException, TimeoutException {
    ProcessWrapper processWrapper = new ProcessWrapper.Builder()
        .mainClass(CacheServerLauncher.class).mainArguments(args).build();
    processWrapper.execute();
    if (regex != null) {
      processWrapper.waitForOutputToMatch(regex, 2 * 60 * 1000);
    }
    processWrapper.waitFor();
  }

  private boolean execAndWaitForOutputToMatch(final String regex, final String... args) {
    ProcessWrapper processWrapper = new ProcessWrapper.Builder()
        .mainClass(CacheServerLauncher.class).mainArguments(args).build();
    try {
      processWrapper.execute();
    } catch (InterruptedException | TimeoutException e) {
      throw new AssertionError(e);
    }
    Pattern pattern = Pattern.compile(regex);
    return pattern.matcher(processWrapper.getOutput(true)).matches();
  }

  private void createCacheXml(final File dir, final String cacheXmlName, final int port)
      throws IOException {
    File file = new File(dir, cacheXmlName);
    assertThat(file.createNewFile()).isTrue();

    try (FileWriter writer = new FileWriter(file)) {
      writer.write("<?xml version=\"1.0\"?>\n");
      writer.write("<cache\n");
      writer.write("    xmlns=\"" + GEODE_NAMESPACE + "\"");
      writer.write("    xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"");
      writer.write(
          "    xsi:schemaLocation=\"" + GEODE_NAMESPACE + " " + LATEST_SCHEMA_LOCATION + "\"");
      writer.write("    version=\"" + CacheXml.VERSION_LATEST + "\">");
      writer.write("  <cache-server port=\"" + port + "\" notify-by-subscription=\"false\"/>\n");
      writer.write("  <region name=\"PartitionedRegion\">\n");
      writer.write("    <region-attributes>\n");
      writer.write("      <partition-attributes redundant-copies=\"0\"/>\n");
      writer.write("      <cache-listener>\n");
      writer
          .write("        <class-name>" + SpecialCacheListener.class.getName() + "</class-name>\n");
      writer.write("      </cache-listener>\n");

      writer.write("    </region-attributes>\n");
      writer.write("  </region>\n");
      writer.write("</cache>\n");

      writer.flush();
    }
  }

  private void createCacheXml(final File dir, final String cacheXmlName, final int port,
      final int numServers) throws IOException {
    File file = new File(dir, cacheXmlName);
    assertThat(file.createNewFile()).isTrue();

    try (FileWriter writer = new FileWriter(file)) {
      writer.write("<?xml version=\"1.0\"?>\n");
      writer.write("<cache\n");
      writer.write("    xmlns=\"" + GEODE_NAMESPACE + "\"");
      writer.write("    xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"");
      writer.write(
          "    xsi:schemaLocation=\"" + GEODE_NAMESPACE + " " + LATEST_SCHEMA_LOCATION + "\"");
      writer.write("    version=\"" + CacheXml.VERSION_LATEST + "\">");
      for (int i = 0; i < numServers; i++) {
        writer.write("  <cache-server port=\"" + port + "\"");
        writer.write("/>\n");
      }
      writer.write("<region name=\"rgn\" />\n");
      writer.write("</cache>\n");
      writer.flush();
    }
  }

  public static class SpecialCacheListener<K, V> extends CacheListenerAdapter<K, V>
      implements Declarable {

    private static final int CONTROLLER_NAMING_PORT =
        Integer.getInteger(CONTROLLER_NAMING_PORT_PROP);

    private static final int CACHESERVER_NAMING_PORT =
        Integer.getInteger(CACHESERVER_NAMING_PORT_PROP);

    public SpecialCacheListener() {
      try {
        Registry registry = LocateRegistry.createRegistry(CACHESERVER_NAMING_PORT);
        FailSafe failsafe = new FailSafe();
        registry.bind(FAIL_SAFE_BINDING, failsafe);
      } catch (RemoteException | AlreadyBoundException e) {
        throw new InternalGemFireError(e);
      }

      InternalResourceManager.setResourceObserver(new ResourceObserverAdapter() {
        @Override
        public void rebalancingOrRecoveryStarted(final Region region) {
          try {
            logger.info("SpecialCacheListener#rebalancingStarted on {}", region);
            Registry registry = LocateRegistry.getRegistry(CONTROLLER_NAMING_PORT);
            RebalanceStatusRemote status =
                (RebalanceStatusRemote) registry.lookup(REBALANCE_STATUS_BINDING);
            if (region.getName().contains("PartitionedRegion")) {
              status.rebalancingStarted();
            }
          } catch (RemoteException | NotBoundException e) {
            throw new InternalGemFireError(e);
          }
        }

        @Override
        public void rebalancingOrRecoveryFinished(final Region region) {
          try {
            logger.info("SpecialCacheListener#rebalancingFinished on {}", region);
            Registry registry = LocateRegistry.getRegistry(CONTROLLER_NAMING_PORT);
            RebalanceStatusRemote status =
                (RebalanceStatusRemote) registry.lookup(REBALANCE_STATUS_BINDING);
            if (region.getName().contains("PartitionedRegion")) {
              status.rebalancingFinished();
            }
          } catch (RemoteException | NotBoundException e) {
            throw new InternalGemFireError(e);
          }
        }
      });
    }

    @Override
    public void init(Properties props) {
      // do nothing
    }
  }

  private interface RebalanceStatusRemote extends Remote {
    void rebalancingStarted() throws RemoteException;

    void rebalancingFinished() throws RemoteException;
  }

  private static class RebalanceStatus extends UnicastRemoteObject
      implements RebalanceStatusRemote {
    private final Object lock = new Object();
    private boolean started = false;
    private boolean finished = false;

    RebalanceStatus() throws RemoteException {
      super();
    }

    @Override
    public void rebalancingStarted() throws RemoteException {
      synchronized (this.lock) {
        this.started = true;
        this.lock.notifyAll();
      }
    }

    @Override
    public void rebalancingFinished() throws RemoteException {
      synchronized (this.lock) {
        this.finished = true;
        this.lock.notifyAll();
      }
    }

    boolean isStarted() {
      return this.started;
    }

    boolean isFinished() {
      return this.finished;
    }
  }

  private interface FailSafeRemote extends Remote {
    void kill() throws RemoteException;
  }

  private static class FailSafe extends UnicastRemoteObject implements FailSafeRemote {

    FailSafe() throws RemoteException {
      super();
    }

    @Override
    public void kill() throws RemoteException {
      System.exit(0);
    }
  }
}

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

import static java.lang.System.lineSeparator;
import static java.util.Arrays.asList;
import static org.apache.geode.cache.client.ClientRegionShortcut.PROXY;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.USE_CLUSTER_CONFIGURATION;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPorts;
import static org.apache.geode.internal.cache.xmlcache.CacheXml.GEODE_NAMESPACE;
import static org.apache.geode.internal.cache.xmlcache.CacheXml.LATEST_SCHEMA_LOCATION;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.Disconnect.disconnectAllFromDS;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import org.apache.geode.InternalGemFireError;
import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.internal.cache.control.InternalResourceManager;
import org.apache.geode.internal.cache.control.InternalResourceManager.ResourceObserverAdapter;
import org.apache.geode.internal.cache.xmlcache.CacheXml;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.process.ProcessWrapper;

/**
 * Tests the deprecated CacheServerLauncher.
 *
 * @since GemFire 6.0
 */
public class DeprecatedCacheServerLauncherIntegrationTest {
  private static final Logger logger = LogService.getLogger();

  private static final long TIMEOUT_MILLIS = GeodeAwaitility.getTimeout().getValueInMS();

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
  private File configPropertiesFile;

  private String directoryPath;
  private String logFileName;
  private String cacheXmlFileName;

  private Registry registry;
  private RebalanceStatus status;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public TestName testName = new TestName();

  @Before
  public void setUp() throws Exception {
    classpath = System.getProperty("java.class.path");
    assertThat(classpath).isNotEmpty();

    directory = temporaryFolder.getRoot();
    directoryPath = directory.getAbsolutePath();
    logFileName = testName.getMethodName() + ".log";
    cacheXmlFileName = testName.getMethodName() + ".xml";
    String configPropertiesFileName = testName.getMethodName() + ".properties";

    configPropertiesFile = new File(directory, configPropertiesFileName);
    cacheXmlFile = new File(directory, cacheXmlFileName);
    logFile = new File(directory, logFileName);

    int[] tcpPorts = getRandomAvailableTCPPorts(5);
    serverPort = tcpPorts[0];
    controllerNamingPort = tcpPorts[1];
    cacheServerNamingPort = tcpPorts[2];
    commandPort = tcpPorts[3];
    xmlPort = tcpPorts[4];

    Properties configProperties = new Properties();
    configProperties.setProperty(HTTP_SERVICE_PORT, "0");
    configProperties.setProperty(LOCATORS, "");
    configProperties.setProperty(USE_CLUSTER_CONFIGURATION, "false");
    try (FileOutputStream fileOutputStream = new FileOutputStream(configPropertiesFile)) {
      configProperties.store(fileOutputStream, null);
    }
  }

  @After
  public void tearDown() throws Exception {
    disconnectAllFromDS();

    invokeFailSafe();

    unexportObject(status);
    unexportObject(registry);

    if (logFile.exists()) {
      System.out.println("------------------------------------------------------------");
      System.out.println("Log file for launched process");
      System.out.println("------------------------------------------------------------");
      try (FileInputStream input = new FileInputStream(logFile)) {
        IOUtils.copy(input, System.out);
      }
      System.out.println("------------------------------------------------------------");
    }
  }

  @Test
  public void testStartStatusStop() throws Exception {
    createCacheXml(directory, cacheXmlFileName, serverPort);

    execWithArgsAndValidate(Operation.START, "CacheServer pid: \\d+ status: running",
        asList(
            "cache-xml-file=" + cacheXmlFileName,
            "log-file=" + logFileName,
            "-classpath=" + createManifestJar(),
            "-dir=" + directoryPath));

    execWithDirAndValidate(Operation.STATUS, "CacheServer pid: \\d+ status: running");

    execWithDirAndValidate(Operation.STOP, ".*The CacheServer has stopped\\.");
  }

  @Test
  public void testStartWithExistingCacheServerDotSerFileCheckStatusAndStop() throws Exception {
    File cacheServerDotSerFile = createCacheServerSerFile(directory);

    createCacheXml(directory, cacheXmlFileName, serverPort, 0);

    execWithArgsAndValidate(Operation.START, "CacheServer pid: \\d+ status: running",
        asList(
            "cache-xml-file=" + cacheXmlFile.getAbsolutePath(),
            "log-file=" + logFile.getAbsolutePath(),
            "-classpath=" + createManifestJar(),
            "-dir=" + directory.getAbsolutePath(),
            "-server-port=" + serverPort));

    execWithDirAndValidate(Operation.STATUS, "CacheServer pid: \\d+ status: running");

    execWithDirAndValidate(Operation.STOP, ".*The CacheServer has stopped\\.");

    await().untilAsserted(() -> assertThat(cacheServerDotSerFile).doesNotExist());
  }

  @Test
  public void testRebalance() throws Exception {
    registry = LocateRegistry.createRegistry(controllerNamingPort);
    status = new RebalanceStatus();
    registry.bind(REBALANCE_STATUS_BINDING, status);

    createCacheXml(directory, cacheXmlFileName, serverPort);

    execWithArgsAndValidate(Operation.START, "CacheServer pid: \\d+ status: running",
        asList(
            "cache-xml-file=" + cacheXmlFileName,
            "log-file=" + logFileName,
            "-classpath=" + createManifestJar(),
            "-dir=" + directoryPath,
            "-rebalance"));

    await().untilAsserted(() -> assertThat(status.isStarted()).isTrue());
    await().untilAsserted(() -> assertThat(status.isFinished()).isTrue());

    execWithDirAndValidate(Operation.STATUS, "CacheServer pid: \\d+ status: running");

    execWithDirAndValidate(Operation.STOP, ".*The CacheServer has stopped\\.");
  }

  @Test
  public void testCreateBuckets() throws Exception {
    registry = LocateRegistry.createRegistry(controllerNamingPort);
    status = new RebalanceStatus();
    registry.bind(REBALANCE_STATUS_BINDING, status);

    createCacheXml(directory, cacheXmlFileName, serverPort);

    execWithArgsAndValidate(Operation.START, "CacheServer pid: \\d+ status: running",
        asList(
            "-J-D" + CacheServerLauncher.ASSIGN_BUCKETS_PROPERTY + "=true",
            "cache-xml-file=" + cacheXmlFileName,
            "log-file=" + logFileName,
            "-classpath=" + createManifestJar(),
            "-dir=" + directoryPath,
            "-rebalance"));

    await().untilAsserted(() -> assertThat(status.isStarted()).isTrue());
    await().untilAsserted(() -> assertThat(status.isFinished()).isTrue());

    execWithDirAndValidate(Operation.STATUS, "CacheServer pid: \\d+ status: running");

    execWithDirAndValidate(Operation.STOP, ".*The CacheServer has stopped\\.");
  }

  @Test
  public void testWithoutServerPort() throws Exception {
    createCacheXml(directory, cacheXmlFileName, xmlPort, 1);

    execWithArgsAndValidate(Operation.START, "CacheServer pid: \\d+ status: running",
        asList(
            "cache-xml-file=" + cacheXmlFileName,
            "log-file=" + logFileName,
            "-classpath=" + createManifestJar(),
            "-dir=" + directoryPath));

    execWithDirAndValidate(Operation.STATUS, "CacheServer pid: \\d+ status: running");

    ClientCache cache = new ClientCacheFactory()
        .create();
    Pool pool = PoolManager.createFactory()
        .addServer("localhost", xmlPort)
        .create("cslPool");
    Region<Integer, Integer> region = cache.<Integer, Integer>createClientRegionFactory(PROXY)
        .setPoolName(pool.getName())
        .create("rgn");

    List<InetSocketAddress> servers = pool.getServers();
    assertThat(servers).hasSize(1);
    assertThat(servers.iterator().next().getPort()).isEqualTo(xmlPort);

    // put should be successful
    region.put(1, 1);

    execWithDirAndValidate(Operation.STOP, "The CacheServer has stopped\\.");
  }

  @Test
  public void testServerPortOneCacheServer() throws Exception {
    createCacheXml(directory, cacheXmlFileName, xmlPort, 1);

    execWithArgsAndValidate(Operation.START, "CacheServer pid: \\d+ status: running",
        asList(
            "cache-xml-file=" + cacheXmlFileName,
            "log-file=" + logFileName,
            "-classpath=" + createManifestJar(),
            "-dir=" + directoryPath,
            "-server-port=" + commandPort));

    execWithDirAndValidate(Operation.STATUS, "CacheServer pid: \\d+ status: running");

    ClientCache cache = new ClientCacheFactory()
        .create();
    Pool pool = PoolManager.createFactory()
        .addServer("localhost", commandPort)
        .create("cslPool");
    Region<Integer, Integer> region = cache.<Integer, Integer>createClientRegionFactory(PROXY)
        .setPoolName(pool.getName())
        .create("rgn");

    List<InetSocketAddress> servers = pool.getServers();
    assertThat(servers).hasSize(1);
    assertThat(servers.iterator().next().getPort()).isEqualTo(commandPort);

    // put should be successful
    region.put(1, 1);

    execWithDirAndValidate(Operation.STOP, "The CacheServer has stopped\\.");
  }

  @Test
  public void testServerPortNoCacheServer() throws Exception {
    createCacheXml(directory, cacheXmlFileName, 0, 0);

    execWithArgsAndValidate(Operation.START, "CacheServer pid: \\d+ status: running",
        asList(
            "cache-xml-file=" + cacheXmlFileName,
            "log-file=" + logFileName,
            "-classpath=" + createManifestJar(),
            "-dir=" + directoryPath,
            "-server-bind-address=" + InetAddress.getLocalHost().getHostName(),
            "-server-port=" + commandPort));

    execWithDirAndValidate(Operation.STATUS, "CacheServer pid: \\d+ status: running");

    ClientCache cache = new ClientCacheFactory()
        .create();
    Pool pool = PoolManager.createFactory()
        .addServer(InetAddress.getLocalHost().getHostName(), commandPort)
        .create("cslPool");
    Region<Integer, Integer> region = cache.<Integer, Integer>createClientRegionFactory(PROXY)
        .setPoolName(pool.getName())
        .create("rgn");

    List<InetSocketAddress> servers = pool.getServers();
    assertThat(servers).hasSize(1);
    assertThat(servers.iterator().next().getPort()).isEqualTo(commandPort);

    // put should be successful
    region.put(1, 1);

    execWithDirAndValidate(Operation.STOP, "The CacheServer has stopped\\.");
  }

  private String createManifestJar() throws IOException {
    List<String> parts = asList(classpath.split(File.pathSeparator));
    return ProcessWrapper.createManifestJar(parts, directoryPath);
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

  private File createCacheServerSerFile(final File directory) throws IOException {
    File cacheServerSerFile = new File(directory, ".cacheserver.ser");
    assertThat(cacheServerSerFile.createNewFile()).isTrue();
    return cacheServerSerFile;
  }

  private void invokeFailSafe() {
    try {
      Registry registry = LocateRegistry.getRegistry(cacheServerNamingPort);
      FailSafeRemote failSafe = (FailSafeRemote) registry.lookup(FAIL_SAFE_BINDING);
      failSafe.kill();
    } catch (RemoteException | NotBoundException ignore) {
      // cacheserver process was probably stopped already
    }
  }

  private void execWithArgsAndValidate(Operation operation, String regex, List<String> arguments)
      throws TimeoutException, InterruptedException {
    List<String> newArguments = new ArrayList<>();
    newArguments.add(operation.value);
    newArguments.add("-J-Xmx" + Runtime.getRuntime().maxMemory());
    newArguments.add("-J-D" + CONTROLLER_NAMING_PORT_PROP + "=" + controllerNamingPort);
    newArguments.add("-J-D" + CACHESERVER_NAMING_PORT_PROP + "=" + cacheServerNamingPort);
    newArguments.add("-J-DgemfirePropertyFile=" + configPropertiesFile.getAbsolutePath());
    newArguments.addAll(arguments);
    execAndValidate(regex, newArguments);
  }

  private void execWithDirAndValidate(Operation operation, String regex)
      throws TimeoutException, InterruptedException {
    execAndValidate(regex, asList(operation.value(), "-dir=" + directoryPath));
  }

  private void execAndValidate(String regex, List<String> arguments)
      throws InterruptedException, TimeoutException {
    execAndValidate(regex, arguments.toArray(new String[0]));
  }

  private void execAndValidate(final String regex, final String[] args)
      throws InterruptedException, TimeoutException {
    ProcessWrapper processWrapper = new ProcessWrapper.Builder()
        .mainClass(CacheServerLauncher.class)
        .mainArguments(args)
        .directory(directory)
        .build();
    processWrapper.setConsumer(c -> logger.info(c));
    processWrapper.execute();
    if (regex != null) {
      processWrapper.waitForOutputToMatch(regex, TIMEOUT_MILLIS);
    }
    processWrapper.waitFor();
  }

  private void createCacheXml(final File dir, final String cacheXmlName, final int port)
      throws IOException {
    File file = new File(dir, cacheXmlName);
    assertThat(file.createNewFile()).isTrue();

    try (FileWriter writer = new FileWriter(file)) {
      writer.write("<?xml version=\"1.0\"?>" + lineSeparator());
      writer.write("<cache" + lineSeparator());
      writer.write("    xmlns=\"" + GEODE_NAMESPACE + "\"");
      writer.write("    xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"");
      writer.write(
          "    xsi:schemaLocation=\"" + GEODE_NAMESPACE + " " + LATEST_SCHEMA_LOCATION + "\"");
      writer.write("    version=\"" + CacheXml.VERSION_LATEST + "\">");
      writer.write("  <cache-server port=\"" + port + "\" notify-by-subscription=\"false\"/>"
          + lineSeparator());
      writer.write("  <region name=\"PartitionedRegion\">" + lineSeparator());
      writer.write("    <region-attributes>" + lineSeparator());
      writer.write("      <partition-attributes redundant-copies=\"0\"/>" + lineSeparator());
      writer.write("      <cache-listener>" + lineSeparator());
      writer.write("        <class-name>" + SpecialCacheListener.class.getName() + "</class-name>"
          + lineSeparator());
      writer.write("      </cache-listener>" + lineSeparator());

      writer.write("    </region-attributes>" + lineSeparator());
      writer.write("  </region>" + lineSeparator());
      writer.write("</cache>" + lineSeparator());

      writer.flush();
    }
  }

  private void createCacheXml(final File dir, final String cacheXmlName, final int port,
      final int numServers) throws IOException {
    File file = new File(dir, cacheXmlName);
    assertThat(file.createNewFile()).isTrue();

    try (FileWriter writer = new FileWriter(file)) {
      writer.write("<?xml version=\"1.0\"?>" + lineSeparator());
      writer.write("<cache" + lineSeparator());
      writer.write("    xmlns=\"" + GEODE_NAMESPACE + "\"");
      writer.write("    xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"");
      writer.write(
          "    xsi:schemaLocation=\"" + GEODE_NAMESPACE + " " + LATEST_SCHEMA_LOCATION + "\"");
      writer.write("    version=\"" + CacheXml.VERSION_LATEST + "\">");
      for (int i = 0; i < numServers; i++) {
        writer.write("  <cache-server port=\"" + port + "\"");
        writer.write("/>" + lineSeparator());
      }
      writer.write("<region name=\"rgn\" />" + lineSeparator());
      writer.write("</cache>" + lineSeparator());
      writer.flush();
    }
  }

  private enum Operation {
    START("start"),
    STATUS("status"),
    STOP("stop");

    private final String value;

    Operation(String value) {
      this.value = value;
    }

    String value() {
      return value;
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
        registry.bind(FAIL_SAFE_BINDING, new FailSafe());
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
  }

  private interface RebalanceStatusRemote extends Remote {

    void rebalancingStarted() throws RemoteException;

    void rebalancingFinished() throws RemoteException;
  }

  private static class RebalanceStatus extends UnicastRemoteObject
      implements RebalanceStatusRemote {

    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean finished = new AtomicBoolean();

    RebalanceStatus() throws RemoteException {
      // nothing
    }

    @Override
    public void rebalancingStarted() {
      started.set(true);
    }

    @Override
    public void rebalancingFinished() {
      finished.set(true);
    }

    boolean isStarted() {
      return started.get();
    }

    boolean isFinished() {
      return finished.get();
    }
  }

  private interface FailSafeRemote extends Remote {

    void kill() throws RemoteException;
  }

  private static class FailSafe extends UnicastRemoteObject implements FailSafeRemote {

    FailSafe() throws RemoteException {
      // nothing
    }

    @Override
    public void kill() {
      System.exit(0);
    }
  }
}

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
package org.apache.geode.internal.cache.snapshot;

import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.TreeMap;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.snapshot.SnapshotIterator;
import org.apache.geode.cache.snapshot.SnapshotOptions.SnapshotFormat;
import org.apache.geode.cache.snapshot.SnapshotReader;
import org.apache.geode.distributed.LocatorLauncher;
import org.apache.geode.distributed.ServerLauncher;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.pdx.ReflectionBasedAutoSerializer;
import org.apache.geode.test.dunit.DistributedTestUtils;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(DistributedTest.class)
public class GFSnapshotDUnitTest extends JUnit4DistributedTestCase {

  private VM locator;
  private VM server;
  private VM client;
  private Host host;

  @Rule
  public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder();

  @Before
  public void before() {
    host = Host.getHost(0);
    locator = host.getVM(0);
    server = host.getVM(1);
    client = host.getVM(2);
  }

  @Test
  public void testDataExportAndIterate() throws IOException, ClassNotFoundException {
    int locatorPort = AvailablePortHelper.getRandomAvailableTCPPort();
    String serverHostName = NetworkUtils.getServerHostName(host);

    Properties properties = configureCommonProperties(new Properties());

    locator.invoke("Start Locator", () -> configureAndStartLocator(locatorPort, serverHostName, properties));
    server.invoke("Start Server", () -> configureAndStartServer(locatorPort, serverHostName, properties));
    client.invoke("Start client", () -> {
      createAndStartClient(locatorPort, serverHostName);
      return null;
    });
    client.invoke("Populate data", () -> populateDataOnClient());
    String snapshotFilePath = server.invoke("Export data snapshot", () -> createSnapshot());
    client.invoke("Iterate over snapshot", () -> {
      ClientCache clientCache = ClientCacheFactory.getAnyInstance();
      clientCache.close();
      createAndStartClient(locatorPort, serverHostName);
      iterateOverSnapshot(snapshotFilePath);
    });
  }

  private void createAndStartClient(final int locatorPort, final String serverHostName) {
    ClientCacheFactory clientCacheFactory = new ClientCacheFactory();
    clientCacheFactory.set("log-level", "config")
                      .addPoolLocator(serverHostName, locatorPort);
    ClientCache clientCache = clientCacheFactory.create();
    clientCache.createClientRegionFactory(ClientRegionShortcut.PROXY).create("TestRegion");
  }

  private Object populateDataOnClient() {
    ClientCache clientCache = ClientCacheFactory.getAnyInstance();
    Region testRegion = clientCache.getRegion("TestRegion");
    for (int i = 0; i < 100; i++) {
      testRegion.put(i, new TestObject(i, "owner_" + i));
    }
    return null;
  }

  private String createSnapshot() throws IOException {
    final String memberName = getUniqueName() + "-server";
    File file = temporaryFolder.newFolder(memberName + "-snapshot");
    Cache cache = CacheFactory.getAnyInstance();
    cache.getSnapshotService().save(file, SnapshotFormat.GEMFIRE);
    return file.getAbsolutePath();
  }

  private void iterateOverSnapshot(final String snapshotFilePath) throws IOException, ClassNotFoundException {

    File mySnapshot = new File(snapshotFilePath + "/snapshot-TestRegion");
    SnapshotIterator<Integer, TestObject> snapshotIterator = SnapshotReader.read(mySnapshot);

    Map<Integer, TestObject> result = new TreeMap<>();

    try {
      while (snapshotIterator.hasNext()) {
        Entry<Integer, TestObject> entry = snapshotIterator.next();
        int key = entry.getKey();
        TestObject value = entry.getValue();
        result.put(key, value);
      }
    } finally {
      snapshotIterator.close();
    }
    assertEquals(100, result.size());
    int count = 0;
    for (Entry<Integer, TestObject> entry : result.entrySet()) {
      assertEquals(count, (int) entry.getKey());
      assertEquals(new TestObject(count, "owner_" + count), entry.getValue());
      count++;
    }
  }

  private Properties configureCommonProperties(final Properties properties) {
    properties.setProperty(MCAST_PORT, "0");
    properties.setProperty(ENABLE_CLUSTER_CONFIGURATION, "false");
    properties.setProperty(USE_CLUSTER_CONFIGURATION, "false");
    return properties;
  }

  private void configureAndStartLocator(final int locatorPort, final String serverHostName, final Properties properties) throws IOException {
    DistributedTestUtils.deleteLocatorStateFile();

    final String memberName = getUniqueName() + "-locator";
    final File workingDirectory = temporaryFolder.newFolder(memberName);

    LocatorLauncher.Builder builder = new LocatorLauncher.Builder();

    for (String propertyName : properties.stringPropertyNames()) {
      builder.set(propertyName, properties.getProperty(propertyName));
    }
    LocatorLauncher locatorLauncher = builder.setBindAddress(serverHostName)
                             .setHostnameForClients(serverHostName)
                             .setMemberName(memberName)
                             .setPort(locatorPort)
                             .setWorkingDirectory(workingDirectory.getCanonicalPath())
                             .build();
    locatorLauncher.start();

  }

  private void configureAndStartServer(final int locatorPort, final String serverHostName, final Properties properties) throws IOException {
    final String memberName = getUniqueName() + "-server";
    final File workingDirectory = temporaryFolder.newFolder(memberName);
    final File pdxDirectory = temporaryFolder.newFolder(memberName + "-pdx");
    final File diskStoreDirectory = temporaryFolder.newFolder(memberName + "-disk");


    ServerLauncher.Builder builder = new ServerLauncher.Builder();

    for (String propertyName : properties.stringPropertyNames()) {
      builder.set(propertyName, properties.getProperty(propertyName));
    }

    ServerLauncher serverLauncher = builder.set("locators", serverHostName + "[" + locatorPort + "]")
                            .setMemberName(memberName)
                            .set("log-level", "config")
                            .setHostNameForClients(serverHostName)
                            .setServerBindAddress(serverHostName)
                            .setServerPort(0)
                            .setWorkingDirectory(workingDirectory.getCanonicalPath())
                            .setPdxDiskStore("pdxDS")
                            .setPdxPersistent(true)
                            .build();
    serverLauncher.start();

    Cache cache = CacheFactory.getAnyInstance();

    cache.createDiskStoreFactory().setDiskDirsAndSizes(new File[] { pdxDirectory }, new int[] { 16000 }).create("pdxDS");

    cache.createDiskStoreFactory().setDiskDirsAndSizes(new File[] { diskStoreDirectory }, new int[] { 16000 }).create("diskStore");

    RegionFactory<Object, Object> regionFactory = cache.createRegionFactory();
    regionFactory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE).setScope(Scope.DISTRIBUTED_ACK).setDiskStoreName("diskStore").create("TestRegion");
  }


}

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

package org.apache.geode.cache.client;

import static org.apache.geode.distributed.ConfigurationProperties.CACHE_XML_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.runners.MethodSorters.NAME_ASCENDING;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.jgroups.util.UUID;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.FixMethodOrder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.RegionService;
import org.apache.geode.cache.client.internal.ProxyCache;
import org.apache.geode.cache.client.internal.UserAttributes;
import org.apache.geode.cache.client.proxy.SniSocketFactory;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.distributed.internal.membership.api.MemberIdentifier;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.xmlcache.CacheXmlGenerator;
import org.apache.geode.internal.cache.xmlcache.ClientCacheCreation;
import org.apache.geode.internal.serialization.Version;
import org.apache.geode.internal.serialization.VersionedDataInputStream;
import org.apache.geode.pdx.ReflectionBasedAutoSerializer;
import org.apache.geode.test.junit.categories.ClientServerTest;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

/**
 * Unit test for the ClientCacheFactory class
 *
 * @since GemFire 6.5
 */
@FixMethodOrder(NAME_ASCENDING)
@Category(ClientServerTest.class)
public class ClientCacheFactoryJUnitTest {
  private ClientCache clientCache;

  @Rule
  public TestName testName = new SerializableTestName();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @After
  public void tearDown() throws Exception {
    if (clientCache != null && !clientCache.isClosed()) {
      clientCache.close();
    }
  }

  @AfterClass
  public static void afterClass() {
    InternalDistributedSystem ids = InternalDistributedSystem.getAnyInstance();
    if (ids != null) {
      ids.disconnect();
    }
  }

  @Test
  public void test000Defaults() throws Exception {
    clientCache = new ClientCacheFactory().create();
    GemFireCacheImpl gfc = (GemFireCacheImpl) clientCache;
    assertThat(gfc.isClient()).isTrue();

    Properties dsProps = clientCache.getDistributedSystem().getProperties();
    assertThat(dsProps.getProperty(MCAST_PORT)).isEqualTo("0");
    assertThat(dsProps.getProperty(LOCATORS)).isEqualTo("");

    Pool defPool = gfc.getDefaultPool();
    assertThat(defPool.getName()).isEqualTo("DEFAULT");
    assertThat(defPool.getLocators()).isEqualTo(Collections.emptyList());
    assertThat(defPool.getSocketConnectTimeout())
        .isEqualTo(PoolFactory.DEFAULT_SOCKET_CONNECT_TIMEOUT);
    assertThat(defPool.getServers()).isEqualTo(Collections.singletonList(
        new InetSocketAddress(InetAddress.getLocalHost(), CacheServer.DEFAULT_PORT)));

    ClientCache cc2 = new ClientCacheFactory().create();
    assertThat(cc2).as("expected cc2 and cc to be == " + cc2 + clientCache)
        .isSameAs(clientCache);
    assertThatThrownBy(() -> new ClientCacheFactory().set(LOG_LEVEL, "severe").create())
        .isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(() -> new ClientCacheFactory().addPoolLocator("127.0.0.1", 36666).create())
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void test001FindDefaultPoolFromXML() throws Exception {
    File cacheXmlFile = temporaryFolder.newFile("ClientCacheFactoryJUnitTest.xml");
    URL url = ClientCacheFactoryJUnitTest.class
        .getResource("ClientCacheFactoryJUnitTest_single_pool.xml");
    FileUtils.copyFile(new File(url.getFile()), cacheXmlFile);

    clientCache =
        new ClientCacheFactory().set(CACHE_XML_FILE, cacheXmlFile.getAbsolutePath()).create();
    GemFireCacheImpl gfc = (GemFireCacheImpl) clientCache;
    assertThat(gfc.isClient()).isTrue();

    Properties dsProps = clientCache.getDistributedSystem().getProperties();
    assertThat(dsProps.getProperty(MCAST_PORT)).isEqualTo("0");
    assertThat(dsProps.getProperty(LOCATORS)).isEqualTo("");

    Pool defPool = gfc.getDefaultPool();
    assertThat(defPool.getName()).isEqualTo("my_pool_name");
    assertThat(defPool.getLocators()).isEqualTo(Collections.emptyList());
    assertThat(defPool.getSocketConnectTimeout())
        .isEqualTo(PoolFactory.DEFAULT_SOCKET_CONNECT_TIMEOUT);
    assertThat(defPool.getServers()).isEqualTo(
        Collections.singletonList(new InetSocketAddress("localhost", CacheServer.DEFAULT_PORT)));

    // verify that the SocketCreator settings were correctly picked up from the xml file
    SocketFactory factory = defPool.getSocketFactory();
    assertThat(factory).isInstanceOf(SniSocketFactory.class);
    SniSocketFactory sniSocketFactory = (SniSocketFactory) factory;
    assertThat(sniSocketFactory.getPort()).isEqualTo(40404);
    assertThat(sniSocketFactory.getHostname()).isEqualTo("localhost");
  }

  /**
   * Make sure if we have a single pool that it will be used as the default
   */
  @Test
  public void test002DPsinglePool() throws Exception {
    Properties dsProps = new Properties();
    dsProps.setProperty(MCAST_PORT, "0");
    DistributedSystem.connect(dsProps);
    Pool p = PoolManager.createFactory().addServer(InetAddress.getLocalHost().getHostName(), 7777)
        .setSocketConnectTimeout(1400).create("singlePool");

    clientCache = new ClientCacheFactory().create();
    GemFireCacheImpl gfc = (GemFireCacheImpl) clientCache;
    assertThat(gfc.isClient()).isTrue();

    Pool defPool = gfc.getDefaultPool();
    assertThat(defPool).isEqualTo(p);
    assertThat(defPool.getSocketConnectTimeout()).isEqualTo(1400);

    // make sure if we can not create a secure user cache when one pool exists that is not multiuser
    // enabled
    Properties suProps = new Properties();
    suProps.setProperty("user", "foo");
    assertThatThrownBy(() -> clientCache.createAuthenticatedView(suProps))
        .isInstanceOf(IllegalStateException.class);

    // however we should be to to create it by configuring a pool
    Pool pool = PoolManager.createFactory()
        .addServer(InetAddress.getLocalHost().getHostName(), CacheServer.DEFAULT_PORT)
        .setMultiuserAuthentication(true).setSocketConnectTimeout(2345).create("pool1");
    RegionService cc = clientCache.createAuthenticatedView(suProps, pool.getName());
    ProxyCache pc = (ProxyCache) cc;
    UserAttributes ua = pc.getUserAttributes();
    Pool proxyDefPool = ua.getPool();
    assertThat(proxyDefPool.getServers()).isEqualTo(Collections.singletonList(
        new InetSocketAddress(InetAddress.getLocalHost(), CacheServer.DEFAULT_PORT)));
    assertThat(proxyDefPool.getMultiuserAuthentication()).isTrue();
    assertThat(proxyDefPool.getSocketConnectTimeout()).isEqualTo(2345);
  }

  /**
   * Make sure if we have more than one pool that we do not have a default
   */
  @Test
  public void test003DPmultiplePool() throws Exception {
    Properties dsProps = new Properties();
    dsProps.setProperty(MCAST_PORT, "0");
    DistributedSystem.connect(dsProps);
    PoolManager.createFactory().addServer(InetAddress.getLocalHost().getHostName(), 7777)
        .setSocketConnectTimeout(2500).create("p7");
    PoolManager.createFactory().addServer(InetAddress.getLocalHost().getHostName(), 6666)
        .setSocketConnectTimeout(5200).create("p6");

    clientCache = new ClientCacheFactory().create();
    GemFireCacheImpl gfc = (GemFireCacheImpl) clientCache;
    assertThat(gfc.isClient()).isTrue();

    Pool defPool = gfc.getDefaultPool();
    assertThat(defPool).isNull();
    assertThat(PoolManager.find("p7").getSocketConnectTimeout()).isEqualTo(2500);
    assertThat(PoolManager.find("p6").getSocketConnectTimeout()).isEqualTo(5200);

    // make sure if we can not create a secure user cache when more than one pool exists that is not
    // multiuser enabled
    Properties suProps = new Properties();
    suProps.setProperty("user", "foo");
    assertThatThrownBy(() -> clientCache.createAuthenticatedView(suProps))
        .isInstanceOf(IllegalStateException.class);

    // however we should be to to create it by configuring a pool
    Pool pool = PoolManager.createFactory()
        .addServer(InetAddress.getLocalHost().getHostName(), CacheServer.DEFAULT_PORT)
        .setMultiuserAuthentication(true).create("pool1");
    RegionService cc = clientCache.createAuthenticatedView(suProps, pool.getName());
    ProxyCache pc = (ProxyCache) cc;
    UserAttributes ua = pc.getUserAttributes();
    Pool proxyDefPool = ua.getPool();
    assertThat(proxyDefPool.getServers()).isEqualTo(Collections.singletonList(
        new InetSocketAddress(InetAddress.getLocalHost(), CacheServer.DEFAULT_PORT)));
    assertThat(proxyDefPool.getMultiuserAuthentication()).isTrue();
    assertThat(proxyDefPool.getSocketConnectTimeout())
        .isEqualTo(PoolFactory.DEFAULT_SOCKET_CONNECT_TIMEOUT);
  }

  @Test
  public void test004SetMethod() {
    clientCache =
        new ClientCacheFactory().set(LOG_LEVEL, "severe").setPoolSocketConnectTimeout(0).create();
    GemFireCacheImpl gfc = (GemFireCacheImpl) clientCache;
    assertThat(gfc.isClient()).isTrue();

    Properties dsProps = clientCache.getDistributedSystem().getProperties();
    assertThat(dsProps.getProperty(MCAST_PORT)).isEqualTo("0");
    assertThat(dsProps.getProperty(LOCATORS)).isEqualTo("");
    assertThat(dsProps.getProperty(LOG_LEVEL)).isEqualTo("severe");
    assertThat(clientCache.getDefaultPool().getSocketConnectTimeout()).isEqualTo(0);

    assertThatThrownBy(() -> new ClientCacheFactory().setPoolSocketConnectTimeout(-1).create())
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void test005SecureUserDefaults() throws Exception {
    Properties suProps = new Properties();
    suProps.setProperty("user", "foo");
    GemFireCacheImpl gfc =
        (GemFireCacheImpl) new ClientCacheFactory().setPoolMultiuserAuthentication(true).create();
    clientCache = gfc;

    RegionService cc1 = clientCache.createAuthenticatedView(suProps);
    assertThat(gfc.isClient()).isTrue();
    Properties dsProps = clientCache.getDistributedSystem().getProperties();
    assertThat(dsProps.getProperty(MCAST_PORT)).isEqualTo("0");
    assertThat(dsProps.getProperty(LOCATORS)).isEqualTo("");

    Pool defPool = gfc.getDefaultPool();
    assertThat(defPool.getName()).isEqualTo("DEFAULT");
    assertThat(defPool.getMultiuserAuthentication()).isTrue();
    assertThat(defPool.getLocators()).isEqualTo(Collections.emptyList());
    assertThat(defPool.getServers()).isEqualTo(Collections.singletonList(
        new InetSocketAddress(InetAddress.getLocalHost(), CacheServer.DEFAULT_PORT)));

    // make sure we can create another secure user cache
    RegionService cc2 = clientCache.createAuthenticatedView(suProps);
    assertThat(gfc.isClient()).isTrue();
    assertThat(dsProps.getProperty(MCAST_PORT)).isEqualTo("0");
    assertThat(dsProps.getProperty(LOCATORS)).isEqualTo("");

    defPool = gfc.getDefaultPool();
    assertThat(defPool.getName()).isEqualTo("DEFAULT");
    assertThat(defPool.getMultiuserAuthentication()).isTrue();
    assertThat(defPool.getLocators()).isEqualTo(Collections.emptyList());
    assertThat(defPool.getServers()).isEqualTo(Collections.singletonList(
        new InetSocketAddress(InetAddress.getLocalHost(), CacheServer.DEFAULT_PORT)));

    assertThat(cc1).as("expected two different secure user caches").isNotSameAs(cc2);
  }

  @Test
  public void test006NonDefaultPool() throws Exception {
    clientCache = new ClientCacheFactory()
        .addPoolServer(InetAddress.getLocalHost().getHostName(), 55555).create();
    GemFireCacheImpl gfc = (GemFireCacheImpl) clientCache;
    assertThat(gfc.isClient()).isTrue();

    Properties dsProps = clientCache.getDistributedSystem().getProperties();
    assertThat(dsProps.getProperty(LOCATORS)).isEqualTo("");
    assertThat(dsProps.getProperty(MCAST_PORT)).isEqualTo("0");

    Pool defPool = gfc.getDefaultPool();
    assertThat(defPool.getName()).isEqualTo("DEFAULT");
    assertThat(defPool.getLocators()).isEqualTo(Collections.emptyList());
    assertThat(defPool.getServers()).isEqualTo(
        Collections.singletonList(new InetSocketAddress(InetAddress.getLocalHost(), 55555)));

    gfc = (GemFireCacheImpl) clientCache;
    assertThat(gfc.isClient()).isTrue();

    dsProps = clientCache.getDistributedSystem().getProperties();
    assertThat(dsProps.getProperty(LOCATORS)).isEqualTo("");
    assertThat(dsProps.getProperty(MCAST_PORT)).isEqualTo("0");

    defPool = gfc.getDefaultPool();
    assertThat(defPool.getName()).isEqualTo("DEFAULT");
    assertThat(defPool.getLocators()).isEqualTo(Collections.emptyList());
    assertThat(defPool.getServers()).isEqualTo(
        Collections.singletonList(new InetSocketAddress(InetAddress.getLocalHost(), 55555)));

    assertThatThrownBy(() -> new ClientCacheFactory()
        .addPoolServer(InetAddress.getLocalHost().getHostName(), 44444).create())
            .isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void test007Bug44907() {
    new ClientCacheFactory().setPdxSerializer(new ReflectionBasedAutoSerializer()).create();
    clientCache =
        new ClientCacheFactory().setPdxSerializer(new ReflectionBasedAutoSerializer()).create();
  }

  @Test
  public void testDefaultPoolTimeoutMultiplier() throws Exception {
    clientCache = new ClientCacheFactory().setPoolSubscriptionTimeoutMultiplier(2)
        .addPoolServer(InetAddress.getLocalHost().getHostName(), 7777).create();
    Pool defaultPool = clientCache.getDefaultPool();
    assertThat(defaultPool.getSubscriptionTimeoutMultiplier()).isEqualTo(2);
  }

  @Test
  public void testOldClientIDDeserialization() throws Exception {
    // during a HandShake a clientID is read w/o knowing the client's version
    clientCache = new ClientCacheFactory().create();
    InternalDistributedMember memberID =
        (InternalDistributedMember) clientCache.getDistributedSystem().getDistributedMember();
    MemberIdentifier gmsID = memberID;
    memberID.setVersionObjectForTest(Version.GFE_82);
    assertThat(memberID.getVersionObject()).isEqualTo(Version.GFE_82);

    ClientProxyMembershipID clientID = ClientProxyMembershipID.getClientId(memberID);
    HeapDataOutputStream out = new HeapDataOutputStream(Version.GFE_82);
    DataSerializer.writeObject(clientID, out);

    DataInputStream in =
        new VersionedDataInputStream(new ByteArrayInputStream(out.toByteArray()),
            Version.CURRENT);
    ClientProxyMembershipID newID = DataSerializer.readObject(in);
    InternalDistributedMember newMemberID =
        (InternalDistributedMember) newID.getDistributedMember();
    assertThat(newMemberID.getVersionObject()).isEqualTo(Version.GFE_82);
    assertThat(newID.getClientVersion()).isEqualTo(Version.GFE_82);

    assertThat(newMemberID.getUuidLeastSignificantBits()).isEqualTo(0);
    assertThat(newMemberID.getUuidMostSignificantBits()).isEqualTo(0);

    gmsID.setUUID(new UUID(1234L, 5678L));
    memberID.setVersionObjectForTest(Version.CURRENT);
    clientID = ClientProxyMembershipID.getClientId(memberID);
    out = new HeapDataOutputStream(Version.CURRENT);
    DataSerializer.writeObject(clientID, out);

    in = new VersionedDataInputStream(new ByteArrayInputStream(out.toByteArray()),
        Version.CURRENT);
    newID = DataSerializer.readObject(in);
    newMemberID = (InternalDistributedMember) newID.getDistributedMember();
    assertThat(newMemberID.getVersionObject()).isEqualTo(Version.CURRENT);
    assertThat(newID.getClientVersion()).isEqualTo(Version.CURRENT);

    assertThat(newMemberID.getUuidLeastSignificantBits())
        .isEqualTo(gmsID.getUuidLeastSignificantBits());
    assertThat(newMemberID.getUuidMostSignificantBits())
        .isEqualTo(gmsID.getUuidMostSignificantBits());
  }

  @Test
  public void configuringPdxPersistenceThroughAPIShouldLogWarningMessage() throws IOException {
    File logFile = temporaryFolder.newFile(testName.getMethodName() + ".log");
    clientCache = new ClientCacheFactory()
        .set(LOG_LEVEL, "warn")
        .set(LOG_FILE, logFile.getAbsolutePath())
        .setPdxPersistent(true)
        .setPdxSerializer(new ReflectionBasedAutoSerializer())
        .create();

    assertThat(FileUtils.readFileToString(logFile, Charset.defaultCharset())
        .contains("PDX persistence is not supported on client side.")).isTrue();
  }

  @Test
  public void configuringPdxDiskStoreThroughAPIShouldLogWarningMessage() throws IOException {
    File logFile = temporaryFolder.newFile(testName.getMethodName() + ".log");
    clientCache = new ClientCacheFactory()
        .set(LOG_LEVEL, "warn")
        .set(LOG_FILE, logFile.getAbsolutePath())
        .setPdxDiskStore("pdxDiskStore")
        .setPdxSerializer(new ReflectionBasedAutoSerializer())
        .create();

    assertThat(FileUtils.readFileToString(logFile, Charset.defaultCharset())
        .contains("PDX persistence is not supported on client side.")).isTrue();
  }

  @Test
  public void configuringPdxPersistenceThroughXMLShouldLogWarningMessage() throws IOException {
    ClientCacheCreation clientCacheCreation = new ClientCacheCreation();
    clientCacheCreation.setPdxPersistent(true);
    clientCacheCreation.setPdxSerializer(new ReflectionBasedAutoSerializer());

    File logFile = temporaryFolder.newFile(testName.getMethodName() + ".log");
    File cacheXmlFile = temporaryFolder.newFile(testName.getMethodName() + ".xml");

    try (PrintWriter printWriter = new PrintWriter(new FileWriter(cacheXmlFile), true)) {
      CacheXmlGenerator.generate(clientCacheCreation, printWriter, false, false);
      clientCache = new ClientCacheFactory()
          .set(LOG_LEVEL, "warn")
          .set(LOG_FILE, logFile.getAbsolutePath())
          .set(CACHE_XML_FILE, cacheXmlFile.getAbsolutePath())
          .create();

      assertThat(FileUtils.readFileToString(logFile, Charset.defaultCharset())
          .contains("PDX persistence is not supported on client side.")).isTrue();
    }
  }

  @Test
  public void configuringPdxDiskStoreThroughXMLShouldLogWarningMessage() throws IOException {
    ClientCacheCreation clientCacheCreation = new ClientCacheCreation();
    clientCacheCreation.setPdxDiskStore("pdxDiskStore");
    clientCacheCreation.setPdxSerializer(new ReflectionBasedAutoSerializer());

    File logFile = temporaryFolder.newFile(testName.getMethodName() + ".log");
    File cacheXmlFile = temporaryFolder.newFile(testName.getMethodName() + ".xml");
    try (PrintWriter printWriter = new PrintWriter(new FileWriter(cacheXmlFile), true)) {
      CacheXmlGenerator.generate(clientCacheCreation, printWriter, false, false);
      clientCache = new ClientCacheFactory()
          .set(LOG_LEVEL, "warn")
          .set(LOG_FILE, logFile.getAbsolutePath())
          .set(CACHE_XML_FILE, cacheXmlFile.getAbsolutePath())
          .create();

      assertThat(FileUtils.readFileToString(logFile, Charset.defaultCharset())
          .contains("PDX persistence is not supported on client side.")).isTrue();
    }
  }
}

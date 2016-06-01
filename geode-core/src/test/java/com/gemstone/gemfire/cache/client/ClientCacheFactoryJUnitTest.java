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

package com.gemstone.gemfire.cache.client;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.RegionService;
import com.gemstone.gemfire.cache.client.internal.ProxyCache;
import com.gemstone.gemfire.cache.client.internal.UserAttributes;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.distributed.internal.membership.gms.GMSMember;
import com.gemstone.gemfire.internal.FileUtil;
import com.gemstone.gemfire.internal.HeapDataOutputStream;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.VersionedDataInputStream;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientProxyMembershipID;
import com.gemstone.gemfire.pdx.ReflectionBasedAutoSerializer;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;
import org.jgroups.util.UUID;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;

import static com.gemstone.gemfire.distributed.DistributedSystemConfigProperties.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.runners.MethodSorters.NAME_ASCENDING;

/**
 * Unit test for the ClientCacheFactory class
 * @since GemFire 6.5
 */
@FixMethodOrder(NAME_ASCENDING)
@Category(IntegrationTest.class)
public class ClientCacheFactoryJUnitTest {
  
  private ClientCache cc;
  private File tmpFile;

  @After
  public void tearDown() throws Exception {
    if (this.cc != null && !this.cc.isClosed()) {
      cc.close();
    }
    if (tmpFile != null && tmpFile.exists()) {
      tmpFile.delete();
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
    this.cc = new ClientCacheFactory().create();
    GemFireCacheImpl gfc = (GemFireCacheImpl)this.cc;
    assertEquals(true, gfc.isClient());
    Properties dsProps = this.cc.getDistributedSystem().getProperties();
    assertEquals("0", dsProps.getProperty(MCAST_PORT));
    assertEquals("", dsProps.getProperty(LOCATORS));
    Pool defPool = gfc.getDefaultPool();
    assertEquals("DEFAULT", defPool.getName());
    assertEquals(new ArrayList(), defPool.getLocators());
    assertEquals(Collections.singletonList(new InetSocketAddress(InetAddress.getLocalHost(),CacheServer.DEFAULT_PORT)), defPool.getServers());

    ClientCache cc2 = new ClientCacheFactory().create();
    if (cc2 != this.cc) {
      fail("expected cc2 and cc to be == " + cc2 + this.cc);
    }

    try {
      new ClientCacheFactory().set(LOG_LEVEL, "severe").create();
      fail("expected create to fail");
    } catch (IllegalStateException expected) {
    }

    try {
      new ClientCacheFactory().addPoolLocator("127.0.0.1", 36666).create();
      fail("expected create to fail");
    } catch (IllegalStateException expected) {
    }
  }
  
  @Test
  public void test001FindDefaultFromXML() throws Exception {
    this.tmpFile = File.createTempFile("ClientCacheFactoryJUnitTest", ".xml");
    this.tmpFile.deleteOnExit();
    URL url = ClientCacheFactoryJUnitTest.class.getResource("ClientCacheFactoryJUnitTest_single_pool.xml");;
    FileUtil.copy(url, this.tmpFile);
    this.cc = new ClientCacheFactory()
        .set(CACHE_XML_FILE, this.tmpFile.getAbsolutePath())
      .create();
    GemFireCacheImpl gfc = (GemFireCacheImpl)this.cc;
    assertEquals(true, gfc.isClient());
    Properties dsProps = this.cc.getDistributedSystem().getProperties();
    assertEquals("0", dsProps.getProperty(MCAST_PORT));
    assertEquals("", dsProps.getProperty(LOCATORS));
    Pool defPool = gfc.getDefaultPool();
    assertEquals("my_pool_name", defPool.getName());
    assertEquals(new ArrayList(), defPool.getLocators());
    assertEquals(Collections.singletonList(new InetSocketAddress("localhost",CacheServer.DEFAULT_PORT)), defPool.getServers());
  }

  /**
   * Make sure if we have a single pool that it will be used as the default
   */
  @Test
  public void test002DPsinglePool() throws Exception {
    Properties dsProps = new Properties();
    dsProps.setProperty(MCAST_PORT, "0");
    DistributedSystem ds = DistributedSystem.connect(dsProps);
    Pool p = PoolManager.createFactory().addServer(InetAddress.getLocalHost().getHostName(), 7777).create("singlePool");
    this.cc = new ClientCacheFactory().create();
    GemFireCacheImpl gfc = (GemFireCacheImpl)this.cc;
    assertEquals(true, gfc.isClient());
    Pool defPool = gfc.getDefaultPool();
    assertEquals(p, defPool);

    // make sure if we can not create a secure user cache when one pool
    // exists that is not multiuser enabled
    try {
      Properties suProps = new Properties();
      suProps.setProperty("user", "foo");
      RegionService cc = this.cc.createAuthenticatedView(suProps);
      fail("expected IllegalStateException");
    } catch (IllegalStateException ignore) {
    }
    // however we should be to to create it by configuring a pool
    {
      Properties suProps = new Properties();
      suProps.setProperty("user", "foo");
      
      Pool pool = PoolManager.createFactory().addServer(InetAddress.getLocalHost().getHostName(),CacheServer.DEFAULT_PORT).setMultiuserAuthentication(true).create("pool1");
      RegionService cc = this.cc.createAuthenticatedView(suProps, pool.getName());
      ProxyCache pc = (ProxyCache)cc;
      UserAttributes ua = pc.getUserAttributes();
      Pool proxyDefPool = ua.getPool();
      assertEquals(Collections.singletonList(new InetSocketAddress(InetAddress.getLocalHost(),CacheServer.DEFAULT_PORT)), proxyDefPool.getServers());
      assertEquals(true, proxyDefPool.getMultiuserAuthentication());
    }
  }
  /**
   * Make sure if we have more than one pool that we do not have a default
   */
  @Test
  public void test003DPmultiplePool() throws Exception {
    Properties dsProps = new Properties();
    dsProps.setProperty(MCAST_PORT, "0");
    DistributedSystem ds = DistributedSystem.connect(dsProps);
    PoolManager.createFactory().addServer(InetAddress.getLocalHost().getHostName(), 7777).create("p7");
    PoolManager.createFactory().addServer(InetAddress.getLocalHost().getHostName(), 6666).create("p6");
    this.cc = new ClientCacheFactory().create();
    GemFireCacheImpl gfc = (GemFireCacheImpl)this.cc;
    assertEquals(true, gfc.isClient());
    Pool defPool = gfc.getDefaultPool();
    assertEquals(null, defPool);

    // make sure if we can not create a secure user cache when more than one pool
    // exists that is not multiuser enabled
    try {
      Properties suProps = new Properties();
      suProps.setProperty("user", "foo");
      RegionService cc = this.cc.createAuthenticatedView(suProps);
      fail("expected IllegalStateException");
    } catch (IllegalStateException ignore) {
    }
    // however we should be to to create it by configuring a pool
    {
      Properties suProps = new Properties();
      suProps.setProperty("user", "foo");
      Pool pool = PoolManager.createFactory().addServer(InetAddress.getLocalHost().getHostName(),CacheServer.DEFAULT_PORT).setMultiuserAuthentication(true).create("pool1");
      RegionService cc = this.cc.createAuthenticatedView(suProps, pool.getName());
      ProxyCache pc = (ProxyCache)cc;
      UserAttributes ua = pc.getUserAttributes();
      Pool proxyDefPool = ua.getPool();
      assertEquals(Collections.singletonList(new InetSocketAddress(InetAddress.getLocalHost(),CacheServer.DEFAULT_PORT)), proxyDefPool.getServers());
      assertEquals(true, proxyDefPool.getMultiuserAuthentication());
    }
  }

  @Test
  public void test004SetMethod() throws Exception {
    this.cc = new ClientCacheFactory().set(LOG_LEVEL, "severe").create();
    GemFireCacheImpl gfc = (GemFireCacheImpl)this.cc;
    assertEquals(true, gfc.isClient());
    Properties dsProps = this.cc.getDistributedSystem().getProperties();
    assertEquals("0", dsProps.getProperty(MCAST_PORT));
    assertEquals("", dsProps.getProperty(LOCATORS));
    assertEquals("severe", dsProps.getProperty(LOG_LEVEL));
  }

  @Test
  public void test005SecureUserDefaults() throws Exception {
    Properties suProps = new Properties();
    suProps.setProperty("user", "foo");
    GemFireCacheImpl gfc = (GemFireCacheImpl) new ClientCacheFactory().setPoolMultiuserAuthentication(true).create();
    this.cc = gfc;
    RegionService cc1 = this.cc.createAuthenticatedView(suProps);
    
    assertEquals(true, gfc.isClient());
    Properties dsProps = this.cc.getDistributedSystem().getProperties();
    assertEquals("0", dsProps.getProperty(MCAST_PORT));
    assertEquals("", dsProps.getProperty(LOCATORS));
    Pool defPool = gfc.getDefaultPool();
    assertEquals("DEFAULT", defPool.getName());
    assertEquals(new ArrayList(), defPool.getLocators());
    assertEquals(Collections.singletonList(new InetSocketAddress(InetAddress.getLocalHost(),CacheServer.DEFAULT_PORT)), defPool.getServers());
    assertEquals(true, defPool.getMultiuserAuthentication());

    // make sure we can create another secure user cache
    RegionService cc2 = this.cc.createAuthenticatedView(suProps);
    assertEquals(true, gfc.isClient());
    assertEquals("0", dsProps.getProperty(MCAST_PORT));
    assertEquals("", dsProps.getProperty(LOCATORS));
    defPool = gfc.getDefaultPool();
    assertEquals("DEFAULT", defPool.getName());
    assertEquals(new ArrayList(), defPool.getLocators());
    assertEquals(Collections.singletonList(new InetSocketAddress(InetAddress.getLocalHost(),CacheServer.DEFAULT_PORT)), defPool.getServers());
    assertEquals(true, defPool.getMultiuserAuthentication());
    if (cc1 == cc2) {
      fail("expected two different secure user caches");
    }
  }

  @Test
  public void test006NonDefaultPool() throws Exception {
    this.cc = new ClientCacheFactory()
      .addPoolServer(InetAddress.getLocalHost().getHostName(), 55555)
      .create();
    GemFireCacheImpl gfc = (GemFireCacheImpl)this.cc;
    assertEquals(true, gfc.isClient());
    Properties dsProps = this.cc.getDistributedSystem().getProperties();
    assertEquals("0", dsProps.getProperty(MCAST_PORT));
    assertEquals("", dsProps.getProperty(LOCATORS));
    Pool defPool = gfc.getDefaultPool();
    assertEquals("DEFAULT", defPool.getName());
    assertEquals(new ArrayList(), defPool.getLocators());
    assertEquals(Collections.singletonList(new InetSocketAddress(InetAddress.getLocalHost(),55555)), defPool.getServers());

    ClientCache cc2 = new ClientCacheFactory().create();
    gfc = (GemFireCacheImpl)this.cc;
    assertEquals(true, gfc.isClient());
    dsProps = this.cc.getDistributedSystem().getProperties();
    assertEquals("0", dsProps.getProperty(MCAST_PORT));
    assertEquals("", dsProps.getProperty(LOCATORS));
    defPool = gfc.getDefaultPool();
    assertEquals("DEFAULT", defPool.getName());
    assertEquals(new ArrayList(), defPool.getLocators());
    assertEquals(Collections.singletonList(new InetSocketAddress(InetAddress.getLocalHost(),55555)), defPool.getServers());
    try {
      new ClientCacheFactory()
        .addPoolServer(InetAddress.getLocalHost().getHostName(), 44444)
        .create();
      fail("expected create to fail");
    } catch (IllegalStateException expected) {
    }
  }
  
  @Test
  public void test007Bug44907() {
    new ClientCacheFactory()
    .setPdxSerializer(new ReflectionBasedAutoSerializer())
    .create();
    new ClientCacheFactory()
    .setPdxSerializer(new ReflectionBasedAutoSerializer())
    .create();
  }
  
  @Test
  public void testOldClientIDDeserialization() throws Exception {
    // during a HandShake a clientID is read w/o knowing the client's
    // version
    cc = new ClientCacheFactory().create();
    GemFireCacheImpl gfc = (GemFireCacheImpl)cc;
    InternalDistributedMember memberID = (InternalDistributedMember)cc.getDistributedSystem().getDistributedMember();
    GMSMember gmsID = (GMSMember)memberID.getNetMember();
    memberID.setVersionObjectForTest(Version.GFE_82);
    assertEquals(Version.GFE_82, memberID.getVersionObject());
    ClientProxyMembershipID clientID = ClientProxyMembershipID.getClientId(memberID);
    HeapDataOutputStream out = new HeapDataOutputStream(Version.GFE_82);
    DataSerializer.writeObject(clientID, out);

    DataInputStream in = new VersionedDataInputStream(new ByteArrayInputStream(out.toByteArray()), Version.CURRENT); 
    ClientProxyMembershipID newID = DataSerializer.readObject(in);
    InternalDistributedMember newMemberID = (InternalDistributedMember)newID.getDistributedMember();
    assertEquals(Version.GFE_82, newMemberID.getVersionObject());
    assertEquals(Version.GFE_82, newID.getClientVersion());
    GMSMember newGmsID = (GMSMember)newMemberID.getNetMember();
    assertEquals(0, newGmsID.getUuidLSBs());
    assertEquals(0, newGmsID.getUuidMSBs());
    
    gmsID.setUUID(new UUID(1234l, 5678l));
    memberID.setVersionObjectForTest(Version.CURRENT);
    clientID = ClientProxyMembershipID.getClientId(memberID);
    out = new HeapDataOutputStream(Version.CURRENT);
    DataSerializer.writeObject(clientID, out);

    in = new VersionedDataInputStream(new ByteArrayInputStream(out.toByteArray()), Version.CURRENT);
    newID = DataSerializer.readObject(in);
    newMemberID = (InternalDistributedMember)newID.getDistributedMember();
    assertEquals(Version.CURRENT, newMemberID.getVersionObject());
    assertEquals(Version.CURRENT, newID.getClientVersion());
    newGmsID = (GMSMember)newMemberID.getNetMember();
    assertEquals(gmsID.getUuidLSBs(), newGmsID.getUuidLSBs());
    assertEquals(gmsID.getUuidMSBs(), newGmsID.getUuidMSBs());

  }
}

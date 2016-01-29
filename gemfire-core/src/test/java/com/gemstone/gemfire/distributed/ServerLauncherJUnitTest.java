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
package com.gemstone.gemfire.distributed;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.distributed.ServerLauncher.Builder;
import com.gemstone.gemfire.distributed.ServerLauncher.Command;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.support.DistributedSystemAdapter;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

import edu.umd.cs.mtc.MultithreadedTestCase;
import edu.umd.cs.mtc.TestFramework;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.lib.concurrent.Synchroniser;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

/**
 * The ServerLauncherJUnitTest class is a test suite of unit tests testing the contract, functionality and invariants
 * of the ServerLauncher class.
 *
 * @author John Blum
 * @author Kirk Lund
 * @see com.gemstone.gemfire.distributed.ServerLauncher
 * @see com.gemstone.gemfire.distributed.ServerLauncher.Builder
 * @see com.gemstone.gemfire.distributed.ServerLauncher.Command
 * @see org.junit.Assert
 * @see org.junit.Test
 * @since 7.0
 */
@SuppressWarnings({"deprecation", "unused"})
@Category(UnitTest.class)
public class ServerLauncherJUnitTest {

  private Mockery mockContext;

  @Rule
  public final RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();
  
  @Rule
  public final TestName testName = new TestName();
  
  @Before
  public void setup() {
    mockContext = new Mockery() {{
      setImposteriser(ClassImposteriser.INSTANCE);
      setThreadingPolicy(new Synchroniser());
    }};
  }

  @After
  public void tearDown() {
    mockContext.assertIsSatisfied();
    mockContext = null;
  }

  @Test
  public void testParseCommand() {
    Builder builder = new Builder();

    assertEquals(Builder.DEFAULT_COMMAND, builder.getCommand());

    builder.parseCommand((String[]) null);

    assertEquals(Builder.DEFAULT_COMMAND, builder.getCommand());

    builder.parseCommand(); // empty String array

    assertEquals(Builder.DEFAULT_COMMAND, builder.getCommand());

    builder.parseCommand(Command.START.getName());

    assertEquals(Command.START, builder.getCommand());

    builder.parseCommand("Status");

    assertEquals(Command.STATUS, builder.getCommand());

    builder.parseCommand("sToP");

    assertEquals(Command.STOP, builder.getCommand());

    builder.parseCommand("--opt", "START", "-o", Command.STATUS.getName());

    assertEquals(Command.START, builder.getCommand());

    builder.setCommand(null);
    builder.parseCommand("badCommandName", "--start", "stat");

    assertEquals(Builder.DEFAULT_COMMAND, builder.getCommand());
  }

  @Test
  public void testParseMemberName() {
    Builder builder = new Builder();

    assertNull(builder.getMemberName());

    builder.parseMemberName((String[]) null);

    assertNull(builder.getMemberName());

    builder.parseMemberName(); // empty String array

    assertNull(builder.getMemberName());

    builder.parseMemberName(Command.START.getName(), "--opt", "-o");

    assertNull(builder.getMemberName());

    builder.parseMemberName("memberOne");

    assertEquals("memberOne", builder.getMemberName());
  }

  @Test
  public void testSetAndGetCommand() {
    Builder builder = new Builder();

    assertEquals(Builder.DEFAULT_COMMAND, builder.getCommand());
    assertSame(builder, builder.setCommand(Command.STATUS));
    assertEquals(Command.STATUS, builder.getCommand());
    assertSame(builder, builder.setCommand(null));
    assertEquals(Builder.DEFAULT_COMMAND, builder.getCommand());
  }

  @Test
  public void testSetAndGetMemberName() {
    Builder builder = new Builder();

    assertNull(builder.getMemberName());
    assertSame(builder, builder.setMemberName("serverOne"));
    assertEquals("serverOne", builder.getMemberName());
    assertSame(builder, builder.setMemberName(null));
    assertNull(builder.getMemberName());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSetMemberNameToBlankString() {
    try {
      new Builder().setMemberName("  ");
    }
    catch (IllegalArgumentException expected) {
      assertEquals(LocalizedStrings.Launcher_Builder_MEMBER_NAME_ERROR_MESSAGE.toLocalizedString("Server"),
        expected.getMessage());
      throw expected;
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSetMemberNameToEmptyString() {
    try {
      new Builder().setMemberName("");
    }
    catch (IllegalArgumentException expected) {
      assertEquals(LocalizedStrings.Launcher_Builder_MEMBER_NAME_ERROR_MESSAGE.toLocalizedString("Server"),
        expected.getMessage());
      throw expected;
    }
  }

  @Test
  public void testSetAndGetPid() {
    Builder builder = new Builder();

    assertNull(builder.getPid());
    assertSame(builder, builder.setPid(0));
    assertEquals(0, builder.getPid().intValue());
    assertSame(builder, builder.setPid(1));
    assertEquals(1, builder.getPid().intValue());
    assertSame(builder, builder.setPid(1024));
    assertEquals(1024, builder.getPid().intValue());
    assertSame(builder, builder.setPid(12345));
    assertEquals(12345, builder.getPid().intValue());
    assertSame(builder, builder.setPid(null));
    assertNull(builder.getPid());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSetPidToInvalidValue() {
    try {
      new Builder().setPid(-1);
    }
    catch (IllegalArgumentException expected) {
      assertEquals(LocalizedStrings.Launcher_Builder_PID_ERROR_MESSAGE.toLocalizedString(), expected.getMessage());
      throw expected;
    }
  }

  @Test
  public void testSetAndGetServerBindAddress() throws Exception {
    Builder builder = new Builder();

    assertNull(builder.getServerBindAddress());
    assertSame(builder, builder.setServerBindAddress(null));
    assertNull(builder.getServerBindAddress());
    assertSame(builder, builder.setServerBindAddress(""));
    assertNull(builder.getServerBindAddress());
    assertSame(builder, builder.setServerBindAddress("  "));
    assertNull(builder.getServerBindAddress());
    assertSame(builder, builder.setServerBindAddress(InetAddress.getLocalHost().getCanonicalHostName()));
    assertEquals(InetAddress.getLocalHost(), builder.getServerBindAddress());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSetServerBindAddressToUnknownHost() {
    try {
      new Builder().setServerBindAddress("badHostName.badCompany.com");
    }
    catch (IllegalArgumentException expected) {
      assertEquals(LocalizedStrings.Launcher_Builder_UNKNOWN_HOST_ERROR_MESSAGE.toLocalizedString("Server"), expected.getMessage());
      assertTrue(expected.getCause() instanceof UnknownHostException);
      throw expected;
    }
  }

  @Test
  public void testSetAndGetHostnameForClients() {
    final Builder builder = new Builder();

    assertNull(builder.getHostNameForClients());
    assertSame(builder, builder.setHostNameForClients("Pegasus"));
    assertEquals("Pegasus", builder.getHostNameForClients());
    assertSame(builder, builder.setHostNameForClients(null));
    assertNull(builder.getHostNameForClients());
  }

  @Test
  public void testSetAndGetServerPort() {
    Builder builder = new Builder();

    assertEquals(ServerLauncher.DEFAULT_SERVER_PORT, builder.getServerPort());
    assertSame(builder, builder.setServerPort(0));
    assertEquals(0, builder.getServerPort().intValue());
    assertSame(builder, builder.setServerPort(1));
    assertEquals(1, builder.getServerPort().intValue());
    assertSame(builder, builder.setServerPort(80));
    assertEquals(80, builder.getServerPort().intValue());
    assertSame(builder, builder.setServerPort(1024));
    assertEquals(1024, builder.getServerPort().intValue());
    assertSame(builder, builder.setServerPort(65535));
    assertEquals(65535, builder.getServerPort().intValue());
    assertSame(builder, builder.setServerPort(null));
    assertEquals(ServerLauncher.DEFAULT_SERVER_PORT, builder.getServerPort());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSetServerPortToOverflow() {
    try {
      new Builder().setServerPort(65536);
    }
    catch (IllegalArgumentException expected) {
      assertEquals(LocalizedStrings.Launcher_Builder_INVALID_PORT_ERROR_MESSAGE.toLocalizedString("Server"),
        expected.getMessage());
      throw expected;
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSetServerPortToUnderflow() {
    try {
      new Builder().setServerPort(-1);
    }
    catch (IllegalArgumentException expected) {
      assertEquals(LocalizedStrings.Launcher_Builder_INVALID_PORT_ERROR_MESSAGE.toLocalizedString("Server"),
        expected.getMessage());
      throw expected;
    }
  }

  @Test
  public void testSetAndGetCriticalHeapPercentage() {
    Builder builder = new Builder();

    assertNull(builder.getCriticalHeapPercentage());
    assertSame(builder, builder.setCriticalHeapPercentage(55.5f));
    assertEquals(55.5f, builder.getCriticalHeapPercentage().floatValue(), 0.0f);
    assertSame(builder, builder.setCriticalHeapPercentage(null));
    assertNull(builder.getCriticalHeapPercentage());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSetCriticalHeapPercentageToOverflow() {
    try {
      new Builder().setCriticalHeapPercentage(100.01f);
    }
    catch (IllegalArgumentException expected) {
      assertEquals("Critical heap percentage (100.01) must be between 0 and 100!", expected.getMessage());
      throw expected;
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSetCriticalHeapPercentageToUnderflow() {
    try {
      new Builder().setCriticalHeapPercentage(-0.01f);
    }
    catch (IllegalArgumentException expected) {
      assertEquals("Critical heap percentage (-0.01) must be between 0 and 100!", expected.getMessage());
      throw expected;
    }
  }

  @Test
  public void testSetAndGetEvictionHeapPercentage() {
    Builder builder = new Builder();

    assertNull(builder.getEvictionHeapPercentage());
    assertSame(builder, builder.setEvictionHeapPercentage(55.55f));
    assertEquals(55.55f, builder.getEvictionHeapPercentage().floatValue(), 0.0f);
    assertSame(builder, builder.setEvictionHeapPercentage(null));
    assertNull(builder.getEvictionHeapPercentage());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSetEvictionHeapPercentageToOverflow() {
    try {
      new Builder().setEvictionHeapPercentage(101.0f);
    }
    catch (IllegalArgumentException expected) {
      assertEquals("Eviction heap percentage (101.0) must be between 0 and 100!", expected.getMessage());
      throw expected;
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSetEvictionHeapPercentageToUnderflow() {
    try {
      new Builder().setEvictionHeapPercentage(-10.0f);
    }
    catch (IllegalArgumentException expected) {
      assertEquals("Eviction heap percentage (-10.0) must be between 0 and 100!", expected.getMessage());
      throw expected;
    }
  }

  @Test
  public void testSetAndGetMaxConnections() {
    Builder builder = new Builder();

    assertNull(builder.getMaxConnections());
    assertSame(builder, builder.setMaxConnections(1000));
    assertEquals(1000, builder.getMaxConnections().intValue());
    assertSame(builder, builder.setMaxConnections(null));
    assertNull(builder.getMaxConnections());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSetMaxConnectionsWithIllegalValue() {
    try {
      new Builder().setMaxConnections(-10);
    }
    catch (IllegalArgumentException expected) {
      assertEquals("Max Connections (-10) must be greater than 0!", expected.getMessage());
      throw expected;
    }
  }

  @Test
  public void testSetAndGetMaxMessageCount() {
    Builder builder = new Builder();

    assertNull(builder.getMaxMessageCount());
    assertSame(builder, builder.setMaxMessageCount(50));
    assertEquals(50, builder.getMaxMessageCount().intValue());
    assertSame(builder, builder.setMaxMessageCount(null));
    assertNull(builder.getMaxMessageCount());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSetMaxMessageCountWithIllegalValue() {
    try {
      new Builder().setMaxMessageCount(0);
    }
    catch (IllegalArgumentException expected) {
      assertEquals("Max Message Count (0) must be greater than 0!", expected.getMessage());
      throw expected;
    }
  }

  @Test
  public void testSetAndGetMaxThreads() {
    Builder builder = new Builder();

    assertNull(builder.getMaxThreads());
    assertSame(builder, builder.setMaxThreads(16));
    assertEquals(16, builder.getMaxThreads().intValue());
    assertSame(builder, builder.setMaxThreads(null));
    assertNull(builder.getMaxThreads());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSetMaxThreadsWithIllegalValue() {
    try {
      new Builder().setMaxThreads(-4);
    }
    catch (IllegalArgumentException expected) {
      assertEquals("Max Threads (-4) must be greater than 0!", expected.getMessage());
      throw expected;
    }
  }

  @Test
  public void testSetAndGetMessageTimeToLive() {
    Builder builder = new Builder();

    assertNull(builder.getMessageTimeToLive());
    assertSame(builder, builder.setMessageTimeToLive(30000));
    assertEquals(30000, builder.getMessageTimeToLive().intValue());
    assertSame(builder, builder.setMessageTimeToLive(null));
    assertNull(builder.getMessageTimeToLive());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSetMessageTimeToLiveWithIllegalValue() {
    try {
      new Builder().setMessageTimeToLive(0);
    }
    catch (IllegalArgumentException expected) {
      assertEquals("Message Time To Live (0) must be greater than 0!", expected.getMessage());
      throw expected;
    }
  }

  @Test
  public void testSetAndGetSocketBufferSize() {
    Builder builder = new Builder();

    assertNull(builder.getSocketBufferSize());
    assertSame(builder, builder.setSocketBufferSize(32768));
    assertEquals(32768, builder.getSocketBufferSize().intValue());
    assertSame(builder, builder.setSocketBufferSize(null));
    assertNull(builder.getSocketBufferSize());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSetSocketBufferSizeWithIllegalValue() {
    try {
      new Builder().setSocketBufferSize(-8192);
    }
    catch (IllegalArgumentException expected) {
      assertEquals("The Server's Socket Buffer Size (-8192) must be greater than 0!", expected.getMessage());
      throw expected;
    }
  }

  @Test
  public void testBuildWithMemberNameSetInApiPropertiesOnStart() {
    ServerLauncher launcher = new Builder()
      .setCommand(ServerLauncher.Command.START)
      .setMemberName(null)
      .set(DistributionConfig.NAME_NAME, "serverABC")
      .build();

    assertNotNull(launcher);
    assertEquals(ServerLauncher.Command.START, launcher.getCommand());
    assertNull(launcher.getMemberName());
    assertEquals("serverABC", launcher.getProperties().getProperty(DistributionConfig.NAME_NAME));
  }

  @Test
  public void testBuildWithMemberNameSetInSystemPropertiesOnStart() {
    System.setProperty(DistributionConfig.GEMFIRE_PREFIX + DistributionConfig.NAME_NAME, "serverXYZ");

    ServerLauncher launcher = new Builder()
      .setCommand(ServerLauncher.Command.START)
      .setMemberName(null)
      .build();

    assertNotNull(launcher);
    assertEquals(ServerLauncher.Command.START, launcher.getCommand());
    assertNull(launcher.getMemberName());
  }

  @Test(expected = IllegalStateException.class)
  public void testBuildNoMemberNameOnStart() {
    try {
      new Builder().setCommand(Command.START).build();
    }
    catch (IllegalStateException expected) {
      assertEquals(LocalizedStrings.Launcher_Builder_MEMBER_NAME_VALIDATION_ERROR_MESSAGE.toLocalizedString("Server"),
        expected.getMessage());
      throw expected;
    }
  }

  @Test
  public void testIsServing() {
    final Cache mockCache = mockContext.mock(Cache.class, "Cache");
    final CacheServer mockCacheServer = mockContext.mock(CacheServer.class, "CacheServer");

    mockContext.checking(new Expectations() {{
      oneOf(mockCache).getCacheServers();
      will(returnValue(Collections.singletonList(mockCacheServer)));
    }});

    final ServerLauncher serverLauncher = new Builder().setMemberName("serverOne").build();

    assertNotNull(serverLauncher);
    assertEquals("serverOne", serverLauncher.getMemberName());
    assertTrue(serverLauncher.isServing(mockCache));
  }

  @Test
  public void testIsServingWhenNoCacheServersExist() {
    final Cache mockCache = mockContext.mock(Cache.class, "Cache");

    mockContext.checking(new Expectations() {{
      oneOf(mockCache).getCacheServers();
      will(returnValue(Collections.emptyList()));
    }});

    final ServerLauncher serverLauncher = new Builder().setMemberName("serverOne").build();

    assertNotNull(serverLauncher);
    assertEquals("serverOne", serverLauncher.getMemberName());
    assertFalse(serverLauncher.isServing(mockCache));
  }

  @Test
  public void testIsWaiting() {
    final Cache mockCache = mockContext.mock(Cache.class, "Cache");
    final DistributedSystem mockDistributedSystem = mockContext.mock(DistributedSystem.class, "DistributedSystem");

    mockContext.checking(new Expectations() {{
      oneOf(mockCache).getDistributedSystem();
      will(returnValue(mockDistributedSystem));
      oneOf(mockDistributedSystem).isConnected();
      will(returnValue(true));
    }});

    final ServerLauncher serverLauncher = new Builder().setMemberName("serverOne").build();

    assertNotNull(serverLauncher);
    assertEquals("serverOne", serverLauncher.getMemberName());

    serverLauncher.running.set(true);

    assertTrue(serverLauncher.isRunning());
    assertTrue(serverLauncher.isWaiting(mockCache));
  }

  @Test
  public void testIsWaitingWhenNotConnected() {
    final Cache mockCache = mockContext.mock(Cache.class, "Cache");
    final DistributedSystem mockDistributedSystem = mockContext.mock(DistributedSystem.class, "DistributedSystem");

    mockContext.checking(new Expectations() {{
      oneOf(mockCache).getDistributedSystem();
      will(returnValue(mockDistributedSystem));
      oneOf(mockDistributedSystem).isConnected();
      will(returnValue(false));
    }});

    final ServerLauncher serverLauncher = new Builder().setMemberName("serverOne").build();

    assertNotNull(serverLauncher);
    assertEquals("serverOne", serverLauncher.getMemberName());

    serverLauncher.running.set(true);

    assertTrue(serverLauncher.isRunning());
    assertFalse(serverLauncher.isWaiting(mockCache));
  }

  @Test
  public void testIsWaitingWhenNotRunning() {
    ServerLauncher serverLauncher = new Builder().setMemberName("serverOne").build();

    assertNotNull(serverLauncher);
    assertEquals("serverOne", serverLauncher.getMemberName());

    serverLauncher.running.set(false);

    assertFalse(serverLauncher.isRunning());
    assertFalse(serverLauncher.isWaiting(null));
  }

  @Test
  public void testWaitOnServer() throws Throwable {
    TestFramework.runOnce(new ServerWaitMultiThreadedTestCase());
  }

  @Test
  public void testIsDefaultServerEnabled() {
    final Cache mockCache = mockContext.mock(Cache.class, "Cache");

    mockContext.checking(new Expectations() {{
      oneOf(mockCache).getCacheServers();
      will(returnValue(Collections.emptyList()));
    }});

    ServerLauncher serverLauncher = new Builder().setMemberName("serverOne").build();

    assertNotNull(serverLauncher);
    assertEquals("serverOne", serverLauncher.getMemberName());
    assertFalse(serverLauncher.isDisableDefaultServer());
    assertTrue(serverLauncher.isDefaultServerEnabled(mockCache));
  }

  @Test
  public void testIsDefaultServerEnabledWhenCacheServersExist() {
    final Cache mockCache = mockContext.mock(Cache.class, "Cache");
    final CacheServer mockCacheServer = mockContext.mock(CacheServer.class, "CacheServer");

    mockContext.checking(new Expectations() {{
      oneOf(mockCache).getCacheServers();
      will(returnValue(Collections.singletonList(mockCacheServer)));
    }});

    final ServerLauncher serverLauncher = new Builder().setMemberName("serverOne").setDisableDefaultServer(false).build();

    assertNotNull(serverLauncher);
    assertEquals("serverOne", serverLauncher.getMemberName());
    assertFalse(serverLauncher.isDisableDefaultServer());
    assertFalse(serverLauncher.isDefaultServerEnabled(mockCache));
  }
  @Test
  public void testIsDefaultServerEnabledWhenNoCacheServersExistAndDefaultServerDisabled() {
    final Cache mockCache = mockContext.mock(Cache.class, "Cache");

    mockContext.checking(new Expectations() {{
      oneOf(mockCache).getCacheServers();
      will(returnValue(Collections.emptyList()));
    }});

    final ServerLauncher serverLauncher = new Builder().setMemberName("serverOne").setDisableDefaultServer(true).build();

    assertNotNull(serverLauncher);
    assertEquals("serverOne", serverLauncher.getMemberName());
    assertTrue(serverLauncher.isDisableDefaultServer());
    assertFalse(serverLauncher.isDefaultServerEnabled(mockCache));
  }

  @Test
  public void testStartCacheServer() throws IOException {
    final Cache mockCache = mockContext.mock(Cache.class, "Cache");
    final CacheServer mockCacheServer = mockContext.mock(CacheServer.class, "CacheServer");

    mockContext.checking(new Expectations() {{
      oneOf(mockCache).getCacheServers();
      will(returnValue(Collections.emptyList()));
      oneOf(mockCache).addCacheServer();
      will(returnValue(mockCacheServer));
      oneOf(mockCacheServer).setBindAddress(with(aNull(String.class)));
      oneOf(mockCacheServer).setPort(with(equal(11235)));
      oneOf(mockCacheServer).start();
    }});

    final ServerLauncher serverLauncher = new Builder().setMemberName("serverOne")
      .setServerBindAddress(null)
      .setServerPort(11235)
      .setDisableDefaultServer(false)
      .build();

    assertNotNull(serverLauncher);
    assertEquals("serverOne", serverLauncher.getMemberName());
    assertFalse(serverLauncher.isDisableDefaultServer());

    serverLauncher.startCacheServer(mockCache);
  }

  @Test
  public void testStartCacheServerWhenDefaultServerDisabled() throws IOException {
    final Cache mockCache = mockContext.mock(Cache.class, "Cache");

    mockContext.checking(new Expectations() {{
      oneOf(mockCache).getCacheServers();
      will(returnValue(Collections.emptyList()));
    }});

    final ServerLauncher serverLauncher = new Builder().setMemberName("serverOne").setDisableDefaultServer(true).build();

    assertNotNull(serverLauncher);
    assertEquals("serverOne", serverLauncher.getMemberName());
    assertTrue(serverLauncher.isDisableDefaultServer());

    serverLauncher.startCacheServer(mockCache);
  }

  @Test
  public void testStartCacheServerWithExistingCacheServer() throws IOException {
    final Cache mockCache = mockContext.mock(Cache.class, "Cache");
    final CacheServer mockCacheServer = mockContext.mock(CacheServer.class, "CacheServer");

    mockContext.checking(new Expectations() {{
      oneOf(mockCache).getCacheServers();
      will(returnValue(Collections.singletonList(mockCacheServer)));
    }});

    final ServerLauncher serverLauncher = new Builder().setMemberName("serverOne").setDisableDefaultServer(false).build();

    assertNotNull(serverLauncher);
    assertEquals("serverOne", serverLauncher.getMemberName());
    assertFalse(serverLauncher.isDisableDefaultServer());

    serverLauncher.startCacheServer(mockCache);
  }
  
  public static void main(final String... args) {
    System.err.printf("Thread (%1$s) is daemon (%2$s)%n", Thread.currentThread().getName(),
      Thread.currentThread().isDaemon());
    new Builder(args).setCommand(Command.START).build().run();
  }

  private final class ServerWaitMultiThreadedTestCase extends MultithreadedTestCase {

    private final AtomicBoolean connectionStateHolder = new AtomicBoolean(true);

    private ServerLauncher serverLauncher;

    @Override
    public void initialize() {
      super.initialize();

      final Cache mockCache = mockContext.mock(Cache.class, "Cache");

      final DistributedSystem mockDistributedSystem = new DistributedSystemAdapter() {
        @Override public boolean isConnected() {
          return connectionStateHolder.get();
        }
      };

      mockContext.checking(new Expectations() {{
        allowing(mockCache).getDistributedSystem();
        will(returnValue(mockDistributedSystem));
        allowing(mockCache).getCacheServers();
        will(returnValue(Collections.emptyList()));
        oneOf(mockCache).close();
      }});

      this.serverLauncher = new Builder().setMemberName("dataMember").setDisableDefaultServer(true)
        .setCache(mockCache).build();

      assertNotNull(this.serverLauncher);
      assertEquals("dataMember", this.serverLauncher.getMemberName());
      assertTrue(this.serverLauncher.isDisableDefaultServer());
      assertTrue(connectionStateHolder.get());
    }

    public void thread1() {
      assertTick(0);

      Thread.currentThread().setName("GemFire Data Member 'main' Thread");
      this.serverLauncher.running.set(true);

      assertTrue(this.serverLauncher.isRunning());
      assertFalse(this.serverLauncher.isServing(this.serverLauncher.getCache()));
      assertTrue(this.serverLauncher.isWaiting(this.serverLauncher.getCache()));

      this.serverLauncher.waitOnServer();

      assertTick(1); // NOTE the tick does not advance when the other Thread terminates
    }

    public void thread2() {
      waitForTick(1);

      Thread.currentThread().setName("GemFire 'shutdown' Thread");

      assertTrue(this.serverLauncher.isRunning());

      this.connectionStateHolder.set(false);
    }

    @Override
    public void finish() {
      super.finish();
      assertFalse(this.serverLauncher.isRunning());
    }
  }
}

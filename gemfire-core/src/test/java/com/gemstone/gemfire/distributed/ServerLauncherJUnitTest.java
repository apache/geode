/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */
package com.gemstone.gemfire.distributed;

import static org.junit.Assert.*;
import static org.junit.Assume.*;

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
import com.gemstone.gemfire.internal.lang.SystemUtils;
import com.gemstone.gemfire.internal.util.IOUtils;
import com.gemstone.junit.UnitTest;

import edu.umd.cs.mtc.MultithreadedTestCase;
import edu.umd.cs.mtc.TestFramework;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * The ServerLauncherJUnitTest class is a test suite of unit tests testing the contract, functionality and invariants
 * of the ServerLauncher class.
 *
 * @author John Blum
 * @author Kirk Lund
 * @see com.gemstone.gemfire.distributed.CommonLauncherTestSuite
 * @see com.gemstone.gemfire.distributed.ServerLauncher
 * @see com.gemstone.gemfire.distributed.ServerLauncher.Builder
 * @see com.gemstone.gemfire.distributed.ServerLauncher.Command
 * @see org.junit.Assert
 * @see org.junit.Test
 * @since 7.0
 */
@SuppressWarnings("deprecation")
@Category(UnitTest.class)
public class ServerLauncherJUnitTest extends CommonLauncherTestSuite {

  private static final String GEMFIRE_PROPERTIES_FILE_NAME = "gemfire.properties";
  private static final String TEMPORARY_FILE_NAME = "beforeServerLauncherJUnitTest_" + GEMFIRE_PROPERTIES_FILE_NAME;

  private Mockery mockContext;

  @BeforeClass
  public static void testSuiteSetup() {
    if (SystemUtils.isWindows()) {
      return;
    }
    File file = new File(GEMFIRE_PROPERTIES_FILE_NAME);
    if (file.exists()) {
      File dest = new File(TEMPORARY_FILE_NAME);
      assertTrue(file.renameTo(dest));
    }
  }

  @AfterClass
  public static void testSuiteTearDown() {
    if (SystemUtils.isWindows()) {
      return;
    }
    File file = new File(TEMPORARY_FILE_NAME);
    if (file.exists()) {
      File dest = new File(GEMFIRE_PROPERTIES_FILE_NAME);
      assertTrue(file.renameTo(dest));
    }
  }

  @Before
  public void setup() {
    mockContext = new Mockery() {{
      setImposteriser(ClassImposteriser.INSTANCE);
    }};
  }

  @After
  public void tearDown() {
    mockContext.assertIsSatisfied();
    mockContext = null;
  }

  @Test
  public void testParseArguments() throws Exception {
    Builder builder = new Builder();

    builder.parseArguments("start", "serverOne", "--assign-buckets", "--disable-default-server", "--debug", "--force",
      "--rebalance", "--redirect-output", "--dir=" + ServerLauncher.DEFAULT_WORKING_DIRECTORY, "--pid=1234",
        "--server-bind-address=" + InetAddress.getLocalHost().getHostAddress(), "--server-port=11235");

    assertEquals(Command.START, builder.getCommand());
    assertEquals("serverOne", builder.getMemberName());
    assertTrue(builder.getAssignBuckets());
    assertTrue(builder.getDisableDefaultServer());
    assertTrue(builder.getDebug());
    assertTrue(builder.getForce());
    assertFalse(Boolean.TRUE.equals(builder.getHelp()));
    assertTrue(builder.getRebalance());
    assertTrue(builder.getRedirectOutput());
    assertEquals(ServerLauncher.DEFAULT_WORKING_DIRECTORY, builder.getWorkingDirectory());
    assertEquals(1234, builder.getPid().intValue());
    assertEquals(InetAddress.getLocalHost(), builder.getServerBindAddress());
    assertEquals(11235, builder.getServerPort().intValue());
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
  public void testSetAndGetWorkingDirectory() {
    Builder builder = new Builder();

    assertEquals(ServerLauncher.DEFAULT_WORKING_DIRECTORY, builder.getWorkingDirectory());
    assertSame(builder, builder.setWorkingDirectory(System.getProperty("java.io.tmpdir")));
    assertEquals(IOUtils.tryGetCanonicalPathElseGetAbsolutePath(new File(System.getProperty("java.io.tmpdir"))),
      builder.getWorkingDirectory());
    assertSame(builder, builder.setWorkingDirectory("  "));
    assertEquals(ServerLauncher.DEFAULT_WORKING_DIRECTORY, builder.getWorkingDirectory());
    assertSame(builder, builder.setWorkingDirectory(""));
    assertEquals(ServerLauncher.DEFAULT_WORKING_DIRECTORY, builder.getWorkingDirectory());
    assertSame(builder, builder.setWorkingDirectory(null));
    assertEquals(ServerLauncher.DEFAULT_WORKING_DIRECTORY, builder.getWorkingDirectory());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSetWorkingDirectoryToNonExistingDirectory() {
    try {
      new Builder().setWorkingDirectory("/path/to/non_existing/directory");
    }
    catch (IllegalArgumentException expected) {
      assertEquals(LocalizedStrings.Launcher_Builder_WORKING_DIRECTORY_NOT_FOUND_ERROR_MESSAGE
        .toLocalizedString("Server"), expected.getMessage());
      assertTrue(expected.getCause() instanceof FileNotFoundException);
      assertEquals("/path/to/non_existing/directory", expected.getCause().getMessage());
      throw expected;
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSetWorkingDirectoryToFile() throws IOException {
    File tmpFile = File.createTempFile("tmp", "file");

    assertNotNull(tmpFile);
    assertTrue(tmpFile.isFile());

    tmpFile.deleteOnExit();

    try {
      new Builder().setWorkingDirectory(tmpFile.getAbsolutePath());
    }
    catch (IllegalArgumentException expected) {
      assertEquals(LocalizedStrings.Launcher_Builder_WORKING_DIRECTORY_NOT_FOUND_ERROR_MESSAGE
        .toLocalizedString("Server"), expected.getMessage());
      assertTrue(expected.getCause() instanceof FileNotFoundException);
      assertEquals(tmpFile.getAbsolutePath(), expected.getCause().getMessage());
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
  public void testBuild() throws Exception {
    ServerLauncher launcher = new Builder()
      .setCommand(Command.STOP)
      .setAssignBuckets(true)
      .setForce(true)
      .setMemberName("serverOne")
      .setRebalance(true)
      .setServerBindAddress(InetAddress.getLocalHost().getHostAddress())
      .setServerPort(11235)
      .setWorkingDirectory(System.getProperty("java.io.tmpdir"))
      .setCriticalHeapPercentage(90.0f)
      .setEvictionHeapPercentage(75.0f)
      .setMaxConnections(100)
      .setMaxMessageCount(512)
      .setMaxThreads(8)
      .setMessageTimeToLive(120000)
      .setSocketBufferSize(32768)
      .build();

    assertNotNull(launcher);
    assertTrue(launcher.isAssignBuckets());
    assertFalse(launcher.isDebugging());
    assertFalse(launcher.isDisableDefaultServer());
    assertTrue(launcher.isForcing());
    assertFalse(launcher.isHelping());
    assertTrue(launcher.isRebalancing());
    assertFalse(launcher.isRunning());
    assertEquals(Command.STOP, launcher.getCommand());
    assertEquals("serverOne", launcher.getMemberName());
    assertEquals(InetAddress.getLocalHost(), launcher.getServerBindAddress());
    assertEquals(11235, launcher.getServerPort().intValue());
    assertEquals(IOUtils.tryGetCanonicalPathElseGetAbsolutePath(new File(System.getProperty("java.io.tmpdir"))),
      launcher.getWorkingDirectory());
    assertEquals(90.0f, launcher.getCriticalHeapPercentage().floatValue(), 0.0f);
    assertEquals(75.0f, launcher.getEvictionHeapPercentage().floatValue(), 0.0f);
    assertEquals(100, launcher.getMaxConnections().intValue());
    assertEquals(512, launcher.getMaxMessageCount().intValue());
    assertEquals(8, launcher.getMaxThreads().intValue());
    assertEquals(120000, launcher.getMessageTimeToLive().intValue());
    assertEquals(32768, launcher.getSocketBufferSize().intValue());
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
  public void testBuildWithMemberNameSetInGemFirePropertiesOnStart() {
    assumeFalse(SystemUtils.isWindows());

    Properties gemfireProperties = new Properties();

    gemfireProperties.setProperty(DistributionConfig.NAME_NAME, "server123");

    File gemfirePropertiesFile = writeGemFirePropertiesToFile(gemfireProperties, "gemfire.properties",
      String.format("Test gemfire.properties file for %1$s.%2$s.", getClass().getSimpleName(),
        "testBuildWithMemberNameSetInGemFirePropertiesOnStart"));

    assertNotNull(gemfirePropertiesFile);
    assertTrue(gemfirePropertiesFile.isFile());

    try {
      ServerLauncher launcher = new Builder()
        .setCommand(ServerLauncher.Command.START)
        .setMemberName(null)
        .build();

      assertNotNull(launcher);
      assertEquals(ServerLauncher.Command.START, launcher.getCommand());
      assertNull(launcher.getMemberName());
    }
    finally {
      assertTrue(gemfirePropertiesFile.delete());
      assertFalse(gemfirePropertiesFile.isFile());
    }
  }

  @Test
  public void testBuildWithMemberNameSetInSystemPropertiesOnStart() {
    try {
      System.setProperty(DistributionConfig.GEMFIRE_PREFIX + DistributionConfig.NAME_NAME, "serverXYZ");

      ServerLauncher launcher = new Builder()
        .setCommand(ServerLauncher.Command.START)
        .setMemberName(null)
        .build();

      assertNotNull(launcher);
      assertEquals(ServerLauncher.Command.START, launcher.getCommand());
      assertNull(launcher.getMemberName());
    }
    finally {
      System.clearProperty(DistributionConfig.GEMFIRE_PREFIX + DistributionConfig.NAME_NAME);
    }
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

  @Test(expected = IllegalStateException.class)
  public void testBuildWithInvalidWorkingDirectoryOnStart() {
    try {
      new Builder().setCommand(Command.START)
        .setMemberName("serverOne")
        .setWorkingDirectory(System.getProperty("java.io.tmpdir"))
        .build();
    }
    catch (IllegalStateException expected) {
      assertEquals(LocalizedStrings.Launcher_Builder_WORKING_DIRECTORY_OPTION_NOT_VALID_ERROR_MESSAGE
        .toLocalizedString("Server"), expected.getMessage());
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

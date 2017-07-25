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
package org.apache.geode.management.internal.cli.commands;

import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_BIND_ADDRESS;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.NAME;
import static org.apache.geode.distributed.ConfigurationProperties.START_DEV_REST_API;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.lib.concurrent.Synchroniser;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.lang.StringUtils;
import org.apache.geode.internal.util.CollectionUtils;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.util.MemberNotFoundException;
import org.apache.geode.test.junit.categories.UnitTest;

/**
 * The GfshCommandJUnitTest class is a test suite of test cases testing the contract and
 * functionality of the GfshCommand class for implementing GemFire shell (Gfsh) commands.
 *
 * @see GfshCommand
 * @see org.jmock.Expectations
 * @see org.jmock.Mockery
 * @see org.jmock.lib.legacy.ClassImposteriser
 * @see org.junit.Assert
 * @see org.junit.Test
 * @since GemFire 7.0
 */
@Category(UnitTest.class)
public class GfshCommandJUnitTest {

  private Mockery mockContext;

  private static class DefaultGfshCommmand implements GfshCommand {
  }

  private DefaultGfshCommmand defaultGfshCommmand;

  @Before
  public void setup() {
    mockContext = new Mockery();
    mockContext.setImposteriser(ClassImposteriser.INSTANCE);
    mockContext.setThreadingPolicy(new Synchroniser());

    defaultGfshCommmand = new DefaultGfshCommmand();
  }

  @After
  public void tearDown() {
    mockContext.assertIsSatisfied();
    mockContext = null;
  }

  private GfshCommand createAbstractCommandsSupport(final InternalCache cache) {
    return new TestCommands(cache);
  }

  private DistributedMember createMockMember(final String memberId, final String memberName) {
    final DistributedMember mockMember =
        mockContext.mock(DistributedMember.class, "DistributedMember " + memberId);

    mockContext.checking(new Expectations() {
      {
        allowing(mockMember).getName();
        will(returnValue(memberName));
        allowing(mockMember).getId();
        will(returnValue(memberId));
      }
    });

    return mockMember;
  }

  @Test
  public void testConvertDefaultValue() {
    assertNull(defaultGfshCommmand.convertDefaultValue(null, StringUtils.EMPTY));
    assertEquals(StringUtils.EMPTY,
        defaultGfshCommmand.convertDefaultValue(StringUtils.EMPTY, "test"));
    assertEquals(StringUtils.SPACE,
        defaultGfshCommmand.convertDefaultValue(StringUtils.SPACE, "testing"));
    assertEquals("tested",
        defaultGfshCommmand.convertDefaultValue(CliMetaData.ANNOTATION_DEFAULT_VALUE, "tested"));
  }

  @Test
  public void testGetMemberWithMatchingMemberId() {
    final InternalCache mockCache = mockContext.mock(InternalCache.class, "InternalCache");

    final DistributedSystem mockDistributedSystem =
        mockContext.mock(DistributedSystem.class, "DistributedSystem");

    final DistributedMember mockMemberSelf = createMockMember("S", "Self");
    final DistributedMember mockMemberOne = createMockMember("1", "One");
    final DistributedMember mockMemberTwo = createMockMember("2", "Two");

    mockContext.checking(new Expectations() {
      {
        oneOf(mockCache).getMembers();
        will(returnValue(CollectionUtils.asSet(mockMemberOne, mockMemberTwo)));
        oneOf(mockCache).getDistributedSystem();
        will(returnValue(mockDistributedSystem));
        oneOf(mockDistributedSystem).getDistributedMember();
        will(returnValue(mockMemberSelf));
      }
    });

    final GfshCommand commands = createAbstractCommandsSupport(mockCache);

    assertSame(mockMemberTwo, commands.getMember(mockCache, "2"));
  }

  @Test
  public void testGetMemberWithMatchingMemberName() {
    final InternalCache mockCache = mockContext.mock(InternalCache.class, "InternalCache");

    final DistributedSystem mockDistributedSystem =
        mockContext.mock(DistributedSystem.class, "DistributedSystem");

    final DistributedMember mockMemberSelf = createMockMember("S", "Self");
    final DistributedMember mockMemberOne = createMockMember("1", "One");
    final DistributedMember mockMemberTwo = createMockMember("2", "Two");

    mockContext.checking(new Expectations() {
      {
        oneOf(mockCache).getMembers();
        will(returnValue(CollectionUtils.asSet(mockMemberOne, mockMemberTwo)));
        oneOf(mockCache).getDistributedSystem();
        will(returnValue(mockDistributedSystem));
        oneOf(mockDistributedSystem).getDistributedMember();
        will(returnValue(mockMemberSelf));
      }
    });

    final GfshCommand commands = createAbstractCommandsSupport(mockCache);

    assertSame(mockMemberOne, commands.getMember(mockCache, "One"));
  }

  @Test
  public void testGetMemberWithMatchingMemberNameCaseInsensitive() {
    final InternalCache mockCache = mockContext.mock(InternalCache.class, "InternalCache");

    final DistributedSystem mockDistributedSystem =
        mockContext.mock(DistributedSystem.class, "DistributedSystem");

    final DistributedMember mockMemberSelf = createMockMember("S", "Self");
    final DistributedMember mockMemberOne = createMockMember("1", "One");
    final DistributedMember mockMemberTwo = createMockMember("2", "Two");

    mockContext.checking(new Expectations() {
      {
        oneOf(mockCache).getMembers();
        will(returnValue(CollectionUtils.asSet(mockMemberOne, mockMemberTwo)));
        oneOf(mockCache).getDistributedSystem();
        will(returnValue(mockDistributedSystem));
        oneOf(mockDistributedSystem).getDistributedMember();
        will(returnValue(mockMemberSelf));
      }
    });

    final GfshCommand commands = createAbstractCommandsSupport(mockCache);

    assertSame(mockMemberSelf, commands.getMember(mockCache, "self"));
  }

  @Test(expected = MemberNotFoundException.class)
  public void testGetMemberThrowsMemberNotFoundException() {
    final InternalCache mockCache = mockContext.mock(InternalCache.class, "InternalCache");

    final DistributedSystem mockDistributedSystem =
        mockContext.mock(DistributedSystem.class, "DistributedSystem");

    final DistributedMember mockMemberSelf = createMockMember("S", "Self");
    final DistributedMember mockMemberOne = createMockMember("1", "One");
    final DistributedMember mockMemberTwo = createMockMember("2", "Two");

    mockContext.checking(new Expectations() {
      {
        oneOf(mockCache).getMembers();
        will(returnValue(CollectionUtils.asSet(mockMemberOne, mockMemberTwo)));
        oneOf(mockCache).getDistributedSystem();
        will(returnValue(mockDistributedSystem));
        oneOf(mockDistributedSystem).getDistributedMember();
        will(returnValue(mockMemberSelf));
      }
    });

    final GfshCommand commands = createAbstractCommandsSupport(mockCache);

    try {
      commands.getMember(mockCache, "zero");
    } catch (MemberNotFoundException expected) {
      assertEquals(CliStrings.format(CliStrings.MEMBER_NOT_FOUND_ERROR_MESSAGE, "zero"),
          expected.getMessage());
      throw expected;
    }
  }

  @Test
  public void testGetMembers() {
    final InternalCache mockCache = mockContext.mock(InternalCache.class, "InternalCache");

    final DistributedSystem mockDistributedSystem =
        mockContext.mock(DistributedSystem.class, "DistributedSystem");

    final DistributedMember mockMemberSelf = createMockMember("S", "Self");
    final DistributedMember mockMemberOne = createMockMember("1", "One");
    final DistributedMember mockMemberTwo = createMockMember("2", "Two");

    mockContext.checking(new Expectations() {
      {
        oneOf(mockCache).getMembers();
        will(returnValue(CollectionUtils.asSet(mockMemberOne, mockMemberTwo)));
        oneOf(mockCache).getDistributedSystem();
        will(returnValue(mockDistributedSystem));
        oneOf(mockDistributedSystem).getDistributedMember();
        will(returnValue(mockMemberSelf));
      }
    });

    final GfshCommand commands = createAbstractCommandsSupport(mockCache);

    final Set<DistributedMember> expectedMembers =
        CollectionUtils.asSet(mockMemberOne, mockMemberTwo, mockMemberSelf);
    final Set<DistributedMember> actualMembers = commands.getMembers(mockCache);

    assertNotNull(actualMembers);
    assertEquals(expectedMembers.size(), actualMembers.size());
    assertTrue(actualMembers.containsAll(expectedMembers));
  }

  @Test
  public void testGetMembersContainsOnlySelf() {
    final InternalCache mockCache = mockContext.mock(InternalCache.class, "InternalCache");

    final DistributedSystem mockDistributedSystem =
        mockContext.mock(DistributedSystem.class, "DistributedSystem");
    final DistributedMember mockMemberSelf = createMockMember("S", "Self");

    mockContext.checking(new Expectations() {
      {
        oneOf(mockCache).getMembers();
        will(returnValue(Collections.emptySet()));
        oneOf(mockCache).getDistributedSystem();
        will(returnValue(mockDistributedSystem));
        oneOf(mockDistributedSystem).getDistributedMember();
        will(returnValue(mockMemberSelf));
      }
    });

    final GfshCommand commands = createAbstractCommandsSupport(mockCache);

    final Set<DistributedMember> expectedMembers = CollectionUtils.asSet(mockMemberSelf);
    final Set<DistributedMember> actualMembers = commands.getMembers(mockCache);

    assertNotNull(actualMembers);
    assertEquals(expectedMembers.size(), actualMembers.size());
    assertTrue(actualMembers.containsAll(expectedMembers));
  }

  @Test
  public void testRegister() {
    try {
      final Function mockFunction = mockContext.mock(Function.class, "Function");

      mockContext.checking(new Expectations() {
        {
          exactly(3).of(mockFunction).getId();
          will(returnValue("testRegister"));
          oneOf(mockFunction).isHA();
          will(returnValue(true));
          oneOf(mockFunction).hasResult();
          will(returnValue(true));
        }
      });

      final GfshCommand commands =
          createAbstractCommandsSupport(mockContext.mock(InternalCache.class));

      assertFalse(FunctionService.isRegistered("testRegister"));
      assertSame(mockFunction, commands.register(mockFunction));
      assertTrue(FunctionService.isRegistered("testRegister"));
    } finally {
      FunctionService.unregisterFunction("testRegister");
    }
  }

  @Test
  public void testRegisteredAlready() {
    try {
      final Function registeredFunction = mockContext.mock(Function.class, "Registered Function");
      final Function unregisteredFunction =
          mockContext.mock(Function.class, "Unregistered Function");

      mockContext.checking(new Expectations() {
        {
          exactly(2).of(registeredFunction).getId();
          will(returnValue("testRegisteredAlready"));
          oneOf(registeredFunction).isHA();
          will(returnValue(false));
          exactly(2).of(unregisteredFunction).getId();
          will(returnValue("testRegisteredAlready"));
        }
      });

      final GfshCommand commands =
          createAbstractCommandsSupport(mockContext.mock(InternalCache.class));

      FunctionService.registerFunction(registeredFunction);

      assertTrue(FunctionService.isRegistered("testRegisteredAlready"));
      assertSame(registeredFunction, commands.register(unregisteredFunction));
      assertTrue(FunctionService.isRegistered("testRegisteredAlready"));
    } finally {
      FunctionService.unregisterFunction("testRegisteredAlready");
    }
  }

  @Test
  public void testToStringOnBoolean() {
    assertEquals("false", defaultGfshCommmand.toString(null, null, null));
    assertEquals("true", defaultGfshCommmand.toString(true, null, null));
    assertEquals("true", defaultGfshCommmand.toString(Boolean.TRUE, null, null));
    assertEquals("false", defaultGfshCommmand.toString(false, null, null));
    assertEquals("false", defaultGfshCommmand.toString(Boolean.FALSE, null, null));
    assertEquals("false", defaultGfshCommmand.toString(true, "false", "true"));
    assertEquals("true", defaultGfshCommmand.toString(false, "false", "true"));
    assertEquals("Yes", defaultGfshCommmand.toString(true, "Yes", "No"));
    assertEquals("Yes", defaultGfshCommmand.toString(false, "No", "Yes"));
    assertEquals("TRUE", defaultGfshCommmand.toString(Boolean.TRUE, "TRUE", "FALSE"));
    assertEquals("FALSE", defaultGfshCommmand.toString(Boolean.FALSE, "TRUE", "FALSE"));
  }

  @Test
  public void testToStringOnThrowable() {
    assertEquals("test", defaultGfshCommmand.toString(new Throwable("test"), false));
  }

  @Test
  public void testToStringOnThrowablePrintingStackTrace() {
    final StringWriter writer = new StringWriter();
    final Throwable t = new Throwable("test");

    t.printStackTrace(new PrintWriter(writer));

    assertEquals(writer.toString(), defaultGfshCommmand.toString(t, true));
  }

  private static class TestCommands implements GfshCommand {

    private final InternalCache cache;

    protected TestCommands(final InternalCache cache) {
      assert cache != null : "The InternalCache cannot be null!";
      this.cache = cache;
    }

    @Override
    public InternalCache getCache() {
      return this.cache;
    }
  }

  @Test
  public void testAddGemFirePropertyFileToCommandLine() {
    final List<String> commandLine = new ArrayList<>();
    assertTrue(commandLine.isEmpty());
    StartMemberUtils.addGemFirePropertyFile(commandLine, null);
    assertTrue(commandLine.isEmpty());
    StartMemberUtils.addGemFirePropertyFile(commandLine, org.apache.commons.lang.StringUtils.EMPTY);
    assertTrue(commandLine.isEmpty());
    StartMemberUtils.addGemFirePropertyFile(commandLine, " ");
    assertTrue(commandLine.isEmpty());
    StartMemberUtils.addGemFirePropertyFile(commandLine, "/path/to/gemfire.properties");
    assertFalse(commandLine.isEmpty());
    assertTrue(commandLine.contains("-DgemfirePropertyFile=/path/to/gemfire.properties"));
  }

  @Test
  public void testAddGemFireSystemPropertiesToCommandLine() {
    final List<String> commandLine = new ArrayList<>();
    assertTrue(commandLine.isEmpty());
    StartMemberUtils.addGemFireSystemProperties(commandLine, new Properties());
    assertTrue(commandLine.isEmpty());

    final Properties gemfireProperties = new Properties();
    gemfireProperties.setProperty(LOCATORS, "localhost[11235]");
    gemfireProperties.setProperty(LOG_LEVEL, "config");
    gemfireProperties.setProperty(LOG_FILE, org.apache.commons.lang.StringUtils.EMPTY);
    gemfireProperties.setProperty(MCAST_PORT, "0");
    gemfireProperties.setProperty(NAME, "machine");
    StartMemberUtils.addGemFireSystemProperties(commandLine, gemfireProperties);

    assertFalse(commandLine.isEmpty());
    assertEquals(4, commandLine.size());

    for (final String propertyName : gemfireProperties.stringPropertyNames()) {
      final String propertyValue = gemfireProperties.getProperty(propertyName);
      if (org.apache.commons.lang.StringUtils.isBlank(propertyValue)) {
        for (final String systemProperty : commandLine) {
          assertFalse(systemProperty.startsWith(
              "-D" + DistributionConfig.GEMFIRE_PREFIX + "".concat(propertyName).concat("=")));
        }
      } else {
        assertTrue(commandLine.contains("-D" + DistributionConfig.GEMFIRE_PREFIX
            + "".concat(propertyName).concat("=").concat(propertyValue)));
      }
    }
  }

  @Test
  public void testAddGemFireSystemPropertiesToCommandLineWithRestAPI() {
    final List<String> commandLine = new ArrayList<>();
    assertTrue(commandLine.isEmpty());
    StartMemberUtils.addGemFireSystemProperties(commandLine, new Properties());
    assertTrue(commandLine.isEmpty());
    final Properties gemfireProperties = new Properties();
    gemfireProperties.setProperty(LOCATORS, "localhost[11235]");
    gemfireProperties.setProperty(LOG_LEVEL, "config");
    gemfireProperties.setProperty(LOG_FILE, StringUtils.EMPTY);
    gemfireProperties.setProperty(MCAST_PORT, "0");
    gemfireProperties.setProperty(NAME, "machine");
    gemfireProperties.setProperty(START_DEV_REST_API, "true");
    gemfireProperties.setProperty(HTTP_SERVICE_PORT, "8080");
    gemfireProperties.setProperty(HTTP_SERVICE_BIND_ADDRESS, "localhost");

    StartMemberUtils.addGemFireSystemProperties(commandLine, gemfireProperties);

    assertFalse(commandLine.isEmpty());
    assertEquals(7, commandLine.size());

    for (final String propertyName : gemfireProperties.stringPropertyNames()) {
      final String propertyValue = gemfireProperties.getProperty(propertyName);
      if (StringUtils.isBlank(propertyValue)) {
        for (final String systemProperty : commandLine) {
          assertFalse(systemProperty.startsWith(
              "-D" + DistributionConfig.GEMFIRE_PREFIX + "".concat(propertyName).concat("=")));
        }
      } else {
        assertTrue(commandLine.contains("-D" + DistributionConfig.GEMFIRE_PREFIX
            + "".concat(propertyName).concat("=").concat(propertyValue)));
      }
    }
  }

  @Test
  public void testAddInitialHeapToCommandLine() {
    final List<String> commandLine = new ArrayList<>();
    assertTrue(commandLine.isEmpty());
    StartMemberUtils.addInitialHeap(commandLine, null);
    assertTrue(commandLine.isEmpty());
    StartMemberUtils.addInitialHeap(commandLine, org.apache.commons.lang.StringUtils.EMPTY);
    assertTrue(commandLine.isEmpty());
    StartMemberUtils.addInitialHeap(commandLine, " ");
    assertTrue(commandLine.isEmpty());
    StartMemberUtils.addInitialHeap(commandLine, "512M");
    assertFalse(commandLine.isEmpty());
    assertEquals("-Xms512M", commandLine.get(0));
  }

  @Test
  public void testAddJvmArgumentsAndOptionsToCommandLine() {
    final List<String> commandLine = new ArrayList<>();
    assertTrue(commandLine.isEmpty());
    StartMemberUtils.addJvmArgumentsAndOptions(commandLine, null);
    assertTrue(commandLine.isEmpty());
    StartMemberUtils.addJvmArgumentsAndOptions(commandLine, new String[] {});
    assertTrue(commandLine.isEmpty());
    StartMemberUtils.addJvmArgumentsAndOptions(commandLine,
        new String[] {"-DmyProp=myVal", "-d64", "-server", "-Xprof"});
    assertFalse(commandLine.isEmpty());
    assertEquals(4, commandLine.size());
    assertEquals("-DmyProp=myVal", commandLine.get(0));
    assertEquals("-d64", commandLine.get(1));
    assertEquals("-server", commandLine.get(2));
    assertEquals("-Xprof", commandLine.get(3));
  }

  @Test
  public void testAddMaxHeapToCommandLine() {
    final List<String> commandLine = new ArrayList<>();
    assertTrue(commandLine.isEmpty());
    StartMemberUtils.addMaxHeap(commandLine, null);
    assertTrue(commandLine.isEmpty());
    StartMemberUtils.addMaxHeap(commandLine, org.apache.commons.lang.StringUtils.EMPTY);
    assertTrue(commandLine.isEmpty());
    StartMemberUtils.addMaxHeap(commandLine, "  ");
    assertTrue(commandLine.isEmpty());
    StartMemberUtils.addMaxHeap(commandLine, "1024M");
    assertFalse(commandLine.isEmpty());
    assertEquals(3, commandLine.size());
    assertEquals("-Xmx1024M", commandLine.get(0));
    assertEquals("-XX:+UseConcMarkSweepGC", commandLine.get(1));
    assertEquals(
        "-XX:CMSInitiatingOccupancyFraction=" + StartMemberUtils.CMS_INITIAL_OCCUPANCY_FRACTION,
        commandLine.get(2));
  }

  @Test(expected = AssertionError.class)
  public void testReadPidWithNull() {
    try {
      StartMemberUtils.readPid(null);
    } catch (AssertionError expected) {
      assertEquals("The file from which to read the process ID (pid) cannot be null!",
          expected.getMessage());
      throw expected;
    }
  }

  @Test
  public void testReadPidWithNonExistingFile() {
    assertEquals(StartMemberUtils.INVALID_PID,
        StartMemberUtils.readPid(new File("/path/to/non_existing/pid.file")));
  }

  @Test
  public void testGetSystemClasspath() {
    assertEquals(System.getProperty("java.class.path"), StartMemberUtils.getSystemClasspath());
  }

  @Test
  public void testToClasspath() {
    final boolean EXCLUDE_SYSTEM_CLASSPATH = false;
    final boolean INCLUDE_SYSTEM_CLASSPATH = true;
    String[] jarFilePathnames =
        {"/path/to/user/libs/A.jar", "/path/to/user/libs/B.jar", "/path/to/user/libs/C.jar"};
    String[] userClasspaths = {"/path/to/classes:/path/to/libs/1.jar:/path/to/libs/2.jar",
        "/path/to/ext/libs/1.jar:/path/to/ext/classes:/path/to/ext/lib/10.jar"};
    String expectedClasspath = StartMemberUtils.GEODE_JAR_PATHNAME.concat(File.pathSeparator)
        .concat(toClasspath(userClasspaths)).concat(File.pathSeparator)
        .concat(toClasspath(jarFilePathnames));
    assertEquals(expectedClasspath,
        StartMemberUtils.toClasspath(EXCLUDE_SYSTEM_CLASSPATH, jarFilePathnames, userClasspaths));
    expectedClasspath = StartMemberUtils.GEODE_JAR_PATHNAME.concat(File.pathSeparator)
        .concat(toClasspath(userClasspaths)).concat(File.pathSeparator)
        .concat(System.getProperty("java.class.path")).concat(File.pathSeparator)
        .concat(toClasspath(jarFilePathnames));
    assertEquals(expectedClasspath,
        StartMemberUtils.toClasspath(INCLUDE_SYSTEM_CLASSPATH, jarFilePathnames, userClasspaths));
    expectedClasspath = StartMemberUtils.GEODE_JAR_PATHNAME.concat(File.pathSeparator)
        .concat(System.getProperty("java.class.path"));
    assertEquals(expectedClasspath,
        StartMemberUtils.toClasspath(INCLUDE_SYSTEM_CLASSPATH, null, (String[]) null));
    assertEquals(StartMemberUtils.GEODE_JAR_PATHNAME,
        StartMemberUtils.toClasspath(EXCLUDE_SYSTEM_CLASSPATH, null, (String[]) null));
    assertEquals(StartMemberUtils.GEODE_JAR_PATHNAME,
        StartMemberUtils.toClasspath(EXCLUDE_SYSTEM_CLASSPATH, new String[0], ""));
  }

  @Test
  public void testToClassPathOrder() {
    String userClasspathOne = "/path/to/user/lib/a.jar:/path/to/user/classes";
    String userClasspathTwo =
        "/path/to/user/lib/x.jar:/path/to/user/lib/y.jar:/path/to/user/lib/z.jar";

    String expectedClasspath = StartMemberUtils.getGemFireJarPath().concat(File.pathSeparator)
        .concat(userClasspathOne).concat(File.pathSeparator).concat(userClasspathTwo)
        .concat(File.pathSeparator).concat(System.getProperty("java.class.path"))
        .concat(File.pathSeparator).concat(StartMemberUtils.CORE_DEPENDENCIES_JAR_PATHNAME)
        .concat(File.pathSeparator).concat(StartMemberUtils.CORE_DEPENDENCIES_JAR_PATHNAME);

    String actualClasspath =
        StartMemberUtils.toClasspath(true,
            new String[] {StartMemberUtils.CORE_DEPENDENCIES_JAR_PATHNAME,
                StartMemberUtils.CORE_DEPENDENCIES_JAR_PATHNAME},
            userClasspathOne, userClasspathTwo);

    assertEquals(expectedClasspath, actualClasspath);
  }

  private String toClasspath(final String... jarFilePathnames) {
    String classpath = org.apache.commons.lang.StringUtils.EMPTY;
    if (jarFilePathnames != null) {
      for (final String jarFilePathname : jarFilePathnames) {
        classpath +=
            (classpath.isEmpty() ? org.apache.commons.lang.StringUtils.EMPTY : File.pathSeparator);
        classpath += jarFilePathname;
      }
    }
    return classpath;
  }
}

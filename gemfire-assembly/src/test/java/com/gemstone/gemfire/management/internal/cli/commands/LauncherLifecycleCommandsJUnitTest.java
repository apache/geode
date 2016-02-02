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
package com.gemstone.gemfire.management.internal.cli.commands;

import com.gemstone.gemfire.GemFireException;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.distributed.ServerLauncher;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.DistributionLocator;
import com.gemstone.gemfire.internal.lang.StringUtils;
import com.gemstone.gemfire.internal.lang.SystemUtils;
import com.gemstone.gemfire.internal.util.IOUtils;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.test.junit.categories.UnitTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.Stack;
import java.util.jar.Attributes;
import java.util.jar.Attributes.Name;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

import static org.junit.Assert.*;

/**
 * The LauncherLifecycleCommandsJUnitTest class is a test suite of test cases testing the contract and functionality of
 * the lifecycle launcher GemFire shell (Gfsh) commands.
 *
 * @author John Blum
 * @see com.gemstone.gemfire.management.internal.cli.commands.LauncherLifecycleCommands
 * @see org.junit.Assert
 * @see org.junit.Test
 * @since 7.0
 */
@SuppressWarnings("unused")
@Category(UnitTest.class)
public class LauncherLifecycleCommandsJUnitTest {

  private static final String GFSH_DEPENDENCIES_JAR_PATHNAME = IOUtils.appendToPath(System.getenv("GEMFIRE"), "lib",
      "gfsh-dependencies.jar");

  private LauncherLifecycleCommands launcherCommands;

  @Before
  public void setup() {
    launcherCommands = new LauncherLifecycleCommands();
  }

  @After
  public void tearDown() {
    launcherCommands = null;
  }

  protected LauncherLifecycleCommands getLauncherLifecycleCommands() {
    return launcherCommands;
  }

  protected void writePid(final File pidFile, final int pid) throws IOException {
    final FileWriter fileWriter = new FileWriter(pidFile, false);
    fileWriter.write(String.valueOf(pid));
    fileWriter.write("\n");
    fileWriter.flush();
    IOUtils.close(fileWriter);
  }

  @Test
  public void testAddGemFirePropertyFileToCommandLine() {
    final List<String> commandLine = new ArrayList<>();

    assertTrue(commandLine.isEmpty());

    getLauncherLifecycleCommands().addGemFirePropertyFile(commandLine, null);

    assertTrue(commandLine.isEmpty());

    getLauncherLifecycleCommands().addGemFirePropertyFile(commandLine, StringUtils.EMPTY_STRING);

    assertTrue(commandLine.isEmpty());

    getLauncherLifecycleCommands().addGemFirePropertyFile(commandLine, " ");

    assertTrue(commandLine.isEmpty());

    getLauncherLifecycleCommands().addGemFirePropertyFile(commandLine, "/path/to/gemfire.properties");

    assertFalse(commandLine.isEmpty());
    assertTrue(commandLine.contains("-DgemfirePropertyFile=/path/to/gemfire.properties"));
  }

  @Test
  public void testAddGemFireSystemPropertiesToCommandLine() {
    final List<String> commandLine = new ArrayList<>();

    assertTrue(commandLine.isEmpty());

    getLauncherLifecycleCommands().addGemFireSystemProperties(commandLine, new Properties());

    assertTrue(commandLine.isEmpty());

    final Properties gemfireProperties = new Properties();

    gemfireProperties.setProperty(DistributionConfig.LOCATORS_NAME, "localhost[11235]");
    gemfireProperties.setProperty(DistributionConfig.LOG_LEVEL_NAME, "config");
    gemfireProperties.setProperty(DistributionConfig.LOG_FILE_NAME, StringUtils.EMPTY_STRING);
    gemfireProperties.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    gemfireProperties.setProperty(DistributionConfig.NAME_NAME, "tidepool");

    getLauncherLifecycleCommands().addGemFireSystemProperties(commandLine, gemfireProperties);

    assertFalse(commandLine.isEmpty());
    assertEquals(4, commandLine.size());

    for (final String propertyName : gemfireProperties.stringPropertyNames()) {
      final String propertyValue = gemfireProperties.getProperty(propertyName);
      if (StringUtils.isBlank(propertyValue)) {
        for (final String systemProperty : commandLine) {
          assertFalse(systemProperty.startsWith("-Dgemfire.".concat(propertyName).concat("=")));
        }
      } else {
        assertTrue(commandLine.contains("-Dgemfire.".concat(propertyName).concat("=").concat(propertyValue)));
      }
    }
  }

  @Test
  public void testAddInitialHeapToCommandLine() {
    final List<String> commandLine = new ArrayList<>();

    assertTrue(commandLine.isEmpty());

    getLauncherLifecycleCommands().addInitialHeap(commandLine, null);

    assertTrue(commandLine.isEmpty());

    getLauncherLifecycleCommands().addInitialHeap(commandLine, StringUtils.EMPTY_STRING);

    assertTrue(commandLine.isEmpty());

    getLauncherLifecycleCommands().addInitialHeap(commandLine, " ");

    assertTrue(commandLine.isEmpty());

    getLauncherLifecycleCommands().addInitialHeap(commandLine, "512M");

    assertFalse(commandLine.isEmpty());
    assertEquals("-Xms512M", commandLine.get(0));
  }

  @Test
  public void testAddJvmArgumentsAndOptionsToCommandLine() {
    final List<String> commandLine = new ArrayList<>();

    assertTrue(commandLine.isEmpty());

    getLauncherLifecycleCommands().addJvmArgumentsAndOptions(commandLine, null);

    assertTrue(commandLine.isEmpty());

    getLauncherLifecycleCommands().addJvmArgumentsAndOptions(commandLine, new String[]{});

    assertTrue(commandLine.isEmpty());

    getLauncherLifecycleCommands().addJvmArgumentsAndOptions(commandLine,
        new String[]{"-DmyProp=myVal", "-d64", "-server", "-Xprof"});

    assertFalse(commandLine.isEmpty());
    assertEquals(4, commandLine.size());
    assertEquals("-DmyProp=myVal", commandLine.get(0));
    assertEquals("-d64", commandLine.get(1));
    assertEquals("-server", commandLine.get(2));
    assertEquals("-Xprof", commandLine.get(3));
  }

  // Fix for Bug #47192 - "Making GemFire (JVM) to exit in case of OutOfMemory"
  @Test
  public void testAddJvmOptionsForOutOfMemoryErrors() {
    final List<String> jvmOptions = new ArrayList<>(1);

    getLauncherLifecycleCommands().addJvmOptionsForOutOfMemoryErrors(jvmOptions);

    if (SystemUtils.isHotSpotVM()) {
      if (SystemUtils.isWindows()) {
        assertTrue(jvmOptions.contains("-XX:OnOutOfMemoryError=taskkill /F /PID %p"));
      } else {
        assertTrue(jvmOptions.contains("-XX:OnOutOfMemoryError=kill -KILL %p"));
      }
    } else if (SystemUtils.isJ9VM()) {
      assertEquals(1, jvmOptions.size());
      assertTrue(jvmOptions.contains("-Xcheck:memory"));
    } else if (SystemUtils.isJRockitVM()) {
      assertEquals(1, jvmOptions.size());
      assertTrue(jvmOptions.contains("-XXexitOnOutOfMemory"));
    } else {
      assertTrue(jvmOptions.isEmpty());
    }
  }

  @Test
  public void testAddMaxHeapToCommandLine() {
    final List<String> commandLine = new ArrayList<>();

    assertTrue(commandLine.isEmpty());

    getLauncherLifecycleCommands().addMaxHeap(commandLine, null);

    assertTrue(commandLine.isEmpty());

    getLauncherLifecycleCommands().addMaxHeap(commandLine, StringUtils.EMPTY_STRING);

    assertTrue(commandLine.isEmpty());

    getLauncherLifecycleCommands().addMaxHeap(commandLine, "  ");

    assertTrue(commandLine.isEmpty());

    getLauncherLifecycleCommands().addMaxHeap(commandLine, "1024M");

    assertFalse(commandLine.isEmpty());
    assertEquals(3, commandLine.size());
    assertEquals("-Xmx1024M", commandLine.get(0));
    assertEquals("-XX:+UseConcMarkSweepGC", commandLine.get(1));
    assertEquals("-XX:CMSInitiatingOccupancyFraction=" + LauncherLifecycleCommands.CMS_INITIAL_OCCUPANCY_FRACTION,
        commandLine.get(2));
  }

  @Test(expected = AssertionError.class)
  public void testReadPidWithNull() {
    try {
      getLauncherLifecycleCommands().readPid(null);
    } catch (AssertionError expected) {
      assertEquals("The file from which to read the process ID (pid) cannot be null!", expected.getMessage());
      throw expected;
    }
  }

  @Test
  public void testReadPidWithNonExistingFile() {
    assertEquals(LauncherLifecycleCommands.INVALID_PID,
        getLauncherLifecycleCommands().readPid(new File("/path/to/non_existing/pid.file")));
  }

  @Test
  public void testReadPid() throws IOException {
    final int expectedPid = 12345;

    File pidFile = new File(getClass().getSimpleName().concat("_testReadPid.pid"));

    assertTrue(pidFile.createNewFile());

    pidFile.deleteOnExit();
    writePid(pidFile, expectedPid);

    final int actualPid = getLauncherLifecycleCommands().readPid(pidFile);

    assertEquals(expectedPid, actualPid);
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testGetClasspath() {
    assertEquals(System.getProperty("java.class.path"), getLauncherLifecycleCommands().getClasspath(null));
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testGetClasspathWithUserDefinedClasspath() {
    assertEquals(System.getProperty("java.class.path") + File.pathSeparator + "/path/to/user/classes",
        getLauncherLifecycleCommands().getClasspath("/path/to/user/classes"));
  }

  @Test
  public void testGemFireCoreClasspath() throws IOException {
    File coreDependenciesJar = new File(LauncherLifecycleCommands.CORE_DEPENDENCIES_JAR_PATHNAME);

    assertNotNull(coreDependenciesJar);
    assertTrue(coreDependenciesJar + " is not a file", coreDependenciesJar.isFile());

    Collection<String> expectedJarDependencies = Arrays.asList("antlr", "commons-io", "commons-lang", "commons-logging",
        "gemfire", "jackson-annotations", "jackson-core", "jackson-databind", "jansi", "jline", "snappy-java",
        "spring-core", "spring-shell", "jetty-server", "jetty-servlet", "jetty-webapp", "jetty-util", "jetty-http",
        "servlet-api", "jetty-io", "jetty-security", "jetty-xml"

    );

    assertJarFileManifestClassPath(coreDependenciesJar, expectedJarDependencies);
  }

  @Test
  public void testGetSpringJars() {
    List<String> actualSpringJarPathnames = new LauncherLifecycleCommands().getSpringJars();

    assertNotNull(actualSpringJarPathnames);
    assertEquals(LauncherLifecycleCommands.SPRING_JAR_NAME_PREFIXES.size(), actualSpringJarPathnames.size());

    int springCoreVersion = -1;
    int springDataCommonsVersion = -1;
    int springDataGemFireVersion = -1;

    Set<String> expectedSpringJarNames = new HashSet<>(LauncherLifecycleCommands.SPRING_JAR_NAME_PREFIXES);

    assertFalse(expectedSpringJarNames.isEmpty());

    for (String springJarPathname : actualSpringJarPathnames) {
      String springJarName = springJarPathname.substring(springJarPathname.lastIndexOf(File.separator) + 1);
      String springJarNamePrefix = springJarName.substring(0, springJarName.lastIndexOf("-"));

      switch (springJarNamePrefix) {
        case LauncherLifecycleCommands.SPRING_BEANS_JAR_NAME_PREFIX:
          springCoreVersion = Integer.parseInt(StringUtils.getDigitsOnly(springJarName));
          break;
        case LauncherLifecycleCommands.SPRING_DATA_COMMONS_JAR_NAME_PREFIX:
          springDataCommonsVersion = Integer.parseInt(StringUtils.getDigitsOnly(springJarName));
          break;
        case LauncherLifecycleCommands.SPRING_DATA_GEMFIRE_JAR_NAME_PREFIX:
          springDataGemFireVersion = Integer.parseInt(StringUtils.getDigitsOnly(springJarName));
          break;
      }

      expectedSpringJarNames.remove(springJarNamePrefix);
    }

    assertTrue(String.format("Expected empty; but was (%1$s)", expectedSpringJarNames),
        expectedSpringJarNames.isEmpty());
    assertEquals(424, springCoreVersion);
    assertEquals(191, springDataCommonsVersion);
    assertEquals(172, springDataGemFireVersion);
  }

  @Test
  public void testGetSystemClasspath() {
    assertEquals(System.getProperty("java.class.path"), getLauncherLifecycleCommands().getSystemClasspath());
  }

  @Test
  public void testLocatorClasspathOrder() {
    String userClasspath = "/path/to/user/lib/app.jar:/path/to/user/classes";

    String expectedClasspath = launcherCommands.getGemFireJarPath().concat(File.pathSeparator).concat(
        userClasspath).concat(File.pathSeparator).concat(System.getProperty("java.class.path")).concat(
        File.pathSeparator).concat(LauncherLifecycleCommands.CORE_DEPENDENCIES_JAR_PATHNAME);

    String actualClasspath = launcherCommands.getLocatorClasspath(true, userClasspath);

    assertEquals(expectedClasspath, actualClasspath);
  }

  @Test
  public void testServerClasspathOrder() {
    String userClasspath = "/path/to/user/lib/app.jar:/path/to/user/classes";

    String expectedClasspath = launcherCommands.getGemFireJarPath().concat(File.pathSeparator).concat(
        userClasspath).concat(File.pathSeparator).concat(
        LauncherLifecycleCommands.CORE_DEPENDENCIES_JAR_PATHNAME).concat(File.pathSeparator).concat(
        toPath(launcherCommands.getSpringJars().toArray()));

    String actualClasspath = launcherCommands.getServerClasspath(false, true, userClasspath);

    assertEquals(expectedClasspath, actualClasspath);
  }

  private String toPath(Object... pathElements) {
    String path = "";

    for (Object pathElement : pathElements) {
      path += (path.isEmpty() ? StringUtils.EMPTY_STRING : File.pathSeparator);
      path += pathElement;
    }

    return path;
  }

  @Test
  public void testToClasspath() {
    final boolean EXCLUDE_SYSTEM_CLASSPATH = false;
    final boolean INCLUDE_SYSTEM_CLASSPATH = true;

    String[] jarFilePathnames = {"/path/to/user/libs/A.jar", "/path/to/user/libs/B.jar", "/path/to/user/libs/C.jar"};

    String[] userClasspaths = {"/path/to/classes:/path/to/libs/1.jar:/path/to/libs/2.jar", "/path/to/ext/libs/1.jar:/path/to/ext/classes:/path/to/ext/lib/10.jar"};

    String expectedClasspath = LauncherLifecycleCommands.GEMFIRE_JAR_PATHNAME.concat(File.pathSeparator).concat(
        toClasspath(userClasspaths)).concat(File.pathSeparator).concat(toClasspath(jarFilePathnames));

    assertEquals(expectedClasspath,
        getLauncherLifecycleCommands().toClasspath(EXCLUDE_SYSTEM_CLASSPATH, jarFilePathnames, userClasspaths));

    expectedClasspath = LauncherLifecycleCommands.GEMFIRE_JAR_PATHNAME.concat(File.pathSeparator).concat(
        toClasspath(userClasspaths)).concat(File.pathSeparator).concat(System.getProperty("java.class.path")).concat(
        File.pathSeparator).concat(toClasspath(jarFilePathnames));

    assertEquals(expectedClasspath,
        getLauncherLifecycleCommands().toClasspath(INCLUDE_SYSTEM_CLASSPATH, jarFilePathnames, userClasspaths));

    expectedClasspath = LauncherLifecycleCommands.GEMFIRE_JAR_PATHNAME.concat(File.pathSeparator).concat(
        System.getProperty("java.class.path"));

    assertEquals(expectedClasspath,
        getLauncherLifecycleCommands().toClasspath(INCLUDE_SYSTEM_CLASSPATH, null, (String[]) null));

    assertEquals(LauncherLifecycleCommands.GEMFIRE_JAR_PATHNAME,
        getLauncherLifecycleCommands().toClasspath(EXCLUDE_SYSTEM_CLASSPATH, null, (String[]) null));

    assertEquals(LauncherLifecycleCommands.GEMFIRE_JAR_PATHNAME,
        getLauncherLifecycleCommands().toClasspath(EXCLUDE_SYSTEM_CLASSPATH, new String[0], ""));
  }

  @Test
  public void testToClassPathOrder() {
    String userClasspathOne = "/path/to/user/lib/a.jar:/path/to/user/classes";
    String userClasspathTwo = "/path/to/user/lib/x.jar:/path/to/user/lib/y.jar:/path/to/user/lib/z.jar";

    String expectedClasspath = launcherCommands.getGemFireJarPath().concat(File.pathSeparator).concat(
        userClasspathOne).concat(File.pathSeparator).concat(userClasspathTwo).concat(File.pathSeparator).concat(
        System.getProperty("java.class.path")).concat(File.pathSeparator).concat(
        LauncherLifecycleCommands.CORE_DEPENDENCIES_JAR_PATHNAME).concat(File.pathSeparator).concat(
        LauncherLifecycleCommands.CORE_DEPENDENCIES_JAR_PATHNAME);

    String actualClasspath = launcherCommands.toClasspath(true,
        new String[]{LauncherLifecycleCommands.CORE_DEPENDENCIES_JAR_PATHNAME, LauncherLifecycleCommands.CORE_DEPENDENCIES_JAR_PATHNAME},
        userClasspathOne, userClasspathTwo);

    assertEquals(expectedClasspath, actualClasspath);
  }

  private void assertJarFileManifestClassPath(final File dependenciesJar,
      final Collection<String> expectedJarDependencies) throws IOException {
    JarFile dependenciesJarFile = new JarFile(dependenciesJar);
    Manifest manifest = dependenciesJarFile.getManifest();

    assertNotNull(manifest);

    Attributes attributes = manifest.getMainAttributes();

    assertNotNull(attributes);
    assertTrue(attributes.containsKey(Name.CLASS_PATH));

    String[] actualJarDependencies = attributes.getValue(Name.CLASS_PATH).split(" ");

    assertNotNull(actualJarDependencies);
    assertTrue(String.format("Expected the actual number of JAR dependencies to be (%1$d); but was (%2$d)!",
        expectedJarDependencies.size(), actualJarDependencies.length),
        actualJarDependencies.length >= expectedJarDependencies.size());
    //assertTrue(Arrays.asList(actualJarDependencies).containsAll(expectedJarDependencies));

    List<String> actualJarDependenciesList = new ArrayList<>(Arrays.asList(actualJarDependencies));
    List<String> missingExpectedJarDependenciesList = new ArrayList<>(expectedJarDependencies.size());

    for (String expectedJarDependency : expectedJarDependencies) {
      boolean containsExpectedJar = false;

      for (int index = 0, size = actualJarDependenciesList.size(); index < size; index++) {
        if (actualJarDependenciesList.get(index).toLowerCase().contains(expectedJarDependency.toLowerCase())) {
          actualJarDependenciesList.remove(index);
          containsExpectedJar = true;
          break;
        }
      }

      if (!containsExpectedJar) {
        missingExpectedJarDependenciesList.add(expectedJarDependency);
      }
    }

    assertTrue(String.format(
        "GemFire dependencies JAR file (%1$s) does not contain the expected dependencies (%2$s) in the Manifest Class-Path attribute (%3$s)!",
        dependenciesJar, missingExpectedJarDependenciesList, attributes.getValue(Name.CLASS_PATH)),
        missingExpectedJarDependenciesList.isEmpty());
  }

  private String toClasspath(final String... jarFilePathnames) {
    String classpath = StringUtils.EMPTY_STRING;

    if (jarFilePathnames != null) {
      for (final String jarFilePathname : jarFilePathnames) {
        classpath += (classpath.isEmpty() ? StringUtils.EMPTY_STRING : File.pathSeparator);
        classpath += jarFilePathname;
      }
    }

    return classpath;
  }

  @Test
  public void testGetJavaPathname() {
    assertEquals(IOUtils.appendToPath(System.getProperty("java.home"), "bin",
        "java" + LauncherLifecycleCommands.getExecutableSuffix()),
        getLauncherLifecycleCommands().getJdkToolPathname("java" + LauncherLifecycleCommands.getExecutableSuffix(),
            new GemFireException() {
            }));
  }

  @Test(expected = NullPointerException.class)
  public void testGetJdkToolPathnameWithNullPathnames() {
    try {
      getLauncherLifecycleCommands().getJdkToolPathname((Stack<String>) null, new GemFireException() {
      });
    } catch (NullPointerException expected) {
      assertEquals("The JDK tool executable pathnames cannot be null!", expected.getMessage());
      throw expected;
    }
  }

  @Test(expected = NullPointerException.class)
  public void testGetJdkToolPathnameWithNullGemFireException() {
    try {
      getLauncherLifecycleCommands().getJdkToolPathname(new Stack<String>(), null);
    } catch (NullPointerException expected) {
      assertEquals("The GemFireException cannot be null!", expected.getMessage());
      throw expected;
    }
  }

  @Test
  public void testGetJdkToolPathnameForNonExistingTool() {
    try {
      final GemFireException expected = new GemFireException() {
        @Override
        public String getMessage() {
          return "expected";
        }
      };

      getLauncherLifecycleCommands().getJdkToolPathname("nonExistingTool.exe", expected);
    } catch (GemFireException expected) {
      assertEquals("expected", expected.getMessage());
    }
  }

  @Test
  public void testGetLocatorId() {
    assertEquals("tidepool[11235]", getLauncherLifecycleCommands().getLocatorId("tidepool", 11235));
    assertEquals("tidepool.gemstone.com[11235]",
        getLauncherLifecycleCommands().getLocatorId("tidepool.gemstone.com", 11235));
    assertEquals("tidepool[" + DistributionLocator.DEFAULT_LOCATOR_PORT + "]",
        getLauncherLifecycleCommands().getLocatorId("tidepool", null));
  }

  @Test
  public void testGetServerId() {
    assertEquals("tidepool[12480]", getLauncherLifecycleCommands().getServerId("tidepool", 12480));
    assertEquals("tidepool.vmware.com[12480]",
        getLauncherLifecycleCommands().getServerId("tidepool.vmware.com", 12480));
    assertEquals("tidepool[" + CacheServer.DEFAULT_PORT + "]",
        getLauncherLifecycleCommands().getServerId("tidepool", null));
  }

  @Test
  public void testCreateJmxServerUrlWithMemberName() {
    assertEquals("service:jmx:rmi://localhost:8192/jndi/rmi://localhost:8192/jmxrmi",
        getLauncherLifecycleCommands().getJmxServiceUrlAsString("localhost[8192]"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCreateJmxServiceUrlWithInvalidMemberName() {
    try {
      System.err.println(getLauncherLifecycleCommands().getJmxServiceUrlAsString("memberOne[]"));
    } catch (IllegalArgumentException expected) {
      assertEquals(CliStrings.START_JCONSOLE__CONNECT_BY_MEMBER_NAME_ID_ERROR_MESSAGE, expected.getMessage());
      throw expected;
    }
  }

  @Test
  public void testCreateServerCommandLine() throws Exception {
    ServerLauncher serverLauncher = new ServerLauncher.Builder().setCommand(
        ServerLauncher.Command.START).setDisableDefaultServer(true).setMemberName(
        "testCreateServerCommandLine").setRebalance(true)
        //.setServerBindAddress("localhost")
        .setServerPort(41214).setCriticalHeapPercentage(95.5f).setEvictionHeapPercentage(85.0f).build();

    String[] commandLineElements = launcherCommands.createStartServerCommandLine(serverLauncher, null, null,
        new Properties(), null, false, new String[0], false, null, null);

    assertNotNull(commandLineElements);
    assertTrue(commandLineElements.length > 0);

    Set<String> expectedCommandLineElements = new HashSet<>(6);

    expectedCommandLineElements.add(serverLauncher.getCommand().getName());
    expectedCommandLineElements.add("--disable-default-server");
    expectedCommandLineElements.add(serverLauncher.getMemberName().toLowerCase());
    expectedCommandLineElements.add("--rebalance");
    //expectedCommandLineElements.add(String.format("--server-bind-address=%1$s", serverLauncher.getServerBindAddress().getHostName()));
    expectedCommandLineElements.add(String.format("--server-port=%1$d", serverLauncher.getServerPort()));
    expectedCommandLineElements.add(
        String.format("--critical-heap-percentage=%1$s", serverLauncher.getCriticalHeapPercentage()));
    expectedCommandLineElements.add(
        String.format("--eviction-heap-percentage=%1$s", serverLauncher.getEvictionHeapPercentage()));

    for (String commandLineElement : commandLineElements) {
      expectedCommandLineElements.remove(commandLineElement.toLowerCase());
    }

    assertTrue(String.format("Expected ([]); but was (%1$s)", expectedCommandLineElements),
        expectedCommandLineElements.isEmpty());
  }

}

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

package org.apache.geode.management.internal.configuration;

import static java.util.Arrays.stream;
import static java.util.stream.Collectors.joining;
import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_CONFIGURATION_DIR;
import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_CLUSTER_CONFIGURATION;
import static org.apache.geode.distributed.ConfigurationProperties.GROUPS;
import static org.apache.geode.distributed.ConfigurationProperties.LOAD_CLUSTER_CONFIGURATION_FROM_DIR;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_FILE_SIZE_LIMIT;
import static org.apache.geode.distributed.ConfigurationProperties.USE_CLUSTER_CONFIGURATION;
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.geode.cache.Cache;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.distributed.internal.SharedConfiguration;
import org.apache.geode.internal.ClassPathLoader;
import org.apache.geode.internal.JarClassLoader;
import org.apache.geode.internal.JarDeployer;
import org.apache.geode.internal.lang.StringUtils;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.configuration.domain.Configuration;
import org.apache.geode.management.internal.configuration.utils.ZipUtils;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.dunit.rules.GfshShellConnectionRule;
import org.apache.geode.test.dunit.rules.LocatorServerStartupRule;
import org.apache.geode.test.dunit.rules.Member;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

@Category(DistributedTest.class)
public class ClusterConfigDUnitTest extends JUnit4DistributedTestCase {

  private Properties locatorProps;
  private Properties serverProps;
  private GfshShellConnectionRule gfshConnector;
  @Rule
  public LocatorServerStartupRule lsRule = new LocatorServerStartupRule();

  @Before
  public void before() throws Exception {
    locatorProps = new Properties();
    locatorProps.setProperty(ENABLE_CLUSTER_CONFIGURATION, "true");

    serverProps = new Properties();
    serverProps.setProperty(USE_CLUSTER_CONFIGURATION, "true");
  }

  @After
  public void after() throws Exception {
    if (gfshConnector != null) {
      gfshConnector.close();
    }
  }

  @Test
  public void testStartLocator() throws Exception {
    Member firstLocator = startLocatorWithLoadCCFromDir();

    locatorProps.setProperty(LOCATORS, "localhost[" + firstLocator.getPort() + "]");
    Member secondLocator = lsRule.startLocatorVM(1, locatorProps);

    verifyClusterConfigZipLoadedInLocator(secondLocator);
  }

  @Test
  public void testStartServerWithSingleGroup() throws Exception {
    Member locator = startLocatorWithLoadCCFromDir();

    Member serverWithNoGroup = lsRule.startServerVM(1, serverProps, locator.getPort());
    verifyServerConfig(NO_GROUP, serverWithNoGroup);

    serverProps.setProperty(GROUPS, "group1");
    Member serverForGroup1 = lsRule.startServerVM(2, serverProps, locator.getPort());
    verifyServerConfig(GROUP1, serverForGroup1);

    serverProps.setProperty(GROUPS, "group2");
    Member serverForGroup2 = lsRule.startServerVM(3, serverProps, locator.getPort());
    verifyServerConfig(GROUP2, serverForGroup2);
  }

  @Test
  public void testStartServerWithMultipleGroup() throws Exception {
    Member locator = startLocatorWithLoadCCFromDir();

    serverProps.setProperty(GROUPS, "group1,group2");
    Member server = lsRule.startServerVM(1, serverProps, locator.getPort());

    verifyServerConfig(GROUP1_AND_2, server);
  }

  @Test
  public void testImportWithRunningServer() throws Exception {
    String zipFilePath = getClass().getResource(EXPORTED_CLUSTER_CONFIG_ZIP_FILENAME).getPath();
    // set up the locator/servers
    Member locator = lsRule.startLocatorVM(0, locatorProps);
    Member server1 = lsRule.startServerVM(1, serverProps, locator.getPort());
    gfshConnector =
        new GfshShellConnectionRule(locator.getPort(), GfshShellConnectionRule.PortType.locator);
    gfshConnector.connect();
    CommandResult result =
        gfshConnector.executeCommand("import cluster-configuration --zip-file-name=" + zipFilePath);

    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
  }

  @Test
  public void testImportClusterConfig() throws Exception {
    String zipFilePath = getClass().getResource(EXPORTED_CLUSTER_CONFIG_ZIP_FILENAME).getPath();
    // set up the locator/servers
    Member locator = lsRule.startLocatorVM(0, locatorProps);
    verifyInitialLocatorConfigInFileSystem(locator);

    gfshConnector =
        new GfshShellConnectionRule(locator.getPort(), GfshShellConnectionRule.PortType.locator);
    gfshConnector.connect();
    assertThat(gfshConnector.isConnected()).isTrue();

    CommandResult result =
        gfshConnector.executeCommand("import cluster-configuration --zip-file-name=" + zipFilePath);
    assertThat(result.getStatus()).isEqualTo(Result.Status.OK);

    // verify that the previous folder is copied to "cluster_configxxxxxx".
    String workingDirFiles = Arrays.stream(locator.getWorkingDir().listFiles()).map(File::getName)
        .collect(joining(", "));
    System.out.println("Locator working dir contains: " + workingDirFiles);
    assertThat(locator.getWorkingDir().listFiles())
        .filteredOn((File file) -> file.getName() != "cluster_config")
        .filteredOn((File file) -> file.getName().startsWith("cluster_config")).isNotEmpty();
    verifyClusterConfigZipLoadedInLocator(locator);

    // start server1 with no group
    Member server1 = lsRule.startServerVM(1, serverProps, locator.getPort());
    verifyServerConfig(NO_GROUP, server1);

    // start server2 in group1
    serverProps.setProperty(GROUPS, "group1");
    Member server2 = lsRule.startServerVM(2, serverProps, locator.getPort());
    verifyServerConfig(GROUP1, server2);

    // start server3 in group1 and group2
    serverProps.setProperty(GROUPS, "group1,group2");
    Member server3 = lsRule.startServerVM(3, serverProps, locator.getPort());
    verifyServerConfig(GROUP1_AND_2, server3);
  }

  @Test
  public void testDeployToNoServer() throws Exception {
    String clusterJarPath = getClass().getResource("cluster.jar").getPath();
    // set up the locator/servers
    Member locator = lsRule.startLocatorVM(0, locatorProps);

    gfshConnector =
        new GfshShellConnectionRule(locator.getPort(), GfshShellConnectionRule.PortType.locator);
    gfshConnector.connect();
    assertThat(gfshConnector.isConnected()).isTrue();

    CommandResult result = gfshConnector.executeCommand("deploy --jar=" + clusterJarPath);
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
  }

  @Test
  public void testDeploy() throws Exception {
    String clusterJar = getClass().getResource("cluster.jar").getPath();
    String group1Jar = getClass().getResource("group1.jar").getPath();
    String group2Jar = getClass().getResource("group2.jar").getPath();

    // set up the locator/servers
    Member locator = lsRule.startLocatorVM(0, locatorProps);
    // server1 in no group
    Member server1 = lsRule.startServerVM(1, serverProps, locator.getPort());
    // server2 in group1
    serverProps.setProperty(GROUPS, "group1");
    Member server2 = lsRule.startServerVM(2, serverProps, locator.getPort());
    // server3 in group1 and group2
    serverProps.setProperty(GROUPS, "group1,group2");
    Member server3 = lsRule.startServerVM(3, serverProps, locator.getPort());

    gfshConnector =
        new GfshShellConnectionRule(locator.getPort(), GfshShellConnectionRule.PortType.locator);
    gfshConnector.connect();
    assertThat(gfshConnector.isConnected()).isTrue();

    CommandResult result = gfshConnector.executeCommand("deploy --jar=" + clusterJar);
    assertThat(result.getStatus()).isEqualTo(Result.Status.OK);

    ExpectedConfig cluster = new ExpectedConfig().jars("cluster.jar").name("cluster");
    verifyLocatorConfig(cluster, locator);
    verifyLocatorConfigNotExist("group1", locator);
    verifyLocatorConfigNotExist("group2", locator);
    verifyServerConfig(cluster, server1);
    verifyServerConfig(cluster, server2);
    verifyServerConfig(cluster, server3);

    result = gfshConnector.executeCommand("deploy --jar=" + group1Jar + " --group=group1");
    assertThat(result.getStatus()).isEqualTo(Result.Status.OK);

    ExpectedConfig serverGroupOne = new ExpectedConfig().jars("group1.jar", "cluster.jar");
    ExpectedConfig locatorGroupOne = new ExpectedConfig().jars("group1.jar").name("group1");
    verifyLocatorConfig(cluster, locator);
    verifyLocatorConfig(locatorGroupOne, locator);
    verifyLocatorConfigNotExist("group2", locator);
    verifyServerConfig(cluster, server1);
    verifyServerConfig(serverGroupOne, server2);
    verifyServerConfig(serverGroupOne, server3);

    result = gfshConnector.executeCommand("deploy --jar=" + group2Jar + " --group=group2");
    assertThat(result.getStatus()).isEqualTo(Result.Status.OK);

    ExpectedConfig groupOneAndTwo =
        new ExpectedConfig().jars("group1.jar", "group2.jar", "cluster.jar");
    ExpectedConfig locatorGroupTwo = new ExpectedConfig().jars("group2.jar").name("group2");
    verifyLocatorConfig(cluster, locator);
    verifyLocatorConfig(locatorGroupOne, locator);
    verifyLocatorConfig(locatorGroupTwo, locator);
    verifyServerConfig(cluster, server1);
    verifyServerConfig(serverGroupOne, server2);
    verifyServerConfig(groupOneAndTwo, server3);
  }

  @Test
  public void testDeployMultiGroup() throws Exception {
    String clusterJar = getClass().getResource("cluster.jar").getPath();
    String group1Jar = getClass().getResource("group1.jar").getPath();

    // set up the locator/servers
    Member locator = lsRule.startLocatorVM(0, locatorProps);
    // start 2 servers in the both groups
    serverProps.setProperty(GROUPS, "group1");
    Member server1 = lsRule.startServerVM(1, serverProps, locator.getPort());
    serverProps.setProperty(GROUPS, "group2");
    Member server2 = lsRule.startServerVM(2, serverProps, locator.getPort());
    serverProps.setProperty(GROUPS, "group1,group2");
    Member server3 = lsRule.startServerVM(3, serverProps, locator.getPort());

    ExpectedConfig clusterConfig = new ExpectedConfig().name("cluster");
    ExpectedConfig group1Config = new ExpectedConfig().name("group1");
    ExpectedConfig group2Config = new ExpectedConfig().name("group2");

    gfshConnector =
        new GfshShellConnectionRule(locator.getPort(), GfshShellConnectionRule.PortType.locator);
    gfshConnector.connect();
    assertThat(gfshConnector.isConnected()).isTrue();

    CommandResult result = gfshConnector.executeCommand("deploy --jar=" + clusterJar);
    assertThat(result.getStatus()).isEqualTo(Result.Status.OK);

    // deploy cluster.jar to the cluster
    clusterConfig.addJar("cluster.jar");
    verifyLocatorConfig(clusterConfig, locator);
    verifyLocatorConfigNotExist("group1", locator);
    verifyLocatorConfigNotExist("group2", locator);
    verifyServerConfig(clusterConfig, server1);
    verifyServerConfig(clusterConfig, server2);
    verifyServerConfig(clusterConfig, server3);

    // deploy group1.jar to both group1 and group2
    result = gfshConnector.executeCommand("deploy --jar=" + group1Jar + " --group=group1,group2");
    assertThat(result.getStatus()).isEqualTo(Result.Status.OK);
    group1Config.addJar("group1.jar");
    group2Config.addJar("group1.jar");
    ExpectedConfig serverConfig = new ExpectedConfig().jars("cluster.jar", "group1.jar");
    verifyLocatorConfig(clusterConfig, locator);
    verifyLocatorConfig(group1Config, locator);
    verifyLocatorConfig(group2Config, locator);
    verifyServerConfig(serverConfig, server1);
    verifyServerConfig(serverConfig, server2);
    verifyServerConfig(serverConfig, server3);

    // test undeploy cluster
    result = gfshConnector.executeCommand("undeploy --jar=cluster.jar");
    assertThat(result.getStatus()).isEqualTo(Result.Status.OK);

    clusterConfig.removeJar("cluster.jar");
    verifyLocatorConfig(clusterConfig, locator);
    verifyLocatorConfig(group1Config, locator);
    verifyLocatorConfig(group2Config, locator);
    serverConfig.removeJar("cluster.jar");
    verifyServerConfig(serverConfig, server1);
    verifyServerConfig(serverConfig, server2);
    verifyServerConfig(serverConfig, server2);

    result = gfshConnector.executeCommand("undeploy --jar=group1.jar --group=group1");
    assertThat(result.getStatus()).isEqualTo(Result.Status.OK);

    group1Config.removeJar("group1.jar");
    verifyLocatorConfig(clusterConfig, locator);
    verifyLocatorConfig(group1Config, locator);
    verifyLocatorConfig(group2Config, locator);
    // server2 is not in group1, so serverConfig remains unchanged
    verifyServerConfig(serverConfig, server2);

    // server1 and server3 is in group1, so their group1.jar is removed
    serverConfig.removeJar("group1.jar");
    verifyServerConfig(serverConfig, server1);
    verifyServerConfig(serverConfig, server3);
  }


  private Member startLocatorWithLoadCCFromDir() throws Exception {
    File locatorDir = lsRule.getRootFolder().newFolder("locator-0");
    File configDir = new File(locatorDir, "cluster_config");

    // The unzip should yield a cluster config directory structure like:
    // tempFolder/locator-0/cluster_config/cluster/cluster.xml
    // tempFolder/locator-0/cluster_config/cluster/cluster.properties
    // tempFolder/locator-0/cluster_config/cluster/cluster.jar
    // tempFolder/locator-0/cluster_config/group1/ {group1.xml, group1.properties, group1.jar}
    // tempFolder/locator-0/cluster_config/group2/ ...
    ZipUtils.unzip(getClass().getResource(EXPORTED_CLUSTER_CONFIG_ZIP_FILENAME).getPath(),
        configDir.getCanonicalPath());

    Properties properties = new Properties();
    properties.setProperty(ENABLE_CLUSTER_CONFIGURATION, "true");
    properties.setProperty(LOAD_CLUSTER_CONFIGURATION_FROM_DIR, "true");
    properties.setProperty(CLUSTER_CONFIGURATION_DIR, locatorDir.getCanonicalPath());

    Member locator = lsRule.startLocatorVM(0, properties);
    verifyClusterConfigZipLoadedInLocator(locator);

    return locator;
  }

  private static String getServerJarName(String jarName) {
    return JarDeployer.JAR_PREFIX + jarName + "#1";
  }


  public static final String EXPORTED_CLUSTER_CONFIG_ZIP_FILENAME = "cluster_config.zip";
  public static final String[] CONFIG_NAMES = new String[] {"cluster", "group1", "group2"};

  public static final ExpectedConfig NO_GROUP =
      new ExpectedConfig().maxLogFileSize("5000").regions("regionForCluster").jars("cluster.jar");

  public static final ExpectedConfig GROUP1 = new ExpectedConfig().maxLogFileSize("6000")
      .regions("regionForCluster", "regionForGroup1").jars("cluster.jar", "group1.jar");

  public static final ExpectedConfig GROUP2 = new ExpectedConfig().maxLogFileSize("7000")
      .regions("regionForCluster", "regionForGroup2").jars("cluster.jar", "group2.jar");

  public static final ExpectedConfig GROUP1_AND_2 = new ExpectedConfig().maxLogFileSize("7000")
      .regions("regionForCluster", "regionForGroup1", "regionForGroup2")
      .jars("cluster.jar", "group1.jar", "group2.jar");


  public static void verifyInitialLocatorConfigInFileSystem(Member member) {
    File clusterConfigDir = new File(member.getWorkingDir(), "cluster_config");
    assertThat(clusterConfigDir).exists();
    File configDir = new File(clusterConfigDir, "cluster");
    assertThat(configDir).exists();
    File properties = new File(configDir, "cluster.properties");
    assertThat(properties).exists();
    File xml = new File(configDir, "cluster.xml");
    assertThat(xml).exists();
  }

  public static void verifyClusterConfigZipLoadedInLocator(Member locator) {
    // verify loaded in memeory
    locator.invoke(() -> {
      InternalLocator internalLocator = LocatorServerStartupRule.locatorStarter.locator;
      SharedConfiguration sc = internalLocator.getSharedConfiguration();

      for (String configName : CONFIG_NAMES) {
        Configuration config = sc.getConfiguration(configName);
        assertThat(config).isNotNull();
      }
    });

    // verify loaded into the file system
    File clusterConfigDir = new File(locator.getWorkingDir(), "cluster_config");
    assertThat(clusterConfigDir).exists();

    for (String configName : CONFIG_NAMES) {
      File configDir = new File(clusterConfigDir, configName);
      assertThat(configDir).exists();

      File jar = new File(configDir, configName + ".jar");
      File properties = new File(configDir, configName + ".properties");
      File xml = new File(configDir, configName + ".xml");
      assertThat(configDir.listFiles()).contains(jar, properties, xml);
    }
  }

  public static void verifyServerConfig(ExpectedConfig expectedConfig, Member server)
      throws ClassNotFoundException {
    verifyServerJarFilesExistInFileSystem(server.getWorkingDir(), expectedConfig.jars);
    server.invoke(() -> verifyServerConfigInMemory(expectedConfig));
  }

  public static void verifyLocatorConfig(ExpectedConfig expectedConfig, Member locator) {
    // verify info exists in memeory
    locator.invoke(() -> {
      InternalLocator internalLocator = LocatorServerStartupRule.locatorStarter.locator;
      SharedConfiguration sc = internalLocator.getSharedConfiguration();
      Configuration config = sc.getConfiguration(expectedConfig.name);
      assertThat(config.getJarNames()).isEqualTo(expectedConfig.jars);
    });

    // verify files exists on disc
    for (String jar : expectedConfig.jars) {
      assertThat(
          new File(locator.getWorkingDir(), "/cluster_config/" + expectedConfig.name + "/" + jar))
              .exists();
    }
  }

  public static void verifyLocatorConfigNotExist(String configName, Member locator) {
    // verify info not in memeory
    locator.invoke(() -> {
      InternalLocator internalLocator = LocatorServerStartupRule.locatorStarter.locator;
      SharedConfiguration sc = internalLocator.getSharedConfiguration();
      Configuration config = sc.getConfiguration(configName);
      assertThat(config).isNull();
    });

    // verify files does not
    assertThat(new File(locator.getWorkingDir(), "/cluster_config/" + configName)).doesNotExist();
  }

  private static void verifyServerConfigInMemory(ExpectedConfig expectedConfig)
      throws ClassNotFoundException {
    Cache cache = LocatorServerStartupRule.serverStarter.cache;

    for (String region : expectedConfig.regions) {
      assertThat(cache.getRegion(region)).isNotNull();
    }

    if (!StringUtils.isBlank(expectedConfig.maxLogFileSize)) {
      Properties props = cache.getDistributedSystem().getProperties();
      assertThat(props.getProperty(LOG_FILE_SIZE_LIMIT)).isEqualTo(expectedConfig.maxLogFileSize);
    }

    for (String jar : expectedConfig.jars) {
      JarClassLoader jarClassLoader = findJarClassLoader(jar);
      assertThat(jarClassLoader).isNotNull();
      assertThat(jarClassLoader.loadClass(nameOfClassContainedInJar(jar))).isNotNull();
    }
  }

  private static void verifyServerJarFilesExistInFileSystem(File workingDir, Set<String> jarNames) {
    Set<String> expectedJarNames = new HashSet<>();
    for (String jarName : jarNames) {
      expectedJarNames.add(getServerJarName(jarName));
    }
    Set<String> actualJarNames =
        Arrays.stream(workingDir.list((dir, filename) -> filename.contains(".jar")))
            .collect(Collectors.toSet());
    assertThat(actualJarNames).isEqualTo(expectedJarNames);
  }

  private static String nameOfClassContainedInJar(String jarName) {
    switch (jarName) {
      case "cluster.jar":
        return "Cluster";
      case "group1.jar":
        return "Group1";
      case "group2.jar":
        return "Group2";
      default:
        throw new IllegalArgumentException(
            EXPORTED_CLUSTER_CONFIG_ZIP_FILENAME + " does not contain a jar named " + jarName);
    }
  }

  private static JarClassLoader findJarClassLoader(final String jarName) {
    Collection<ClassLoader> classLoaders = ClassPathLoader.getLatest().getClassLoaders();
    for (ClassLoader classLoader : classLoaders) {
      if (classLoader instanceof JarClassLoader
          && ((JarClassLoader) classLoader).getJarName().equals(jarName)) {
        return (JarClassLoader) classLoader;
      }
    }
    return null;
  }

  private static class ExpectedConfig implements Serializable {
    public String maxLogFileSize;
    public Set<String> regions = new HashSet<>();
    public Set<String> jars = new HashSet<>();
    public String name;

    public ExpectedConfig maxLogFileSize(String maxLogFileSize) {
      this.maxLogFileSize = maxLogFileSize;
      return this;
    }

    public ExpectedConfig regions(String... regions) {
      this.regions.addAll(Arrays.asList(regions));
      return this;
    }

    public ExpectedConfig jars(String... jars) {
      this.jars.addAll(Arrays.asList(jars));
      return this;
    }

    public ExpectedConfig removeJar(String jar) {
      this.jars.remove(jar);
      return this;
    }

    public ExpectedConfig addJar(String jar) {
      this.jars.add(jar);
      return this;
    }

    public ExpectedConfig name(String name) {
      this.name = name;
      return this;
    }
  }


}

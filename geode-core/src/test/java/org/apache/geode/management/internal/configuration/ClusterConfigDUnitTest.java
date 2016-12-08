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
import org.apache.geode.management.internal.configuration.domain.Configuration;
import org.apache.geode.management.internal.configuration.utils.ZipUtils;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.dunit.rules.LocatorServerStartupRule;
import org.apache.geode.test.dunit.rules.Member;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Properties;

@Category(DistributedTest.class)
public class ClusterConfigDUnitTest extends JUnit4DistributedTestCase {
  private static final String EXPORTED_CLUSTER_CONFIG_ZIP_FILENAME = "cluster_config.zip";
  private static final String[] CONFIG_NAMES = new String[] {"cluster", "group1", "group2"};

  private static final ExpectedConfig NO_GROUP =
      new ExpectedConfig().maxLogFileSize("5000").regions("regionForCluster").jars("cluster.jar");

  private static final ExpectedConfig GROUP1 = new ExpectedConfig().maxLogFileSize("6000")
      .regions("regionForCluster", "regionForGroup1").jars("cluster.jar", "group1.jar");

  private static final ExpectedConfig GROUP2 = new ExpectedConfig().maxLogFileSize("7000")
      .regions("regionForCluster", "regionForGroup2").jars("cluster.jar", "group2.jar");

  private static final ExpectedConfig GROUP1_AND_2 = new ExpectedConfig().maxLogFileSize("7000")
      .regions("regionForCluster", "regionForGroup1", "regionForGroup2")
      .jars("cluster.jar", "group1.jar", "group2.jar");


  private String locatorString;

  @Rule
  public LocatorServerStartupRule lsRule = new LocatorServerStartupRule();

  @Before
  public void setupFirstLocatorWithClusterConfigFromDirectory() throws Exception {
    File locatorDir = lsRule.getRootFolder().newFolder("locator-0");

    // The unzip should yield a cluster config directory structure like:
    // tempFolder/locator-0/cluster_config/cluster/cluster.xml
    // tempFolder/locator-0/cluster_config/cluster/cluster.properties
    // tempFolder/locator-0/cluster_config/cluster/cluster.jar
    // tempFolder/locator-0/cluster_config/group1/ {group1.xml, group1.properties, group1.jar}
    // tempFolder/locator-0/cluster_config/group2/ ...
    ZipUtils.unzip(getClass().getResource(EXPORTED_CLUSTER_CONFIG_ZIP_FILENAME).getPath(),
        locatorDir.getCanonicalPath());

    Properties locatorProps = new Properties();
    locatorProps.setProperty(ENABLE_CLUSTER_CONFIGURATION, "true");
    locatorProps.setProperty(LOAD_CLUSTER_CONFIGURATION_FROM_DIR, "true");
    locatorProps.setProperty(CLUSTER_CONFIGURATION_DIR, locatorDir.getCanonicalPath());

    Member firstLocator = lsRule.startLocatorVM(0, locatorProps);
    locatorString = "localhost[" + firstLocator.getPort() + "]";

    verifyLocatorConfigExistsInFileSystem(firstLocator.getWorkingDir());
    firstLocator.invoke(this::verifyLocatorConfigExistsInInternalRegion);
  }

  @Test
  public void secondLocatorLoadsClusterConfigFromFirstLocator() throws IOException {
    Properties secondLocatorProps = new Properties();
    secondLocatorProps.setProperty(LOCATORS, locatorString);
    secondLocatorProps.setProperty(ENABLE_CLUSTER_CONFIGURATION, "true");
    Member secondLocator = lsRule.startLocatorVM(1, secondLocatorProps);

    verifyLocatorConfig(secondLocator);
  }

  @Test
  public void serverWithZeroOrOneGroupsLoadCorrectConfigFromLocator() throws Exception {
    Properties serverProps = new Properties();
    serverProps.setProperty(LOCATORS, locatorString);
    serverProps.setProperty(USE_CLUSTER_CONFIGURATION, "true");

    Member serverWithNoGroup = lsRule.startServerVM(1, serverProps);
    verifyServerConfig(NO_GROUP, serverWithNoGroup);

    serverProps.setProperty(GROUPS, "group1");
    Member serverForGroup1 = lsRule.startServerVM(2, serverProps);
    verifyServerConfig(GROUP1, serverForGroup1);

    serverProps.setProperty(GROUPS, "group2");
    Member serverForGroup2 = lsRule.startServerVM(3, serverProps);
    verifyServerConfig(GROUP2, serverForGroup2);
  }

  @Test
  public void oneServerWithMultipleGroupsLoadsCorrectConfigFromLocator() throws Exception {
    Properties serverProps = new Properties();
    serverProps.setProperty(LOCATORS, locatorString);
    serverProps.setProperty(USE_CLUSTER_CONFIGURATION, "true");
    serverProps.setProperty(GROUPS, "group1,group2");
    Member serverWithNoGroup = lsRule.startServerVM(1, serverProps);

    serverWithNoGroup.invoke(() -> this.verifyServerConfig(GROUP1_AND_2, serverWithNoGroup));
  }

  private void verifyLocatorConfig(Member locator) {
    verifyLocatorConfigExistsInFileSystem(locator.getWorkingDir());
    locator.invoke(this::verifyLocatorConfigExistsInInternalRegion);
  }

  private void verifyServerConfig(ExpectedConfig expectedConfig, Member server)
      throws ClassNotFoundException {
    verifyServerJarFilesExistInFileSystem(server.getWorkingDir(), expectedConfig.jars);
    server.invoke(() -> this.verifyServerConfigInMemory(expectedConfig));
  }

  private void verifyLocatorConfigExistsInFileSystem(File workingDir) {
    File clusterConfigDir = new File(workingDir, "cluster_config");
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

  private void verifyLocatorConfigExistsInInternalRegion() throws Exception {
    InternalLocator internalLocator = LocatorServerStartupRule.locatorStarter.locator;
    SharedConfiguration sc = internalLocator.getSharedConfiguration();

    for (String configName : CONFIG_NAMES) {
      Configuration config = sc.getConfiguration(configName);
      assertThat(config).isNotNull();
    }
  }

  private void verifyServerConfigInMemory(ExpectedConfig expectedConfig)
      throws ClassNotFoundException {
    Cache cache = LocatorServerStartupRule.serverStarter.cache;
    for (String region : expectedConfig.regions) {
      assertThat(cache.getRegion(region)).isNotNull();
    }
    Properties props = cache.getDistributedSystem().getProperties();
    assertThat(props.getProperty(LOG_FILE_SIZE_LIMIT)).isEqualTo(expectedConfig.maxLogFileSize);

    for (String jar : expectedConfig.jars) {
      JarClassLoader jarClassLoader = findJarClassLoader(jar);
      assertThat(jarClassLoader).isNotNull();
      assertThat(jarClassLoader.loadClass(nameOfClassContainedInJar(jar))).isNotNull();
    }
  }

  private void verifyServerJarFilesExistInFileSystem(File workingDir, String[] jarNames) {
    assertThat(workingDir.listFiles()).isNotEmpty();

    for (String jarName : jarNames) {
      assertThat(workingDir.listFiles()).filteredOn((File file) -> file.getName().contains(jarName))
          .isNotEmpty();
    }
  }

  private String nameOfClassContainedInJar(String jarName) {
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

  private JarClassLoader findJarClassLoader(final String jarName) {
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
    public String[] regions;
    public String[] jars;

    public ExpectedConfig maxLogFileSize(String maxLogFileSize) {
      this.maxLogFileSize = maxLogFileSize;
      return this;
    }

    public ExpectedConfig regions(String... regions) {
      this.regions = regions;
      return this;
    }

    public ExpectedConfig jars(String... jars) {
      this.jars = jars;
      return this;
    }
  }
}

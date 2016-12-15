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
 *
 */

package org.apache.geode.management.internal.configuration;


import static org.apache.geode.distributed.ConfigurationProperties.LOG_FILE_SIZE_LIMIT;
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.geode.cache.Cache;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.distributed.internal.SharedConfiguration;
import org.apache.geode.internal.ClassPathLoader;
import org.apache.geode.internal.JarClassLoader;
import org.apache.geode.internal.JarDeployer;
import org.apache.geode.internal.lang.StringUtils;
import org.apache.geode.management.internal.configuration.domain.Configuration;
import org.apache.geode.test.dunit.rules.Locator;
import org.apache.geode.test.dunit.rules.LocatorServerStartupRule;
import org.apache.geode.test.dunit.rules.Member;
import org.apache.geode.test.dunit.rules.Server;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

public class ClusterConfig implements Serializable {
  private List<ConfigGroup> groups;

  public ClusterConfig(ConfigGroup... configGroups) {
    this.groups = new ArrayList<>();

    Collections.addAll(this.groups, configGroups);
  }

  public String getMaxLogFileSize() {
    if (this.groups.size() == 0) {
      return null;
    }
    ConfigGroup lastGroupAdded = this.groups.get(this.groups.size() - 1);
    return lastGroupAdded.getMaxLogFileSize();
  }

  public List<String> getJarNames() {
    return groups.stream().flatMap((ConfigGroup configGroup) -> configGroup.getJars().stream())
        .collect(Collectors.toList());
  }

  public List<String> getRegions() {
    return groups.stream().flatMap((ConfigGroup configGroup) -> configGroup.getRegions().stream())
        .collect(Collectors.toList());
  }

  public List<ConfigGroup> getGroups() {
    return Collections.unmodifiableList(groups);
  }

  public void verify(Locator locator) {
    verifyLocator(locator);
  }

  public void verify(Server server) throws ClassNotFoundException {
    verifyServer(server);
  }

  public void verifyLocator(Member locator) {
    Set<String> expectedGroupConfigs =
        this.getGroups().stream().map(ConfigGroup::getName).collect(Collectors.toSet());

    // verify info exists in memeory
    locator.invoke(() -> {
      InternalLocator internalLocator = LocatorServerStartupRule.locatorStarter.locator;
      SharedConfiguration sc = internalLocator.getSharedConfiguration();

      // verify no extra configs exist in memory
      Set<String> actualGroupConfigs = sc.getEntireConfiguration().keySet();
      assertThat(actualGroupConfigs).isEqualTo(expectedGroupConfigs);

      // verify jars are as expected
      for (ConfigGroup configGroup : this.getGroups()) {
        Configuration config = sc.getConfiguration(configGroup.name);
        assertThat(config.getJarNames()).isEqualTo(configGroup.getJars());
      }

      // TODO: assert that groupConfig.getXml() contains expected region names
    });

    File clusterConfigDir = new File(locator.getWorkingDir(), "/cluster_config");
    Set<String> actualGroupDirs = toSetIgnoringHiddenFiles(clusterConfigDir.list());
    assertThat(actualGroupDirs).isEqualTo(expectedGroupConfigs);

    for (ConfigGroup configGroup : this.getGroups()) {
      Set<String> actualFiles =
          toSetIgnoringHiddenFiles(new File(clusterConfigDir, configGroup.name).list());

      Set<String> expectedFiles = configGroup.getAllFiles();
      assertThat(actualFiles).isEqualTo(expectedFiles);
    }
  }

  public void verifyServer(Member server) throws ClassNotFoundException {
    // verify files exist in filesystem
    Set<String> expectedJarNames = this.getJarNames().stream().map(ClusterConfig::getServerJarName)
        .collect(Collectors.toSet());
    Set<String> actualJarNames = toSetIgnoringHiddenFiles(
        server.getWorkingDir().list((dir, filename) -> filename.contains(".jar")));
    assertThat(actualJarNames).isEqualTo(expectedJarNames);

    // verify config exists in memory
    server.invoke(() -> {
      Cache cache = LocatorServerStartupRule.serverStarter.cache;

      // TODO: set compare to fail if there are extra regions
      for (String region : this.getRegions()) {
        assertThat(cache.getRegion(region)).isNotNull();
      }

      if (!StringUtils.isBlank(this.getMaxLogFileSize())) {
        Properties props = cache.getDistributedSystem().getProperties();
        assertThat(props.getProperty(LOG_FILE_SIZE_LIMIT)).isEqualTo(this.getMaxLogFileSize());
      }

      for (String jar : this.getJarNames()) {
        JarClassLoader jarClassLoader = findJarClassLoader(jar);
        assertThat(jarClassLoader).isNotNull();
        assertThat(jarClassLoader.loadClass(nameOfClassContainedInJar(jar))).isNotNull();
      }
    });
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



  private static Set<String> toSetIgnoringHiddenFiles(String[] array) {
    return Arrays.stream(array).filter((String name) -> !name.startsWith("."))
        .collect(Collectors.toSet());
  }

  private static String getServerJarName(String jarName) {
    return JarDeployer.JAR_PREFIX + jarName + "#1";
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
            "We don't know what class to expect in the jar named " + jarName);
    }
  }
}

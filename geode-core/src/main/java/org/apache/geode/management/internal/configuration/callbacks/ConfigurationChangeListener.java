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
package org.apache.geode.management.internal.configuration.callbacks;

import org.apache.commons.io.FileUtils;
import org.apache.geode.distributed.internal.ClusterConfigurationService;
import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.management.internal.configuration.domain.Configuration;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/****
 * CacheListener on ConfigRegion on Locators to write the configuration changes to file-system.
 *
 */
public class ConfigurationChangeListener extends CacheListenerAdapter<String, Configuration> {
  private static final Logger logger = LogService.getLogger();

  private final ClusterConfigurationService sharedConfig;

  public ConfigurationChangeListener(ClusterConfigurationService sharedConfig) {
    this.sharedConfig = sharedConfig;
  }

  @Override
  public void afterUpdate(EntryEvent<String, Configuration> event) {
    super.afterUpdate(event);
    addOrRemoveJarFromFilesystem(event);
  }

  @Override
  public void afterCreate(EntryEvent<String, Configuration> event) {
    super.afterCreate(event);
    addOrRemoveJarFromFilesystem(event);
  }

  // when a new jar is added, if it does not exist in the current locator, download it from
  // another locator.
  // when a jar is removed, if it exists in the current locator, remove it.
  private void addOrRemoveJarFromFilesystem(EntryEvent<String, Configuration> event) {
    String group = event.getKey();
    Configuration newConfig = (Configuration) event.getNewValue();
    Configuration oldConfig = (Configuration) event.getOldValue();
    Set<String> newJars = newConfig.getJarNames();
    Set<String> oldJars = (oldConfig == null) ? new HashSet<>() : oldConfig.getJarNames();
    Set<String> jarsAdded = new HashSet<>(newJars);
    Set<String> jarsRemoved = new HashSet<>(oldJars);

    jarsAdded.removeAll(oldJars);
    jarsRemoved.removeAll(newJars);

    if (!jarsAdded.isEmpty() && !jarsRemoved.isEmpty()) {
      throw new IllegalStateException(
          "We don't expect to have jars both added and removed in one event");
    }

    for (String jarAdded : jarsAdded) {
      if (!jarExistsInFilesystem(group, jarAdded)) {
        try {
          sharedConfig.downloadJarFromOtherLocators(group, jarAdded);
        } catch (Exception e) {
          logger.error("Unable to add jar: " + jarAdded, e);
        }
      }
    }

    for (String jarRemoved : jarsRemoved) {
      File jar = sharedConfig.getPathToJarOnThisLocator(group, jarRemoved).toFile();
      if (jar.exists()) {
        try {
          FileUtils.forceDelete(jar);
        } catch (IOException e) {
          logger.error(
              "Exception occurred while attempting to delete a jar from the filesystem: {}",
              jarRemoved, e);
        }
      }
    }
  }

  private boolean jarExistsInFilesystem(String groupName, String jarName) {
    return sharedConfig.getPathToJarOnThisLocator(groupName, jarName).toFile().exists();
  }

}

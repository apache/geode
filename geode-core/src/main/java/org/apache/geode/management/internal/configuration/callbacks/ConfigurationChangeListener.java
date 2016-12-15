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

import com.google.common.collect.Sets;

import org.apache.commons.io.FileUtils;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.distributed.internal.SharedConfiguration;
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

  private final SharedConfiguration sharedConfig;

  private static final String clusterConfigDirPath = GemFireCacheImpl.getInstance().getDistributedSystem().getConfig().getClusterConfigDir();
  private static final File clusterConfigDir = new File(clusterConfigDirPath);

  public ConfigurationChangeListener(SharedConfiguration sharedConfig) {
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

  // when a new jar is added, if it exist in the current locator, upload to other locators,
  // otherwise, download from other locators.
  // when a jar is removed, remove it from all the locators' file system
  private void addOrRemoveJarFromFilesystem(EntryEvent<String, Configuration> event) {
    String group = event.getKey();
    Configuration newConfig = (Configuration) event.getNewValue();
    Configuration oldConfig = (Configuration) event.getOldValue();
    Set<String> newJars = newConfig.getJarNames();
    Set<String> oldJars = (oldConfig == null) ? new HashSet<>() : oldConfig.getJarNames();

    Set<String> jarsAdded = Sets.difference(newJars, oldJars);
    Set<String> jarsRemoved = Sets.difference(oldJars, newJars);

    if (!jarsAdded.isEmpty() && !jarsRemoved.isEmpty()) {
      throw new IllegalStateException("We don't expect to have jars both added and removed in one event");
    }

    for(String jarAdded: jarsAdded){
      if (!jarExistsInFilesystem(group, jarAdded)){
        try {
          sharedConfig.addJarFromOtherLocators(group, jarAdded);
        } catch (Exception e) {
          logger.error("Unable to add jar: " + jarAdded, e);
        }
      }
    }

    for(String jarRemoved: jarsRemoved){
        File jar = sharedConfig.getPathToJarOnThisLocator(group,jarRemoved).toFile();
        if (jar.exists()) {
          try {
            FileUtils.forceDelete(jar);
          } catch (IOException e) {
            logger.error(
                "Exception occurred while attempting to delete a jar from the filesystem: {}",jarRemoved, e);
          }
        }
    }

    //locator1,2,3 already have an existing jar 1
    //deploy jar --name=jar1  (new version of jar)
    //locator1 overwrites jar1 on its filesystem
    //listener is called on locator 1,2,3
  }

  private boolean jarExistsInFilesystem(String groupName, String jarName) {
    return sharedConfig.getPathToJarOnThisLocator(groupName, jarName).toFile().exists();
  }

}

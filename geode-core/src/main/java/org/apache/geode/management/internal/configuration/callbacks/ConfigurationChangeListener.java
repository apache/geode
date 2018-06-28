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

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.management.internal.configuration.domain.Configuration;

/****
 * CacheListener on ConfigRegion on Locators to write the configuration changes to file-system.
 *
 */
public class ConfigurationChangeListener extends CacheListenerAdapter<String, Configuration> {
  private static final Logger logger = LogService.getLogger();

  private final InternalConfigurationPersistenceService sharedConfig;
  private final InternalCache cache;

  public ConfigurationChangeListener(InternalConfigurationPersistenceService sharedConfig,
      InternalCache cache) {
    this.sharedConfig = sharedConfig;
    this.cache = cache;
  }

  // Don't process the event locally. The action of adding or removing a jar should already have
  // been performed by DeployCommand or UndeployCommand.
  @Override
  public void afterUpdate(EntryEvent<String, Configuration> event) {
    super.afterUpdate(event);
    if (event.isOriginRemote()) {
      addOrRemoveJarFromFilesystem(event);
    }
  }

  @Override
  public void afterCreate(EntryEvent<String, Configuration> event) {
    super.afterCreate(event);
    if (event.isOriginRemote()) {
      addOrRemoveJarFromFilesystem(event);
    }
  }

  // Here we first remove any jars which are not used anymore and then we re-add all of the
  // necessary jars again. This may appear a bit blunt but it also accounts for the situation
  // where a jar is only being updated - i.e. the name does not change, only the content.
  private void addOrRemoveJarFromFilesystem(EntryEvent<String, Configuration> event) {
    String group = event.getKey();
    Configuration newConfig = event.getNewValue();
    Configuration oldConfig = event.getOldValue();
    Set<String> newJars = newConfig.getJarNames();
    Set<String> oldJars = (oldConfig == null) ? new HashSet<>() : oldConfig.getJarNames();

    Set<String> jarsRemoved = new HashSet<>(oldJars);
    jarsRemoved.removeAll(newJars);

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

    String triggerMemberId = (String) event.getCallbackArgument();
    if (triggerMemberId == null || newJars.isEmpty()) {
      return;
    }

    DistributedMember locator = getDistributedMember(triggerMemberId);
    for (String jarAdded : newJars) {
      try {
        sharedConfig.downloadJarFromLocator(group, jarAdded, locator);
      } catch (Exception e) {
        logger.error("Unable to add jar: " + jarAdded, e);
      }
    }
  }

  private DistributedMember getDistributedMember(String memberName) {
    Set<DistributedMember> locators = new HashSet<>(
        cache.getDistributionManager().getAllHostedLocatorsWithSharedConfiguration().keySet());

    return locators.stream()
        .filter(x -> x.getId().equals(memberName))
        .findFirst()
        .orElse(null);
  }
}

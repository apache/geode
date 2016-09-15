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
package org.apache.geode.management.internal.configuration.callbacks;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.distributed.internal.SharedConfiguration;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.management.internal.configuration.domain.Configuration;
/****
 * CacheListener on ConfigRegion to write the configuration changes to file-system.
 *
 */
public class ConfigurationChangeListener extends CacheListenerAdapter<String, Configuration> {
  private static final Logger logger = LogService.getLogger();
  
  private final SharedConfiguration sharedConfig;
  
  public ConfigurationChangeListener(SharedConfiguration sharedConfig) {
    this.sharedConfig = sharedConfig;
  }
  @Override
  public void afterUpdate(EntryEvent<String, Configuration> event) {
    super.afterUpdate(event);
    writeToFileSystem(event);
  }
  
  @Override
  public void afterCreate(EntryEvent<String, Configuration> event) {
    super.afterCreate(event);
    writeToFileSystem(event);
  }
  
  private void writeToFileSystem(EntryEvent<String, Configuration> event) {
    Configuration newConfig = (Configuration)event.getNewValue();
    try {
      sharedConfig.writeConfig(newConfig);
    } catch (Exception e) {
      logger.info("Exception occurred while writing the configuration changes to the filesystem: {}", e.getMessage(), e);
    }
  }
}

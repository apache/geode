/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management.internal.configuration.callbacks;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;
import com.gemstone.gemfire.distributed.internal.SharedConfiguration;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.management.internal.configuration.domain.Configuration;
/****
 * CacheListener on ConfigRegion to write the configuration changes to file-system.
 * @author bansods
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

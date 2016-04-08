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
package com.gemstone.gemfire.admin.internal;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.admin.AdminDistributedSystem;
import com.gemstone.gemfire.admin.ManagedEntity;
import com.gemstone.gemfire.internal.ClassPathLoader;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LogMarker;

/**
 * Creates ManagedEntityController for administration (starting, stopping, etc.) 
 * of GemFire {@link ManagedEntity}s.
 * 
 */
public class ManagedEntityControllerFactory {

  private static final Logger logger = LogService.getLogger();
  
  private static final String ENABLED_MANAGED_ENTITY_CONTROLLER_CLASS_NAME = "com.gemstone.gemfire.admin.internal.EnabledManagedEntityController";
  
  static ManagedEntityController createManagedEntityController(final AdminDistributedSystem system) {
    if (isEnabledManagedEntityController()) {
      logger.info(LogMarker.CONFIG, "Local and remote OS command invocations are enabled for the Admin API.");
      return createEnabledManagedEntityController(system);
    } else {
      logger.info(LogMarker.CONFIG, "Local and remote OS command invocations are disabled for the Admin API.");
      return new DisabledManagedEntityController();
    }
  }

  public static boolean isEnabledManagedEntityController() {
    try {
      ClassPathLoader.getLatest().forName(ENABLED_MANAGED_ENTITY_CONTROLLER_CLASS_NAME);
      return true;
    } catch (ClassNotFoundException e) {
      return false;
    }
  }
  
  private static ManagedEntityController createEnabledManagedEntityController(final AdminDistributedSystem system) {
    return new EnabledManagedEntityController(system);
  }
}

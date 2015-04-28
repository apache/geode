/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
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
 * @author Kirk Lund
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

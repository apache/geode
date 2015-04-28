/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.admin.internal;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.admin.DistributedSystemConfig;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LogMarker;

/**
 * This is a disabled implementation of ManagedEntityController for bug #47909.
 *
 * The old ManagedEntityController was a concrete class which has been renamed
 * to ManagedEntityControllerImpl. The build.xml now skips building
 * ManagedEntityControllerImpl. If ManagedEntityControllerImpl is not found
 * in the classpath then the code uses DisabledManagedEntityController as a
 * place holder.
 *
 * @author Kirk Lund
 */
class DisabledManagedEntityController implements ManagedEntityController {

  private static final Logger logger = LogService.getLogger();

  private static final String EXCEPTION_MESSAGE = "Local and remote OS command invocations are disabled for the Admin API.";
  
  DisabledManagedEntityController() {
  }
  
  @Override
  public void start(InternalManagedEntity entity) {
    if (logger.isTraceEnabled(LogMarker.MANAGED_ENTITY)){
      logger.warn(LogMarker.MANAGED_ENTITY, "DisabledManagedEntityController#start {}", EXCEPTION_MESSAGE);
    }
    throw new UnsupportedOperationException(EXCEPTION_MESSAGE);
  }

  @Override
  public void stop(InternalManagedEntity entity) {
    if (logger.isTraceEnabled(LogMarker.MANAGED_ENTITY)){
      logger.warn(LogMarker.MANAGED_ENTITY, "DisabledManagedEntityController#stop {}", EXCEPTION_MESSAGE);
    }
    throw new UnsupportedOperationException(EXCEPTION_MESSAGE);
  }

  @Override
  public boolean isRunning(InternalManagedEntity entity) {
    if (logger.isTraceEnabled(LogMarker.MANAGED_ENTITY)){
      logger.warn(LogMarker.MANAGED_ENTITY, "DisabledManagedEntityController#isRunning {}", EXCEPTION_MESSAGE);
    }
    throw new UnsupportedOperationException(EXCEPTION_MESSAGE);
  }

  @Override
  public String getLog(DistributionLocatorImpl locator) {
    if (logger.isTraceEnabled(LogMarker.MANAGED_ENTITY)){
      logger.warn(LogMarker.MANAGED_ENTITY, "DisabledManagedEntityController#getLog {}", EXCEPTION_MESSAGE);
    }
    throw new UnsupportedOperationException(EXCEPTION_MESSAGE);
  }

  @Override
  public String buildSSLArguments(DistributedSystemConfig config) {
    if (logger.isTraceEnabled(LogMarker.MANAGED_ENTITY)){
      logger.warn(LogMarker.MANAGED_ENTITY, "DisabledManagedEntityController#buildSSLArguments {}", EXCEPTION_MESSAGE);
    }
    throw new UnsupportedOperationException(EXCEPTION_MESSAGE);
  }

  @Override
  public String getProductExecutable(InternalManagedEntity entity, String executable) {
    if (logger.isTraceEnabled(LogMarker.MANAGED_ENTITY)){
      logger.warn(LogMarker.MANAGED_ENTITY, "DisabledManagedEntityController#getProductExecutable {}", EXCEPTION_MESSAGE);
    }
    throw new UnsupportedOperationException(EXCEPTION_MESSAGE);
  }
}

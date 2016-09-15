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
package org.apache.geode.admin.internal;

import org.apache.logging.log4j.Logger;

import org.apache.geode.admin.DistributedSystemConfig;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LogMarker;

/**
 * This is a disabled implementation of ManagedEntityController for bug #47909.
 *
 * The old ManagedEntityController was a concrete class which has been renamed
 * to ManagedEntityControllerImpl. The build.xml now skips building
 * ManagedEntityControllerImpl. If ManagedEntityControllerImpl is not found
 * in the classpath then the code uses DisabledManagedEntityController as a
 * place holder.
 *
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

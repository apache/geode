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

package org.apache.geode.services.bootstrapping;


import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.services.management.impl.ComponentIdentifier;
import org.apache.geode.services.module.ModuleService;
import org.apache.geode.services.result.ServiceResult;
import org.apache.geode.services.result.impl.Failure;
import org.apache.geode.services.result.impl.Success;

/**
 * Service responsible for bootstrapping the environment and Geode components.
 *
 * @since Geode 1.14.0
 *
 * @see ModuleService
 * @see ServiceResult
 * @see ComponentIdentifier
 */
@Experimental
public interface BootstrappingService {

  /**
   * Start and initialize Geode.
   *
   * @param moduleService the {@link ModuleService} to use to help bootstrap the environment.
   * @param logger the {@link Logger} to use to log things
   */
  void init(ModuleService moduleService, Logger logger);

  /**
   * Shuts down the environment and previously bootstrapped Geode components.
   *
   */
  void shutdown();

  /**
   * Returns the registered {@link ModuleService}.
   *
   * @return the registered {@link ModuleService}.
   */
  ModuleService getModuleService();

  /**
   * Ensures the environment is properly setup to create and initialize the Geode component
   * represented by the given {@link ComponentIdentifier}.
   *
   * @param componentIdentifier Identifier representing the Geode component to be created.
   * @return {@link Success} when the environment can be successfully setup (all modules required by
   *         the specified component are loaded) and {@link Failure} on failure.
   */
  ServiceResult<Boolean> bootStrapModule(ComponentIdentifier componentIdentifier);
}

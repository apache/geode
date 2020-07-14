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
package org.apache.geode.services.management;

import java.util.Optional;

import org.apache.logging.log4j.Logger;

import org.apache.geode.services.management.impl.ComponentIdentifier;
import org.apache.geode.services.module.ModuleService;
import org.apache.geode.services.result.ServiceResult;
import org.apache.geode.services.result.impl.Failure;
import org.apache.geode.services.result.impl.Success;

/**
 * Ze interface used by the {@link ManagementService} to create or destroy a Geode component. The
 * implementers of this interface will know how to create the component using the provided arguments
 * and register itself correctly with the system.
 *
 * @since Geode 1.14.0
 */
public interface ComponentManagementService<T> {

  /**
   * Determines if the {@link ComponentManagementService} can create the Geode Component for the
   * {@link ComponentIdentifier}.
   *
   * @param componentIdentifier {@link ComponentIdentifier} used to determine if the Component can
   *        be
   *        created by this {@link ComponentManagementService}.
   * @return {@literal true} if {@link ComponentManagementService} can create the Geode Component
   *         represented by the {@link ComponentIdentifier} and {@literal false} otherwise.
   */
  boolean canCreateComponent(ComponentIdentifier componentIdentifier);

  /**
   * Creates a Geode Component using the {@link ModuleService} and arguments provided.
   *
   * @param moduleService the {@link ModuleService} to use to load resources and services.
   * @param logger the logger to use to log
   * @param args arguments to be passed to the Geode Component being created.
   * @return {@link Success} when the component is created and {@link Failure} on failure.
   */
  ServiceResult<Boolean> init(ModuleService moduleService, Logger logger,
      Object[] args);

  /**
   * This returns the initialized component of type T.
   *
   * @return {@link Optional} of type T, which represents the initialized component. Returns
   *         {@link Optional#empty()} if the component is not initialized.
   */
  Optional<T> getInitializedComponent();

  /**
   * Destroys the Geode Component previously created by this {@link ComponentManagementService}.
   *
   * @param args arguments to be passed to the Geode Component being closed
   * @return {@link Success} when the component is destroyed and {@link Failure} on
   *         failure.
   */
  ServiceResult<Boolean> close(Object[] args);
}

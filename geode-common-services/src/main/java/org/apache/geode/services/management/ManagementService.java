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

import org.apache.geode.annotations.Experimental;
import org.apache.geode.services.management.impl.ComponentIdentifier;
import org.apache.geode.services.result.ServiceResult;
import org.apache.geode.services.result.impl.Failure;
import org.apache.geode.services.result.impl.Success;

/**
 * Entry point for managing Geode Components/modules/features.
 *
 * @since Geode 1.14.0
 */
@Experimental
public interface ManagementService {
  /**
   * Creates a Geode Component that is described by the {@link ComponentIdentifier} using the
   * provided args.
   *
   * @param componentIdentifier a {@link ComponentIdentifier} representing the Geode Component to be
   *        created.
   * @param args arguments to be used in creating the specified Component.
   * @return {@link Success} upon creating the Component and {@link Failure} upon failure.
   */
  ServiceResult<Boolean> createComponent(ComponentIdentifier componentIdentifier, Object... args);

  /**
   * Destroys a previously created Geode Component that is described by the
   * {@link ComponentIdentifier}.
   *
   * @param componentIdentifier a {@link ComponentIdentifier} representing the Geode Component to be
   *        destroyed.
   * @return {@link Success} upon destroying the Component and {@link Failure} upon
   *         failure.
   */
  ServiceResult<Boolean> closeComponent(ComponentIdentifier componentIdentifier, Object... args);
}

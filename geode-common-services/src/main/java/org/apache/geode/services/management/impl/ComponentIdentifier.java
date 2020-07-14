/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.services.management.impl;

import java.util.Optional;

import org.apache.commons.lang3.StringUtils;

import org.apache.geode.services.bootstrapping.BootstrappingService;
import org.apache.geode.services.management.ManagementService;

/**
 * A generic identifier of a Geode module/component/feature used by the {@link ManagementService}
 * and {@link BootstrappingService} containing the necessary information to correctly setup, create,
 * and initialize.
 *
 * @since Geode 1.14.0
 *
 * @see ManagementService
 * @see BootstrappingService
 */
public class ComponentIdentifier {
  private final String componentName;
  private final String path;

  public ComponentIdentifier(String componentName) {
    this(componentName, null);
  }

  public ComponentIdentifier(String componentName, String path) {
    if (StringUtils.isBlank(componentName)) {
      throw new IllegalArgumentException("ComponentName cannot be null or empty");
    }
    this.componentName = componentName;
    this.path = path;
  }

  /**
   * gets the name of the component.
   *
   * @return the component name.
   */
  public String getComponentName() {
    return componentName;
  }

  /**
   * Gets the path to the module associated with this component.
   *
   * @return an {@link Optional} containing the path to the associated module, if it exists.
   */
  public Optional<String> getPath() {
    return Optional.ofNullable(path);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ComponentIdentifier that = (ComponentIdentifier) o;
    return componentName.equals(that.componentName);
  }

  @Override
  public int hashCode() {
    return componentName.hashCode();
  }
}

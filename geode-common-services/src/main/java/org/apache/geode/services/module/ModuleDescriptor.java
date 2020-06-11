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

package org.apache.geode.services.module;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import org.apache.geode.annotations.Experimental;

/**
 * Holds configuration information to describe a classloader-isolated module.
 *
 * @see Builder
 * @see ModuleService
 * @since Geode 1.14.0
 */
@Experimental
public class ModuleDescriptor {

  // The name of the module
  private final String name;

  // The version module. Maybe be null
  private final String version;

  // A collection of source paths for the module
  private final Set<String> resourceJarPaths;

  // A collection of module names on which this module depends on
  private final Set<String> dependencies;

  private ModuleDescriptor(String name, String version, Set<String> resourceJarPaths,
      Set<String> dependencies) {
    this.name = name;
    this.version = version;
    this.resourceJarPaths = resourceJarPaths;
    this.dependencies = dependencies;
  }

  /**
   * A collection of resource paths that are to be loaded by the module
   *
   * @return a collection of resource paths for the module
   */
  public Set<String> getResourceJarPaths() {
    return resourceJarPaths;
  }

  /**
   * A collection of module names on which this module is dependent on
   *
   * @return A collection of module names on which this module is dependent on
   */
  public Set<String> getDependedOnModules() {
    return dependencies;
  }

  /**
   * The name of the module concatenated with the version provided. In the case of the version being
   * null
   * the name does not contain the version.
   *
   */
  public String getName() {
    return name + (version != null ? "-" + version : "");
  }

  /**
   * A Builder used to construct a {@link ModuleDescriptor}
   */
  public static class Builder {

    private final String name;
    private final String version;
    private final Set<String> dependencies = new HashSet<>();
    private final Set<String> sources = new HashSet<>();

    public Builder(String name) {
      this(name, null);
    }

    public Builder(String name, String version) {
      if (!StringUtils.isEmpty(name)) {
        this.name = name;
      } else {
        throw new IllegalArgumentException(
            "Name in the ModuleDescriptor.Builder cannot be null or empty");
      }
      this.version = version;
    }

    public Builder fromResourcePaths(String... resourcePaths) {
      return fromResourcePaths(Arrays.asList(resourcePaths));
    }

    public Builder fromResourcePaths(Collection<String> resourcePaths) {
      this.sources.addAll(resourcePaths);
      return this;
    }

    public Builder dependsOnModules(String... dependencies) {
      return this.dependsOnModules(Arrays.asList(dependencies));
    }

    public Builder dependsOnModules(Collection<String> dependencies) {
      this.dependencies.addAll(dependencies);
      return this;
    }

    public ModuleDescriptor build() {
      return new ModuleDescriptor(name, version, sources, dependencies);
    }
  }
}

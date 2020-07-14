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

  private final boolean requiresJDKPaths;

  private ModuleDescriptor(String name, String version, Set<String> resourceJarPaths,
      Set<String> dependencies, boolean requiresJDKPaths) {
    this.name = name;
    this.version = version;
    this.resourceJarPaths = resourceJarPaths;
    this.dependencies = dependencies;
    this.requiresJDKPaths = requiresJDKPaths;
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
   * Returns {@literal true} if this {@link ModuleDescriptor} is dependent on JDK path and
   * {@literal false} otherwise.
   *
   * @return {@literal true} if this {@link ModuleDescriptor} is dependent on JDK path and
   *         {@literal false} otherwise.
   */
  public boolean requiresJDKPaths() {
    return requiresJDKPaths;
  }

  /**
   * A Builder used to construct a {@link ModuleDescriptor}
   */
  public static class Builder {

    private final String name;
    private final String version;
    private final Set<String> dependedOnModuleNames = new HashSet<>();
    private final Set<String> resourcePaths = new HashSet<>();
    private boolean requiresJDKPaths = false;

    public Builder(String name) {
      this(name, null);
    }

    public Builder(String name, String version) {
      if (!StringUtils.isBlank(name)) {
        this.name = name;
      } else {
        throw new IllegalArgumentException(
            "Name in the ModuleDescriptor.Builder cannot be null or empty");
      }
      this.version = version;
    }

    /**
     * Adds the provided resource path to this {@link Builder}
     *
     * @param resourcePaths paths to jar files to use as the modules resources.
     * @return this {@link Builder}
     */
    public Builder fromResourcePaths(String... resourcePaths) {
      return fromResourcePaths(Arrays.asList(resourcePaths));
    }

    /**
     * Adds the provided resource path to this {@link Builder}
     *
     * @param resourcePaths paths to jar files to use as the modules resources.
     * @return this {@link Builder}
     */
    public Builder fromResourcePaths(Collection<String> resourcePaths) {
      this.resourcePaths.addAll(resourcePaths);
      return this;
    }

    /**
     * Adds dependent modules to this {@link Builder}
     *
     * @param dependencies names of modules for the {@link ModuleDescriptor} built by this
     *        {@link Builder} to be dependent on.
     * @return this {@link Builder}
     */
    public Builder dependsOnModules(String... dependencies) {
      return dependsOnModules(Arrays.asList(dependencies));
    }

    /**
     * Adds dependent modules to this {@link Builder}
     *
     * @param dependencies names of modules for the {@link ModuleDescriptor} built by this
     *        {@link Builder} to be dependent on.
     * @return this {@link Builder}
     */
    public Builder dependsOnModules(Collection<String> dependencies) {
      this.dependedOnModuleNames.addAll(dependencies);
      return this;
    }

    /**
     * Enables the module to depend on JDK paths.
     *
     * @param requiresJDKPaths a {@literal boolean} indicating whether or not this module should
     *        depend on the JDK paths.
     * @return this {@link Builder}
     */
    public Builder requiresJDKPaths(boolean requiresJDKPaths) {
      this.requiresJDKPaths = requiresJDKPaths;
      return this;
    }

    /**
     * Builds a {@link ModuleDescriptor} using the information previously set on this
     * {@link Builder}.
     *
     * @return the newly created {@link ModuleDescriptor}.
     */
    public ModuleDescriptor build() {
      return new ModuleDescriptor(name, version, resourcePaths, dependedOnModuleNames,
          requiresJDKPaths);
    }

    /**
     * Gets the name of the {@link ModuleDescriptor} created by this {@link Builder}.
     *
     * @return the name of the {@link ModuleDescriptor} created by this {@link Builder}.
     */
    public String getName() {
      return name;
    }
  }
}

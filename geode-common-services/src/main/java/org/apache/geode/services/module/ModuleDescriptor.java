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
import java.util.Collections;
import java.util.List;

import org.apache.geode.annotations.Experimental;

/**
 * Holds information to describe a classloader-isolated module including how to create it.
 *
 * @see Builder
 * @see ModuleService
 *
 * @since Geode 1.13.0
 */
@Experimental
public class ModuleDescriptor {

  private String name;

  private String version;

  private List<String> sources;

  private List<String> dependencies;

  private ModuleDescriptor(String name, String version, List<String> sources,
      List<String> dependencies) {
    this.name = name;
    this.version = version;
    this.sources = sources;
    this.dependencies = dependencies;
  }

  public String getName() {
    return name;
  }

  public String getVersion() {
    return version;
  }

  public List<String> getSources() {
    return sources;
  }

  public List<String> getDependedOnModules() {
    return dependencies;
  }

  public String getVersionedName() {
    return name + ":" + version;
  }

  /**
   * A Builder used to construct a {@link ModuleDescriptor}
   */
  public static class Builder {

    private final String name;
    private final String version;
    private List<String> dependencies = Collections.emptyList();
    private List<String> sources = Collections.emptyList();

    public Builder(String name, String version) {
      this.name = name;
      this.version = version;
    }

    public Builder fromSources(String... sources) {
      this.sources = Arrays.asList(sources);
      return this;
    }

    public Builder dependsOnModules(String... dependencies) {
      this.dependencies = Arrays.asList(dependencies);
      return this;
    }

    public ModuleDescriptor build() {
      return new ModuleDescriptor(name, version, sources, dependencies);
    }
  }
}

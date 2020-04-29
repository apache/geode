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

package org.apache.geode.module;

import org.apache.geode.annotations.Experimental;

/**
 * Holds information to describe a classloader-isolated module including how to create it.
 *
 * @since Geode 1.13.0
 */
@Experimental
public class ModuleDescriptor {

  private String name;

  private String version;

  private String path;

  public ModuleDescriptor(String name, String version, String path) {
    this.name = name;
    this.version = version;
    this.path = path;
  }

  public String getName() {
    return name;
  }

  public String getVersion() {
    return version;
  }

  public String getResourcePath() {
    return path;
  }
}

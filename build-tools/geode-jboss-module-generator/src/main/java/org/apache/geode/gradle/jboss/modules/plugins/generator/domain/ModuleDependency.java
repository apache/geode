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
package org.apache.geode.gradle.jboss.modules.plugins.generator.domain;

import java.util.Objects;

public class ModuleDependency {
  private final String name;
  private final boolean export;
  private final boolean optional;

  public ModuleDependency(String name, boolean export, boolean optional) {
    this.name = name;
    this.export = export;
    this.optional = optional;
  }

  public String getName() {
    return name;
  }

  public boolean isExport() {
    return export;
  }

  public boolean isOptional() {
    return optional;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ModuleDependency that = (ModuleDependency) o;
    return export == that.export && optional == that.optional && name.equals(that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, export, optional);
  }

  @Override
  public String toString() {
    return "ModuleDependency{" +
            "name='" + name + '\'' +
            ", export=" + export +
            ", optional=" + optional +
            '}';
  }
}

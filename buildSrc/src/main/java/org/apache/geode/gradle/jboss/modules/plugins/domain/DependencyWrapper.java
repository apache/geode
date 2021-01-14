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
package org.apache.geode.gradle.jboss.modules.plugins.domain;

import org.gradle.api.artifacts.Dependency;
import org.gradle.api.artifacts.SelfResolvingDependency;

public class DependencyWrapper {
  private final boolean embed;
  private final Dependency dependency;
  private final boolean projectDependency;

  public DependencyWrapper(Dependency dependency, boolean embed, boolean isProjectDependency) {
    this.embed = embed;
    this.projectDependency = isProjectDependency;
    this.dependency = dependency;
  }

  public boolean isEmbedded() {
    return embed;
  }

  public boolean isProjectDependency() {
    return projectDependency;
  }

  public Dependency getDependency() {
    return dependency;
  }

  public boolean isSelfResolving() {
    return dependency instanceof SelfResolvingDependency;
  }

  @Override
  public String toString() {
    return "DependencyWrapper{" +
        "embed=" + embed +
        ", dependency=" + dependency +
        ", projectDependency=" + projectDependency +
        '}';
  }
}

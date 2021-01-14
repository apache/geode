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
package org.apache.geode.deployment.internal.modules.dependency;

import org.jboss.modules.filter.PathFilter;

public class InboundModuleDependency {
  private String inboundDependencyModuleName;
  private PathFilter exportFilter;

  public InboundModuleDependency(String inboundDependencyModuleName,
      PathFilter exportFilter) {
    this.inboundDependencyModuleName = inboundDependencyModuleName;
    this.exportFilter = exportFilter;
  }

  public String getInboundDependencyModuleName() {
    return inboundDependencyModuleName;
  }

  public void setInboundDependencyModuleName(String inboundDependencyModuleName) {
    this.inboundDependencyModuleName = inboundDependencyModuleName;
  }

  public PathFilter getExportFilter() {
    return exportFilter;
  }

  public void setExportFilter(PathFilter exportFilter) {
    this.exportFilter = exportFilter;
  }
}

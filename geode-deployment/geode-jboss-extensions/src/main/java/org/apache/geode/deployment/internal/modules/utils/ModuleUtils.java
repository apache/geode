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
package org.apache.geode.deployment.internal.modules.utils;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.jboss.modules.filter.MultiplePathFilterBuilder;
import org.jboss.modules.filter.PathFilter;
import org.jboss.modules.filter.PathFilters;

public class ModuleUtils {
  private static final String GEODE_BASE_PACKAGE_PATH = "org/apache/geode";

  public static PathFilter createPathFilter(Set<String> paths) {
    List<String> restrictPaths =
        paths.stream().filter(packageName -> packageName.split("/").length <= 2)
            .collect(Collectors.toList());
    List<String> restrictPathsAndChildren =
        paths.stream().filter(packageName -> packageName.split("/").length == 3)
            .collect(Collectors.toList());

    restrictPathsAndChildren.add(GEODE_BASE_PACKAGE_PATH);

    return createPathFilter(restrictPaths, restrictPathsAndChildren);
  }

  public static PathFilter createPathFilter(List<String> restrictPaths,
      List<String> restrictPathsAndChildren) {
    MultiplePathFilterBuilder pathFilterBuilder = PathFilters.multiplePathFilterBuilder(true);

    if (restrictPaths != null) {
      for (String path : restrictPaths) {
        pathFilterBuilder.addFilter(PathFilters.is(path), false);
      }
    }

    if (restrictPathsAndChildren != null) {
      for (String path : restrictPathsAndChildren) {
        pathFilterBuilder.addFilter(PathFilters.isOrIsChildOf(path), false);
      }
    }

    return pathFilterBuilder.create();
  }
}

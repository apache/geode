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
package org.apache.geode.management.internal.cli.commands;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.geode.management.internal.cli.AbstractCliAroundInterceptor;
import org.apache.geode.management.internal.cli.GfshParseResult;
import org.apache.geode.management.internal.cli.commands.ShowMetricsCommand.Category;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.i18n.CliStrings;

public class ShowMetricsInterceptor extends AbstractCliAroundInterceptor {
  @Override
  public ResultModel preExecution(GfshParseResult parseResult) {
    String export_to_report_to = parseResult.getParamValueAsString(CliStrings.SHOW_METRICS__FILE);
    if (export_to_report_to != null && !export_to_report_to.endsWith(".csv")) {
      return ResultModel.createError(CliStrings.format(CliStrings.INVALID_FILE_EXTENSION, ".csv"));
    }

    String regionName = parseResult.getParamValueAsString(CliStrings.SHOW_METRICS__REGION);
    String port = parseResult.getParamValueAsString(CliStrings.SHOW_METRICS__CACHESERVER__PORT);
    String member = parseResult.getParamValueAsString(CliStrings.MEMBER);
    String[] categoryArgs = (String[]) parseResult.getParamValue(CliStrings.SHOW_METRICS__CATEGORY);

    if (regionName != null && port != null) {
      return ResultModel.createError(
          CliStrings.SHOW_METRICS__CANNOT__USE__REGION__WITH__CACHESERVERPORT);
    }

    if (port != null && member == null) {
      return ResultModel.createError(CliStrings.SHOW_METRICS__CANNOT__USE__CACHESERVERPORT);
    }

    if (categoryArgs != null) {
      boolean regionProvided = regionName != null;
      boolean portProvided = port != null;
      boolean memberProvided = member != null;
      List<String> validCategories =
          getValidCategoriesAsStrings(regionProvided, memberProvided, portProvided);
      Set<String> userCategories = new HashSet<>(Arrays.asList(categoryArgs));
      userCategories.removeAll(validCategories);
      if (!userCategories.isEmpty()) {
        return getInvalidCategoryResult(userCategories);
      }
    }

    return ResultModel.createInfo("OK");
  }

  static List<Category> getValidCategories(boolean regionProvided, boolean memberProvided,
      boolean portProvided) {
    if (regionProvided && memberProvided) {
      return ShowMetricsCommand.REGION_METRIC_CATEGORIES;
    }
    if (regionProvided) {
      return ShowMetricsCommand.SYSTEM_REGION_METRIC_CATEGORIES;
    }
    if (memberProvided && portProvided) {
      return ShowMetricsCommand.MEMBER_WITH_PORT_METRIC_CATEGORIES;
    }
    if (memberProvided) {
      return ShowMetricsCommand.MEMBER_METRIC_CATEGORIES;
    }
    return ShowMetricsCommand.SYSTEM_METRIC_CATEGORIES;
  }

  static List<String> getValidCategoriesAsStrings(boolean regionProvided, boolean memberProvided,
      boolean portProvided) {

    return getValidCategories(regionProvided, memberProvided, portProvided).stream().map(Enum::name)
        .collect(Collectors.toList());
  }


  private ResultModel getInvalidCategoryResult(Set<String> invalidCategories) {
    StringBuilder sb = new StringBuilder();
    sb.append("Invalid Categories\n");
    for (String category : invalidCategories) {
      sb.append(category);
      sb.append('\n');
    }
    return ResultModel.createError(sb.toString());
  }

  @Override
  public ResultModel postExecution(GfshParseResult parseResult, ResultModel resultModel,
      Path tempFile) throws IOException {
    String saveAs = parseResult.getParamValueAsString(CliStrings.SHOW_METRICS__FILE);

    if (saveAs == null) {
      return resultModel;
    }

    File file = new File(saveAs).getAbsoluteFile();
    resultModel.saveFileTo(file.getParentFile());
    return resultModel;
  }
}

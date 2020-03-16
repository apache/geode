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
import java.util.List;
import java.util.Optional;

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.execute.FunctionInvocationTargetException;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.cache.snapshot.RegionSnapshotService;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.GfshCommand;
import org.apache.geode.management.internal.cli.functions.ExportDataFunction;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;

public class ExportDataCommand extends GfshCommand {
  private final ExportDataFunction exportDataFunction = new ExportDataFunction();

  @CliCommand(value = CliStrings.EXPORT_DATA, help = CliStrings.EXPORT_DATA__HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_DATA, CliStrings.TOPIC_GEODE_REGION})
  public ResultModel exportData(
      @CliOption(key = CliStrings.EXPORT_DATA__REGION, mandatory = true,
          optionContext = ConverterHint.REGION_PATH,
          help = CliStrings.EXPORT_DATA__REGION__HELP) String regionName,
      @CliOption(key = CliStrings.EXPORT_DATA__FILE,
          help = CliStrings.EXPORT_DATA__FILE__HELP) String filePath,
      @CliOption(key = CliStrings.EXPORT_DATA__DIR,
          help = CliStrings.EXPORT_DATA__DIR__HELP) String dirPath,
      @CliOption(key = CliStrings.MEMBER, optionContext = ConverterHint.MEMBERIDNAME,
          mandatory = true, help = CliStrings.EXPORT_DATA__MEMBER__HELP) String memberNameOrId,
      @CliOption(key = CliStrings.EXPORT_DATA__PARALLEL, unspecifiedDefaultValue = "false",
          specifiedDefaultValue = "true",
          help = CliStrings.EXPORT_DATA__PARALLEL_HELP) boolean parallel) {

    authorize(Resource.DATA, Operation.READ, regionName);
    final DistributedMember targetMember = getMember(memberNameOrId);

    Optional<ResultModel> validationResult = validatePath(filePath, dirPath, parallel);
    if (validationResult.isPresent()) {
      return validationResult.get();
    }

    ResultModel result;
    try {
      String path = dirPath != null ? defaultFileName(dirPath, regionName) : filePath;
      final String[] args = {regionName, path, Boolean.toString(parallel)};

      ResultCollector<?, ?> rc = executeFunction(exportDataFunction, args, targetMember);
      @SuppressWarnings("unchecked")
      final List<CliFunctionResult> results = (List<CliFunctionResult>) rc.getResult();
      result = ResultModel.createMemberStatusResult(results);
    } catch (CacheClosedException e) {
      result = ResultModel.createError(e.getMessage());
    } catch (FunctionInvocationTargetException e) {
      result = ResultModel.createError(
          CliStrings.format(CliStrings.COMMAND_FAILURE_MESSAGE, CliStrings.IMPORT_DATA));
    }
    return result;
  }

  private String defaultFileName(String dirPath, String regionName) {
    return new File(dirPath, regionName + RegionSnapshotService.SNAPSHOT_FILE_EXTENSION)
        .getAbsolutePath();
  }

  private Optional<ResultModel> validatePath(String filePath, String dirPath, boolean parallel) {
    if (filePath == null && dirPath == null) {
      return Optional
          .of(ResultModel.createError("Must specify a location to save snapshot"));
    } else if (filePath != null && dirPath != null) {
      return Optional.of(ResultModel.createError(
          "Options \"file\" and \"dir\" cannot be specified at the same time"));
    } else if (parallel && dirPath == null) {
      return Optional.of(
          ResultModel.createError("Must specify a directory to save snapshot files"));
    }

    if (dirPath == null && !filePath.endsWith(CliStrings.GEODE_DATA_FILE_EXTENSION)) {
      return Optional.of(ResultModel.createError(CliStrings.format(
          CliStrings.INVALID_FILE_EXTENSION, CliStrings.GEODE_DATA_FILE_EXTENSION)));
    }
    return Optional.empty();
  }
}

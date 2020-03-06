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

import java.util.List;
import java.util.Optional;

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.execute.FunctionInvocationTargetException;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.GfshCommand;
import org.apache.geode.management.internal.cli.functions.ImportDataFunction;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;

public class ImportDataCommand extends GfshCommand {
  private final ImportDataFunction importDataFunction = new ImportDataFunction();

  @CliCommand(value = CliStrings.IMPORT_DATA, help = CliStrings.IMPORT_DATA__HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_DATA, CliStrings.TOPIC_GEODE_REGION})
  public ResultModel importData(
      @CliOption(key = CliStrings.IMPORT_DATA__REGION, optionContext = ConverterHint.REGION_PATH,
          mandatory = true, help = CliStrings.IMPORT_DATA__REGION__HELP) String regionName,
      @CliOption(key = CliStrings.IMPORT_DATA__FILE,
          help = CliStrings.IMPORT_DATA__FILE__HELP) String filePath,
      @CliOption(key = CliStrings.IMPORT_DATA__DIR,
          help = CliStrings.IMPORT_DATA__DIR__HELP) String dirPath,
      @CliOption(key = CliStrings.MEMBER, mandatory = true,
          optionContext = ConverterHint.MEMBERIDNAME,
          help = CliStrings.IMPORT_DATA__MEMBER__HELP) String memberNameOrId,
      @CliOption(key = CliStrings.IMPORT_DATA__INVOKE_CALLBACKS, unspecifiedDefaultValue = "false",
          help = CliStrings.IMPORT_DATA__INVOKE_CALLBACKS__HELP) boolean invokeCallbacks,
      @CliOption(key = CliStrings.IMPORT_DATA__PARALLEL, unspecifiedDefaultValue = "false",
          specifiedDefaultValue = "true",
          help = CliStrings.IMPORT_DATA__PARALLEL_HELP) boolean parallel) {

    authorize(Resource.DATA, Operation.WRITE, regionName);

    final DistributedMember targetMember = getMember(memberNameOrId);

    Optional<ResultModel> validationResult = validatePath(filePath, dirPath, parallel);
    if (validationResult.isPresent()) {
      return validationResult.get();
    }

    ResultModel result;
    try {
      String path = dirPath != null ? dirPath : filePath;
      final Object[] args = {regionName, path, invokeCallbacks, parallel};

      ResultCollector<?, ?> rc = executeFunction(importDataFunction, args, targetMember);
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

  private Optional<ResultModel> validatePath(String filePath, String dirPath, boolean parallel) {
    if (filePath == null && dirPath == null) {
      return Optional
          .of(ResultModel.createError("Must specify a location to load snapshot from"));
    } else if (filePath != null && dirPath != null) {
      return Optional.of(ResultModel.createError(
          "Options \"file\" and \"dir\" cannot be specified at the same time"));
    } else if (parallel && dirPath == null) {
      return Optional
          .of(ResultModel.createError("Must specify a directory to load snapshot files from"));
    }

    if (dirPath == null && !filePath.endsWith(CliStrings.GEODE_DATA_FILE_EXTENSION)) {
      return Optional.of(ResultModel.createError(CliStrings
          .format(CliStrings.INVALID_FILE_EXTENSION, CliStrings.GEODE_DATA_FILE_EXTENSION)));
    }
    return Optional.empty();
  }
}

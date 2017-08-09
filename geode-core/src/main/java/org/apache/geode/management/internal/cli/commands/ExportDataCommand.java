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

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.execute.FunctionInvocationTargetException;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.cli.functions.ExportDataFunction;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.ResultBuilder;

public class ExportDataCommand implements GfshCommand {
  private final ExportDataFunction exportDataFunction = new ExportDataFunction();

  @CliCommand(value = CliStrings.EXPORT_DATA, help = CliStrings.EXPORT_DATA__HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_DATA, CliStrings.TOPIC_GEODE_REGION})
  public Result exportData(
      @CliOption(key = CliStrings.EXPORT_DATA__REGION, mandatory = true,
          optionContext = ConverterHint.REGION_PATH,
          help = CliStrings.EXPORT_DATA__REGION__HELP) String regionName,
      @CliOption(key = CliStrings.EXPORT_DATA__FILE, mandatory = true,
          help = CliStrings.EXPORT_DATA__FILE__HELP) String filePath,
      @CliOption(key = CliStrings.MEMBER, optionContext = ConverterHint.MEMBERIDNAME,
          mandatory = true, help = CliStrings.EXPORT_DATA__MEMBER__HELP) String memberNameOrId) {

    getSecurityService().authorizeRegionRead(regionName);
    final DistributedMember targetMember = CliUtil.getDistributedMemberByNameOrId(memberNameOrId);
    Result result;

    if (!filePath.endsWith(CliStrings.GEODE_DATA_FILE_EXTENSION)) {
      return ResultBuilder.createUserErrorResult(CliStrings
          .format(CliStrings.INVALID_FILE_EXTENSION, CliStrings.GEODE_DATA_FILE_EXTENSION));
    }
    try {
      if (targetMember != null) {
        final String args[] = {regionName, filePath};

        ResultCollector<?, ?> rc = CliUtil.executeFunction(exportDataFunction, args, targetMember);
        List<Object> results = (List<Object>) rc.getResult();

        if (results != null) {
          Object resultObj = results.get(0);
          if (resultObj instanceof String) {
            result = ResultBuilder.createInfoResult((String) resultObj);
          } else if (resultObj instanceof Exception) {
            result = ResultBuilder.createGemFireErrorResult(((Exception) resultObj).getMessage());
          } else {
            result = ResultBuilder.createGemFireErrorResult(
                CliStrings.format(CliStrings.COMMAND_FAILURE_MESSAGE, CliStrings.EXPORT_DATA));
          }
        } else {
          result = ResultBuilder.createGemFireErrorResult(
              CliStrings.format(CliStrings.COMMAND_FAILURE_MESSAGE, CliStrings.EXPORT_DATA));
        }
      } else {
        result = ResultBuilder.createUserErrorResult(
            CliStrings.format(CliStrings.EXPORT_DATA__MEMBER__NOT__FOUND, memberNameOrId));
      }
    } catch (CacheClosedException e) {
      result = ResultBuilder.createGemFireErrorResult(e.getMessage());
    } catch (FunctionInvocationTargetException e) {
      result = ResultBuilder.createGemFireErrorResult(
          CliStrings.format(CliStrings.COMMAND_FAILURE_MESSAGE, CliStrings.IMPORT_DATA));
    }
    return result;
  }
}

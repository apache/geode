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
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.springframework.shell.event.ParseResult;

import org.apache.geode.management.internal.cli.AbstractCliAroundInterceptor;
import org.apache.geode.management.internal.cli.GfshParseResult;
import org.apache.geode.management.internal.cli.domain.DataCommandResult;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.result.model.DataResultModel;
import org.apache.geode.management.internal.cli.result.model.ResultModel;

public class QueryInterceptor extends AbstractCliAroundInterceptor {
  public static final String FILE_ALREADY_EXISTS_MESSAGE =
      "The specified output file already exists.";

  @Override
  public ResultModel preExecution(GfshParseResult parseResult) {
    File outputFile = getOutputFile(parseResult);

    if (outputFile != null && outputFile.exists()) {
      return ResultModel.createError(FILE_ALREADY_EXISTS_MESSAGE);
    }

    return new ResultModel();
  }

  @Override
  public ResultModel postExecution(GfshParseResult parseResult, ResultModel model, Path tempFile)
      throws Exception {
    File outputFile = getOutputFile(parseResult);

    if (outputFile == null) {
      return model;
    }

    Map<String, String> sectionResultData =
        model.getDataSection(DataCommandResult.DATA_INFO_SECTION).getContent();

    String limit = sectionResultData.get("Limit");
    String resultString = sectionResultData.get("Result");
    String rows = sectionResultData.get("Rows");

    if ("false".equalsIgnoreCase(resultString)) {
      return model;
    }

    writeResultTableToFile(outputFile, model);
    ResultModel newModel = new ResultModel();
    DataResultModel data = newModel.addData(DataCommandResult.DATA_INFO_SECTION);

    data.addData("Result", resultString);
    if (StringUtils.isNotBlank(limit)) {
      data.addData("Limit", limit);
    }
    data.addData("Rows", rows);

    newModel.addInfo().addLine("Query results output to " + outputFile.getAbsolutePath());

    return newModel;
  }

  private File getOutputFile(ParseResult parseResult) {
    return (File) parseResult.getArguments()[1];
  }

  private void writeResultTableToFile(File file, ResultModel resultModel) throws IOException {
    CommandResult commandResult = new CommandResult(resultModel);
    try (FileWriter fileWriter = new FileWriter(file)) {
      fileWriter.write(commandResult.asString());
    }
  }
}

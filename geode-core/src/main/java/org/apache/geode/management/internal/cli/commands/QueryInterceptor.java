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

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.SystemUtils;
import org.springframework.shell.event.ParseResult;

import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.AbstractCliAroundInterceptor;
import org.apache.geode.management.internal.cli.GfshParseResult;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.result.CompositeResultData;
import org.apache.geode.management.internal.cli.result.InfoResultData;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.cli.result.TabularResultData;

public class QueryInterceptor extends AbstractCliAroundInterceptor {
  public static final String FILE_ALREADY_EXISTS_MESSAGE =
      "The specified output file already exists.";

  @Override
  public Result preExecution(GfshParseResult parseResult) {
    File outputFile = getOutputFile(parseResult);

    if (outputFile != null && outputFile.exists()) {
      return ResultBuilder.createUserErrorResult(FILE_ALREADY_EXISTS_MESSAGE);
    }

    return ResultBuilder.createInfoResult("");
  }

  @Override
  public Result postExecution(GfshParseResult parseResult, Result result, Path tempFile) {
    File outputFile = getOutputFile(parseResult);

    if (outputFile == null) {
      return result;
    }

    CommandResult commandResult = (CommandResult) result;
    CompositeResultData resultData = (CompositeResultData) commandResult.getResultData();
    CompositeResultData.SectionResultData sectionResultData = resultData.retrieveSectionByIndex(0);

    String limit = sectionResultData.retrieveString("Limit");
    String resultString = sectionResultData.retrieveString("Result");
    String rows = sectionResultData.retrieveString("Rows");

    if ("false".equalsIgnoreCase(resultString)) {
      return result;
    }

    TabularResultData tabularResultData = sectionResultData.retrieveTableByIndex(0);
    CommandResult resultTable = new CommandResult(tabularResultData);
    try {
      writeResultTableToFile(outputFile, resultTable);
      // return a result w/ message explaining limit
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    InfoResultData infoResultData = ResultBuilder.createInfoResultData();
    infoResultData.addLine("Result : " + resultString);
    if (StringUtils.isNotBlank(limit)) {
      infoResultData.addLine("Limit  : " + limit);
    }
    infoResultData.addLine("Rows   : " + rows);
    infoResultData.addLine(SystemUtils.LINE_SEPARATOR);
    infoResultData.addLine("Query results output to " + outputFile.getAbsolutePath());

    return new CommandResult(infoResultData);
  }

  private File getOutputFile(ParseResult parseResult) {
    return (File) parseResult.getArguments()[1];
  }

  private void writeResultTableToFile(File file, CommandResult commandResult) throws IOException {
    try (FileWriter fileWriter = new FileWriter(file)) {
      while (commandResult.hasNextLine()) {
        fileWriter.write(commandResult.nextLine());

        if (commandResult.hasNextLine()) {
          fileWriter.write(SystemUtils.LINE_SEPARATOR);
        }
      }
    }
  }
}

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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.Iterator;

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.GfshParser;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.ErrorResultData;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.cli.shell.Gfsh;
import org.apache.geode.management.internal.cli.shell.jline.GfshHistory;

public class HistoryCommand extends InternalGfshCommand {
  @CliCommand(value = CliStrings.HISTORY, help = CliStrings.HISTORY__HELP)
  @CliMetaData(shellOnly = true, relatedTopic = {CliStrings.TOPIC_GFSH})
  public Result history(
      @CliOption(key = {CliStrings.HISTORY__FILE},
          help = CliStrings.HISTORY__FILE__HELP) String saveHistoryTo,
      @CliOption(key = {CliStrings.HISTORY__CLEAR}, specifiedDefaultValue = "true",
          unspecifiedDefaultValue = "false",
          help = CliStrings.HISTORY__CLEAR__HELP) Boolean clearHistory) {
    // process clear history
    if (clearHistory) {
      return executeClearHistory();
    } else {
      // Process file option
      Gfsh gfsh = Gfsh.getCurrentInstance();
      ErrorResultData errorResultData;
      StringBuilder contents = new StringBuilder();
      Writer output = null;

      int historySize = gfsh.getHistorySize();
      String historySizeString = String.valueOf(historySize);
      int historySizeWordLength = historySizeString.length();

      GfshHistory gfshHistory = gfsh.getGfshHistory();
      Iterator<?> it = gfshHistory.entries();
      boolean flagForLineNumbers = !(saveHistoryTo != null && saveHistoryTo.length() > 0);
      long lineNumber = 0;

      while (it.hasNext()) {
        String line = it.next().toString();
        if (!line.isEmpty()) {
          if (flagForLineNumbers) {
            lineNumber++;
            contents.append(String.format("%" + historySizeWordLength + "s  ", lineNumber));
          }
          contents.append(line);
          contents.append(GfshParser.LINE_SEPARATOR);
        }
      }

      try {
        // write to a user file
        if (saveHistoryTo != null && saveHistoryTo.length() > 0) {
          File saveHistoryToFile = new File(saveHistoryTo);
          output = new BufferedWriter(new FileWriter(saveHistoryToFile));

          if (!saveHistoryToFile.exists()) {
            errorResultData =
                ResultBuilder.createErrorResultData().setErrorCode(ResultBuilder.ERRORCODE_DEFAULT)
                    .addLine(CliStrings.HISTORY__MSG__FILE_DOES_NOT_EXISTS);
            return ResultBuilder.buildResult(errorResultData);
          }
          if (!saveHistoryToFile.isFile()) {
            errorResultData =
                ResultBuilder.createErrorResultData().setErrorCode(ResultBuilder.ERRORCODE_DEFAULT)
                    .addLine(CliStrings.HISTORY__MSG__FILE_SHOULD_NOT_BE_DIRECTORY);
            return ResultBuilder.buildResult(errorResultData);
          }
          if (!saveHistoryToFile.canWrite()) {
            errorResultData =
                ResultBuilder.createErrorResultData().setErrorCode(ResultBuilder.ERRORCODE_DEFAULT)
                    .addLine(CliStrings.HISTORY__MSG__FILE_CANNOT_BE_WRITTEN);
            return ResultBuilder.buildResult(errorResultData);
          }

          output.write(contents.toString());
        }

      } catch (IOException ex) {
        return ResultBuilder
            .createInfoResult("File error " + ex.getMessage() + " for file " + saveHistoryTo);
      } finally {
        try {
          if (output != null) {
            output.close();
          }
        } catch (IOException e) {
          errorResultData = ResultBuilder.createErrorResultData()
              .setErrorCode(ResultBuilder.ERRORCODE_DEFAULT).addLine("exception in closing file");
          return ResultBuilder.buildResult(errorResultData);
        }
      }
      if (saveHistoryTo != null && saveHistoryTo.length() > 0) {
        // since written to file no need to display the content
        return ResultBuilder.createInfoResult("Wrote successfully to file " + saveHistoryTo);
      } else {
        return ResultBuilder.createInfoResult(contents.toString());
      }
    }
  }

  private Result executeClearHistory() {
    Gfsh gfsh = Gfsh.getCurrentInstance();
    gfsh.clearHistory();
    return ResultBuilder.createInfoResult(CliStrings.HISTORY__MSG__CLEARED_HISTORY);
  }
}

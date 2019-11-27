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
import java.util.Iterator;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.internal.cli.GfshParser;
import org.apache.geode.management.internal.cli.result.model.InfoResultModel;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.shell.Gfsh;
import org.apache.geode.management.internal.cli.shell.jline.GfshHistory;
import org.apache.geode.management.internal.i18n.CliStrings;

public class HistoryCommand extends OfflineGfshCommand {
  @CliCommand(value = CliStrings.HISTORY, help = CliStrings.HISTORY__HELP)
  @CliMetaData(shellOnly = true, relatedTopic = {CliStrings.TOPIC_GFSH})
  public ResultModel history(
      @CliOption(key = {CliStrings.HISTORY__FILE},
          help = CliStrings.HISTORY__FILE__HELP) String saveHistoryTo,
      @CliOption(key = {CliStrings.HISTORY__CLEAR}, specifiedDefaultValue = "true",
          unspecifiedDefaultValue = "false",
          help = CliStrings.HISTORY__CLEAR__HELP) Boolean clearHistory)
      throws IOException {
    // process clear history
    if (clearHistory) {
      return executeClearHistory();
    } else {
      // Process file option
      Gfsh gfsh = getGfsh();

      boolean hasFile = StringUtils.isNotBlank(saveHistoryTo);

      File saveHistoryToFile = null;
      if (hasFile) {
        saveHistoryToFile = new File(saveHistoryTo);
        if (saveHistoryToFile.exists()) {
          return ResultModel.createError("File exists already");
        }
        if (saveHistoryToFile.isDirectory()) {
          return ResultModel.createError(CliStrings.HISTORY__MSG__FILE_SHOULD_NOT_BE_DIRECTORY);
        }
      }

      GfshHistory gfshHistory = gfsh.getGfshHistory();
      Iterator<?> it = gfshHistory.entries();

      ResultModel result = new ResultModel();
      InfoResultModel histories = result.addInfo("history");
      while (it.hasNext()) {
        String line = it.next().toString();
        if (!line.isEmpty()) {
          if (hasFile) {
            FileUtils.writeStringToFile(saveHistoryToFile, line + GfshParser.LINE_SEPARATOR,
                "UTF-8", true);
          } else {
            histories.addLine(line);
          }
        }
      }

      if (hasFile) {
        // since written to file no need to display the content
        return ResultModel.createInfo("Wrote successfully to file " + saveHistoryTo);
      } else {
        return result;
      }
    }
  }

  private ResultModel executeClearHistory() {
    getGfsh().clearHistory();
    return ResultModel.createInfo(CliStrings.HISTORY__MSG__CLEARED_HISTORY);
  }
}

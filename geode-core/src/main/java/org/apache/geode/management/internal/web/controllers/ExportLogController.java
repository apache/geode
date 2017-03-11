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

package org.apache.geode.management.internal.web.controllers;

import org.apache.commons.io.FileUtils;
import org.apache.geode.internal.lang.StringUtils;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import java.io.File;
import java.io.FileInputStream;

@Controller("exportLogController")
@RequestMapping(AbstractCommandsController.REST_API_VERSION)
public class ExportLogController extends AbstractCommandsController {

  @RequestMapping(method = RequestMethod.GET, value = "/logs")
  public ResponseEntity<InputStreamResource> exportLogs(
      @RequestParam(value = CliStrings.EXPORT_LOGS__DIR, required = false) final String directory,
      @RequestParam(value = CliStrings.EXPORT_LOGS__GROUP, required = false) final String[] groups,
      @RequestParam(value = CliStrings.EXPORT_LOGS__MEMBER,
          required = false) final String memberNameId,
      @RequestParam(value = CliStrings.EXPORT_LOGS__LOGLEVEL,
          required = false) final String logLevel,
      @RequestParam(value = CliStrings.EXPORT_LOGS__UPTO_LOGLEVEL,
          defaultValue = "false") final Boolean onlyLogLevel,
      @RequestParam(value = CliStrings.EXPORT_LOGS__MERGELOG,
          defaultValue = "false") final Boolean mergeLog,
      @RequestParam(value = CliStrings.EXPORT_LOGS__STARTTIME,
          required = false) final String startTime,
      @RequestParam(value = CliStrings.EXPORT_LOGS__ENDTIME, required = false) final String endTime,
      @RequestParam(value = CliStrings.EXPORT_LOGS__LOGSONLY,
          required = false) final boolean logsOnly,
      @RequestParam(value = CliStrings.EXPORT_LOGS__STATSONLY,
          required = false) final boolean statsOnly) {
    final CommandStringBuilder command = new CommandStringBuilder(CliStrings.EXPORT_LOGS);

    command.addOption(CliStrings.EXPORT_LOGS__DIR, decode(directory));

    if (hasValue(groups)) {
      command.addOption(CliStrings.EXPORT_LOGS__GROUP,
          StringUtils.concat(groups, StringUtils.COMMA_DELIMITER));
    }

    if (hasValue(memberNameId)) {
      command.addOption(CliStrings.EXPORT_LOGS__MEMBER, memberNameId);
    }

    if (hasValue(logLevel)) {
      command.addOption(CliStrings.EXPORT_LOGS__LOGLEVEL, logLevel);
    }

    command.addOption(CliStrings.EXPORT_LOGS__UPTO_LOGLEVEL, String.valueOf(onlyLogLevel));
    command.addOption(CliStrings.EXPORT_LOGS__MERGELOG, String.valueOf(mergeLog));
    command.addOption(CliStrings.EXPORT_LOGS__LOGSONLY, String.valueOf(logsOnly));
    command.addOption(CliStrings.EXPORT_LOGS__STATSONLY, String.valueOf(statsOnly));

    if (hasValue(startTime)) {
      command.addOption(CliStrings.EXPORT_LOGS__STARTTIME, startTime);
    }

    if (hasValue(endTime)) {
      command.addOption(CliStrings.EXPORT_LOGS__ENDTIME, endTime);
    }

    // the result is json string from CommandResult
    String result = processCommand(command.toString());

    // parse the result to get the file path. This file Path should always exist in the file system
    String filePath = ResultBuilder.fromJson(result).nextLine().trim();

    HttpHeaders respHeaders = new HttpHeaders();
    File zipFile = new File(filePath);
    try {
      InputStreamResource isr = new InputStreamResource(new FileInputStream(zipFile));
      return new ResponseEntity<InputStreamResource>(isr, respHeaders, HttpStatus.OK);
    } catch (Exception ex) {
      throw new RuntimeException("IOError writing file to output stream", ex);
    } finally {
      FileUtils.deleteQuietly(zipFile);
    }
  }
}

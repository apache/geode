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
import org.apache.commons.io.IOUtils;
import org.apache.geode.internal.lang.StringUtils;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
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
      @RequestParam(value = CliStrings.GROUP, required = false) final String[] groups,
      @RequestParam(value = CliStrings.MEMBER, required = false) final String memberNameId,
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
          required = false) final boolean statsOnly,
      @RequestParam(value = CliStrings.EXPORT_LOGS__FILESIZELIMIT,
          required = false) final String fileSizeLimit) {
    final CommandStringBuilder command = new CommandStringBuilder(CliStrings.EXPORT_LOGS);

    command.addOption(CliStrings.EXPORT_LOGS__DIR, decode(directory));

    if (hasValue(groups)) {
      command.addOption(CliStrings.GROUP, StringUtils.join(groups, StringUtils.COMMA_DELIMITER));
    }

    if (hasValue(memberNameId)) {
      command.addOption(CliStrings.MEMBER, memberNameId);
    }

    if (hasValue(logLevel)) {
      command.addOption(CliStrings.EXPORT_LOGS__LOGLEVEL, logLevel);
    }

    command.addOption(CliStrings.EXPORT_LOGS__UPTO_LOGLEVEL, String.valueOf(onlyLogLevel));
    command.addOption(CliStrings.EXPORT_LOGS__MERGELOG, String.valueOf(mergeLog));
    command.addOption(CliStrings.EXPORT_LOGS__LOGSONLY, String.valueOf(logsOnly));
    command.addOption(CliStrings.EXPORT_LOGS__STATSONLY, String.valueOf(statsOnly));
    command.addOption(CliStrings.EXPORT_LOGS__FILESIZELIMIT, fileSizeLimit);

    if (hasValue(startTime)) {
      command.addOption(CliStrings.EXPORT_LOGS__STARTTIME, startTime);
    }

    if (hasValue(endTime)) {
      command.addOption(CliStrings.EXPORT_LOGS__ENDTIME, endTime);
    }

    String result = processCommand(command.toString());
    return getResponse(result);

  }

  ResponseEntity<InputStreamResource> getResponse(String result) {
    // the result is json string from CommandResult
    Result commandResult = ResultBuilder.fromJson(result);
    if (commandResult.getStatus().equals(Result.Status.OK)) {
      return getOKResponse(commandResult);

    } else {
      return getErrorResponse(result);
    }
  }

  private ResponseEntity<InputStreamResource> getErrorResponse(String result) {
    HttpHeaders respHeaders = new HttpHeaders();
    InputStreamResource isr;// if the command is successful, the output is the filepath,
    // else we need to send the orignal result back so that the receiver will know to turn it
    // into a Result object
    try {
      isr = new InputStreamResource(IOUtils.toInputStream(result, "UTF-8"));
      respHeaders.set(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE);
      return new ResponseEntity<InputStreamResource>(isr, respHeaders, HttpStatus.OK);
    } catch (Exception e) {
      throw new RuntimeException("IO Error writing file to output stream", e);
    }
  }

  private ResponseEntity<InputStreamResource> getOKResponse(Result commandResult) {
    HttpHeaders respHeaders = new HttpHeaders();
    InputStreamResource isr;// if the command is successful, the output is the filepath,
    String filePath = commandResult.nextLine().trim();
    File zipFile = new File(filePath);
    try {
      isr = new InputStreamResource(new FileInputStream(zipFile));
      respHeaders.set(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_OCTET_STREAM_VALUE);
      return new ResponseEntity<InputStreamResource>(isr, respHeaders, HttpStatus.OK);
    } catch (Exception e) {
      throw new RuntimeException("IO Error writing file to output stream", e);
    } finally {
      FileUtils.deleteQuietly(zipFile);
    }
  }

}

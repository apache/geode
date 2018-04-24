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

package org.apache.geode.management.internal.cli.result.model;

import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.cli.result.ResultData;

public class ErrorResultModel extends InfoResultModel {

  private int errorCode;

  public ErrorResultModel() {}

  public ErrorResultModel(int errorCode, String message) {
    this.errorCode = errorCode;
    addLine(message);
  }

  @Override
  public String getType() {
    return ResultData.TYPE_ERROR;
  }

  @Override
  public Status getStatus() {
    return Status.ERROR;
  }

  public int getErrorCode() {
    return errorCode;
  }

  public void setErrorCode(int errorCode) {
    this.errorCode = errorCode;
  }

  public static ErrorResultModel createConnectionErrorResult(String message) {
    String errorMessage = message != null ? message : "Connection Error occurred.";

    return new ErrorResultModel(ResultBuilder.ERRORCODE_CONNECTION_ERROR, errorMessage);
  }

  public static ErrorResultModel createShellClientErrorResult(String message) {
    return new ErrorResultModel(ResultBuilder.ERRORCODE_SHELLCLIENT_ERROR, message);
  }

  public static ErrorResultModel createShellClientAbortOperationResult(String message) {
    return new ErrorResultModel(ResultBuilder.CODE_SHELLCLIENT_ABORT_OP, message);
  }

  public static ErrorResultModel createParsingErrorResult(String message) {
    return new ErrorResultModel(ResultBuilder.ERRORCODE_PARSING_ERROR,
        "Could not parse command string. " + message);
  }

  public static ErrorResultModel createBadConfigurationErrorResult(String message) {
    return new ErrorResultModel(ResultBuilder.ERRORCODE_BADCONFIG_ERROR,
        "Configuration error. " + message);
  }

  public static ErrorResultModel createGemFireErrorResult(String message) {
    return new ErrorResultModel(ResultBuilder.ERRORCODE_GEODE_ERROR,
        "Could not process command due to error. " + message);
  }

  public static ErrorResultModel createGemFireUnAuthorizedErrorResult(String message) {
    return new ErrorResultModel(ResultBuilder.ERRORCODE_UNAUTHORIZED, message);
  }

  public static ErrorResultModel createUserErrorResult(String message) {
    return new ErrorResultModel(ResultBuilder.ERRORCODE_USER_ERROR, message);
  }
}

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
package org.apache.geode.management.internal.cli.result;

import java.nio.file.Paths;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.cli.json.GfJsonException;
import org.apache.geode.management.internal.cli.json.GfJsonObject;
import org.apache.geode.management.internal.cli.result.model.ResultModel;

/**
 * Provides methods for creating {@link Result} objects to return from Gfsh command functions
 */
public class ResultBuilder {
  public static final int CODE_SHELLCLIENT_ABORT_OP = 110;

  // error on gfsh
  public static final int ERRORCODE_DEFAULT = 400;
  public static final int ERRORCODE_CONNECTION_ERROR = 405;
  public static final int ERRORCODE_SHELLCLIENT_ERROR = 410;
  public static final int ERRORCODE_UNAUTHORIZED = 415;

  // errors on member
  public static final int ERRORCODE_PARSING_ERROR = 501;
  public static final int ERRORCODE_GEODE_ERROR = 505;
  public static final int ERRORCODE_BADRESPONSE_ERROR = 510;
  public static final int ERRORCODE_BADCONFIG_ERROR = 515;
  public static final int ERRORCODE_USER_ERROR = 520;

  /**
   * Method for convenience to create error result for connection error.
   * <p/>
   * Note: To build your own error result, use {@link #createErrorResultData()} to build
   * {@link ErrorResultData} & then use {@link #buildResult(ResultData)}
   *
   * @param message Message to be shown to the user
   */
  public static CommandResult createConnectionErrorResult(String message) {
    String errorMessage = message != null ? message : "Connection Error occurred.";

    return createErrorResult(ERRORCODE_CONNECTION_ERROR, errorMessage);
  }

  public static CommandResult createShellClientErrorResult(String message) {
    return createErrorResult(ERRORCODE_SHELLCLIENT_ERROR, message);
  }

  public static CommandResult createShellClientAbortOperationResult(String message) {
    return createErrorResult(CODE_SHELLCLIENT_ABORT_OP, message);
  }

  /**
   * Method for convenience to create error result for parsing error.
   * <p/>
   * Note: To build your own error result, use {@link #createErrorResultData()} to build
   * {@link ErrorResultData} & then use {@link #buildResult(ResultData)}
   *
   * @param message Message to be shown to the user
   */
  public static CommandResult createParsingErrorResult(String message) {
    return createErrorResult(ERRORCODE_PARSING_ERROR, "Could not parse command string. " + message);
  }

  public static CommandResult createBadConfigurationErrorResult(String message) {
    return createErrorResult(ERRORCODE_BADCONFIG_ERROR, "Configuration error. " + message);
  }

  /**
   * Method for convenience to create error result for error in Geode while executing command.
   * <p/>
   * Note: To build your own error result, use {@link #createErrorResultData()} to build
   * {@link ErrorResultData} & then use {@link #buildResult(ResultData)}
   *
   * @param message Message to be shown to the user
   */
  public static CommandResult createGemFireErrorResult(String message) {
    return createErrorResult(ERRORCODE_GEODE_ERROR,
        "Could not process command due to error. " + message);
  }

  public static CommandResult createGemFireUnAuthorizedErrorResult(String message) {
    return createErrorResult(ERRORCODE_UNAUTHORIZED, message);
  }

  public static CommandResult createUserErrorResult(String message) {
    return createErrorResult(ERRORCODE_USER_ERROR, message);
  }

  /**
   * Method for convenience to create error result for unreadable command response.
   * <p/>
   * Note: To build your own error result, use {@link #createErrorResultData()} to build
   * {@link ErrorResultData} & then use {@link #buildResult(ResultData)}
   *
   * @param message Message to be shown to the user
   */
  public static CommandResult createBadResponseErrorResult(String message) {
    return createErrorResult(ERRORCODE_BADRESPONSE_ERROR,
        "Could not read command response. " + message);
  }

  /**
   * Creates error Result with given error code & message
   * <p/>
   * Note: To build your own error result, use {@link #createErrorResultData()} to build
   * {@link ErrorResultData} & then use {@link #buildResult(ResultData)}
   *
   * @param errorCode error code should be one of ResultBuilder.ERRORCODE_**
   * @param message message for the error
   */
  private static CommandResult createErrorResult(int errorCode, String message) {
    ErrorResultData errorResultData = new ErrorResultData();
    errorResultData.setErrorCode(errorCode);
    errorResultData.addLine(message);
    return buildResult(errorResultData);
  }

  /**
   * Convenience method to create a simple Info Result that takes a message.
   *
   * @param message Message for the OK Result
   */
  public static Result createInfoResult(String message) {
    InfoResultData infoResultData = new InfoResultData();
    infoResultData.addLine(message);
    return buildResult(infoResultData);
  }

  /**
   * Creates a {@link TabularResultData} object to start building result that should be shown in a
   * Tabular Format.
   *
   * @return TabularResultData instance
   */
  public static TabularResultData createTabularResultData() {
    return new TabularResultData();
  }

  public static CompositeResultData createCompositeResultData() {
    return new CompositeResultData();
  }

  /**
   * Creates a {@link InfoResultData} object to start building result that is required to be shown
   * as an information without any specific format.
   *
   * @return InfoResultData instance
   */
  public static InfoResultData createInfoResultData() {
    return new InfoResultData();
  }

  /**
   * Creates a {@link ErrorResultData} object to start building result for an error.
   *
   * @return ErrorResultData instance
   */
  public static ErrorResultData createErrorResultData() {
    return new ErrorResultData();
  }

  /**
   * Build a Result object from the given ResultData
   *
   * @param resultData data to use to build Result
   */
  public static CommandResult buildResult(ResultData resultData) {
    return new LegacyCommandResult(resultData);
  }

  public static CommandResult buildResult(List<CliFunctionResult> functionResults) {
    return buildResult(functionResults, null, null);
  }

  public static CommandResult buildResult(List<CliFunctionResult> functionResults, String header,
      String footer) {
    TabularResultData tabularData = ResultBuilder.createTabularResultData();
    boolean success = false;
    for (CliFunctionResult result : functionResults) {
      tabularData.accumulate("Member", result.getMemberIdOrName());
      tabularData.accumulate("Status", result.getLegacyStatus());
      // if one member returns back successful results, the command results in success
      if (result.isSuccessful()) {
        success = true;
      }
    }

    if (header != null) {
      tabularData.setHeader(header);
    }
    if (footer != null) {
      tabularData.setFooter(footer);
    }

    tabularData.setStatus(success ? Result.Status.OK : Result.Status.ERROR);
    return ResultBuilder.buildResult(tabularData);
  }

  /**
   * Prepare Result from JSON. Type of result is expected to there in the JSON as 'contentType'
   * which should be one of {@link ResultData#TYPE_TABULAR}, {@link ResultData#TYPE_COMPOSITE},
   * {@link ResultData#TYPE_INFO}, {@link ResultData#TYPE_ERROR}.
   *
   * @param gfJsonObject GemFire JSON Object to use to prepare Result
   */
  public static CommandResult fromJson(GfJsonObject gfJsonObject) {
    return fromJson(gfJsonObject.toString());
  }


  /**
   * Prepare a Result object from a JSON String. This is useful on gfsh/client to read the response
   * & prepare a Result object from the JSON response
   *
   * @param json JSON string for Result
   */
  public static CommandResult fromJson(String json) {
    CommandResult result;
    try {
      GfJsonObject jsonObject = new GfJsonObject(json);

      if (jsonObject.has("legacy") && !jsonObject.getBoolean("legacy")) {
        return new ModelCommandResult(ResultModel.fromJson(json));
      }

      GfJsonObject data = jsonObject.getJSONObject("data");
      String contentType = jsonObject.getString("contentType");

      AbstractResultData resultData;
      if (ResultData.TYPE_TABULAR.equals(contentType)) {
        resultData = new TabularResultData(data);
      } else if (ResultData.TYPE_INFO.equals(contentType)) {
        resultData = new InfoResultData(data);
      } else if (ResultData.TYPE_ERROR.equals(contentType)) {
        resultData = new ErrorResultData(data);
      } else if (ResultData.TYPE_COMPOSITE.equals(contentType)) {
        resultData = new CompositeResultData(data);
      } else {
        ErrorResultData errorResultData = new ErrorResultData();
        errorResultData.addLine("Can not detect result type, unknown response format: " + json);
        resultData = errorResultData;
      }

      Integer statusCode = jsonObject.getInt("status");
      if (Result.Status.OK.getCode() == statusCode) {
        resultData.setStatus(Result.Status.OK);
      } else {
        resultData.setStatus(Result.Status.ERROR);
      }

      result = buildResult(resultData);

      String fileToDownloadPath = jsonObject.getString("fileToDownload");
      if (StringUtils.isNotBlank(fileToDownloadPath) && !fileToDownloadPath.equals("null")) {
        result.setFileToDownload(Paths.get(fileToDownloadPath));
      }

    } catch (GfJsonException e) {
      result = createBadResponseErrorResult(json);
    }

    return result;
  }

  public static String resultAsString(Result result) {
    StringBuilder builder = new StringBuilder();

    if (result != null) {
      while (result.hasNextLine()) {
        builder.append(result.nextLine());
      }
    }

    return builder.toString();
  }

  /**
   * Wraps a given ResultData and wraps it into appropriate ResultData of the same type but the
   * returned object is immutable & throws UnsupportedOperationException on invoking those methods.
   *
   * @param resultData to be wrapped
   * @return Read only ResultData of the same type
   */
  static ResultData getReadOnlyResultData(ResultData resultData) {
    ResultData wrapperResultData;
    String contentType = resultData.getType();
    if (ResultData.TYPE_TABULAR.equals(contentType)) {
      wrapperResultData = new TabularResultData(resultData.getGfJsonObject()) {
        @Override
        public ResultData addAsFile(String fileName, byte[] data, int fileType, String message,
            boolean addTimeStampToName) {
          throw new UnsupportedOperationException("This is read only result data");
        }

        @Override
        public ResultData addAsFile(String fileName, String fileContents, String message,
            boolean addTimeStampToName) {
          throw new UnsupportedOperationException("This is read only result data");
        }

        @Override
        public TabularResultData accumulate(String accumulateFor, Object value) {
          throw new UnsupportedOperationException("This is read only result data");
        }
      };
    } else if (ResultData.TYPE_INFO.equals(contentType)) {
      wrapperResultData = new InfoResultData(resultData.getGfJsonObject()) {
        @Override
        public ResultData addAsFile(String fileName, byte[] data, int fileType, String message,
            boolean addTimeStampToName) {
          throw new UnsupportedOperationException("This is read only result data");
        }

        @Override
        public ResultData addAsFile(String fileName, String fileContents, String message,
            boolean addTimeStampToName) {
          throw new UnsupportedOperationException("This is read only result data");
        }

        @Override
        public ErrorResultData addLine(String line) {
          throw new UnsupportedOperationException("This is read only result data");
        }
      };
    } else if (ResultData.TYPE_ERROR.equals(contentType)) {
      wrapperResultData = new ErrorResultData(resultData.getGfJsonObject()) {
        @Override
        public ResultData addAsFile(String fileName, byte[] data, int fileType, String message,
            boolean addTimeStampToName) {
          throw new UnsupportedOperationException("This is read only result data");
        }

        @Override
        public ResultData addAsFile(String fileName, String fileContents, String message,
            boolean addTimeStampToName) {
          throw new UnsupportedOperationException("This is read only result data");
        }

        @Override
        public ErrorResultData addLine(String line) {
          throw new UnsupportedOperationException("This is read only result data");
        }

        @Override
        public ErrorResultData setErrorCode(int errorCode) {
          throw new UnsupportedOperationException("This is read only result data");
        }
      };
    } else if (ResultData.TYPE_COMPOSITE.equals(contentType)) {
      wrapperResultData = new CompositeResultData(resultData.getGfJsonObject()) {
        @Override
        public ResultData addAsFile(String fileName, byte[] data, int fileType, String message,
            boolean addTimeStampToName) {
          throw new UnsupportedOperationException("This is read only result data");
        }

        @Override
        public ResultData addAsFile(String fileName, String fileContents, String message,
            boolean addTimeStampToName) {
          throw new UnsupportedOperationException("This is read only result data");
        }

        @Override
        public SectionResultData addSection() {
          throw new UnsupportedOperationException("This is read only result data");
        }

        @Override
        public SectionResultData addSection(String keyToRetrieve) {
          throw new UnsupportedOperationException("This is read only result data");
        }
      };
    } else {
      ErrorResultData errorResultData = new ErrorResultData();
      errorResultData
          .addLine("Can not detect result type, unknown result data format for: " + resultData);
      wrapperResultData = errorResultData;
    }

    return wrapperResultData;
  }
}

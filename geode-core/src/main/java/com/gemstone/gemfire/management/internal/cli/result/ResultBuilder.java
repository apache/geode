/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.management.internal.cli.result;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

import com.gemstone.gemfire.management.cli.Result;
import com.gemstone.gemfire.management.internal.cli.json.GfJsonException;
import com.gemstone.gemfire.management.internal.cli.json.GfJsonObject;

/**
 * 
 * 
 * @since GemFire 7.0
 */
public class ResultBuilder {
  public static final int CODE_SHELLCLIENT_ABORT_OP   = 110;
//  public static final int OKCODE  = 200;
  
  // error on gfsh
  public static final int ERRORCODE_DEFAULT           = 400;
  public static final int ERRORCODE_CONNECTION_ERROR  = 405;
  public static final int ERRORCODE_SHELLCLIENT_ERROR = 410;
  public static final int ERRORCODE_UNAUTHORIZED      = 415;
  
  // errors on member
  public static final int ERRORCODE_PARSING_ERROR     = 501;
  public static final int ERRORCODE_GEMFIRE_ERROR     = 505;
  public static final int ERRORCODE_BADRESPONSE_ERROR = 510;
  public static final int ERRORCODE_BADCONFIG_ERROR   = 515;
  public static final int ERRORCODE_USER_ERROR        = 520;
  
  // Result with constant message & error code
  public static final Result ERROR_RESULT_DEFAULT = 
                createErrorResult(ERRORCODE_DEFAULT, 
                                  "Error occurred while executing command.");
  
  /**
   * Method for convenience to create error result for connection error.
   * <p/>
   * Note: To build your own error result, use {@link #createErrorResultData()}
   * to build {@link ErrorResultData} & then use
   * {@link #buildResult(ResultData)}
   * 
   * @param message
   *          Message to be shown to the user
   * @return Result for connection error
   */
  public static Result createConnectionErrorResult(String message) {
    String errorMessage = message != null ? message: "Connection Error occurred."; 
    
    return createErrorResult(ERRORCODE_CONNECTION_ERROR, errorMessage);
  }
  
  public static Result createShellClientErrorResult(String message) {
    return createErrorResult(ERRORCODE_SHELLCLIENT_ERROR, message);
  }

  public static Result createShellClientAbortOperationResult(String message) {
    return createErrorResult(CODE_SHELLCLIENT_ABORT_OP, message);
  }

  /**
   * Method for convenience to create error result for parsing error.
   * <p/>
   * Note: To build your own error result, use {@link #createErrorResultData()} 
   * to build {@link ErrorResultData} & then use
   * {@link #buildResult(ResultData)}
   * 
   * @param message
   *          Message to be shown to the user
   * @return Result for parsing error
   */
  public static Result createParsingErrorResult(String message) {
    return createErrorResult(ERRORCODE_PARSING_ERROR, 
                  "Could not parse command string. "+message);
  }
  
  public static Result createBadConfigurationErrorResult(String message) {
    return createErrorResult(ERRORCODE_BADCONFIG_ERROR, 
                  "Configuration error. " + message);
  }

  /**
   * Method for convenience to create error result for error in GemFire while
   * executing command.
   * <p/>
   * Note: To build your own error result, use {@link #createErrorResultData()} 
   * to build {@link ErrorResultData} & then use
   * {@link #buildResult(ResultData)}
   * 
   * @param message
   *          Message to be shown to the user
   * @return Result for error in GemFire while executing command.
   */
  public static Result createGemFireErrorResult(String message) {
    return createErrorResult(ERRORCODE_GEMFIRE_ERROR, 
                  "Could not process command due to GemFire error. " + message);
  }

  public static Result createGemFireUnAuthorizedErrorResult(String message) {
    return createErrorResult(ERRORCODE_UNAUTHORIZED, message);
  }

  public static Result createUserErrorResult(String message) {
    return createErrorResult(ERRORCODE_USER_ERROR, message);
  }

  /**
   * Method for convenience to create error result for unreadable command
   * response.
   * <p/>
   * Note: To build your own error result, use {@link #createErrorResultData()} 
   * to build {@link ErrorResultData} & then use
   * {@link #buildResult(ResultData)}
   * 
   * @param message
   *          Message to be shown to the user
   * @return Result for unreadable command response.
   */
  public static Result createBadResponseErrorResult(String message) {
    return createErrorResult(ERRORCODE_BADRESPONSE_ERROR, 
                  "Could not read command response. " + message);
  }
  
  /**
   * Creates error Result with given error code & message
   * <p/>
   * Note: To build your own error result, use {@link #createErrorResultData()} 
   * to build {@link ErrorResultData} & then use
   * {@link #buildResult(ResultData)}
   * 
   * @param errorCode
   *          error code should be one of ResultBuilder.ERRORCODE_**
   * @param message
   *          message for the error
   * @return Result object with the given error code & message. If there's an
   *         exception while building result object, returns
   *         {@link #ERROR_RESULT_DEFAULT}
   */
  private static Result createErrorResult(int errorCode, String message) {
    ErrorResultData errorResultData = new ErrorResultData();
    errorResultData.setErrorCode(errorCode);
    errorResultData.addLine(message);
    return buildResult(errorResultData);
  }
  
  /**
   * Convenience method to create a simple Info Result that takes a message.
   * 
   * @param message
   *          Message for the OK Result
   * @return Result of InfoResultData type
   */
  public static Result createInfoResult(String message) {    
    InfoResultData infoResultData = new InfoResultData();
    infoResultData.addLine(message);
    return buildResult(infoResultData);
  }
  
  /**
   * Creates a {@link TabularResultData} object to start building result that
   * should be shown in a Tabular Format.
   * 
   * @return TabularResultData instance
   */
  public static TabularResultData createTabularResultData() {
    return new TabularResultData();
  }
  
  public static CompositeResultData createCompositeResultData() {
    return new CompositeResultData();
  }
  
  public static <T extends CliJsonSerializable> ObjectResultData<T> createObjectResultData() {
    return new ObjectResultData<T>();
  }
  
//  public static CatalogedResultData createCatalogedResultData() {
//    return new CatalogedResultData();
//  }
  
  /**
   * Creates a {@link InfoResultData} object to start building result that
   * is required to be shown as an information without any specific format.
   * 
   * @return InfoResultData instance
   */
  public static InfoResultData createInfoResultData() {
    return new InfoResultData();
  }
  
  /**
   * Creates a {@link ErrorResultData} object to start building result for an
   * error.
   * 
   * @return ErrorResultData instance
   */
  public static ErrorResultData createErrorResultData() {
    return new ErrorResultData();
  }
  
  /**
   * Build a Result object from the given ResultData
   * 
   * @param resultData
   *          data to use to build Result
   * @return Result object built from the given ResultData
   */
  public static Result buildResult(ResultData resultData) {
    return new CommandResult(resultData);
  }
  
  /**
   * Prepare Result from JSON. Type of result is expected to there in the JSON
   * as 'contentType' which should be one of {@link ResultData#TYPE_TABULAR},
   * {@link ResultData#TYPE_COMPOSITE}, {@link ResultData#TYPE_INFO},
   * {@link ResultData#TYPE_ERROR}, {@link ResultData#TYPE_OBJECT}.
   * 
   * @param gfJsonObject
   *          GemFire JSON Object to use to prepare Result
   * @return Result from the given GemFire JSON Object
   */
  public static Result fromJson(GfJsonObject gfJsonObject) {
    return fromJson(gfJsonObject.toString());
  }
  
  
  /**
   * Prepare a Result object from a JSON String. This is useful on gfsh/client
   * to read the response & prepare a Result object from the JSON response
   * 
   * @param json
   *          JSON string for Result
   * @return Result object prepare from the JSON string. If it fails, creates an
   *         error Result for Bad Response.
   */
  public static Result fromJson(String json) {
    Result result = null;
    try {
      GfJsonObject jsonObject  = new GfJsonObject(json);
      String       contentType = jsonObject.getString("contentType");
      GfJsonObject data        = jsonObject.getJSONObject("data");
      
      AbstractResultData resultData = null;
      if (ResultData.TYPE_TABULAR.equals(contentType)) {
        resultData = new TabularResultData(data);
      } /*else if (ResultData.TYPE_CATALOGED.equals(contentType)) {
        resultData = new CatalogedResultData(new GfJsonObject(String.valueOf(content)));
      }*/ else if (ResultData.TYPE_INFO.equals(contentType)) {
        resultData = new InfoResultData(data);
      } else if (ResultData.TYPE_ERROR.equals(contentType)) {
        resultData = new ErrorResultData(data);
      } else if (ResultData.TYPE_COMPOSITE.equals(contentType)) {
        resultData = new CompositeResultData(data);
      } else if (ResultData.TYPE_OBJECT.equals(contentType)) {
        resultData = new ObjectResultData<CliJsonSerializable>(data);
      } else {
        ErrorResultData errorResultData = new ErrorResultData();
        errorResultData.addLine("Can not detect result type, unknown response format: "+json);
        resultData = errorResultData;
      }
      
      result = buildResult(resultData);
      
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
      //TODO - Abhishek - what to do with incoming files??
    }
    
    return builder.toString();
  }

  /**
   * Wraps a given ResultData and wraps it into appropriate ResultData of the
   * same type but the returned object is immutable & throws
   * UnsupportedOperationException on invoking those methods.
   * 
   * @param resultData to be wrapped
   * @return Read only ResultData of the same type
   */
  static ResultData getReadOnlyResultData(ResultData resultData) {
    ResultData wrapperResultData = null;
    String contentType = resultData.getType();
    if (ResultData.TYPE_TABULAR.equals(contentType)) {
      wrapperResultData = new TabularResultData(resultData.getGfJsonObject()) {
        @Override
        public ResultData addAsFile(String fileName, byte[] data, int fileType,
            String message, boolean addTimeStampToName) {
          throw new UnsupportedOperationException("This is read only result data");
        }
        
        @Override
        public ResultData addAsFile(String fileName, String fileContents,
            String message, boolean addTimeStampToName) {
          throw new UnsupportedOperationException("This is read only result data");
        }
        
        @Override
        public ResultData addByteDataFromFileFile(String filePath,
            int fileType, String message, boolean addTimeStampToName)
            throws FileNotFoundException, IOException {
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
        public ResultData addAsFile(String fileName, byte[] data, int fileType,
            String message, boolean addTimeStampToName) {
          throw new UnsupportedOperationException("This is read only result data");
        }
        
        @Override
        public ResultData addAsFile(String fileName, String fileContents,
            String message, boolean addTimeStampToName) {
          throw new UnsupportedOperationException("This is read only result data");
        }
        
        @Override
        public ResultData addByteDataFromFileFile(String filePath,
            int fileType, String message, boolean addTimeStampToName)
            throws FileNotFoundException, IOException {
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
        public ResultData addAsFile(String fileName, byte[] data, int fileType,
            String message, boolean addTimeStampToName) {
          throw new UnsupportedOperationException("This is read only result data");
        }
        
        @Override
        public ResultData addAsFile(String fileName, String fileContents,
            String message, boolean addTimeStampToName) {
          throw new UnsupportedOperationException("This is read only result data");
        }
        
        @Override
        public ResultData addByteDataFromFileFile(String filePath,
            int fileType, String message, boolean addTimeStampToName)
            throws FileNotFoundException, IOException {
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
        public ResultData addAsFile(String fileName, byte[] data, int fileType,
            String message, boolean addTimeStampToName) {
          throw new UnsupportedOperationException("This is read only result data");
        }
        
        @Override
        public ResultData addAsFile(String fileName, String fileContents,
            String message, boolean addTimeStampToName) {
          throw new UnsupportedOperationException("This is read only result data");
        }
        
        @Override
        public ResultData addByteDataFromFileFile(String filePath,
            int fileType, String message, boolean addTimeStampToName)
            throws FileNotFoundException, IOException {
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
        
        public CompositeResultData addSeparator(char buildSeparatorFrom) {
          throw new UnsupportedOperationException("This is read only result data");
        }
      };
    } else if (ResultData.TYPE_OBJECT.equals(contentType)) {
      final ObjectResultData<?> wrapped = (ObjectResultData<?>) resultData;
      wrapperResultData = new ObjectResultData<CliJsonSerializable>() {
        @Override
        public ResultData addAsFile(String fileName, byte[] data, int fileType,
            String message, boolean addTimeStampToName) {
          throw new UnsupportedOperationException("This is read only result data");
        }
        
        @Override
        public ResultData addAsFile(String fileName, String fileContents,
            String message, boolean addTimeStampToName) {
          throw new UnsupportedOperationException("This is read only result data");
        }
        
        @Override
        public ResultData addByteDataFromFileFile(String filePath,
            int fileType, String message, boolean addTimeStampToName)
            throws FileNotFoundException, IOException {
          throw new UnsupportedOperationException("This is read only result data");
        }
        
        @Override
        public ObjectResultData<CliJsonSerializable> addCollection(
            Collection<CliJsonSerializable> infoBeans) {
          throw new UnsupportedOperationException("This is read only result data");
        }
        
        @Override
        public ObjectResultData<CliJsonSerializable> addObject(CliJsonSerializable infoBean) {
          throw new UnsupportedOperationException("This is read only result data");
        }
        
        @Override
        public List<CliJsonSerializable> getAllObjects() {
          return wrapped.getAllObjects();
        }
      };
    } else {
      ErrorResultData errorResultData = new ErrorResultData();
      errorResultData.addLine("Can not detect result type, unknown result data format for: "+resultData);
      wrapperResultData = errorResultData;
    }
    
    return wrapperResultData;
  }
}

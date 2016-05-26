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

import com.gemstone.gemfire.management.cli.Result.Status;
import com.gemstone.gemfire.management.internal.cli.json.GfJsonException;
import com.gemstone.gemfire.management.internal.cli.json.GfJsonObject;

/**
 * 
 * 
 * @since GemFire 7.0
 */
public class ErrorResultData extends InfoResultData {
  private static final String ERROR_CODE = "errorCode";
  
  /*package*/ErrorResultData() {
    super();
  }
  
  // Useful on client/gfsh side to reconstruct the object
  /*package*/ErrorResultData(GfJsonObject gfJsonObject) {
    super(gfJsonObject);
  }
  
  public int getErrorCode() {
    Integer code = (Integer) contentObject.get(ERROR_CODE);
    if(code==null){
      return ResultBuilder.ERRORCODE_DEFAULT;
    }
    return code;
  }
  
  /**
   * 
   * @param errorCode
   * @return this ErrorResultData
   * @throws ResultDataException
   *           If the errorCode value is a non-finite number or invalid.
   */
  public ErrorResultData setErrorCode(int errorCode) {
    try {
      contentObject.putOpt(ERROR_CODE, errorCode);
    } catch (GfJsonException e) {
      throw new ResultDataException(e.getMessage());
    }
    
    return this;
  }

  /**
   * 
   * @param headerText
   * @return this ErrorResultData
   * @throws ResultDataException 
   */
  public ErrorResultData setHeader(String headerText) {
    return (ErrorResultData) super.setHeader(headerText);
  }
  
  /**
   * 
   * @param line message to add
   * @return this ErrorResultData
   */
  public ErrorResultData addLine(String line) {
    return (ErrorResultData) super.addLine(line);
  }
  
  /**
   * 
   * @param footerText
   * @return this InfoResultData
   * @throws ResultDataException 
   */
  public ErrorResultData setFooter(String footerText) {    
    return (ErrorResultData) super.setFooter(footerText);
  }

  public String getType() {
    return TYPE_ERROR;
  }
  
  @Override
  public void setStatus(final Status status) {
    throw new UnsupportedOperationException("The status of an ErrorResultData result must always be ERROR");
  }
  
  @Override
  public Status getStatus() {
    return Status.ERROR;
  }
}

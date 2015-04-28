/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.result;

import com.gemstone.gemfire.management.cli.Result.Status;
import com.gemstone.gemfire.management.internal.cli.json.GfJsonException;
import com.gemstone.gemfire.management.internal.cli.json.GfJsonObject;

/**
 * 
 * @author Abhishek Chaudhari
 * 
 * @since 7.0
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
  
  public int getErrorCode(int errorCode) {
    return (Integer) contentObject.get(ERROR_CODE);
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

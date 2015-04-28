/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.result;

import com.gemstone.gemfire.management.internal.cli.json.GfJsonException;
import com.gemstone.gemfire.management.internal.cli.json.GfJsonObject;

/**
 * 
 * @author Abhishek Chaudhari
 * 
 * @since 7.0
 */
public class InfoResultData extends AbstractResultData {
  public static final String RESULT_CONTENT_MESSAGE = "message";
  /*package*/InfoResultData() {
    super();  
  }
  
  /*package*/InfoResultData(GfJsonObject gfJsonObject) {
    super(gfJsonObject);
  }
  
  /**
   * 
   * @param headerText
   * @return this InfoResultData
   * @throws ResultDataException 
   */
  public InfoResultData setHeader(String headerText) {
    return (InfoResultData) super.setHeader(headerText);
  }
  
  /**
   * 
   * @param line message to add
   * @return this InfoResultData
   */
  public InfoResultData addLine(String line) {
    try {
      contentObject.accumulate(RESULT_CONTENT_MESSAGE, line);
    } catch (GfJsonException e) {
      throw new ResultDataException(e.getMessage());
    }
    
    return this;
  }
  
  /**
   * 
   * @param footerText
   * @return this InfoResultData
   * @throws ResultDataException 
   */
  public InfoResultData setFooter(String footerText) {    
    return (InfoResultData) super.setFooter(footerText);
  }

  /**
   * @return the gfJsonObject
   */
  public GfJsonObject getGfJsonObject() {
    return gfJsonObject;
  }

  public String getType() {
    return TYPE_INFO;
  }

  public String getHeader() {
    return gfJsonObject.getString(RESULT_HEADER);
  }

  public String getFooter() {
    return gfJsonObject.getString(RESULT_FOOTER);
  }
}

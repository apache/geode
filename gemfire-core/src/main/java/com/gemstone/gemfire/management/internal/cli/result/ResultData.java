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
import com.gemstone.gemfire.management.internal.cli.json.GfJsonObject;

/**
 * 
 * @author Abhishek Chaudhari
 * 
 * @since 7.0
 */
public interface ResultData {
  String RESULT_HEADER  = "header";
  String RESULT_CONTENT = "content";
  String RESULT_FOOTER  = "footer";
  

//  String TYPE_CATALOGED = "catalog";
  String TYPE_COMPOSITE = "composite";
  String TYPE_ERROR     = "error";
  String TYPE_INFO      = "info";
  String TYPE_OBJECT    = "object";
//  String TYPE_SECTION   = "composite-section";
  String TYPE_TABULAR   = "table";
  
  String getHeader();
  
  String getFooter();
  
  GfJsonObject getGfJsonObject();
  
  String getType();
  
  public Status getStatus();
  
  public void setStatus(final Status status);
}

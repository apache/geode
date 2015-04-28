/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.result;

import java.util.Map;

import com.gemstone.gemfire.management.internal.cli.json.GfJsonObject;

/**
 * 
 * @author Abhishek Chaudhari
 * 
 * @since 7.0
 */
public interface CliJsonSerializable extends CliJsonSerializableIds {
  String FIELDS_TO_SKIP                = "fieldNameToDisplayName, JSId, outputFormat, fieldsToSkipOnUI";
  String JSID                          = "JSId";
  String FIELDS_TO_DISPLAYNAME_MAPPING = "fieldNameToDisplayName";
  String FIELDS_TO_SKIP_ON_UI          = "fieldsToSkipOnUI";
  String OUTPUT_FORMAT                 = "outputFormat";
  
  int getJSId();
  
  Map<String, String> getFieldNameToDisplayName();
  
  String[] getFieldsToSkipOnUI();
  
  void setFieldsToSkipOnUI(String ... fieldsToSkipOnUI);
  
  void fromJson(GfJsonObject objectStateAsjson);
}

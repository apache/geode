/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.result;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.gemstone.gemfire.management.internal.cli.json.GfJsonArray;
import com.gemstone.gemfire.management.internal.cli.json.GfJsonException;
import com.gemstone.gemfire.management.internal.cli.json.GfJsonObject;

/**
 * 
 * @author Abhishek Chaudhari
 * 
 * @since 7.0
 */
public class TabularResultData extends AbstractResultData {
  /*package*/TabularResultData() {
    super();
  }
  
  /*package*/TabularResultData(GfJsonObject gfJsonObject) {
    super(gfJsonObject);
  }

  public TabularResultData accumulate(String accumulateFor, Object value) {
    try {
      contentObject.accumulate(accumulateFor, value);
    } catch (GfJsonException e) {
      throw new ResultDataException(e.getMessage());
    }
    return this;
  }
  
  public GfJsonArray getHeaders() {
    try {
      return this.contentObject.names();
    } catch (GfJsonException e) {
      e.printStackTrace();
    }
    return null;
  }

  /**
   * @return the gfJsonObject
   */
  public GfJsonObject getGfJsonObject() {
    return gfJsonObject;
  }

  @Override
  public String getType() {
    return TYPE_TABULAR;
  }

  /**
   * 
   * @param headerText
   * @return this TabularResultData
   * @throws ResultDataException
   *           If the value is non-finite number or if the key is null.
   */
  public TabularResultData setHeader(String headerText) {
    return (TabularResultData) super.setHeader(headerText);
  }
  
  /**
   * 
   * @param footerText
   * @return this TabularResultData
   * @throws ResultDataException
   *           If the value is non-finite number or if the key is null.
   */
  public TabularResultData setFooter(String footerText) {
    return (TabularResultData) super.setFooter(footerText);
  }

  @Override
  public String getHeader() {
    return gfJsonObject.getString(RESULT_HEADER);
  }

  @Override
  public String getFooter() {
    return gfJsonObject.getString(RESULT_FOOTER);
  }
  
  public Map<String, String> retrieveDataByValueInColumn(String columnName, String valueToSearch) {
    Map<String, String> foundValues = Collections.emptyMap();
    try {
      GfJsonArray jsonArray = contentObject.getJSONArray(columnName);
      int size = jsonArray.size();
      int foundIndex = -1;
      for (int i = 0; i < size; i++) {
        Object object = jsonArray.get(i);
        if (object != null && object.equals(valueToSearch)) {
          foundIndex = i;
          break;
        }
      }
      
      if (foundIndex != -1) {
        foundValues = new LinkedHashMap<String, String>();
        for (Iterator<String> iterator = contentObject.keys(); iterator.hasNext();) {
          String storedColumnNames = (String) iterator.next();
          GfJsonArray storedColumnValues = contentObject.getJSONArray(storedColumnNames);
          foundValues.put(storedColumnNames, String.valueOf(storedColumnValues.get(foundIndex)));
        }
      }
    } catch (GfJsonException e) {
      throw new ResultDataException(e.getMessage());
    }
    return foundValues;
  }
  
  public List<Map<String, String>> retrieveAllDataByValueInColumn(String columnName, String valueToSearch) {
    List<Map<String, String>> foundValuesList = new ArrayList<Map<String,String>>();
    try {
      GfJsonArray jsonArray = contentObject.getJSONArray(columnName);
      int size = jsonArray.size();
      for (int i = 0; i < size; i++) {
        Object object = jsonArray.get(i);
        if (object != null && object.equals(valueToSearch)) {
          Map<String, String> foundValues = new LinkedHashMap<String, String>();

          for (Iterator<String> iterator = contentObject.keys(); iterator.hasNext();) {
            String storedColumnNames = (String) iterator.next();
            GfJsonArray storedColumnValues = contentObject.getJSONArray(storedColumnNames);
            foundValues.put(storedColumnNames, String.valueOf(storedColumnValues.get(i)));
          }
          
          foundValuesList.add(foundValues);
        }
      }
    } catch (GfJsonException e) {
      throw new ResultDataException(e.getMessage());
    }
    return foundValuesList;
  }
  
  public List<String> retrieveAllValues(String columnName) {
    List<String> values = new ArrayList<String>();

    try {
      GfJsonArray jsonArray = contentObject.getJSONArray(columnName);
      int size = jsonArray.size();
      for (int i = 0; i < size; i++) {
        values.add(String.valueOf(jsonArray.get(i)));
      }
    } catch (GfJsonException e) {
      throw new ResultDataException(e.getMessage());
    }
    return values;
  }
}

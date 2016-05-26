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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.zip.DataFormatException;

import com.gemstone.gemfire.management.cli.Result;
import com.gemstone.gemfire.management.internal.cli.GfshParser;
import com.gemstone.gemfire.management.internal.cli.LogWrapper;
import com.gemstone.gemfire.management.internal.cli.json.GfJsonArray;
import com.gemstone.gemfire.management.internal.cli.json.GfJsonException;
import com.gemstone.gemfire.management.internal.cli.json.GfJsonObject;
import com.gemstone.gemfire.management.internal.cli.result.TableBuilder.Row;
import com.gemstone.gemfire.management.internal.cli.result.TableBuilder.RowGroup;
import com.gemstone.gemfire.management.internal.cli.result.TableBuilder.Table;

/**
 * Wraps the Result of a command execution. 
 * 
 * @since GemFire 7.0
 */
//Should this have info about the command String??
public class CommandResult implements Result {
  
  private GfJsonObject gfJsonObject;
  private Status       status;
  private int          index;
  private boolean      isDataBuilt;
  
  private ResultData   resultData;
  private List<String> resultLines;
  private boolean failedToPersist = false;
  
  private transient int numTimesSaved;
  
  
  public CommandResult(ResultData resultData) {
    this.resultData   = resultData;
    this.gfJsonObject = this.resultData.getGfJsonObject();
    this.status       = this.resultData.getStatus();
    this.resultLines  = new Vector<String>();
  }

  @Override
  public Status getStatus() {
    return this.status;
  }
  
  public ResultData getResultData() {
    return ResultBuilder.getReadOnlyResultData(resultData);
  }
  
  GfJsonObject getGfJsonObject() {
    return gfJsonObject;
  }

  @Override
  public void resetToFirstLine() {
    index = 0;
  }
  
  //TODO -Abhishek - extract this code out in a FormatBuilder or PresentationBuilder??
  private void buildData() {
    try {
      if (resultData.getType() == ResultData.TYPE_OBJECT) {
        buildObjectResultOutput();
      } else if (resultData.getType() == ResultData.TYPE_COMPOSITE) {
        buildComposite();
      } else {
        GfJsonObject content = getContent();
        if (content != null) {
          Table resultTable = TableBuilder.newTable();

          addHeaderInTable(resultTable, getGfJsonObject());

          RowGroup rowGroup = resultTable.newRowGroup();
          
          if (resultData.getType() == ResultData.TYPE_TABULAR) {
    //        resultTable.setColumnSeparator(" | ");
            resultTable.setColumnSeparator("   ");            
            resultTable.setTabularResult(true);
            buildTable(rowGroup, content);
          } else {
            buildInfoErrorData(rowGroup, content);
          }
          
          addFooterInTable(resultTable, getGfJsonObject());
  
          resultLines.addAll(resultTable.buildTableList());
        }
      }
    } catch (GfJsonException e) {
      resultLines.add("Error occurred while processing Command Result. Internal Error - Invalid Result.");
      //TODO - Abhishek. Add stack trace when 'debug' is enabled. Log to LogWrapper always 
    } finally {
      isDataBuilt = true;
    }
  }

  private void addHeaderInTable(Table resultTable, GfJsonObject fromJsonObject) {
    String header = getHeader(fromJsonObject);
    if (header != null && !header.isEmpty()) {
      resultTable.newRow().newLeftCol(header);
    }
  }

  private void addHeaderInRowGroup(RowGroup rowGroup, GfJsonObject fromJsonObject) {
    String header = getHeader(fromJsonObject);
    if (header != null && !header.isEmpty()) {
      rowGroup.newRow().newLeftCol(header);
    }
  }

  private void addFooterInTable(Table resultTable, GfJsonObject fromJsonObject) {
    String footer = getFooter(fromJsonObject);
    if (footer != null && !footer.isEmpty()) {
      resultTable.newRow().newLeftCol(footer);
    }
  }

  private void addFooterInRowGroup(RowGroup rowGroup, GfJsonObject fromJsonObject) {
    String footer = getFooter(fromJsonObject);
    if (footer != null && !footer.isEmpty()) {
      rowGroup.newRow().newLeftCol(footer);
    }
  }

  private void buildInfoErrorData(RowGroup rowGroup, GfJsonObject content) throws GfJsonException {
    GfJsonArray accumulatedData = content.getJSONArray(InfoResultData.RESULT_CONTENT_MESSAGE);
    if (accumulatedData != null) {
      buildRows(rowGroup, null, accumulatedData);
    }
  }
  
  /*private*/ void buildObjectResultOutput() {
    try {
      Table resultTable = TableBuilder.newTable();
      resultTable.setColumnSeparator(" : ");

      addHeaderInTable(resultTable, getGfJsonObject());
      
      GfJsonObject content = getContent();
      
      GfJsonArray objectsArray = content.getJSONArray(ObjectResultData.OBJECTS_ACCESSOR);
      if (objectsArray != null) {
        int numOfObjects = objectsArray.size();
        
        for (int i = 0; i < numOfObjects; i++) {
          GfJsonObject object  = objectsArray.getJSONObject(i);
          buildObjectSection(resultTable, null, object, 0);
        }
      } /*else {
//        GfJsonObject jsonObject = content.getJSONObject(ObjectResultData.ROOT_OBJECT_ACCESSOR);
//        buildObjectSection(resultTable, null, jsonObject, 0);
      }*/

      addFooterInTable(resultTable, getGfJsonObject());

      resultLines.addAll(resultTable.buildTableList());
      
    } catch (GfJsonException e) {
      resultLines.add("Error occurred while processing Command Result. Internal Error - Invalid Result.");
      //TODO - Abhishek. Add stack trace when 'debug' is enabled. Log to LogWrapper always
    } finally {
      isDataBuilt = true;
    }
  }
  
  private void buildObjectSection(Table table, RowGroup parentRowGroup, GfJsonObject object, int depth) throws GfJsonException {
    Iterator<String> keys = object.keys();
    RowGroup rowGroup = null;
    if (parentRowGroup != null) {
      rowGroup = parentRowGroup;
    } else {
      rowGroup = table.newRowGroup();
    }
    GfJsonArray  nestedCollection = null;    
    GfJsonObject nestedObject     = null;
    
    GfJsonObject fieldDisplayNames = object.getJSONObject(CliJsonSerializable.FIELDS_TO_DISPLAYNAME_MAPPING);
    
    List<String> fieldsToSkipOnUI = null;
    if (object.has(CliJsonSerializable.FIELDS_TO_SKIP_ON_UI)) {
      GfJsonArray jsonArray = object.getJSONArray(CliJsonSerializable.FIELDS_TO_SKIP_ON_UI);;
      fieldsToSkipOnUI = new ArrayList<String>();
      for (int i = 0; i < jsonArray.size(); i++) {
        fieldsToSkipOnUI.add(String.valueOf(jsonArray.get(i)));
      }
    }
    
    while (keys.hasNext()) {
      String key = keys.next();
      
      if (CliJsonSerializable.FIELDS_TO_SKIP.contains(key) || (fieldsToSkipOnUI != null && fieldsToSkipOnUI.contains(key))) {
        continue;
      }
      
      try {
        nestedCollection = object.getJSONArray(key);
      } catch (GfJsonException e) {/* next check if it's a nested object  */}
      
      Object field = null;
      if (nestedCollection == null) { 
        field = object.get(key);
        if (!isPrimitiveOrStringOrWrapper(field)) {
          nestedObject = object.getJSONObject(key);
        }
      }
      if (nestedCollection != null && isPrimitiveOrStringOrWrapperArray(nestedCollection)) {
        String str = nestedCollection.toString();
        field = str.substring(1, str.length() - 1);
        nestedCollection = null;
      }

      Row newRow = rowGroup.newRow();
      String prefix = "";
      /*if (nestedCollection != null) */{
        for (int i = 0; i < depth; i++) {
          prefix += " . ";
        }
      }
      String fieldNameToDisplay = fieldDisplayNames.getString(key);
      
      if (nestedCollection == null) {
        newRow.newLeftCol(prefix + fieldNameToDisplay);
      }
      
      if (nestedCollection != null) {
        Map<String, Integer> columnsMap = new HashMap<String, Integer>();
        
        GfJsonArray rowsArray = nestedCollection;
        RowGroup newRowGroup = table.newRowGroup();
        newRowGroup.setColumnSeparator(" | ");
        newRowGroup.newBlankRow();
        newRowGroup.newRow().newLeftCol(fieldNameToDisplay);
        Row headerRow = newRowGroup.newRow();
        
        int numOfRows = rowsArray.size();
        List<String> tableFieldsToSkipOnUI = null;
        for (int j = 0; j < numOfRows; j++) {
          GfJsonObject content = rowsArray.getJSONObject(j);
          if (content.has(CliJsonSerializable.FIELDS_TO_SKIP_ON_UI)) {
            GfJsonArray jsonArray = content.getJSONArray(CliJsonSerializable.FIELDS_TO_SKIP_ON_UI);
            tableFieldsToSkipOnUI = new ArrayList<String>();
            for (int i = 0; i < jsonArray.size(); i++) {
              tableFieldsToSkipOnUI.add(String.valueOf(jsonArray.get(i)));
            }
          }
          GfJsonArray columnNames = content.names();
          int numOfColumns = columnNames.size();
          
          if (headerRow.isEmpty()) {
            GfJsonObject innerFieldDisplayNames = content.getJSONObject(CliJsonSerializable.FIELDS_TO_DISPLAYNAME_MAPPING);
            for (int i = 0; i < numOfColumns; i++) {
              
              Object columnName = columnNames.get(i);
              if (CliJsonSerializable.FIELDS_TO_SKIP.contains((String)columnName) || (tableFieldsToSkipOnUI != null && tableFieldsToSkipOnUI.contains(columnName))) {
                // skip file data if any //TODO - make response format better
                continue;
              }
              
              headerRow.newCenterCol(innerFieldDisplayNames.getString((String)columnName));
              columnsMap.put((String) columnName, i);
            }
            newRowGroup.newRowSeparator('-', false);
          }
          newRow = newRowGroup.newRow();
          for (int i = 0; i < numOfColumns; i++) {
            
            Object columnName = columnNames.get(i);
            if (CliJsonSerializable.FIELDS_TO_SKIP.contains((String)columnName) || (tableFieldsToSkipOnUI != null && tableFieldsToSkipOnUI.contains(columnName))) {
              // skip file data if any //TODO - make response format better
              continue;
            }
            newRow.newLeftCol(String.valueOf(content.get((String) columnName)));
          }
        }
      } else if (nestedObject != null) {
        buildObjectSection(table, rowGroup, nestedObject, depth+1);
      } else {
//        Row newRow = rowGroup.newRow();
//        String prefix = "";
//        for (int i = 0; i < depth; i++) {
//          prefix += " . ";
//        }
//        newRow.newLeftCol(prefix+fieldDisplayNames.getString(key)).newLeftCol(field);
        Object value = field;
/*        if (isPrimitiveOrStringOrWrapperArray(value)) {
          value = Arrays.toString((String[])value);
        }*/
        newRow.newLeftCol(value);
      }
      nestedCollection = null;
      nestedObject = null;
    }
  }
  
  private boolean isPrimitiveOrStringOrWrapper(Object object) {
    boolean isPrimitive = false;
    if (String.class.isInstance(object)) {
      isPrimitive = true;
    } else if (byte.class.isInstance(object) || Byte.class.isInstance(object)) {
      isPrimitive = true;
    } else if (short.class.isInstance(object) || Short.class.isInstance(object)) {
      isPrimitive = true;
    } else if (int.class.isInstance(object) || Integer.class.isInstance(object)) {
      isPrimitive = true;
    } else if (long.class.isInstance(object) || Long.class.isInstance(object)) {
      isPrimitive = true;
    } else if (float.class.isInstance(object) || Float.class.isInstance(object)) {
      isPrimitive = true;
    } else if (double.class.isInstance(object) || Double.class.isInstance(object)) {
      isPrimitive = true;
    } else if (boolean.class.isInstance(object) || Boolean.class.isInstance(object)) {
      isPrimitive = true;
    } else if (char.class.isInstance(object) || Character.class.isInstance(object)) {
      isPrimitive = true;
    }
    
    return isPrimitive;
  }
  
  private boolean isPrimitiveOrStringOrWrapperArray(Object object) {
    boolean isPrimitive = false;
    if (String[].class.isInstance(object)) {
      isPrimitive = true;
    } else if (byte[].class.isInstance(object) || Byte[].class.isInstance(object)) {
      isPrimitive = true;
    } else if (short[].class.isInstance(object) || Short[].class.isInstance(object)) {
      isPrimitive = true;
    } else if (int[].class.isInstance(object) || Integer[].class.isInstance(object)) {
      isPrimitive = true;
    } else if (long[].class.isInstance(object) || Long[].class.isInstance(object)) {
      isPrimitive = true;
    } else if (float[].class.isInstance(object) || Float[].class.isInstance(object)) {
      isPrimitive = true;
    } else if (double[].class.isInstance(object) || Double[].class.isInstance(object)) {
      isPrimitive = true;
    } else if (boolean[].class.isInstance(object) || Boolean[].class.isInstance(object)) {
      isPrimitive = true;
    } else if (char[].class.isInstance(object) || Character[].class.isInstance(object)) {
      isPrimitive = true;
    } else if (GfJsonArray.class.isInstance(object)) {
      GfJsonArray jsonArr = (GfJsonArray) object;
      try {
        isPrimitive = isPrimitiveOrStringOrWrapper(jsonArr.get(0));
      } catch (GfJsonException e) {
      }
    }
    
    return isPrimitive;
  }

  /*private*/ void buildComposite() {
    try {
      GfJsonObject content = getContent();
      if (content != null) {
        Table resultTable = TableBuilder.newTable();
        resultTable.setColumnSeparator(" : ");
  
        addHeaderInTable(resultTable, getGfJsonObject());
        
        for (Iterator<String> it = content.keys(); it.hasNext();) {
          String key = it.next();
          if (key.startsWith(CompositeResultData.SECTION_DATA_ACCESSOR)) {
            GfJsonObject subSection = content.getJSONObject(key);
            buildSection(resultTable, null, subSection, 0);
          } else if (key.equals(CompositeResultData.SEPARATOR)) {
            String separatorString = content.getString(key);
            resultTable.newRowGroup().newRowSeparator(separatorString.charAt(0), true);
          }
        }
  
        addFooterInTable(resultTable, getGfJsonObject());
        resultLines.addAll(resultTable.buildTableList());
      }
    } catch (GfJsonException e) {
      resultLines.add("Error occurred while processing Command Result. Internal Error - Invalid Result.");
      LogWrapper.getInstance().info("Error occurred while processing Command Result. Internal Error - Invalid Result.", e);
    } finally {
      isDataBuilt = true;
    }
  }
  
  private void buildSection(Table table, RowGroup parentRowGroup, GfJsonObject section, int depth) throws GfJsonException {
    Iterator<String> keys = section.keys();
    RowGroup rowGroup = null;
    if (parentRowGroup != null) {
      rowGroup = parentRowGroup;
    } else {
      rowGroup = table.newRowGroup();
    }
    addHeaderInRowGroup(rowGroup, section);
    while (keys.hasNext()) {
      String key = keys.next();
      Object object = section.get(key);
//      System.out.println(key +" : " + object);
      if (key.startsWith(CompositeResultData.TABLE_DATA_ACCESSOR)) {
        GfJsonObject tableObject = section.getJSONObject(key);

        addHeaderInTable(table, tableObject);

        RowGroup rowGroupForTable = table.newRowGroup();
        buildTable(rowGroupForTable, tableObject.getJSONObject(ResultData.RESULT_CONTENT));

        addFooterInTable(table, tableObject);
      } else if (key.startsWith(CompositeResultData.SECTION_DATA_ACCESSOR)) {
        GfJsonObject subSection = section.getJSONObject(key);
        buildSection(table, rowGroup, subSection, depth + 1);
      } else if (key.equals(CompositeResultData.SEPARATOR)) {
        String separatorString = section.getString(key);
        rowGroup.newRowSeparator(separatorString.charAt(0), true);
      } else if (key.equals(ResultData.RESULT_HEADER) || key.equals(ResultData.RESULT_FOOTER)) {
        // skip header & footer
        continue;
      } else {
        Row newRow = rowGroup.newRow();
        String prefix = "";
        for (int i = 0; i < depth; i++) {
          prefix += " . ";
        }
        String[] value = getValuesSeparatedByLines(object);
        if (value.length == 1) {
          newRow.newLeftCol(prefix+key).newLeftCol(value[0]);
        } else {
          if (value.length != 0) { // possible when object == CliConstants.LINE_SEPARATOR
            newRow.newLeftCol(prefix+key).newLeftCol(value[0]);
            for (int i = 1; i < value.length; i++) {
              newRow = rowGroup.newRow();
              newRow.setColumnSeparator("   ");
              newRow.newLeftCol("").newLeftCol(value[i]);
            }
          } else {
            newRow.newLeftCol(prefix+key).newLeftCol("");
          }
        }
//        System.out.println(key+" : "+object);
      }
    }
    addFooterInRowGroup(rowGroup, section);
  }
  
//  public static void main(String[] args) {
//    String[] valuesSeparatedByLines = getValuesSeparatedByLines(CliConstants.LINE_SEPARATOR);
//    System.out.println(valuesSeparatedByLines +" -- "+valuesSeparatedByLines.length);
//  }

  private static String[] getValuesSeparatedByLines(Object object) {
    String[] values = null;
    String valueString = String.valueOf(object);
    values = valueString.split(GfshParser.LINE_SEPARATOR);
    return values ;
  }

  /**
   * @param rowGroup
   * @param content
   * @throws GfJsonException
   */
  private void buildTable(RowGroup rowGroup, GfJsonObject content) throws GfJsonException {
    GfJsonArray columnNames = content.names();
    int numOfColumns = columnNames.size();
    Row headerRow = rowGroup.newRow();
    rowGroup.setColumnSeparator(" | ");
    rowGroup.newRowSeparator('-', false);
    
    // build Table Header first
    for (int i = 0; i < numOfColumns; i++) {
      Object object = columnNames.get(i);
      if (AbstractResultData.BYTE_DATA_ACCESSOR.equals(object)) {
        // skip file data if any //TODO - make response format better
        continue;
      }
      headerRow.newCenterCol((String) object);
    }
    
    // Build remaining rows by extracting data column-wise from JSON object
    Row[] dataRows = null;
    for (int i = 0; i < numOfColumns; i++) {
      Object object = columnNames.get(i);
      if (AbstractResultData.BYTE_DATA_ACCESSOR.equals(object)) {
        // skip file data if any //TODO - make response format better
        continue;
      }
      GfJsonArray accumulatedData = content.getJSONArray((String) object);
      
      dataRows = buildRows(rowGroup, dataRows, accumulatedData);
    }
  }

  /**
   * @param rowGroup
   * @param dataRows
   * @param accumulatedData
   * @return rows
   * @throws GfJsonException
   */
  private Row[] buildRows(RowGroup rowGroup, Row[] dataRows,
      GfJsonArray accumulatedData) throws GfJsonException {
    int size = accumulatedData.size();
    
    // Initialize rows' array as required 
    if (dataRows == null) {
      dataRows = new Row[size];
      
      for (int j = 0; j < dataRows.length; j++) {
        dataRows[j] = rowGroup.newRow();
      }
    }

    // Add data column-wise
    for (int j = 0; j < size; j++) {
      dataRows[j].newLeftCol(accumulatedData.get(j));
    }
    return dataRows;
  }
  
  public boolean hasIncomingFiles() {
    GfJsonArray fileDataArray = null;
    try {
      GfJsonObject content = getContent();
      if (content != null) {
        fileDataArray = content.getJSONArray(CompositeResultData.BYTE_DATA_ACCESSOR);
      }
    } catch (GfJsonException e) {
      e.printStackTrace();
    }
    return fileDataArray != null;
  }

  public int getNumTimesSaved() {
    return numTimesSaved;
  }

  public void saveIncomingFiles(String directory) throws IOException {
    // dump file data if any
    try {
      GfJsonObject content = getContent();
      if (content != null) {
        GfJsonArray bytesArray = content.getJSONArray(CompositeResultData.BYTE_DATA_ACCESSOR);
        AbstractResultData.readFileDataAndDump(bytesArray, directory);
      } else {
        throw new RuntimeException("No associated files to save .. "); // TODO Abhishek - add i18n string
      }
      numTimesSaved = numTimesSaved + 1;
    } catch (DataFormatException e) {
      throw new RuntimeException(e);
    } catch (GfJsonException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean hasNextLine() {
    if (!isDataBuilt) {
      buildData();
    }
    return index < resultLines.size();
  }

  /**
   * @throws ArrayIndexOutOfBoundsException
   *           if this method is called more number of times than the data items
   *           it contains
   */
  @Override
  public String nextLine() {
    if (!isDataBuilt) {
      buildData();
    }
    return resultLines.get(index++);
  }

  public String toJson() {
    return gfJsonObject.toString();
  }
  
  public String getType() {
    return resultData.getType();
  }

  public String getHeader() {
    return getHeader(gfJsonObject);
  }

  public String getHeader(GfJsonObject gfJsonObject) {
    return gfJsonObject.getString(ResultData.RESULT_HEADER);
  }
  
  public GfJsonObject getContent() throws GfJsonException {
    return gfJsonObject.getJSONObject(ResultData.RESULT_CONTENT);
  }
  
//  public String getContentStr() {
//    return gfJsonObject.getString(ResultData.RESULT_CONTENT);
//  }

  public String getFooter() {
    return getFooter(gfJsonObject);
  }

  public String getFooter(GfJsonObject gfJsonObject) {
    return gfJsonObject.getString(ResultData.RESULT_FOOTER);
  }
  
  @Override
  public boolean equals(Object obj) {
    if ( !(obj instanceof CommandResult) ) {
      return false;
    }
    CommandResult other = (CommandResult) obj;
    
    return this.gfJsonObject.toString().equals(other.gfJsonObject.toString());
  }

  public int hashCode() {
    return this.gfJsonObject.hashCode(); // any arbitrary constant will do
    
  }
  
  @Override
  public String toString() {
    return "CommandResult [gfJsonObject=" + gfJsonObject + ", status=" + status
        + ", index=" + index + ", isDataBuilt=" + isDataBuilt + ", resultData="
        + resultData + ", resultLines=" + resultLines + ", failedToPersist="
        + failedToPersist + "]";
  }

  @Override
  public boolean failedToPersist() {
    return this.failedToPersist;
  }

  @Override
  public void setCommandPersisted(boolean commandPersisted) {
    this.failedToPersist = !commandPersisted;
  }
  
}

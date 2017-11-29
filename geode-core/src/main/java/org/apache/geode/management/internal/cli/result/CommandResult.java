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

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.zip.DataFormatException;

import org.json.JSONArray;

import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.GfshParser;
import org.apache.geode.management.internal.cli.LogWrapper;
import org.apache.geode.management.internal.cli.json.GfJsonArray;
import org.apache.geode.management.internal.cli.json.GfJsonException;
import org.apache.geode.management.internal.cli.json.GfJsonObject;
import org.apache.geode.management.internal.cli.result.TableBuilder.Row;
import org.apache.geode.management.internal.cli.result.TableBuilder.RowGroup;
import org.apache.geode.management.internal.cli.result.TableBuilder.Table;

/**
 * Wraps the Result of a command execution.
 *
 * @since GemFire 7.0
 */

public class CommandResult implements Result {

  private GfJsonObject gfJsonObject;
  private Status status;
  private int index;
  private boolean isDataBuilt;

  private ResultData resultData;
  private List<String> resultLines;
  private boolean failedToPersist = false;

  private transient int numTimesSaved;

  public Path getFileToDownload() {
    return fileToDownload;
  }

  private Path fileToDownload;


  public CommandResult(ResultData resultData) {
    this.resultData = resultData;
    this.gfJsonObject = this.resultData.getGfJsonObject();
    this.status = this.resultData.getStatus();
    this.resultLines = new Vector<>();
  }

  public CommandResult(Path fileToDownload) {
    this(new InfoResultData(fileToDownload.toString()));
    this.fileToDownload = fileToDownload.toAbsolutePath();
  }

  public boolean hasFileToDownload() {
    return fileToDownload != null;
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

  private void buildData() {
    try {
      if (ResultData.TYPE_OBJECT.equals(resultData.getType())) {
        buildObjectResultOutput();
      } else if (ResultData.TYPE_COMPOSITE.equals(resultData.getType())) {
        buildComposite();
      } else {
        GfJsonObject content = getContent();
        if (content != null) {
          Table resultTable = TableBuilder.newTable();

          addHeaderInTable(resultTable, getGfJsonObject());

          RowGroup rowGroup = resultTable.newRowGroup();

          if (ResultData.TYPE_TABULAR.equals(resultData.getType())) {
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
      resultLines
          .add("Error occurred while processing Command Result. Internal Error - Invalid Result.");
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

  void buildObjectResultOutput() {
    try {
      Table resultTable = TableBuilder.newTable();
      resultTable.setColumnSeparator(" : ");

      addHeaderInTable(resultTable, getGfJsonObject());

      GfJsonObject content = getContent();

      GfJsonArray objectsArray = content.getJSONArray(ObjectResultData.OBJECTS_ACCESSOR);
      if (objectsArray != null) {
        int numOfObjects = objectsArray.size();

        for (int i = 0; i < numOfObjects; i++) {
          GfJsonObject object = objectsArray.getJSONObject(i);
          buildObjectSection(resultTable, null, object, 0);
        }
      }
      addFooterInTable(resultTable, getGfJsonObject());

      resultLines.addAll(resultTable.buildTableList());

    } catch (GfJsonException e) {
      resultLines
          .add("Error occurred while processing Command Result. Internal Error - Invalid Result.");
    } finally {
      isDataBuilt = true;
    }
  }

  private void buildObjectSection(Table table, RowGroup parentRowGroup, GfJsonObject object,
      int depth) throws GfJsonException {
    Iterator<String> keys = object.keys();
    RowGroup rowGroup;
    if (parentRowGroup != null) {
      rowGroup = parentRowGroup;
    } else {
      rowGroup = table.newRowGroup();
    }
    GfJsonArray nestedCollection = null;
    GfJsonObject nestedObject = null;

    GfJsonObject fieldDisplayNames =
        object.getJSONObject(CliJsonSerializable.FIELDS_TO_DISPLAYNAME_MAPPING);

    List<String> fieldsToSkipOnUI = null;
    if (object.has(CliJsonSerializable.FIELDS_TO_SKIP_ON_UI)) {
      GfJsonArray jsonArray = object.getJSONArray(CliJsonSerializable.FIELDS_TO_SKIP_ON_UI);
      fieldsToSkipOnUI = new ArrayList<>();
      for (int i = 0; i < jsonArray.size(); i++) {
        fieldsToSkipOnUI.add(String.valueOf(jsonArray.get(i)));
      }
    }

    while (keys.hasNext()) {
      String key = keys.next();

      if (CliJsonSerializable.FIELDS_TO_SKIP.contains(key)
          || (fieldsToSkipOnUI != null && fieldsToSkipOnUI.contains(key))) {
        continue;
      }

      try {
        nestedCollection = object.getJSONArray(key);
      } catch (GfJsonException ignored) {
      }

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
      for (int i = 0; i < depth; i++) {
        prefix += " . ";
      }
      String fieldNameToDisplay = fieldDisplayNames.getString(key);

      if (nestedCollection == null) {
        newRow.newLeftCol(prefix + fieldNameToDisplay);
      }

      if (nestedCollection != null) {
        Map<String, Integer> columnsMap = new HashMap<>();

        RowGroup newRowGroup = table.newRowGroup();
        newRowGroup.setColumnSeparator(" | ");
        newRowGroup.newBlankRow();
        newRowGroup.newRow().newLeftCol(fieldNameToDisplay);
        Row headerRow = newRowGroup.newRow();

        int numOfRows = nestedCollection.size();
        List<String> tableFieldsToSkipOnUI = null;
        for (int j = 0; j < numOfRows; j++) {
          GfJsonObject content = nestedCollection.getJSONObject(j);
          if (content.has(CliJsonSerializable.FIELDS_TO_SKIP_ON_UI)) {
            GfJsonArray jsonArray = content.getJSONArray(CliJsonSerializable.FIELDS_TO_SKIP_ON_UI);
            tableFieldsToSkipOnUI = new ArrayList<>();
            for (int i = 0; i < jsonArray.size(); i++) {
              tableFieldsToSkipOnUI.add(String.valueOf(jsonArray.get(i)));
            }
          }
          GfJsonArray columnNames = content.names();
          int numOfColumns = columnNames.size();

          if (headerRow.isEmpty()) {
            GfJsonObject innerFieldDisplayNames =
                content.getJSONObject(CliJsonSerializable.FIELDS_TO_DISPLAYNAME_MAPPING);
            for (int i = 0; i < numOfColumns; i++) {

              Object columnName = columnNames.get(i);
              if (CliJsonSerializable.FIELDS_TO_SKIP.contains((String) columnName)
                  || (tableFieldsToSkipOnUI != null
                      && tableFieldsToSkipOnUI.contains(columnName))) {
                // skip file data if any
                continue;
              }

              headerRow.newCenterCol(innerFieldDisplayNames.getString((String) columnName));
              columnsMap.put((String) columnName, i);
            }
            newRowGroup.newRowSeparator('-', false);
          }
          newRow = newRowGroup.newRow();
          for (int i = 0; i < numOfColumns; i++) {

            Object columnName = columnNames.get(i);
            if (CliJsonSerializable.FIELDS_TO_SKIP.contains((String) columnName)
                || (tableFieldsToSkipOnUI != null && tableFieldsToSkipOnUI.contains(columnName))) {
              // skip file data if any
              continue;
            }
            newRow.newLeftCol(String.valueOf(content.get((String) columnName)));
          }
        }
      } else if (nestedObject != null) {
        buildObjectSection(table, rowGroup, nestedObject, depth + 1);
      } else {
        newRow.newLeftCol(field);
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
      } catch (GfJsonException ignored) {
      }
    }

    return isPrimitive;
  }

  void buildComposite() {
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
      resultLines
          .add("Error occurred while processing Command Result. Internal Error - Invalid Result.");
      LogWrapper.getInstance().info(
          "Error occurred while processing Command Result. Internal Error - Invalid Result.", e);
    } finally {
      isDataBuilt = true;
    }
  }

  private void buildSection(Table table, RowGroup parentRowGroup, GfJsonObject section, int depth)
      throws GfJsonException {
    Iterator<String> keys = section.keys();
    RowGroup rowGroup;
    if (parentRowGroup != null) {
      rowGroup = parentRowGroup;
    } else {
      rowGroup = table.newRowGroup();
    }
    addHeaderInRowGroup(rowGroup, section);
    while (keys.hasNext()) {
      String key = keys.next();
      Object object = section.get(key);
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
      } else {
        Row newRow = rowGroup.newRow();
        String prefix = "";
        for (int i = 0; i < depth; i++) {
          prefix += " . ";
        }
        String[] value = getValuesSeparatedByLines(object);
        if (value.length == 1) {
          newRow.newLeftCol(prefix + key).newLeftCol(value[0]);
        } else {
          if (value.length != 0) { // possible when object == CliConstants.LINE_SEPARATOR
            newRow.newLeftCol(prefix + key).newLeftCol(value[0]);
            for (int i = 1; i < value.length; i++) {
              newRow = rowGroup.newRow();
              newRow.setColumnSeparator("   ");
              newRow.newLeftCol("").newLeftCol(value[i]);
            }
          } else {
            newRow.newLeftCol(prefix + key).newLeftCol("");
          }
        }
      }
    }
    addFooterInRowGroup(rowGroup, section);
  }

  private static String[] getValuesSeparatedByLines(Object object) {
    String valueString = String.valueOf(object);
    return valueString.split(GfshParser.LINE_SEPARATOR);
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
        // skip file data if any
        continue;
      }
      headerRow.newCenterCol(object);
    }

    // Build remaining rows by extracting data column-wise from JSON object
    Row[] dataRows = null;
    for (int i = 0; i < numOfColumns; i++) {
      Object object = columnNames.get(i);
      if (AbstractResultData.BYTE_DATA_ACCESSOR.equals(object)) {
        // skip file data if any
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
  private Row[] buildRows(RowGroup rowGroup, Row[] dataRows, GfJsonArray accumulatedData)
      throws GfJsonException {
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
        throw new RuntimeException("No associated files to save .. ");
      }
      numTimesSaved = numTimesSaved + 1;
    } catch (DataFormatException | GfJsonException e) {
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
   * @throws ArrayIndexOutOfBoundsException if this method is called more number of times than the
   *         data items it contains
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

  public GfJsonObject getContent() {
    return gfJsonObject.getJSONObject(ResultData.RESULT_CONTENT);
  }

  /**
   * The intent is that this method should be able to handle both ResultData as well as
   * CompositeResultData
   *
   * @return the extracted GfJsonObject table
   */
  public GfJsonObject getTableContent() {
    return getTableContent(0, 0);
  }

  /**
   * Most frequently, only two index values are required: a section index followed by a table index.
   * Some commands, such as 'describe region', may return command results with subsections, however.
   * Include these in order, e.g., getTableContent(sectionIndex, subsectionIndex, tableIndex);
   */
  public GfJsonObject getTableContent(int... sectionAndTableIDs) {
    GfJsonObject topLevelContent = getContent();
    // Most common is receiving exactly one section index and one table index.
    // Some results, however, will have subsections before the table listings.
    assert (sectionAndTableIDs.length >= 2);

    GfJsonObject sectionObject = topLevelContent;
    for (int i = 0; i < sectionAndTableIDs.length - 1; i++) {
      int idx = sectionAndTableIDs[i];
      sectionObject = sectionObject.getJSONObject("__sections__-" + idx);
      if (sectionObject == null) {
        return topLevelContent;
      }
    }

    int tableId = sectionAndTableIDs[sectionAndTableIDs.length - 1];
    GfJsonObject tableContent = sectionObject.getJSONObject("__tables__-" + tableId);
    if (tableContent == null) {
      return topLevelContent;
    }

    return tableContent.getJSONObject("content");
  }

  public String getFooter() {
    return getFooter(gfJsonObject);
  }

  public String getFooter(GfJsonObject gfJsonObject) {
    return gfJsonObject.getString(ResultData.RESULT_FOOTER);
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof CommandResult)) {
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
    return "CommandResult [gfJsonObject=" + gfJsonObject + ", status=" + status + ", index=" + index
        + ", isDataBuilt=" + isDataBuilt + ", resultData=" + resultData + ", resultLines="
        + resultLines + ", failedToPersist=" + failedToPersist + "]";
  }

  @Override
  public boolean failedToPersist() {
    return this.failedToPersist;
  }

  @Override
  public void setCommandPersisted(boolean commandPersisted) {
    this.failedToPersist = !commandPersisted;
  }

  public void setFileToDownload(Path fileToDownload) {
    this.fileToDownload = fileToDownload;
  }

  public List<Object> getColumnValues(String columnName) {
    Object[] actualValues =
        toArray(getTableContent().getInternalJsonObject().getJSONArray(columnName));
    return Arrays.asList(actualValues);
  }

  private Object[] toArray(JSONArray array) {
    Object[] values = new Object[array.length()];

    for (int i = 0; i < array.length(); i++) {
      values[i] = array.get(i);
    }

    return values;
  }
}

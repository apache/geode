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
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.stream.Collectors;
import java.util.zip.DataFormatException;

import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import org.apache.geode.cache.configuration.CacheElement;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.GfshParser;
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
  private static final Logger logger = LogService.getLogger();

  private GfJsonObject gfJsonObject;
  private Status status;
  private int index;
  private boolean isDataBuilt;

  private ResultData resultData;
  private List<String> resultLines;
  private boolean failedToPersist = false;
  private CacheElement cacheElemnt;

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

  public void setStatus(Status status) {
    this.status = status;
  }

  public ResultData getResultData() {
    return ResultBuilder.getReadOnlyResultData(resultData);
  }

  private GfJsonObject getGfJsonObject() {
    return gfJsonObject;
  }

  public CacheElement getCacheElemnt() {
    return cacheElemnt;
  }

  public void setCacheElemnt(CacheElement cacheElemnt) {
    this.cacheElemnt = cacheElemnt;
  }

  @Override
  public void resetToFirstLine() {
    index = 0;
  }

  private void buildData() {
    try {
      if (ResultData.TYPE_COMPOSITE.equals(resultData.getType())) {
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

  private void buildComposite() {
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
      logger.info(
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

  @Override
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

  @Override
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

  public String getMessageFromContent() {
    return getContent().getString("message");
  }

  public String getValueFromContent(String key) {
    return getContent().get(key).toString();
  }

  public List<String> getListFromContent(String key) {
    return getContent().getArrayValues(key);
  }

  public List<String> getColumnFromTableContent(String column, int... sectionAndTableIDs) {
    List<String> ids =
        Arrays.stream(sectionAndTableIDs).mapToObj(Integer::toString).collect(Collectors.toList());
    return CommandResult.toList(
        getTableContent(ids.toArray(new String[0])).getInternalJsonObject().getJSONArray(column));
  }

  public Map<String, List<String>> getMapFromTableContent(int... sectionAndTableIDs) {
    Map<String, List<String>> result = new LinkedHashMap<>();

    List<String> ids =
        Arrays.stream(sectionAndTableIDs).mapToObj(Integer::toString).collect(Collectors.toList());
    JSONObject table = getTableContent(ids.toArray(new String[0])).getInternalJsonObject();
    for (String column : table.keySet()) {
      result.put(column, CommandResult.toList(table.getJSONArray(column)));
    }

    return result;
  }

  public Map<String, List<String>> getMapFromTableContent(String... sectionAndTableIDs) {
    Map<String, List<String>> result = new LinkedHashMap<>();

    JSONObject table = getTableContent(sectionAndTableIDs).getInternalJsonObject();
    for (String column : table.keySet()) {
      result.put(column, CommandResult.toList(table.getJSONArray(column)));
    }

    return result;
  }

  public Map<String, String> getMapFromSection(String sectionID) {
    Map<String, String> result = new LinkedHashMap<>();
    GfJsonObject obj = getContent().getJSONObject("__sections__-" + sectionID);

    Iterator<String> iter = obj.keys();
    while (iter.hasNext()) {
      String key = iter.next();
      result.put(key, obj.getString(key));
    }

    return result;
  }

  /**
   * The intent is that this method should be able to handle both ResultData as well as
   * CompositeResultData
   *
   * @return the extracted GfJsonObject table
   */
  private GfJsonObject getTableContent() {
    return getTableContent("0", "0");
  }

  /**
   * Most frequently, only two index values are required: a section index followed by a table index.
   * Some commands, such as 'describe region', may return command results with subsections, however.
   * Include these in order, e.g., getTableContent(sectionIndex, subsectionIndex, tableIndex);
   */
  private GfJsonObject getTableContent(String... sectionAndTableIDs) {
    GfJsonObject topLevelContent = getContent();
    // Most common is receiving exactly one section index and one table index.
    // Some results, however, will have subsections before the table listings.
    assert (sectionAndTableIDs.length >= 2);

    GfJsonObject sectionObject = topLevelContent;
    for (int i = 0; i < sectionAndTableIDs.length - 1; i++) {
      String idx = sectionAndTableIDs[i];
      sectionObject = sectionObject.getJSONObject("__sections__-" + idx);
      if (sectionObject == null) {
        return topLevelContent;
      }
    }

    String tableId = sectionAndTableIDs[sectionAndTableIDs.length - 1];
    GfJsonObject tableContent = sectionObject.getJSONObject("__tables__-" + tableId);
    if (tableContent == null) {
      return topLevelContent;
    }

    return tableContent.getJSONObject("content");
  }

  public String getFooter() {
    return getFooter(gfJsonObject);
  }

  private String getFooter(GfJsonObject gfJsonObject) {
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

  public List<String> getColumnValues(String columnName) {
    return toList(getTableContent().getInternalJsonObject().getJSONArray(columnName));
  }

  public static List<String> toList(JSONArray array) {
    Object[] values = new Object[array.length()];

    for (int i = 0; i < array.length(); i++) {
      values[i] = array.get(i);
    }

    return Arrays.stream(values).map(Object::toString).collect(Collectors.toList());
  }
}

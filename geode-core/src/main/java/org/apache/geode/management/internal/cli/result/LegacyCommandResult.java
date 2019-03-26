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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.logging.log4j.Logger;

import org.apache.geode.internal.logging.LogService;
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

public class LegacyCommandResult implements CommandResult {
  private static final Logger logger = LogService.getLogger();

  private GfJsonObject gfJsonObject;
  private Status status;
  private int index;
  private boolean isDataBuilt;

  private ResultData resultData;
  private List<String> resultLines;
  private boolean failedToPersist = false;

  private transient int numTimesSaved;

  @Override
  public Path getFileToDownload() {
    return fileToDownload;
  }

  private Path fileToDownload;

  public LegacyCommandResult(ResultData resultData) {
    this.resultData = resultData;
    this.gfJsonObject = this.resultData.getGfJsonObject();
    this.status = this.resultData.getStatus();
    this.resultLines = new Vector<>();
  }

  public LegacyCommandResult(Path fileToDownload) {
    this(new InfoResultData(fileToDownload.toString()));
    this.fileToDownload = fileToDownload.toAbsolutePath();
  }

  @Override
  public boolean hasFileToDownload() {
    return fileToDownload != null;
  }

  @Override
  public Status getStatus() {
    return this.status;
  }

  @Override
  public void setStatus(Status status) {
    this.status = status;
  }

  @Override
  public ResultData getResultData() {
    return ResultBuilder.getReadOnlyResultData(resultData);
  }

  private GfJsonObject getGfJsonObject() {
    return gfJsonObject;
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

  private void buildComposite() {
    try {
      GfJsonObject content = getContent();
      if (content != null) {
        Table resultTable = TableBuilder.newTable();
        resultTable.setColumnSeparator(" : ");

        addHeaderInTable(resultTable, getGfJsonObject());

        for (Iterator<String> it = content.keys(); it.hasNext();) {
          String key = it.next();
          if (key.startsWith(ResultData.SECTION_DATA_ACCESSOR)) {
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
      if (key.startsWith(ResultData.TABLE_DATA_ACCESSOR)) {
        GfJsonObject tableObject = section.getJSONObject(key);

        addHeaderInTable(table, tableObject);

        RowGroup rowGroupForTable = table.newRowGroup();
        buildTable(rowGroupForTable, tableObject.getJSONObject(ResultData.RESULT_CONTENT));

        addFooterInTable(table, tableObject);
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
    String valueString = "" + object;
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
      Object object = columnNames.getString(i);
      if (ResultData.BYTE_DATA_ACCESSOR.equals(object)) {
        // skip file data if any
        continue;
      }
      headerRow.newCenterCol(object);
    }

    // Build remaining rows by extracting data column-wise from JSON object
    Row[] dataRows = null;
    for (int i = 0; i < numOfColumns; i++) {
      Object object = columnNames.getString(i);
      if (ResultData.BYTE_DATA_ACCESSOR.equals(object)) {
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
      dataRows[j].newLeftCol(accumulatedData.getString(j));
    }
    return dataRows;
  }

  @Override
  public boolean hasIncomingFiles() {
    GfJsonArray fileDataArray = null;
    try {
      GfJsonObject content = getContent();
      if (content != null) {
        fileDataArray = content.getJSONArray(ResultData.BYTE_DATA_ACCESSOR);
      }
    } catch (GfJsonException e) {
      e.printStackTrace();
    }
    return fileDataArray != null;
  }

  @Override
  public int getNumTimesSaved() {
    return numTimesSaved;
  }

  @Override
  public void saveIncomingFiles(String directory) throws IOException {
    // dump file data if any
    try {
      GfJsonObject content = getContent();
      if (content != null) {
        GfJsonArray bytesArray = content.getJSONArray(ResultData.BYTE_DATA_ACCESSOR);
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

  @Override
  public String toJson() {
    return gfJsonObject.toString();
  }

  @Override
  public String getType() {
    return resultData.getType();
  }

  @Override
  public String getHeader() {
    return getHeader(gfJsonObject);
  }

  @Override
  public String getHeader(GfJsonObject gfJsonObject) {
    return gfJsonObject.getString(ResultData.RESULT_HEADER);
  }

  @Override
  public GfJsonObject getContent() {
    return gfJsonObject.getJSONObject(ResultData.RESULT_CONTENT);
  }

  @Override
  public String getMessageFromContent() {
    List<String> messages;
    try {
      GfJsonArray jsonArray = getContent().getJSONArray("message");
      if (jsonArray == null) {
        return "";
      }
      messages = toList(jsonArray.getInternalJsonArray());
    } catch (GfJsonException jex) {
      return "";
    }

    return messages.stream().collect(Collectors.joining(". "));
  }

  @Override
  public String getValueFromContent(String key) {
    return getContent().get(key).toString();
  }

  @Override
  public List<String> getListFromContent(String key) {
    return getContent().getArrayValues(key);
  }

  @Override
  public List<String> getColumnFromTableContent(String column, String sectionId, String tableId) {
    return toList((ArrayNode) getTableContent(sectionId, tableId).get(column));
  }

  @Override
  public Map<String, List<String>> getMapFromTableContent(String sectionId, String tableId) {
    Map<String, List<String>> result = new LinkedHashMap<>();

    JsonNode table = getTableContent(sectionId, tableId).getInternalJsonObject();
    Iterator<String> fieldNames = table.fieldNames();
    while (fieldNames.hasNext()) {
      String column = fieldNames.next();
      result.put(column, toList((ArrayNode) table.get(column)));
    }

    return result;
  }

  @Override
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
   * Tables can only occur within a section. Sections cannot be nested.
   */
  private GfJsonObject getTableContent(String sectionId, String tableId) {
    GfJsonObject topLevelContent = getContent();

    GfJsonObject sectionObject = topLevelContent;
    sectionObject = sectionObject.getJSONObject("__sections__-" + sectionId);
    if (sectionObject == null) {
      return topLevelContent;
    }

    GfJsonObject tableContent = sectionObject.getJSONObject("__tables__-" + tableId);
    if (tableContent == null) {
      return topLevelContent;
    }

    return tableContent.getJSONObject("content");
  }

  @Override
  public String getFooter() {
    return getFooter(gfJsonObject);
  }

  private String getFooter(GfJsonObject gfJsonObject) {
    return gfJsonObject.getString(ResultData.RESULT_FOOTER);
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof LegacyCommandResult)) {
      return false;
    }
    LegacyCommandResult other = (LegacyCommandResult) obj;

    return this.gfJsonObject.toString().equals(other.gfJsonObject.toString());
  }

  @Override
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

  @Override
  public void setFileToDownload(Path fileToDownload) {
    this.fileToDownload = fileToDownload;
  }

  @Override
  public List<String> getTableColumnValues(String columnName) {
    return getTableContent("0", "0").getArrayValues(columnName);
  }

  private List<String> toList(ArrayNode array) {
    Object[] values = new Object[array.size()];

    for (int i = 0; i < array.size(); i++) {
      values[i] = array.get(i).textValue();
    }

    return Arrays.stream(values).map(Object::toString).collect(Collectors.toList());
  }
}

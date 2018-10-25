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

import static org.apache.commons.lang.SystemUtils.LINE_SEPARATOR;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang.NotImplementedException;

import org.apache.geode.management.internal.cli.GfshParser;
import org.apache.geode.management.internal.cli.json.GfJsonObject;
import org.apache.geode.management.internal.cli.result.model.AbstractResultModel;
import org.apache.geode.management.internal.cli.result.model.DataResultModel;
import org.apache.geode.management.internal.cli.result.model.InfoResultModel;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.result.model.TabularResultModel;

public class ModelCommandResult implements CommandResult {
  private static final String DO_NOT_USE_PERSISTED_FLAG =
      "The persisted flag is an artifact of the legacy model and should not be used ModelCommandResult.";
  private static final String FILE_LOGIC_NOT_YET_IMPLEMENTED =
      "File transfer logic has not yet been implemented in ModelCommandResult";
  private static final String CANNOT_PASS_GF_JSON_OBJECT_TO_MODEL_COMMAND_RESULT =
      "Cannot pass GfJsonObject to ModelCommandResult";
  private static final String CANNOT_USE_GF_JSON_OBJECT_FROM_MODEL_COMMAND_RESULT =
      "Cannot use GfJsonObject from ModelCommandResult";

  private ResultModel result;
  private List<String> commandOutput;
  private int commandOutputIndex;
  private static final Map<String, List<String>> EMPTY_TABLE_MAP = new LinkedHashMap<>();
  private static final List<String> EMPTY_LIST = new ArrayList<>();

  public ModelCommandResult(ResultModel result) {
    this.result = result;
  }

  @Override
  public Path getFileToDownload() {
    return null;
  }

  @Override
  public boolean hasFileToDownload() {
    return false;
  }

  @Override
  public Status getStatus() {
    return result.getStatus();
  }

  public void setStatus(Status status) {}

  @Override
  public ResultModel getResultData() {
    return result;
  }

  @Override
  public void resetToFirstLine() {
    commandOutputIndex = 0;
  }

  // ModelCommandResult should not handle saving files. File saving should be done by each
  // command's postExecutor in the ResultModel
  @Override
  public boolean hasIncomingFiles() {
    return false;
  }

  @Override
  public int getNumTimesSaved() {
    return 0;
  }

  @Override
  public void saveIncomingFiles(String directory) throws IOException {}

  @Override
  public boolean hasNextLine() {
    if (commandOutput == null) {
      buildCommandOutput();
    }
    return commandOutputIndex < commandOutput.size();
  }

  @Override
  public String nextLine() {
    if (commandOutput == null) {
      buildCommandOutput();
    }
    return commandOutput.get(commandOutputIndex++);
  }

  @Override
  public String toJson() {
    return getResultData().toJson();
  }

  @Override
  public String getType() {
    return "model";
  }

  @Override
  public String getHeader() {
    return result.getHeader();
  }

  @Override
  public String getHeader(GfJsonObject gfJsonObject) {
    throw new IllegalArgumentException(CANNOT_PASS_GF_JSON_OBJECT_TO_MODEL_COMMAND_RESULT);
  }

  @Override
  public GfJsonObject getContent() {
    throw new IllegalArgumentException(CANNOT_USE_GF_JSON_OBJECT_FROM_MODEL_COMMAND_RESULT);
  }

  @Override
  public String getMessageFromContent() {
    List<InfoResultModel> infos = result.getInfoSections();
    if (infos.size() == 0) {
      return "";
    }

    List<String> messages = infos.get(0).getContent();
    return messages.stream().collect(Collectors.joining(". "));
  }

  @Override
  public String getErrorMessage() {
    return getMessageFromContent();
  }

  @Override
  public String getValueFromContent(String key) {
    return null;
  }

  @Override
  public List<String> getListFromContent(String key) {
    return null;
  }

  @Override
  public List<String> getColumnFromTableContent(String column, String tableId) {
    TabularResultModel table = result.getTableSection(tableId);
    if (table == null) {
      return EMPTY_LIST;
    }

    return table.getContent().get(column);
  }

  @Override
  public Map<String, List<String>> getMapFromTableContent(String tableId) {
    TabularResultModel table = result.getTableSection(tableId);
    if (table == null) {
      return EMPTY_TABLE_MAP;
    }

    return table.getContent();
  }

  @Override
  public Map<String, String> getMapFromSection(String sectionID) {
    return result.getDataSection(sectionID).getContent();
  }

  @Override
  public String getFooter() {
    return result.getFooter();
  }

  @Override
  public boolean failedToPersist() {
    throw new IllegalArgumentException(DO_NOT_USE_PERSISTED_FLAG);
  }

  @Override
  public void setCommandPersisted(boolean commandPersisted) {
    throw new IllegalArgumentException(DO_NOT_USE_PERSISTED_FLAG);
  }

  @Override
  public void setFileToDownload(Path fileToDownload) {
    throw new NotImplementedException(FILE_LOGIC_NOT_YET_IMPLEMENTED);
  }

  // Convenience implementation using the first table found
  @Override
  public List<String> getTableColumnValues(String columnName) {
    List<TabularResultModel> tables = result.getTableSections();
    if (tables.size() == 0) {
      return EMPTY_LIST;
    }

    return tables.get(0).getContent().get(columnName);
  }

  @Override
  public List<String> getTableColumnValues(String sectionId, String columnName) {
    return result.getTableSection(sectionId).getContent().get(columnName);
  }

  // same as legacy buildData()
  private void buildCommandOutput() {
    commandOutputIndex = 0;
    commandOutput = new ArrayList<>();
    TableBuilder.Table resultTable = TableBuilder.newTable();

    addSpacedRowInTable(resultTable, result.getHeader());

    int index = 0;
    int sectionSize = result.getContent().size();
    for (AbstractResultModel section : result.getContent().values()) {
      index++;
      if (section instanceof DataResultModel) {
        buildData(resultTable, (DataResultModel) section);
      } else if (section instanceof TabularResultModel) {
        buildTabularCommandOutput(resultTable, (TabularResultModel) section);
      } else if (section instanceof InfoResultModel) {
        buildInfoOrErrorCommandOutput(resultTable, (InfoResultModel) section);
      } else {
        throw new IllegalArgumentException(
            "Unable to process output for " + section.getClass().getName());
      }
      // only add the spacer in between the sections.
      if (index < sectionSize) {
        addSpacedRowInTable(resultTable, LINE_SEPARATOR);
      }
    }

    addSpacedRowInTable(resultTable, result.getFooter());

    commandOutput.addAll(resultTable.buildTableList());
  }

  private void addHeaderInTable(TableBuilder.Table resultTable, ResultModel model) {
    String header = model.getHeader();
    if (header != null && !header.isEmpty()) {
      resultTable.newRow().newLeftCol(header);
      resultTable.newRow().newLeftCol("");
    }
  }

  private void addSpacedRowInTable(TableBuilder.Table resultTable, String row) {
    if (row != null && !row.isEmpty()) {
      resultTable.newRow().newLeftCol(row);
      resultTable.newRow().newLeftCol("");
    }
  }

  private void addRowInRowGroup(TableBuilder.RowGroup rowGroup, String row) {
    if (row != null && !row.isEmpty()) {
      rowGroup.newRow().newLeftCol(row);
    }
  }

  private void buildTabularCommandOutput(TableBuilder.Table resultTable, TabularResultModel model) {
    addSpacedRowInTable(resultTable, model.getHeader());

    resultTable.setColumnSeparator("   ");
    resultTable.setTabularResult(true);

    TableBuilder.RowGroup rowGroup = resultTable.newRowGroup();
    buildTable(rowGroup, model);

    addSpacedRowInTable(resultTable, model.getFooter());
  }

  private void buildTable(TableBuilder.RowGroup rowGroup, TabularResultModel model) {
    TableBuilder.Row headerRow = rowGroup.newRow();
    rowGroup.setColumnSeparator(" | ");
    rowGroup.newRowSeparator('-', false);

    Map<String, List<String>> rows = model.getContent();
    if (!rows.isEmpty()) {
      // build table header first
      rows.keySet().forEach(c -> headerRow.newCenterCol(c));

      // each row should have the same number of entries, so just look at the first one
      int rowCount = rows.values().iterator().next().size();
      for (int i = 0; i < rowCount; i++) {
        TableBuilder.Row oneRow = rowGroup.newRow();
        for (String column : rows.keySet()) {
          oneRow.newLeftCol(rows.get(column).get(i));
        }
      }
    }
  }

  private void buildData(TableBuilder.Table resultTable, DataResultModel section) {
    TableBuilder.RowGroup rowGroup = resultTable.newRowGroup();
    rowGroup.setColumnSeparator(" : ");

    addRowInRowGroup(rowGroup, section.getHeader());

    // finally process map values
    for (Map.Entry<String, String> entry : section.getContent().entrySet()) {
      TableBuilder.Row newRow = rowGroup.newRow();
      String key = entry.getKey();
      String value = entry.getValue();
      String[] values = entry.getValue().split(GfshParser.LINE_SEPARATOR);
      if (values.length == 1) {
        newRow.newLeftCol(key).newLeftCol(values[0]);
      } else {
        if (values.length != 0) { // possible when object == CliConstants.LINE_SEPARATOR
          newRow.newLeftCol(key).newLeftCol(values[0]);
          for (int i = 1; i < values.length; i++) {
            newRow = rowGroup.newRow();
            newRow.setColumnSeparator("   ");
            newRow.newLeftCol("").newLeftCol(values[i]);
          }
        } else {
          newRow.newLeftCol(key).newLeftCol("");
        }
      }
    }
    addRowInRowGroup(rowGroup, section.getFooter());
  }

  private void buildInfoOrErrorCommandOutput(TableBuilder.Table resultTable,
      InfoResultModel model) {
    TableBuilder.RowGroup rowGroup = resultTable.newRowGroup();

    addRowInRowGroup(rowGroup, model.getHeader());

    model.getContent().forEach(c -> rowGroup.newRow().newLeftCol(c));

    addRowInRowGroup(rowGroup, model.getFooter());
  }
}

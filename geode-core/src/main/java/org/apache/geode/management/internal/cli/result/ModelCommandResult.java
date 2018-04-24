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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.geode.management.internal.cli.GfshParser;
import org.apache.geode.management.internal.cli.ModelCommandResponse;
import org.apache.geode.management.internal.cli.json.GfJsonObject;
import org.apache.geode.management.internal.cli.result.model.CompositeResultModel;
import org.apache.geode.management.internal.cli.result.model.ErrorResultModel;
import org.apache.geode.management.internal.cli.result.model.InfoResultModel;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.result.model.SectionResultModel;
import org.apache.geode.management.internal.cli.result.model.TabularResultModel;

public class ModelCommandResult implements CommandResult {

  private String header = "";
  private String footer = "";
  private ModelCommandResponse response;
  private List<String> commandOutput;
  private int commandOutputIndex;
  private static final Map<String, String> EMPTY_MAP = new LinkedHashMap<>();
  private static final Map<String, List<String>> EMPTY_TABLE_MAP = new LinkedHashMap<>();

  public ModelCommandResult(ModelCommandResponse response) {
    this.response = response;
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
    return response.getStatus() == 0 ? Status.OK : Status.ERROR;
  }

  public void setStatus(Status status) {

  }

  @Override
  public ResultData getResultData() {
    return response.getData();
  }

  @Override
  public void resetToFirstLine() {
    commandOutputIndex = 0;
  }

  @Override
  public boolean hasIncomingFiles() {
    return false;
  }

  @Override
  public int getNumTimesSaved() {
    return 0;
  }

  @Override
  public void saveIncomingFiles(String directory) throws IOException {

  }

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
    return null;
  }

  @Override
  public String getType() {
    return response.getContentType();
  }

  @Override
  public String getHeader() {
    return header;
  }

  @Override
  public String getHeader(GfJsonObject gfJsonObject) {
    return null;
  }

  @Override
  public GfJsonObject getContent() {
    return null;
  }

  @Override
  public String getMessageFromContent() {
    // Only Info and Error types have messages
    List<String> messages = ((InfoResultModel) response.getData()).getContent();
    return messages.stream().collect(Collectors.joining(". "));
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
  public List<String> getColumnFromTableContent(String column, String sectionId, String tableId) {
    return null;
  }

  @Override
  public Map<String, List<String>> getMapFromTableContent(String sectionId, String tableId) {
    if (!(response.getData() instanceof CompositeResultModel)) {
      throw new IllegalArgumentException(
          "cannot get table from type " + response.getData().getClass().getSimpleName());
    }
    SectionResultModel section =
        ((CompositeResultModel) response.getData()).getContent().get(sectionId);
    if (section == null) {
      return EMPTY_TABLE_MAP;
    }

    TabularResultModel table = section.getTables().get(tableId);
    if (table == null) {
      return EMPTY_TABLE_MAP;
    }

    return table.getContent();
  }

  @Override
  public Map<String, String> getMapFromSection(String sectionID) {
    if (!(response.getData() instanceof CompositeResultModel)) {
      throw new IllegalArgumentException(
          "cannot get section from type " + response.getData().getClass().getSimpleName());
    }

    SectionResultModel section =
        ((CompositeResultModel) response.getData()).getContent().get(sectionID);
    if (section == null) {
      return EMPTY_MAP;
    }

    return section.getContent();
  }

  @Override
  public String getFooter() {
    return footer;
  }

  @Override
  public boolean failedToPersist() {
    return false;
  }

  @Override
  public void setCommandPersisted(boolean commandPersisted) {

  }

  @Override
  public void setFileToDownload(Path fileToDownload) {

  }

  @Override
  public List<String> getColumnValues(String columnName) {
    if (!(response.getData() instanceof TabularResultModel)) {
      throw new IllegalArgumentException(
          "cannot get column from type " + response.getData().getClass().getSimpleName());
    }

    return ((TabularResultModel) response.getData()).getContent().get(columnName);
  }

  // same as legacy buildData()
  private void buildCommandOutput() {
    commandOutputIndex = 0;
    commandOutput = new ArrayList<>();
    TableBuilder.Table resultTable;

    ResultModel data = response.getData();
    if (data instanceof CompositeResultModel) {
      resultTable = buildCompositeCommandOutput((CompositeResultModel) data);
    } else if (data instanceof TabularResultModel) {
      resultTable = buildTabularCommandOutput((TabularResultModel) data);
    } else if (data instanceof ErrorResultModel) {
      resultTable = buildErrorCommandOutput((ErrorResultModel) data);
    } else {
      throw new IllegalArgumentException(
          "Unable to process output for " + data.getClass().getName());
    }

    commandOutput.addAll(resultTable.buildTableList());
  }

  private void addHeaderInTable(TableBuilder.Table resultTable, ResultModel model) {
    String header = model.getHeader();
    if (header != null && !header.isEmpty()) {
      resultTable.newRow().newLeftCol(header);
    }
  }

  private void addFooterInTable(TableBuilder.Table resultTable, ResultModel model) {
    String footer = model.getFooter();
    if (footer != null && !footer.isEmpty()) {
      resultTable.newRow().newLeftCol(footer);
    }
  }

  private void addHeaderInRowGroup(TableBuilder.RowGroup rowGroup, ResultModel model) {
    String header = model.getHeader();
    if (header != null && !header.isEmpty()) {
      rowGroup.newRow().newLeftCol(header);
    }
  }

  private void addFooterInRowGroup(TableBuilder.RowGroup rowGroup, ResultModel model) {
    String footer = model.getFooter();
    if (footer != null && !footer.isEmpty()) {
      rowGroup.newRow().newLeftCol(footer);
    }
  }

  private TableBuilder.Table buildTabularCommandOutput(TabularResultModel model) {
    TableBuilder.Table resultTable = TableBuilder.newTable();
    addHeaderInTable(resultTable, model);

    resultTable.setColumnSeparator("   ");
    resultTable.setTabularResult(true);

    TableBuilder.RowGroup rowGroup = resultTable.newRowGroup();
    buildTable(rowGroup, model);

    addFooterInTable(resultTable, model);

    return resultTable;
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

  private TableBuilder.Table buildCompositeCommandOutput(CompositeResultModel model) {
    TableBuilder.Table resultTable = TableBuilder.newTable();
    resultTable.setColumnSeparator(" : ");
    addHeaderInTable(resultTable, model);

    for (SectionResultModel section : model.getContent().values()) {
      buildSection(resultTable, null, section, 0);
    }

    return resultTable;
  }

  private void buildSection(TableBuilder.Table displayTable, TableBuilder.RowGroup parentRowGroup,
      SectionResultModel section, int depth) {
    TableBuilder.RowGroup rowGroup;

    if (parentRowGroup != null) {
      rowGroup = parentRowGroup;
    } else {
      rowGroup = displayTable.newRowGroup();
    }

    addHeaderInRowGroup(rowGroup, section);

    // process table values first
    for (TabularResultModel table : section.getTableValues()) {
      addHeaderInTable(displayTable, table);

      TableBuilder.RowGroup rowGroupForTable = displayTable.newRowGroup();
      buildTable(rowGroupForTable, table);

      addFooterInTable(displayTable, table);
    }

    if (section.getSeparator() != 0) {
      rowGroup.newRowSeparator(section.getSeparator(), true);
    }

    // finally process map values
    for (Map.Entry<String, String> entry : section.getContent().entrySet()) {
      TableBuilder.Row newRow = rowGroup.newRow();
      String key = entry.getKey();
      String value = entry.getValue();
      String prefix = "";
      for (int i = 0; i < depth; i++) {
        prefix += " . ";
      }
      String[] values = entry.getValue().split(GfshParser.LINE_SEPARATOR);
      if (values.length == 1) {
        newRow.newLeftCol(prefix + key).newLeftCol(values[0]);
      } else {
        if (values.length != 0) { // possible when object == CliConstants.LINE_SEPARATOR
          newRow.newLeftCol(prefix + key).newLeftCol(values[0]);
          for (int i = 1; i < values.length; i++) {
            newRow = rowGroup.newRow();
            newRow.setColumnSeparator("   ");
            newRow.newLeftCol("").newLeftCol(values[i]);
          }
        } else {
          newRow.newLeftCol(prefix + key).newLeftCol("");
        }
      }
    }
    addFooterInRowGroup(rowGroup, section);
  }

  private TableBuilder.Table buildErrorCommandOutput(ErrorResultModel model) {
    TableBuilder.Table resultTable = TableBuilder.newTable();
    TableBuilder.RowGroup rowGroup = resultTable.newRowGroup();

    model.getContent().forEach(c -> rowGroup.newRow().newLeftCol(c));

    return resultTable;
  }
}

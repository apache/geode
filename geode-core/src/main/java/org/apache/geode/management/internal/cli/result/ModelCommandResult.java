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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.geode.management.internal.cli.GfshParser;
import org.apache.geode.management.internal.cli.ModelCommandResponse;
import org.apache.geode.management.internal.cli.json.GfJsonObject;
import org.apache.geode.management.internal.cli.result.model.DataResultModel;
import org.apache.geode.management.internal.cli.result.model.ErrorResultModel;
import org.apache.geode.management.internal.cli.result.model.InfoResultModel;
import org.apache.geode.management.internal.cli.result.model.TabularResultModel;

public class ModelCommandResult implements CommandResult {

  private String header = "";
  private String footer = "";
  private ModelCommandResponse response;
  private List<String> commandOutput;
  private int commandOutputIndex;
  private Object configObject;
  private static final Map<String, String> EMPTY_MAP = new LinkedHashMap<>();
  private static final Map<String, List<String>> EMPTY_TABLE_MAP = new LinkedHashMap<>();
  private static final List<String> EMPTY_LIST = new ArrayList<>();

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

  public void setStatus(Status status) {}

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

  @JsonIgnore
  @Override
  public Object getConfigObject() {
    return configObject;
  }

  @JsonIgnore
  @Override
  public void setConfigObject(Object configObject) {
    this.configObject = configObject;
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
    ObjectMapper mapper = new ObjectMapper();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try {
      mapper.writeValue(baos, getResultData());
    } catch (IOException e) {
      return e.getMessage();
    }

    return baos.toString();
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
    throw new IllegalArgumentException("Cannot pass GfJsonObject to ModelCommandResult");
  }

  @Override
  public GfJsonObject getContent() {
    throw new IllegalArgumentException("Cannot use GfJsonObject from ModelCommandResult");
  }

  @Override
  public String getMessageFromContent() {
    List<InfoResultModel> infos = response.getData().getInfoSections();
    if (infos.size() == 0) {
      return "";
    }

    List<String> messages = infos.get(0).getContent();
    return messages.stream().collect(Collectors.joining(". "));
  }

  @Override
  public String getErrorMessage() {
    ErrorResultModel error = response.getData().getError();
    if (error == null) {
      return "";
    }

    List<String> messages = error.getContent();
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
  public List<String> getColumnFromTableContent(String column, String tableId) {
    TabularResultModel table = response.getData().getTableSection(tableId);
    if (table == null) {
      return EMPTY_LIST;
    }

    return table.getContent().get(column);
  }

  @Override
  public Map<String, List<String>> getMapFromTableContent(String tableId) {
    TabularResultModel table = response.getData().getTableSection(tableId);
    if (table == null) {
      return EMPTY_TABLE_MAP;
    }

    return table.getContent();
  }

  @Override
  public Map<String, String> getMapFromSection(String sectionID) {
    return response.getData().getDataSection(sectionID).getContent();
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

  // Convenience implementation using the first table found
  @Override
  public List<String> getTableColumnValues(String columnName) {
    List<TabularResultModel> tables = response.getData().getTableSections();
    if (tables.size() == 0) {
      return EMPTY_LIST;
    }

    return tables.get(0).getContent().get(columnName);
  }

  @Override
  public List<String> getTableColumnValues(String sectionId, String columnName) {
    return response.getData().getTableSection(sectionId).getContent().get(columnName);
  }

  // same as legacy buildData()
  private void buildCommandOutput() {
    commandOutputIndex = 0;
    commandOutput = new ArrayList<>();
    TableBuilder.Table resultTable = TableBuilder.newTable();

    addHeaderInTable(resultTable, response.getData());

    for (ResultData section : response.getData().getContent().values()) {
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
    }

    if (response.getData().getError() != null) {
      buildInfoOrErrorCommandOutput(resultTable, response.getData().createOrGetError());
    }

    addFooterInTable(resultTable, response.getData());

    commandOutput.addAll(resultTable.buildTableList());
  }

  private void addHeaderInTable(TableBuilder.Table resultTable, ResultData model) {
    String header = model.getHeader();
    if (header != null && !header.isEmpty()) {
      resultTable.newRow().newLeftCol(header);
      resultTable.newRow().newLeftCol("");
    }
  }

  private void addFooterInTable(TableBuilder.Table resultTable, ResultData model) {
    String footer = model.getFooter();
    if (footer != null && !footer.isEmpty()) {
      resultTable.newRow().newLeftCol(footer);
      resultTable.newRow().newLeftCol("");
    }
  }

  private void addHeaderInRowGroup(TableBuilder.RowGroup rowGroup, ResultData model) {
    String header = model.getHeader();
    if (header != null && !header.isEmpty()) {
      rowGroup.newRow().newLeftCol(header);
    }
  }

  private void addFooterInRowGroup(TableBuilder.RowGroup rowGroup, ResultData model) {
    String footer = model.getFooter();
    if (footer != null && !footer.isEmpty()) {
      rowGroup.newRow().newLeftCol(footer);
    }
  }

  private void buildTabularCommandOutput(TableBuilder.Table resultTable, TabularResultModel model) {
    addHeaderInTable(resultTable, model);

    resultTable.setColumnSeparator("   ");
    resultTable.setTabularResult(true);

    TableBuilder.RowGroup rowGroup = resultTable.newRowGroup();
    buildTable(rowGroup, model);

    addFooterInTable(resultTable, model);
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

    addHeaderInRowGroup(rowGroup, section);

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
    addFooterInRowGroup(rowGroup, section);
  }

  private void buildInfoOrErrorCommandOutput(TableBuilder.Table resultTable,
      InfoResultModel model) {
    TableBuilder.RowGroup rowGroup = resultTable.newRowGroup();

    model.getContent().forEach(c -> rowGroup.newRow().newLeftCol(c));
  }
}

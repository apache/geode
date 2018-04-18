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

package org.apache.geode.management.internal.cli.result.model;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonIgnore;

import org.apache.geode.management.cli.GfshCommand;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.cli.result.ResultData;

/**
 * This class is the primary container for results returned from a {@link GfshCommand}.
 * <br/>
 * The following different types of 'models' can be added to an instance of {@code ResultModel}.
 * <ol>
 * <li>{@code InfoResultModel}</li>
 * This model holds multiple lines of text.
 * <li>{@code TabularResultModel}</li>
 * This model holds a table of named columns and associated row values.
 * <li>{@code DataResultModel}</li>
 * This model holds a map of key/value pairs
 * </ol>
 * The order in which models are added is maintained and will be presented to the user in the same
 * order.
 * <br/>
 * A single error model can be added with {@link #createOrGetError()}. Once an error has been
 * created, the status of the {@code ResultModel} will also be set to {@link Result.Status#ERROR}
 *
 */
// Only implement ResultData for API compatibility during migration
public class ResultModel implements ResultData {

  private String header;
  private String footer;
  private Map<String, ResultData> sections = new LinkedHashMap<>();
  private int sectionCount = 0;
  private ErrorResultModel error;

  @Override
  public Result.Status getStatus() {
    return error == null ? Result.Status.OK : Result.Status.ERROR;
  }

  public ErrorResultModel createOrGetError() {
    if (error == null) {
      error = new ErrorResultModel();
    }

    return error;
  }

  public ErrorResultModel getError() {
    return error;
  }

  @JsonIgnore
  public int getErrorCode() {
    return error != null ? error.getErrorCode() : 0;
  }

  public String getHeader() {
    return header;
  }

  public void setHeader(String header) {
    this.header = header;
  }

  public String getFooter() {
    return footer;
  }

  public void setFooter(String footer) {
    this.footer = footer;
  }

  public Map<String, ResultData> getContent() {
    return sections;
  }

  public void setContent(Map<String, ResultData> content) {
    this.sections = content;
  }

  public InfoResultModel addInfo() {
    return addInfo(Integer.toString(sectionCount++));
  }

  public InfoResultModel addInfo(String namedSection) {
    InfoResultModel section = new InfoResultModel();
    sections.put(namedSection, section);

    return section;
  }

  @JsonIgnore
  public List<InfoResultModel> getInfoSections() {
    return sections.values().stream().filter(InfoResultModel.class::isInstance)
        .map(InfoResultModel.class::cast).collect(Collectors.toList());
  }

  public TabularResultModel addTable() {
    return addTable(Integer.toString(sectionCount++));
  }

  public TabularResultModel addTable(String namedSection) {
    TabularResultModel section = new TabularResultModel();
    sections.put(namedSection, section);

    return section;
  }

  @JsonIgnore
  public List<TabularResultModel> getTableSections() {
    return sections.values().stream().filter(TabularResultModel.class::isInstance)
        .map(TabularResultModel.class::cast).collect(Collectors.toList());
  }

  public TabularResultModel getTableSection(String name) {
    return (TabularResultModel) sections.get(name);
  }

  public DataResultModel addData() {
    return addData(Integer.toString(sectionCount++));
  }

  public DataResultModel addData(String namedSection) {
    DataResultModel section = new DataResultModel();
    sections.put(namedSection, section);

    return section;
  }

  @JsonIgnore
  public List<DataResultModel> getDataSections() {
    return sections.values().stream().filter(DataResultModel.class::isInstance)
        .map(DataResultModel.class::cast).collect(Collectors.toList());
  }

  public DataResultModel getDataSection(String name) {
    return (DataResultModel) sections.get(name);
  }

  public ResultModel createGemFireErrorResult(String message) {
    ErrorResultModel error = createOrGetError();
    error.setErrorCode(ResultBuilder.ERRORCODE_GEODE_ERROR);
    error.addLine("Could not process command due to error. " + message);

    return this;
  }

  public ResultModel createUserErrorResult(String message) {
    ErrorResultModel error = createOrGetError();
    error.setErrorCode(ResultBuilder.ERRORCODE_USER_ERROR);
    error.addLine(message);

    return this;
  }
}

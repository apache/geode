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
 * Errors should just be added as {@code InfoResultModel}s and then the status should be set
 * appropriately to indicate an error.
 *
 */
public class ResultModel {

  private String header;
  private String footer;
  private Map<String, ResultData> sections = new LinkedHashMap<>();
  private int sectionCount = 0;
  private Result.Status status = Result.Status.OK;
  private Object configObject;

  @JsonIgnore
  public Object getConfigObject() {
    return configObject;
  }

  public boolean getLegacy() {
    return false;
  }

  public void setLegacy(boolean legacy) {
    // no-op
  }

  public void setConfigObject(Object configObject) {
    this.configObject = configObject;
  }

  @JsonIgnore
  public boolean isSuccessful() {
    return status == Result.Status.OK;
  }

  public void setStatus(Result.Status status) {
    this.status = status;
  }

  public Result.Status getStatus() {
    return status;
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

  /**
   * Convenience method which creates an {@code InfoResultModel} section. The provided message is
   * prepended with the string "Error processing command:". The status will be set to
   * {@code Result.Status.ERROR}
   */
  public ResultModel createCommandProcessingError(String message) {
    return createError("Error processing command: " + message);
  }

  public ResultModel createError(String message) {
    addInfo().addLine(message);
    setStatus(Result.Status.ERROR);

    return this;
  }
}

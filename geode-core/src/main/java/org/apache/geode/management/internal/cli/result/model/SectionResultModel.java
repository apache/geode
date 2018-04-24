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

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class SectionResultModel extends AbstractResultModel {

  private Map<String, String> data = new LinkedHashMap<>();
  private Map<String, TabularResultModel> tables = new LinkedHashMap<>();
  private int tableCount = 0;
  private char separator;

  @Override
  public String getType() {
    return "section";
  }

  public void setType(String type) {
    // no-op
  }

  @Override
  public Map<String, String> getContent() {
    return data;
  }

  public Map<String, TabularResultModel> getTables() {
    return tables;
  }

  @JsonIgnore
  public Collection<TabularResultModel> getTableValues() {
    return tables.values();
  }

  public void setContent(Map<String, String> content) {
    this.data = content;
  }

  public void addData(String key, Object value) {
    data.put(key, value.toString());
  }

  public char getSeparator() {
    return separator;
  }

  public void setSeparator(char separator) {
    this.separator = separator;
  }

  public void addSeparator(char separator) {
    setSeparator(separator);
  }

  public TabularResultModel addTable() {
    TabularResultModel table = new TabularResultModel();
    tables.put(Integer.toString(tableCount), table);
    tableCount += 1;

    return table;
  }

  public TabularResultModel addTable(String namedTable) {
    TabularResultModel table = new TabularResultModel();
    tables.put(namedTable, table);

    return table;
  }
}

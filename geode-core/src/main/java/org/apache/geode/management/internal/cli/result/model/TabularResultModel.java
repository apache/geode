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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class TabularResultModel extends AbstractResultModel {

  /*
   * Table data mapped by column name. The map needs to be able to maintain insertion order.
   */
  private Map<String, List<String>> table = new LinkedHashMap<>();

  TabularResultModel() {}

  public TabularResultModel accumulate(String column, String value) {
    if (table.containsKey(column)) {
      table.get(column).add(value);
    } else {
      List<String> list = new ArrayList<>();
      list.add(value);
      table.put(column, list);
    }

    return this;
  }

  @Override
  public Map<String, List<String>> getContent() {
    return table;
  }

  public void setColumnHeader(String... columnHeaders) {
    for (String columnHeader : columnHeaders) {
      table.put(columnHeader, new ArrayList<>());
    }
  }

  public void addRow(String... values) {
    if (values.length != table.size()) {
      throw new IllegalStateException("row size is different than the column header size.");
    }
    List<String> columnHeaders = getHeaders();
    for (int i = 0; i < values.length; i++) {
      table.get(columnHeaders.get(i)).add(values[i]);
    }
  }

  @JsonIgnore
  public List<String> getHeaders() {
    // this should maintain the original insertion order
    List<String> headers = new ArrayList<>();
    headers.addAll(table.keySet());
    return headers;
  }

  @JsonIgnore
  public String getValue(String columnName, int rowIndex) {
    return table.get(columnName).get(rowIndex);
  }

  @JsonIgnore
  public int getColumnSize() {
    return table.size();
  }

  @JsonIgnore
  public int getRowSize() {
    if (table.size() == 0) {
      return 0;
    }
    return table.values().iterator().next().size();
  }

  @JsonIgnore
  public List<String> getValuesInColumn(String header) {
    return table.get(header);
  }

  @JsonIgnore
  public List<String> getValuesInRow(int index) {
    List<String> headers = getHeaders();
    List<String> values = new ArrayList<>();
    for (String header : headers) {
      values.add(table.get(header).get(index));
    }
    return values;
  }
}

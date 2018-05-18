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
import org.apache.commons.lang.ArrayUtils;

import org.apache.geode.management.internal.cli.functions.CliFunctionResult;

public class TabularResultModel extends AbstractResultModel {

  /*
   * Table data mapped by column name. The map needs to be able to maintain insertion order.
   */
  private Map<String, List<String>> table = new LinkedHashMap<>();

  @JsonIgnore
  private String[] columnHeaders = new String[0];

  TabularResultModel() {}

  public TabularResultModel accumulate(String column, String value) {
    if (table.containsKey(column)) {
      table.get(column).add(value);
    } else {
      List<String> list = new ArrayList<>();
      list.add(value);
      table.put(column, list);
      columnHeaders = (String[]) ArrayUtils.add(columnHeaders, column);
    }

    return this;
  }

  @Override
  public Map<String, List<String>> getContent() {
    return table;
  }

  public void setColumnHeader(String... columnHeaders) {
    this.columnHeaders = columnHeaders;
    for (String columnHeader : columnHeaders) {
      table.put(columnHeader, new ArrayList<>());
    }
  }

  public void addRow(String... values) {
    if (values.length != columnHeaders.length) {
      throw new IllegalStateException("row size is different than the column header size.");
    }
    for (int i = 0; i < values.length; i++) {
      table.get(columnHeaders[i]).add(values[i]);
    }
  }

  public boolean setContent(List<CliFunctionResult> functionResults, boolean skipIgnore) {
    // clear the old content if any
    table.clear();
    boolean atLeastOneSuccess = false;
    setColumnHeader("Member", "Status", "Message");
    for (CliFunctionResult functionResult : functionResults) {
      addRow(functionResult.getMemberIdOrName(), functionResult.getStatus(skipIgnore),
          functionResult.getStatusMessage());
      if (functionResult.isSuccessful()) {
        atLeastOneSuccess = true;
      } else if (functionResult.isIgnorableFailure() && skipIgnore) {
        atLeastOneSuccess = true;
      }
    }
    return atLeastOneSuccess;
  }
}

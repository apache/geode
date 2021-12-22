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

import java.util.ArrayList;
import java.util.List;

class Table {

  private final List<RowGroup> rowGroups = new ArrayList<>();
  private final Screen screen;

  private String columnSeparator = "   ";
  private boolean isTabularResult;

  Table(Screen screen) {
    this.screen = screen;
  }

  @Override
  public String toString() {
    return "Table{rowGroups=" + rowGroups + "}";
  }

  void setTabularResult(boolean isTabularResult) {
    this.isTabularResult = isTabularResult;
  }

  void setColumnSeparator(String columnSeparator) {
    this.columnSeparator = columnSeparator;
  }

  String getColumnSeparator() {
    return columnSeparator;
  }

  RowGroup newRowGroup() {
    RowGroup rowGroup = new RowGroup(this, screen);
    rowGroups.add(rowGroup);
    return rowGroup;
  }

  Row newRow() {
    RowGroup rowGroup = newRowGroup();
    return rowGroup.newRow();
  }

  void newBlankRow() {
    RowGroup rowGroup = newRowGroup();
    rowGroup.newBlankRow();
  }

  RowGroup getLastRowGroup() {
    if (rowGroups.isEmpty()) {
      return null;
    }
    return rowGroups.get(rowGroups.size() - 1);
  }

  /**
   * Computes total Max Row Length across table - for all row groups.
   */
  int getMaxLength() {
    int rowGroupMaxTotalLength = 0;
    for (RowGroup rowGroup : rowGroups) {
      List<Row> rows = rowGroup.getRows();

      int rowMaxTotalLength = 0;
      for (Row row : rows) {
        int rowTotalLength = 0;
        for (int i = 0; i < row.getNumCols(); i++) {
          rowTotalLength += row.getMaxColLength(i);
        }

        if (rowGroup.getColumnSeparator() != null) {
          rowTotalLength += row.getNumCols() * rowGroup.getColumnSeparator().length();
        }
        if (rowMaxTotalLength < rowTotalLength) {
          rowMaxTotalLength = rowTotalLength;
        }
      }

      int rowGroupTotalLength = 0;
      rowGroupTotalLength += rowMaxTotalLength;
      if (rowGroupMaxTotalLength < rowGroupTotalLength) {
        rowGroupMaxTotalLength = rowGroupTotalLength;
      }
    }
    return (int) (rowGroupMaxTotalLength * 1.1);
  }

  String buildTable() {
    StringBuilder StringBuilder = new StringBuilder();
    for (RowGroup rowGroup : rowGroups) {
      StringBuilder.append(rowGroup.buildRowGroup(isTabularResult));
    }

    return StringBuilder.toString();
  }

  List<String> buildTableList() {
    List<String> list = new ArrayList<>();
    for (RowGroup rowGroup : rowGroups) {
      list.add(rowGroup.buildRowGroup(isTabularResult));
    }

    return list;
  }
}

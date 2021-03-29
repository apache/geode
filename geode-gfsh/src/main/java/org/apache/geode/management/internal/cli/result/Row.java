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

import org.apache.geode.annotations.VisibleForTesting;

class Row {

  private final RowGroup rowGroup;
  private final Character rowSeparator;
  private final List<Column> columns = new ArrayList<>();
  private final boolean isTableWideSeparator;
  private final Screen screen;

  private String columnSeparator;

  private boolean isBlank;

  Row(RowGroup rowGroup, Screen screen) {
    this(rowGroup, screen, null, false);
  }

  Row(RowGroup rowGroup, Screen screen, Character rowSeparator, boolean isTableWideSeparator) {
    this.rowGroup = rowGroup;
    this.screen = screen;
    this.rowSeparator = rowSeparator;
    this.isTableWideSeparator = isTableWideSeparator;
  }

  @Override
  public String toString() {
    return "Row{columns=" + columns + "}";
  }

  Row newLeftCol(Object value) {
    Column column = new Column(value, Align.LEFT);
    columns.add(column);
    return this;
  }

  Row newRightCol(Object value) {
    Column column = new Column(value, Align.RIGHT);
    columns.add(column);
    return this;
  }

  Row newCenterCol(Object value) {
    Column column = new Column(value, Align.CENTER);
    columns.add(column);
    return this;
  }

  int getNumCols() {
    return columns.size();
  }

  int getMaxColLength(int colNum) {
    if (colNum >= columns.size()) {
      return 0;
    }

    return columns.get(colNum).getLength();
  }

  void setColumnSeparator(String columnSeparator) {
    this.columnSeparator = columnSeparator;
  }

  String buildRow(boolean isTabularResult) {
    StringBuilder stringBuilder = new StringBuilder();
    if (rowSeparator != null) {
      if (isTableWideSeparator) {
        int maxColLength = rowGroup.getTable().getMaxLength();
        // Trim only for tabular results
        if (isTabularResult) {
          maxColLength = screen.trimWidthForScreen(maxColLength);
        }

        for (int j = 0; j < maxColLength; j++) {
          stringBuilder.append(rowSeparator);
        }
      } else {
        int maxNumCols = rowGroup.getNumCols();

        for (int i = 0; i < maxNumCols; i++) {
          int maxColLength = rowGroup.getColSize(i);
          for (int j = 0; j < maxColLength; j++) {
            stringBuilder.append(rowSeparator);
          }
          if (i < maxNumCols - 1) {
            stringBuilder.append(rowGroup.getColumnSeparator());
          }
        }
      }
    } else {
      for (int i = 0; i < columns.size(); i++) {
        boolean lastColumn = !(i < columns.size() - 1);
        stringBuilder
            .append(columns.get(i).buildColumn(rowGroup.getColSize(i), lastColumn));
        if (!lastColumn) {
          stringBuilder.append(getColumnSeparator());
        }
      }
    }

    return stringBuilder.toString();
  }

  boolean isBlank() {
    return isBlank;
  }

  void setBlank(boolean blank) {
    isBlank = blank;
  }

  @VisibleForTesting
  boolean isEmpty() {
    return columns.isEmpty();
  }

  private String getColumnSeparator() {
    return columnSeparator != null ? columnSeparator : rowGroup.getColumnSeparator();
  }
}

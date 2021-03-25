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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import org.apache.geode.management.internal.cli.GfshParser;

/**
 * A group of rows. Widths for all columns within a group will be the same and when built will
 * automatically be set to the length of the longest value in the column.
 *
 * @since GemFire 7.0
 */
class RowGroup {

  private static final int SCREEN_WIDTH_MARGIN_BUFFER = 5;

  private final Table table;
  private final List<Row> rows = new ArrayList<>();
  private final Screen screen;

  private int[] colSizes;
  private String columnSeparator;

  RowGroup(Table table, Screen screen) {
    this.table = table;
    this.screen = screen;
  }

  @Override
  public String toString() {
    return "RowGroup [rows=" + rows + "]";
  }

  Table getTable() {
    return table;
  }

  Row newRow() {
    Row row = new Row(this, screen);
    rows.add(row);
    return row;
  }

  void newRowSeparator(Character character, boolean isTableWideSeparator) {
    Row row = new Row(this, screen, character, isTableWideSeparator);
    rows.add(row);
  }

  void newBlankRow() {
    newRow().newCenterCol("").setBlank(true);
  }

  String buildRowGroup(boolean isTabularResult) {
    colSizes = computeColSizes(isTabularResult);

    StringBuilder stringBuilder = new StringBuilder();
    for (Row row : rows) {
      String builtRow = row.buildRow(isTabularResult);
      stringBuilder.append(builtRow);
      if (StringUtils.isNotBlank(builtRow) || row.isBlank()) {
        stringBuilder.append(GfshParser.LINE_SEPARATOR);
      }
    }
    return stringBuilder.toString();
  }

  int getColSize(int colNum) {
    return colSizes[colNum];
  }

  int getNumCols() {
    int maxNumCols = 0;

    for (Row row : rows) {
      int numCols = row.getNumCols();
      if (numCols > maxNumCols) {
        maxNumCols = numCols;
      }
    }

    return maxNumCols;
  }

  void setColumnSeparator(String columnSeparator) {
    this.columnSeparator = columnSeparator;
  }

  String getColumnSeparator() {
    return columnSeparator != null ? columnSeparator : table.getColumnSeparator();
  }


  List<Row> getRows() {
    return Collections.unmodifiableList(rows);
  }

  private int[] computeColSizes(boolean isTabularResult) {
    int[] localColSizes = new int[getNumCols()];

    for (int i = 0; i < localColSizes.length; i++) {
      localColSizes[i] = getMaxColLength(i);
    }

    if (isTabularResult) {
      localColSizes = recalculateColSizesForScreen(
          screen.getScreenWidth(), localColSizes, getColumnSeparator());
    }

    return localColSizes;
  }

  private int getMaxColLength(int colNum) {
    int maxLength = 0;

    for (Row row : rows) {
      int colLength = row.getMaxColLength(colNum);
      if (colLength > maxLength) {
        maxLength = colLength;
      }
    }

    return maxLength;
  }

  private int[] recalculateColSizesForScreen(int screenWidth, int[] colSizes,
      String colSeparators) {
    if (!screen.shouldTrimColumns()) {
      // Returning original colSizes since reader is set to external
      return colSizes;
    }

    // change the screen width to account for separator chars
    screenWidth -= (colSizes.length - 1) * colSeparators.length();

    // build sorted list and find total width
    List<ComparableColumn> stringList = new ArrayList<>();
    int index = 0;
    int totalLength = 0;
    for (int k : colSizes) {
      ComparableColumn cs = new ComparableColumn();
      cs.originalIndex = index++;
      cs.length = k;
      stringList.add(cs);
      totalLength += k;
    }

    // No need to reduce the column width return orig array
    if (totalLength <= screenWidth) {
      return colSizes;
    }

    Collections.sort(stringList);

    // find out columns which need trimming
    totalLength = 0;
    int spaceLeft = 0;
    int totalExtra = 0;
    for (ComparableColumn s : stringList) {
      int newLength = totalLength + s.length;
      // Ensure that the spaceLeft is never < 2 which would prevent displaying a trimmed value
      // even when there is space available on the screen.
      if (newLength + SCREEN_WIDTH_MARGIN_BUFFER > screenWidth) {
        s.markForTrim = true;
        totalExtra += s.length;
        if (spaceLeft == 0) {
          spaceLeft = screenWidth - totalLength;
        }
      }
      totalLength = newLength;
    }

    stringList.sort(Comparator.comparingInt(o -> o.originalIndex));

    // calculate trimmed width for columns marked for
    // distribute the trimming as per percentage
    int[] finalColSizes = new int[colSizes.length];
    int i = 0;
    for (ComparableColumn s : stringList) {
      if (totalLength > screenWidth) {
        if (s.markForTrim) {
          s.trimmedLength = (int) Math.floor(spaceLeft * ((double) s.length / totalExtra));
        } else {
          s.trimmedLength = s.length;
        }
      } else {
        s.trimmedLength = s.length;
      }
      finalColSizes[i] = s.trimmedLength;
      i++;
    }

    totalLength = 0;
    index = 0;
    for (int colSize : finalColSizes) {
      if (colSize != colSizes[index] && colSize < 2) {
        throw new TooManyColumnsException("Computed ColSize=" + colSize
            + " Set RESULT_VIEWER to external. This uses the 'less' command (with horizontal scrolling) to see wider results");
      }
      totalLength += colSize;
      index++;
    }
    return finalColSizes;
  }
}

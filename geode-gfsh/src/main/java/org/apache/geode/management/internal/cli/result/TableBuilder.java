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

/**
 * Helper class to build rows of columnized strings & build a table from those rows.
 *
 * Sample usage:
 *
 * <code>
 *     public final Table createTable() {
 *       Table resultTable = TableBuilder.newTable();
 *       resultTable.setColumnSeparator(" | ");
 *
 *       resultTable.newBlankRow();
 *       resultTable.newRow().newLeftCol("Displaying all fields for member: " + memberName);
 *       resultTable.newBlankRow();
 *       RowGroup rowGroup = resultTable.newRowGroup();
 *       rowGroup.newRow().newCenterCol("FIELD1").newCenterCol("FIELD2");
 *       rowGroup.newRowSeparator('-');
 *       for (int i = 0; i < counter; i++) {
 *         rowGroup.newRow().newLeftCol(myFirstField[i]).newLeftCol(mySecondField[i]);
 *       }
 *       resultTable.newBlankRow();
 *
 *       return resultTable;
 *     }
 * </code>
 *
 * Will result in this:
 *
 * <literal>
 *
 * Displaying all fields for member: Member1
 *
 * FIELD1 | FIELD2 -------------- | --------------- My First Field | My Second Field Another Fld1 |
 * Another Fld2 Last Fld1 | Last Fld2
 *
 * </literal>
 *
 *
 * @since GemFire 7.0
 */

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import org.apache.geode.management.internal.cli.GfshParser;

public class TableBuilder {

  public static Table newTable() {
    return new Table();
  }

  public static class Table {
    private final List<RowGroup> rowGroups = new ArrayList<>();
    private String columnSeparator = "   ";
    private boolean isTabularResult = false;

    public void setTabularResult(boolean isTabularResult) {
      this.isTabularResult = isTabularResult;
    }

    public void setColumnSeparator(final String columnSeparator) {
      this.columnSeparator = columnSeparator;
    }

    public String getColumnSeparator() {
      return this.columnSeparator;
    }

    public RowGroup newRowGroup() {
      RowGroup rowGroup = new RowGroup(this);
      this.rowGroups.add(rowGroup);
      return rowGroup;
    }

    public Row newRow() {
      RowGroup rowGroup = newRowGroup();
      return rowGroup.newRow();
    }

    public void newBlankRow() {
      RowGroup rowGroup = newRowGroup();
      rowGroup.newBlankRow();
    }

    public RowGroup getLastRowGroup() {
      if (rowGroups.size() == 0) {
        return null;
      }
      return rowGroups.get(rowGroups.size() - 1);
    }

    /**
     * Computes total Max Row Length across table - for all row groups.
     */
    private int getMaxLength() {
      int rowGroupMaxTotalLength = 0;
      for (RowGroup rowGroup : this.rowGroups) {
        List<Row> rows = rowGroup.rows;
        int rowGroupTotalLength = 0;

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

        rowGroupTotalLength += rowMaxTotalLength;
        if (rowGroupMaxTotalLength < rowGroupTotalLength) {
          rowGroupMaxTotalLength = rowGroupTotalLength;
        }
      }
      return (int) (rowGroupMaxTotalLength * 1.1);
    }

    public String buildTable() {
      StringBuilder stringBuffer = new StringBuilder();
      for (RowGroup rowGroup : this.rowGroups) {
        stringBuffer.append(rowGroup.buildRowGroup(isTabularResult));
      }

      return stringBuffer.toString();
    }

    public List<String> buildTableList() {
      List<String> list = new ArrayList<>();
      for (RowGroup rowGroup : this.rowGroups) {
        list.add(rowGroup.buildRowGroup(isTabularResult));
      }

      return list;
    }

    @Override
    public String toString() {
      return "Table [rowGroups=" + rowGroups + "]";
    }
  }

  /**
   * A group of rows. Widths for all columns within a group will be the same and when built will
   * automatically be set to the length of the longest value in the column.
   *
   * @since GemFire 7.0
   */
  public static class RowGroup {
    private final Table table;
    private final List<Row> rows = new ArrayList<>();
    private int[] colSizes;

    private String columnSeparator;

    private RowGroup(final Table table) {
      this.table = table;
    }

    private Table getTable() {
      return this.table;
    }

    public Row newRow() {
      Row row = new Row(this);
      rows.add(row);
      return row;
    }

    public void newRowSeparator(Character character, boolean isTablewideSeparator) {
      Row row = new Row(this, character, isTablewideSeparator);
      rows.add(row);
    }

    public void newBlankRow() {
      Row row = newRow();
      row.newCenterCol("");
      row.isBlank = true;
    }

    public String buildRowGroup(boolean isTabularResult) {
      this.colSizes = computeColSizes(isTabularResult);

      StringBuilder stringBuffer = new StringBuilder();
      for (Row row : rows) {
        String builtRow = row.buildRow(isTabularResult);
        stringBuffer.append(builtRow);
        if (StringUtils.isNotBlank(builtRow) || row.isBlank) {
          stringBuffer.append(GfshParser.LINE_SEPARATOR);
        }
      }
      return stringBuffer.toString();
    }

    private int getColSize(final int colNum) {
      return this.colSizes[colNum];
    }

    private int[] computeColSizes(boolean isTabularResult) {
      int[] localColSizes = new int[getNumCols()];

      for (int i = 0; i < localColSizes.length; i++) {
        localColSizes[i] = getMaxColLength(i);
      }

      if (isTabularResult) {
        localColSizes = TableBuilderHelper.recalculateColSizesForScreen(
            TableBuilderHelper.getScreenWidth(), localColSizes, getColumnSeparator());
      }

      return localColSizes;
    }

    private int getNumCols() {
      int maxNumCols = 0;

      for (Row row : rows) {
        int numCols = row.getNumCols();
        if (numCols > maxNumCols) {
          maxNumCols = numCols;
        }
      }

      return maxNumCols;
    }

    private int getMaxColLength(final int colNum) {
      int maxLength = 0;

      for (Row row : rows) {
        int colLength = row.getMaxColLength(colNum);
        if (colLength > maxLength) {
          maxLength = colLength;
        }
      }

      return maxLength;
    }

    public void setColumnSeparator(final String columnSeparator) {
      this.columnSeparator = columnSeparator;
    }

    public String getColumnSeparator() {
      return this.columnSeparator != null ? this.columnSeparator : table.getColumnSeparator();
    }

    @Override
    public String toString() {
      return "RowGroup [rows=" + rows + "]";
    }
  }

  public static class Row {
    private final RowGroup rowGroup;
    private final Character rowSeparator;
    private final List<Column> columns = new ArrayList<>();
    boolean isBlank;
    private boolean isTablewideSeparator;

    private String columnSeparator;

    private Row(final RowGroup rowGroup) {
      this.rowGroup = rowGroup;
      this.rowSeparator = null;
    }

    private Row(final RowGroup rowGroup, final Character rowSeparator,
        final boolean isTablewideSeparator) {
      this.rowGroup = rowGroup;
      this.rowSeparator = rowSeparator;
      this.isTablewideSeparator = isTablewideSeparator;
    }

    public Row newLeftCol(Object value) {
      Column column = new Column(value, Align.LEFT);
      this.columns.add(column);
      return this;
    }

    public Row newRightCol(Object value) {
      Column column = new Column(value, Align.RIGHT);
      this.columns.add(column);
      return this;
    }

    public Row newCenterCol(Object value) {
      Column column = new Column(value, Align.CENTER);
      this.columns.add(column);
      return this;
    }

    public boolean isEmpty() {
      return columns.isEmpty();
    }

    private int getNumCols() {
      return this.columns.size();
    }

    private int getMaxColLength(final int colNum) {
      if (colNum >= this.columns.size()) {
        return 0;
      }

      return this.columns.get(colNum).getLength();
    }

    public void setColumnSeparator(final String columnSeparator) {
      this.columnSeparator = columnSeparator;
    }

    public String getColumnSeparator() {
      return this.columnSeparator != null ? this.columnSeparator : rowGroup.getColumnSeparator();
    }

    private String buildRow(boolean isTabularResult) {
      StringBuilder stringBuffer = new StringBuilder();
      if (this.rowSeparator != null) {
        if (isTablewideSeparator) {
          int maxColLength = this.rowGroup.getTable().getMaxLength();
          // Trim only for tabular results
          if (isTabularResult) {
            maxColLength = TableBuilderHelper.trimWidthForScreen(maxColLength);
          }

          for (int j = 0; j < maxColLength; j++) {
            stringBuffer.append(this.rowSeparator);
          }
        } else {
          int maxNumCols = this.rowGroup.getNumCols();

          for (int i = 0; i < maxNumCols; i++) {
            int maxColLength = this.rowGroup.getColSize(i);
            for (int j = 0; j < maxColLength; j++) {
              stringBuffer.append(this.rowSeparator);
            }
            if (i < (maxNumCols - 1)) {
              stringBuffer.append(this.rowGroup.getColumnSeparator());
            }
          }
        }
      } else {
        for (int i = 0; i < this.columns.size(); i++) {
          boolean lastColumn = !(i < (this.columns.size() - 1));
          stringBuffer
              .append(this.columns.get(i).buildColumn(this.rowGroup.getColSize(i), lastColumn));
          if (!lastColumn) {
            stringBuffer.append(getColumnSeparator());
          }
        }
      }

      return stringBuffer.toString();
    }

    @Override
    public String toString() {
      return "Row [columns=" + columns + "]";
    }
  }

  private enum Align {
    LEFT, RIGHT, CENTER
  }

  private static class Column {

    private final Align align;
    private final String stringValue;

    private Column(final Object value, final Align align) {
      if (value == null) {
        this.stringValue = "";
      } else {
        this.stringValue = value.toString();
      }

      this.align = align;
    }

    private int getLength() {
      return this.stringValue.length();
    }

    private String buildColumn(int colWidth, boolean trimIt) {

      // If string value is greater than colWidth
      // This can happen because colSizes are re-computed
      // to fit the screen width
      if (this.stringValue.length() > colWidth) {
        StringBuilder stringBuffer = new StringBuilder();
        int endIndex = colWidth - 2;
        if (endIndex < 0) {
          return "";
        }
        return stringBuffer.append(stringValue.substring(0, endIndex)).append("..").toString();
      }

      int numSpaces = colWidth - this.stringValue.length();
      if (trimIt) {
        numSpaces = 0;
      }

      StringBuilder stringBuffer = new StringBuilder();

      switch (align) {
        case LEFT:
          stringBuffer.append(stringValue);
          for (int i = 0; i < numSpaces; i++) {
            stringBuffer.append(" ");
          }
          break;
        case RIGHT:
          for (int i = 0; i < numSpaces; i++) {
            stringBuffer.append(" ");
          }
          stringBuffer.append(stringValue);
          break;
        case CENTER:
          int i = 0;
          for (; i < numSpaces / 2; i++) {
            stringBuffer.append(" ");
          }
          stringBuffer.append(stringValue);
          for (; i < numSpaces; i++) {
            stringBuffer.append(" ");
          }
          break;
      }

      return stringBuffer.toString();

    }



    private String buildColumn(int colWidth) {
      return buildColumn(colWidth, false);
    }

    @Override
    public String toString() {
      return "Column [align=" + align + ", stringValue=" + stringValue + "]";
    }
  }
}

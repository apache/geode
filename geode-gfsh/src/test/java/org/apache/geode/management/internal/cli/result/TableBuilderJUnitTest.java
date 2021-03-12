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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import org.apache.geode.management.internal.cli.GfshParser;
import org.apache.geode.management.internal.cli.result.TableBuilder.Row;
import org.apache.geode.management.internal.cli.result.TableBuilder.RowGroup;
import org.apache.geode.management.internal.cli.result.TableBuilder.Table;

public class TableBuilderJUnitTest {

  @Rule
  public TestName testName = new TestName();

  private TableBuilder builder;
  private final int screenWidth = 40;

  @Before
  public void setUp() {
    builder = spy(new TableBuilder());
    doReturn(screenWidth).when(builder).getScreenWidth();
    doReturn(true).when(builder).shouldTrimColumns();
  }

  private Table createTableStructure(int cols, String separator) {
    String[] colNames = new String[cols];
    for (int i = 0; i < cols; i++) {
      colNames[i] = "Field";
    }
    return createTableStructure(cols, separator, colNames);
  }

  private Table createTableStructure(int cols, String separator, String... colNames) {
    Table resultTable = builder.newTable();
    resultTable.setTabularResult(true);
    resultTable.setColumnSeparator(separator);

    resultTable.newBlankRow();
    RowGroup rowGroup = resultTable.newRowGroup();
    Row row = rowGroup.newRow();
    for (int colIndex = 0; colIndex < cols; colIndex++) {
      row.newCenterCol(colNames[colIndex] + colIndex);
    }

    rowGroup.newRowSeparator('-', false);

    return resultTable;
  }

  private List<String> validateTable(Table table, boolean shouldTrim) {
    String st = table.buildTable();
    System.out.println(st);

    List<String> lines = Arrays.asList(st.split(GfshParser.LINE_SEPARATOR));

    int line = 0;
    for (String s : lines) {
      System.out.println("For line " + line++ + " length is " + s.length() + " isWider = "
          + (s.length() > screenWidth));

      if (shouldTrim) {
        assertThat(s.length()).isLessThanOrEqualTo(screenWidth);
      } else {
        assertThat(s.length()).satisfiesAnyOf(
            length -> assertThat(length).isZero(),
            length -> assertThat(length).isGreaterThan(screenWidth));
      }
    }

    return lines;
  }

  /**
   * Test Variations table-wide separator true false
   */
  @Test
  public void testSanity() {
    Table table = createTableStructure(3, "|");
    RowGroup rowGroup = table.getLastRowGroup();
    Row row1 = rowGroup.newRow();
    row1.newLeftCol("1").newLeftCol("1").newLeftCol("1");

    List<String> result = validateTable(table, true);
    // Check the last line
    assertThat(result.get(3)).isEqualTo("1     |1     |1");
  }

  @Test
  public void testLastColumnTruncated() {
    Table table = createTableStructure(4, "|");
    RowGroup rowGroup = table.getLastRowGroup();
    Row row1 = rowGroup.newRow();
    row1.newLeftCol("1").newLeftCol("123456789-").newLeftCol("123456789-")
        .newLeftCol("123456789-123456789-12345");

    List<String> result = validateTable(table, true);
    // Check the last line
    assertThat(result.get(3)).isEqualTo("1     |123456789-|123456789-|123456789..");
  }

  @Test
  public void testLongestColumnFirstTruncated() {
    Table table = createTableStructure(4, "|");
    RowGroup rowGroup = table.getLastRowGroup();
    Row row1 = rowGroup.newRow();
    row1.newLeftCol("123456789-123456789-").newLeftCol("123456789-12345").newLeftCol("123456789-")
        .newLeftCol("1");

    List<String> result = validateTable(table, true);
    // Check the last line
    assertThat(result.get(3)).isEqualTo("1234..|123456789-12345|123456789-|1");
  }

  @Test
  public void testMultipleColumnsTruncated() {
    Table table = createTableStructure(4, "|");
    RowGroup rowGroup = table.getLastRowGroup();
    Row row1 = rowGroup.newRow();
    row1.newLeftCol("1").newLeftCol("123456789-").newLeftCol("123456789-123456789-123456789-")
        .newLeftCol("123456789-123456789-12345");

    List<String> result = validateTable(table, true);
    // Check the last line
    assertThat(result.get(3)).isEqualTo("1     |123456789-|123456789..|1234567..");
  }

  @Test
  public void testMultipleColumnsTruncatedLongestFirst() {
    Table table = createTableStructure(4, "|");
    RowGroup rowGroup = table.getLastRowGroup();
    Row row1 = rowGroup.newRow();
    row1.newLeftCol("123456789-123456789-123456789-").newLeftCol("123456789-123456789-12345")
        .newLeftCol("1").newLeftCol("123456789-");

    List<String> result = validateTable(table, true);
    // Check the last line
    assertThat(result.get(3)).isEqualTo("123456789..|1234567..|1     |123456789-");
  }

  @Test
  public void testColumnsWithShortNames() {
    doReturn(9).when(builder).getScreenWidth();

    Table table = createTableStructure(3, "|", "A", "A", "A");
    RowGroup rowGroup = table.getLastRowGroup();
    Row row1 = rowGroup.newRow();
    row1.newLeftCol("123").newLeftCol("123").newLeftCol("123");

    List<String> result = validateTable(table, true);
    // Check the last line
    assertThat(result.get(3)).isEqualTo("..|..|..");
  }

  @Test
  public void testExceptionTooSmallWidth() {
    doReturn(7).when(builder).getScreenWidth();

    Table table = createTableStructure(3, "|", "A", "A", "A");
    RowGroup rowGroup = table.getLastRowGroup();
    Row row1 = rowGroup.newRow();
    row1.newLeftCol("12").newLeftCol("12").newLeftCol("12");

    // This should throw an exception
    assertThatThrownBy(() -> validateTable(table, true))
        .isInstanceOf(TableBuilder.TooManyColumnsException.class);
  }

  @Test
  public void testTooLittleSpaceOnNextToLastColumn() {
    Table table = createTableStructure(4, "|");
    RowGroup rowGroup = table.getLastRowGroup();
    Row row1 = rowGroup.newRow();
    row1.newLeftCol("1").newLeftCol("123456789-").newLeftCol("123456789-123456789-123456789-")
        .newLeftCol("123456789-");

    List<String> result = validateTable(table, true);
    // Check the last line
    assertThat(result.get(3)).isEqualTo("1     |123456789-|123456789..|123456789-");
  }

  @Test
  public void testSeparatorWithMultipleChars() {
    Table table = createTableStructure(4, "<|>");
    RowGroup rowGroup = table.getLastRowGroup();
    Row row1 = rowGroup.newRow();
    row1.newLeftCol("1").newLeftCol("123456789-").newLeftCol("123456789-")
        .newLeftCol("123456789-123456789-12345");

    List<String> result = validateTable(table, true);
    // Check the last line
    assertThat(result.get(3)).isEqualTo("1     <|>123456789-<|>123456789-<|>123..");
  }

  /**
   * multiple columns upto 8 : done
   */
  @Test
  public void testManyColumns() {
    Table table = createTableStructure(8, "|");
    RowGroup rowGroup = table.getLastRowGroup();
    Row row1 = rowGroup.newRow();
    row1.newLeftCol("123456789-").newLeftCol("123456789-").newLeftCol("123456789-")
        .newLeftCol("123456789-").newLeftCol("123456789-").newLeftCol("123456789-")
        .newLeftCol("123456789-").newLeftCol("123456789-");

    List<String> result = validateTable(table, true);
    // Check the last line
    assertThat(result.get(3)).isEqualTo("123456789-|123456789-|..|..|..|..|..|..");
  }

  @Test
  public void testDisableColumnAdjustment() {
    doReturn(false).when(builder).shouldTrimColumns();

    Table table = createTableStructure(5, "|");
    RowGroup rowGroup = table.getLastRowGroup();
    Row row1 = rowGroup.newRow();
    row1.newLeftCol("1").newLeftCol("123456789-").newLeftCol("123456789-")
        .newLeftCol("123456789-123456789-12345").newLeftCol("1");

    List<String> result = validateTable(table, false);
    // Check the last line
    assertThat(result.get(3)).isEqualTo("1     |123456789-|123456789-|123456789-123456789-12345|1");
  }
}

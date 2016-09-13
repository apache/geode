/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.management.internal.cli;

import com.gemstone.gemfire.management.internal.cli.result.TableBuilder;
import com.gemstone.gemfire.management.internal.cli.result.TableBuilder.Row;
import com.gemstone.gemfire.management.internal.cli.result.TableBuilder.RowGroup;
import com.gemstone.gemfire.management.internal.cli.result.TableBuilder.Table;
import com.gemstone.gemfire.management.internal.cli.result.TableBuilderHelper;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;
import static org.powermock.api.mockito.PowerMockito.spy;
import static org.powermock.api.mockito.PowerMockito.when;

/**
 * Testing TableBuilder and TableBuilderHelper using mocks for Gfsh
 *
 */
@Category(IntegrationTest.class)
@RunWith(PowerMockRunner.class)
@PowerMockIgnore("*.IntegrationTest")
@PrepareForTest(TableBuilderHelper.class)
public class TableBuilderJUnitTest {

  @Rule
  public TestName testName = new TestName();

  @Before
  public void setUp() throws Exception {
    // This sets up a partial mock for some static methods
    spy(TableBuilderHelper.class);
    when(TableBuilderHelper.class, "getScreenWidth").thenReturn(40);
    when(TableBuilderHelper.class, "shouldTrimColumns").thenReturn(true);
  }

  private Table createTableStructure(int cols, String separator) {
    String[] colNames = new String[cols];
    for (int i = 0; i < cols; i++) {
      colNames[i] = "Field";
    }
    return createTableStructure(cols, separator, colNames);
  }

  private Table createTableStructure(int cols, String separator, String... colNames) {
    Table resultTable = TableBuilder.newTable();
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
    int screenWidth = TableBuilderHelper.getScreenWidth();
    String st = table.buildTable();
    System.out.println(st);

    List<String> lines = Arrays.asList(st.split(GfshParser.LINE_SEPARATOR));

    int line = 0;
    for (String s : lines) {
      System.out.println("For line " + line++ + " length is " + s.length() + " isWider = " + (s.length() > screenWidth));

      if (shouldTrim) {
        if (s.length() > screenWidth) {
          fail("Expected length > screenWidth: " + s.length() + " > " + screenWidth);
        }
      } else {
        if (s.length() != 0 && s.length() <= screenWidth) {
          fail("Expected length <= screenWidth: " + s.length() + " <= " + screenWidth);
        }
      }
    }

    return lines;
  }

  /**
   * Test Variations table-wide separator true false
   */
  @Test
  public void testSanity() throws Exception {
    assertTrue(TableBuilderHelper.shouldTrimColumns());

    Table table = createTableStructure(3, "|");
    RowGroup rowGroup = table.getLastRowGroup();
    Row row1 = rowGroup.newRow();
    row1.newLeftCol("1")
        .newLeftCol("1")
        .newLeftCol("1");

    List<String> result = validateTable(table, true);
    // Check the last line
    assertEquals("1     |1     |1", result.get(3));
  }

  @Test
  public void testLastColumnTruncated() throws Exception {
    assertTrue(TableBuilderHelper.shouldTrimColumns());

    Table table = createTableStructure(4, "|");
    RowGroup rowGroup = table.getLastRowGroup();
    Row row1 = rowGroup.newRow();
    row1.newLeftCol("1")
        .newLeftCol("123456789-")
        .newLeftCol("123456789-")
        .newLeftCol("123456789-123456789-12345");

    List<String> result = validateTable(table, true);
    // Check the last line
    assertEquals("1     |123456789-|123456789-|123456789..", result.get(3));
  }

  @Test
  public void testLongestColumnFirstTruncated() throws Exception {
    assertTrue(TableBuilderHelper.shouldTrimColumns());

    Table table = createTableStructure(4, "|");
    RowGroup rowGroup = table.getLastRowGroup();
    Row row1 = rowGroup.newRow();
    row1.newLeftCol("123456789-123456789-")
        .newLeftCol("123456789-12345")
        .newLeftCol("123456789-")
        .newLeftCol("1");

    List<String> result = validateTable(table, true);
    // Check the last line
    assertEquals("1234..|123456789-12345|123456789-|1", result.get(3));
  }

  @Test
  public void testMultipleColumnsTruncated() throws Exception {
    assertTrue(TableBuilderHelper.shouldTrimColumns());

    Table table = createTableStructure(4, "|");
    RowGroup rowGroup = table.getLastRowGroup();
    Row row1 = rowGroup.newRow();
    row1.newLeftCol("1")
        .newLeftCol("123456789-")
        .newLeftCol("123456789-123456789-123456789-")
        .newLeftCol("123456789-123456789-12345");

    List<String> result = validateTable(table, true);
    // Check the last line
    assertEquals("1     |123456789-|123456789..|1234567..", result.get(3));
  }

  @Test
  public void testMultipleColumnsTruncatedLongestFirst() throws Exception {
    assertTrue(TableBuilderHelper.shouldTrimColumns());

    Table table = createTableStructure(4, "|");
    RowGroup rowGroup = table.getLastRowGroup();
    Row row1 = rowGroup.newRow();
    row1.newLeftCol("123456789-123456789-123456789-")
        .newLeftCol("123456789-123456789-12345")
        .newLeftCol("1")
        .newLeftCol("123456789-");

    List<String> result = validateTable(table, true);
    // Check the last line
    assertEquals("123456789..|1234567..|1     |123456789-", result.get(3));
  }

  @Test
  public void testColumnsWithShortNames() throws Exception {
    when(TableBuilderHelper.class, "getScreenWidth").thenReturn(9);
    assertTrue(TableBuilderHelper.shouldTrimColumns());

    Table table = createTableStructure(3, "|", "A", "A", "A");
    RowGroup rowGroup = table.getLastRowGroup();
    Row row1 = rowGroup.newRow();
    row1.newLeftCol("123")
        .newLeftCol("123")
        .newLeftCol("123");

    List<String> result = validateTable(table, true);
    // Check the last line
    assertEquals("..|..|..", result.get(3));
  }

  @Test(expected = TableBuilderHelper.TooManyColumnsException.class)
  public void testExceptionTooSmallWidth() throws Exception {
    when(TableBuilderHelper.class, "getScreenWidth").thenReturn(7);
    assertTrue(TableBuilderHelper.shouldTrimColumns());

    Table table = createTableStructure(3, "|", "A", "A", "A");
    RowGroup rowGroup = table.getLastRowGroup();
    Row row1 = rowGroup.newRow();
    row1.newLeftCol("12")
        .newLeftCol("12")
        .newLeftCol("12");

    // This should throw an exception
    List<String> result = validateTable(table, true);
  }

  @Test
  public void testTooLittleSpaceOnNextToLastColumn() throws Exception {
    assertTrue(TableBuilderHelper.shouldTrimColumns());

    Table table = createTableStructure(4, "|");
    RowGroup rowGroup = table.getLastRowGroup();
    Row row1 = rowGroup.newRow();
    row1.newLeftCol("1")
        .newLeftCol("123456789-")
        .newLeftCol("123456789-123456789-123456789-")
        .newLeftCol("123456789-123456789-12345");

    List<String> result = validateTable(table, true);
    // Check the last line
    assertEquals("1     |123456789-|123456789..|1234567..", result.get(3));
  }

  @Test
  public void testSeparatorWithMultipleChars() throws Exception {
    assertTrue(TableBuilderHelper.shouldTrimColumns());

    Table table = createTableStructure(4, "<|>");
    RowGroup rowGroup = table.getLastRowGroup();
    Row row1 = rowGroup.newRow();
    row1.newLeftCol("1")
        .newLeftCol("123456789-")
        .newLeftCol("123456789-")
        .newLeftCol("123456789-123456789-12345");

    List<String> result = validateTable(table, true);
    // Check the last line
    assertEquals("1     <|>123456789-<|>123456789-<|>123..", result.get(3));
  }

  /**
   * multiple columns upto 8 : done
   */
  @Test
  public void testManyColumns() throws Exception {
    assertTrue(TableBuilderHelper.shouldTrimColumns());

    Table table = createTableStructure(8, "|");
    RowGroup rowGroup = table.getLastRowGroup();
    Row row1 = rowGroup.newRow();
    row1.newLeftCol("123456789-")
        .newLeftCol("123456789-")
        .newLeftCol("123456789-")
        .newLeftCol("123456789-")
        .newLeftCol("123456789-")
        .newLeftCol("123456789-")
        .newLeftCol("123456789-")
        .newLeftCol("123456789-");

    List<String> result = validateTable(table, true);
    // Check the last line
    assertEquals("123456789-|123456789-|..|..|..|..|..|..", result.get(3));
  }

  /**
   * set gfsh env property result_viewer to basic disable for external reader
   */
  @Test
  public void testDisableColumnAdjustment() throws Exception {
    when(TableBuilderHelper.class, "shouldTrimColumns").thenReturn(false);
    assertFalse(TableBuilderHelper.shouldTrimColumns());

    Table table = createTableStructure(5, "|");
    RowGroup rowGroup = table.getLastRowGroup();
    Row row1 = rowGroup.newRow();
    row1.newLeftCol("1")
        .newLeftCol("123456789-")
        .newLeftCol("123456789-")
        .newLeftCol("123456789-123456789-12345")
        .newLeftCol("1");

    List<String> result = validateTable(table, false);
    // Check the last line
    assertEquals("1     |123456789-|123456789-|123456789-123456789-12345|1", result.get(3));
  }
}

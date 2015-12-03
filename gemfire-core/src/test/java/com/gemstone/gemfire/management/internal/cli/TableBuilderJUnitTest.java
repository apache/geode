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
import com.gemstone.gemfire.management.internal.cli.shell.Gfsh;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import java.io.IOException;
import java.util.Properties;
import java.util.Random;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * TODO: fails when running integrationTest from gradle command-line or in Eclipse on Windows 7
 * <p>
 * com.gemstone.gemfire.management.internal.cli.TableBuilderJUnitTest > testBasicScrapping FAILED
 * java.lang.AssertionError: Expected length < 100 is 101 at org.junit.Assert.fail(Assert.java:88) at
 * com.gemstone.gemfire.management.internal.cli.TableBuilderJUnitTest.doTableBuilderTestUnit(TableBuilderJUnitTest.java:115)
 * at com.gemstone.gemfire.management.internal.cli.TableBuilderJUnitTest.testBasicScrapping(TableBuilderJUnitTest.java:134)
 * <p>
 * com.gemstone.gemfire.management.internal.cli.TableBuilderJUnitTest > testManyColumns FAILED java.lang.AssertionError:
 * Expected length < 100 is 101 at org.junit.Assert.fail(Assert.java:88) at com.gemstone.gemfire.management.internal.cli.TableBuilderJUnitTest.doTableBuilderTestUnit(TableBuilderJUnitTest.java:115)
 * at com.gemstone.gemfire.management.internal.cli.TableBuilderJUnitTest.testManyColumns(TableBuilderJUnitTest.java:155)
 *
 * @author tushark
 */
@Category(IntegrationTest.class)
public class TableBuilderJUnitTest {

  @Rule
  public TestName testName = new TestName();

  private final Table createTable(int rows, int cols, int width, String separator) {
    Table resultTable = TableBuilder.newTable();
    resultTable.setTabularResult(true);
    resultTable.setColumnSeparator(separator);

    resultTable.newBlankRow();
    resultTable.newRow().newLeftCol("Displaying all fields for member: ");
    resultTable.newBlankRow();
    RowGroup rowGroup = resultTable.newRowGroup();
    Row row = rowGroup.newRow();
    for (int colIndex = 0; colIndex < cols; colIndex++) {
      row.newCenterCol("Field" + colIndex);
    }

    rowGroup.newRowSeparator('-', false);

    int counter = rows;
    for (int i = 0; i < counter; i++) {
      row = rowGroup.newRow();
      for (int k = 0; k < cols; k++) {
        row.newLeftCol(getString(i, width / cols));
      }
    }
    resultTable.newBlankRow();

    return resultTable;
  }

  private Object getString(int i, int width) {
    StringBuilder sb = new StringBuilder();
    Random random = new Random();
    int k = 0;
    double d = random.nextDouble();
    // .09 probability
    if (d <= 0.9) {
      k = random.nextInt(width);
    } else {
      k = width / 2 + random.nextInt(width);
    }
    random.nextInt(10);
    for (int j = 0; j < k; j++) {
      sb.append(i);
      if (sb.length() > k) break;
    }
    return sb.toString();
  }

  private HeadlessGfsh createShell(Properties props) throws ClassNotFoundException, IOException {
    String shellId = getClass().getSimpleName() + "_" + testName;
    HeadlessGfsh shell = new HeadlessGfsh(shellId, 30, props);
    return shell;
  }

  private void doTableBuilderTestUnit(int rows, int cols, String sep, boolean shouldTrim,
      boolean expectTooManyColEx) throws ClassNotFoundException, IOException {
    int width = Gfsh.getCurrentInstance().getTerminalWidth();
    Table table = createTable(rows, cols, width, sep);
    String st = table.buildTable();
    System.out.println(st);

    String[] array = st.split("\n");

    int line = 0;
    for (String s : array) {
      System.out.println("For line " + line++ + " length is " + s.length() + " isWider = " + (s.length() > width));

      if (shouldTrim) {
        if (s.length() > width) {
          fail("Expected length < " + width + " is " + s.length());
        }
      } else {
        if (s.length() > 50 && s.length() <= width) {
          fail("Expected length <= " + width + " is " + s.length());
        }
      }

    }
  }

  /**
   * Test Variations tablewide separator true false
   */
  @Test
  public void testBasicScraping() throws ClassNotFoundException, IOException {
    Properties props = new Properties();
    props.setProperty(Gfsh.ENV_APP_RESULT_VIEWER, Gfsh.DEFAULT_APP_RESULT_VIEWER);
    createShell(props);
    assertTrue(TableBuilderHelper.shouldTrimColumns());
    doTableBuilderTestUnit(15, 4, "|", true, false);
  }


  @Test
  public void testSeparatorWithMultipleChars() throws ClassNotFoundException, IOException {
    Properties props = new Properties();
    props.setProperty(Gfsh.ENV_APP_RESULT_VIEWER, Gfsh.DEFAULT_APP_RESULT_VIEWER);
    createShell(props);
    assertTrue(TableBuilderHelper.shouldTrimColumns());
    doTableBuilderTestUnit(15, 4, " | ", true, false);
  }

  /**
   * multiple columns upto 8 : done
   */
  @Test
  @Ignore("Bug 52051")
  public void testManyColumns() throws ClassNotFoundException, IOException {
    createShell(null);
    assertTrue(TableBuilderHelper.shouldTrimColumns());
    doTableBuilderTestUnit(15, 6, "|", true, true);
  }

  /**
   * set gfsh env property result_viewer to basic disable for external reader
   */
  //
  @Test
  public void testDisableColumnAdjustment() throws ClassNotFoundException, IOException {
    createShell(null);
    assertFalse(TableBuilderHelper.shouldTrimColumns());
    doTableBuilderTestUnit(15, 12, "|", false, false);
  }

}

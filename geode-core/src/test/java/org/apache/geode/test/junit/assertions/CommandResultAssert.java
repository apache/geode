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
package org.apache.geode.test.junit.assertions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.Assertions;

import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.result.ModelCommandResult;
import org.apache.geode.management.internal.cli.result.model.DataResultModel;
import org.apache.geode.management.internal.cli.result.model.InfoResultModel;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.result.model.TabularResultModel;


public class CommandResultAssert
    extends AbstractAssert<CommandResultAssert, CommandResultExecution> {

  public CommandResultAssert(CommandResult commandResult) {
    super(new CommandResultExecution(commandResult.toJson(), commandResult),
        CommandResultAssert.class);
  }

  public CommandResultAssert(String output, CommandResult commandResult) {
    super(new CommandResultExecution(output, commandResult), CommandResultAssert.class);
  }

  public CommandResult getCommandResult() {
    return actual.getCommandResult();
  }

  /**
   * Verifies that the gfsh output contains the given key, value pair.
   *
   * For example, given the following gfsh output:
   *
   * <pre>
   * {@code
   * Result : true
   * Key Class : java.lang.String
   * Key : key92
   * Locations Found : 2
   * }
   * </pre>
   *
   * We might assert that:
   *
   * <pre>
   * <code> containsKeyValuePair("Key Class", "java.lang.String"); </code>
   * </pre>
   */
  public CommandResultAssert containsKeyValuePair(String key, String value) {
    assertThat(actual.getOutput()).containsPattern(key + "\\s+: " + value);

    return this;
  }

  /**
   * Verifies the gfsh output contains the given output
   */
  public CommandResultAssert containsOutput(String... expectedOutputs) {
    for (String expectedOutput : expectedOutputs) {
      assertThat(actual.getOutput()).contains(expectedOutput);
    }

    return this;
  }

  /**
   * Verifies the gfsh output does not contain the given output
   */
  public CommandResultAssert doesNotContainOutput(String... expectedOutputs) {
    for (String expectedOutput : expectedOutputs) {
      assertThat(actual.getOutput()).doesNotContain(expectedOutput);
    }

    return this;
  }

  /**
   * Verifies that gfsh executed with status OK
   */
  public CommandResultAssert statusIsSuccess() {
    CommandResult result = actual.getCommandResult();
    Assertions.assertThat(result.getStatus()).describedAs(actual.getOutput())
        .isEqualTo(Result.Status.OK);

    return this;
  }

  public CommandResultAssert hasFailToPersistError() {
    Assertions.assertThat(actual.getCommandResult().failedToPersist()).isTrue();
    return this;
  }

  public CommandResultAssert hasNoFailToPersistError() {
    Assertions.assertThat(actual.getCommandResult().failedToPersist()).isFalse();
    return this;
  }

  /**
   * Verifies that gfsh executed with status ERROR
   */
  public CommandResultAssert statusIsError() {
    CommandResult result = actual.getCommandResult();
    Assertions.assertThat(result.getStatus()).describedAs(actual.getOutput())
        .isEqualTo(Result.Status.ERROR);

    return this;
  }

  /**
   * Verifies that the gfsh output contains a table that has a column with the specified header and
   * the specified values in the specified order.
   *
   * For example, given the following gfsh output:
   *
   * <pre>
   * Index Name | Region Path | Server Name |      Indexed Fields      |
   * ---------- | ----------- | ----------- | ------------------------ |
   * index1      | /region1     | server-0    | [field1, field3, field2] |
   * index2      | /region2     | server-0    | [field1, field3, field2] |
   * </pre>
   *
   * We might assert that:
   *
   * <pre>
   * <code> tableHasColumnWithExactValuesInExactOrder("Region Path", "/region1", "/region2");
   * </code>
   * </pre>
   */
  public CommandResultAssert tableHasColumnWithExactValuesInExactOrder(String header,
      String... expectedValues) {
    List<String> actualValues = actual.getCommandResult().getTableColumnValues(header);
    assertThat(actualValues).containsExactly(expectedValues);

    return this;
  }

  /**
   * Verifies that the gfsh output contains a table that has a column with the specified header and
   * the specified values in any order.
   *
   * For example, given the following gfsh output:
   *
   * <pre>
   * Index Name | Region Path | Server Name |      Indexed Fields      |
   * ---------- | ----------- | ----------- | ------------------------ |
   * index1      | /region1     | server-0    | [field1, field3, field2] |
   * index2      | /region2     | server-0    | [field1, field3, field2] |
   *
   * </pre>
   *
   * We might assert that:
   *
   * <pre>
   * <code> tableHasColumnWithExactValuesInAnyOrder("Region Path", "/region2", "/region1"); </code>
   * </pre>
   */
  public CommandResultAssert tableHasColumnWithExactValuesInAnyOrder(String header,
      String... expectedValues) {
    List<String> actualValues = actual.getCommandResult().getTableColumnValues(header);
    assertThat(actualValues).containsExactlyInAnyOrder(expectedValues);

    return this;
  }


  public CommandResultAssert tableHasRowWithValues(String... headersThenValues) {
    assertThat(headersThenValues.length % 2)
        .describedAs("You need to pass even number of parameters.").isEqualTo(0);

    int numberOfColumn = headersThenValues.length / 2;

    String[] headers = Arrays.copyOfRange(headersThenValues, 0, numberOfColumn);
    String[] expectedValues =
        Arrays.copyOfRange(headersThenValues, numberOfColumn, headersThenValues.length);

    Map<String, List<String>> allValues = new HashMap<>();
    int numberOfRows = -1;
    for (String header : headers) {
      List<String> columnValues = actual.getCommandResult().getTableColumnValues(header);
      if (numberOfRows > 0) {
        assertThat(columnValues.size()).isEqualTo(numberOfRows);
      }
      numberOfRows = columnValues.size();
      allValues.put(header, columnValues);
    }

    for (int rowIndex = 0; rowIndex < numberOfRows; rowIndex++) {
      Object[] rowValues = new Object[headers.length];
      for (int columnIndex = 0; columnIndex < headers.length; columnIndex++) {
        rowValues[columnIndex] = allValues.get(headers[columnIndex]).get(rowIndex).toString();
      }

      // check if entire row is equal, but if not, continue to next row
      if (Arrays.deepEquals(expectedValues, rowValues)) {
        return this;
      }
    }

    // did not find any matching rows, then this would pass only if we do not pass in any values
    assertThat(headersThenValues.length)
        .describedAs("No matching row found containing expected values: "
            + StringUtils.join(expectedValues, ","))
        .isEqualTo(0);
    return this;
  }

  public CommandResultAssert tableHasRowCount(String anyColumnHeader, int rowSize) {
    assertThat(actual.getCommandResult().getTableColumnValues(anyColumnHeader).size())
        .isEqualTo(rowSize);
    return this;
  }

  /**
   * Verifies that each of the actual values in the column with the given header contains at least
   * one of the expectedValues.
   */
  public CommandResultAssert tableHasColumnWithValuesContaining(String header,
      String... expectedValues) {
    List<String> actualValues = actual.getCommandResult().getTableColumnValues(header);

    for (Object actualValue : actualValues) {
      String actualValueString = (String) actualValue;
      boolean actualValueContainsAnExpectedValue =
          Arrays.stream(expectedValues).anyMatch(actualValueString::contains);

      if (!actualValueContainsAnExpectedValue) {
        failWithMessage(
            "Expecting: " + Arrays.toString(expectedValues) + ", but found: " + actualValue);
      }
    }

    return this;
  }

  /**
   * Verifies that each of the actual values in the column with the given header contains at least
   * one of the expectedValues.
   */
  public CommandResultAssert tableHasColumnOnlyWithValues(String header, String... expectedValues) {
    List<String> actualValues = actual.getCommandResult().getTableColumnValues(header);
    assertThat(actualValues).containsOnly(expectedValues);

    return this;
  }

  public CommandResultAssert hasResult() {
    containsKeyValuePair("Result", "true");

    return this;
  }

  public CommandResultAssert hasNoResult() {
    containsKeyValuePair("Result", "false");

    return this;
  }

  /*
   * methods that are only applicable to ModelCommandResult
   */

  // Will throw error unless this is a ModelCommand Result
  public ResultModel getResultModel() {
    ModelCommandResult modelCommandResult = (ModelCommandResult) actual.getCommandResult();
    return modelCommandResult.getResultData();
  }

  public CommandResultAssert hasSection(String... sectionName) {
    ResultModel resultModel = getResultModel();
    assertThat(resultModel.getSectionNames()).contains(sectionName);
    return this;
  }

  // convenience method to get the first info section. if more than one info section
  // use the sectionName to get it
  public InfoResultModelAssert hasInfoSection() {
    return new InfoResultModelAssert(getResultModel().getInfoSections().get(0));
  }

  public InfoResultModelAssert hasInfoSection(String sectionName) {
    ResultModel resultModel = getResultModel();
    InfoResultModel section = resultModel.getInfoSection(sectionName);
    if (section == null) {
      fail(sectionName + " section not found");
    }
    return new InfoResultModelAssert(section);
  }

  // convenience method to get the first table section. if more than one table section
  // use the sectionName to get it
  public TabularResultModelAssert hasTableSection() {
    return new TabularResultModelAssert(getResultModel().getTableSections().get(0));
  }

  public TabularResultModelAssert hasTableSection(String sectionName) {
    ResultModel resultModel = getResultModel();
    TabularResultModel section = resultModel.getTableSection(sectionName);
    if (section == null) {
      fail(sectionName + " section not found");
    }
    return new TabularResultModelAssert(section);
  }

  // convenience method to get the first data section. if more than one data section
  // use the sectionName to get it
  public DataResultModelAssert hasDataSection() {
    return new DataResultModelAssert(getResultModel().getDataSections().get(0));
  }

  public DataResultModelAssert hasDataSection(String sectionName) {
    ResultModel resultModel = getResultModel();
    DataResultModel section = resultModel.getDataSection(sectionName);
    if (section == null) {
      fail(sectionName + " section not found");
    }
    return new DataResultModelAssert(section);
  }
}

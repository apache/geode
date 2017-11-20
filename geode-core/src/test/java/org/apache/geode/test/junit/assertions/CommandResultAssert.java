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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.Assertions;
import org.json.JSONArray;

import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.json.GfJsonException;
import org.apache.geode.management.internal.cli.json.GfJsonObject;
import org.apache.geode.management.internal.cli.result.CommandResult;


public class CommandResultAssert
    extends AbstractAssert<CommandResultAssert, CommandResultExecution> {

  public CommandResultAssert(CommandResult commandResult) {
    super(new CommandResultExecution(commandResult.toJson(), commandResult),
        CommandResultAssert.class);
  }

  public CommandResultAssert(String output, CommandResult commandResult) {
    super(new CommandResultExecution(output, commandResult), CommandResultAssert.class);
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
      Object... expectedValues) {
    GfJsonObject resultContentJSON = actual.getCommandResult().getContent();
    Object content = getColumnContent(header, resultContentJSON);

    if (content == null) {
      failWithMessage(
          "Command result did not contain <" + header + ">: " + resultContentJSON.toString());
    }

    Object[] actualValues = toArray((JSONArray) content);
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
      Object... expectedValues) {
    GfJsonObject resultContentJSON = actual.getCommandResult().getContent();
    Object content = getColumnContent(header, resultContentJSON);

    if (content == null) {
      failWithMessage("Command result did not contain a table with column header <" + header + ">: "
          + resultContentJSON.toString());
    }

    Object[] actualValues = toArray((JSONArray) content);
    assertThat(actualValues).containsExactlyInAnyOrder(expectedValues);

    return this;
  }



  public CommandResultAssert tableHasRowWithValues(String... headersThenValues)
      throws GfJsonException {
    assertThat(headersThenValues.length % 2)
        .describedAs("You need to pass even number of parameters.").isEqualTo(0);

    int numberOfColumn = headersThenValues.length / 2;

    String[] headers = Arrays.copyOfRange(headersThenValues, 0, numberOfColumn);
    String[] expectedValues =
        Arrays.copyOfRange(headersThenValues, numberOfColumn, headersThenValues.length);

    GfJsonObject content = actual.getCommandResult().getContent();
    Map<String, Object[]> allValues = new HashMap<>();
    int numberOfRows = -1;
    for (String header : headers) {
      Object[] values = toArray((JSONArray) getColumnContent(header, content));
      if (numberOfRows > 0) {
        assertThat(values.length).isEqualTo(numberOfRows);
      }
      numberOfRows = values.length;
      allValues.put(header, values);
    }

    for (int rowIndex = 0; rowIndex < numberOfRows; rowIndex++) {
      Object[] rowValues = new Object[headers.length];
      for (int columnIndex = 0; columnIndex < headers.length; columnIndex++) {
        rowValues[columnIndex] = allValues.get(headers[columnIndex])[rowIndex];
      }

      // check if entire row is equal, but if not, continue to next row
      if (Arrays.deepEquals(expectedValues, rowValues)) {
        return this;
      }
    }

    // did not find any matching rows, then this would pass only if we do not pass in any values
    assertThat(headersThenValues.length).isEqualTo(0);
    return this;
  }

  /**
   * Verifies that each of the actual values in the column with the given header contains at least
   * one of the expectedValues.
   */
  public CommandResultAssert tableHasColumnWithValuesContaining(String header,
      String... expectedValues) {
    GfJsonObject resultContentJSON = actual.getCommandResult().getContent();
    Object content = getColumnContent(header, resultContentJSON);

    if (content == null) {
      failWithMessage("Command result did not contain a table with column header <" + header + ">: "
          + resultContentJSON.toString());
    }

    Object[] actualValues = toArray((JSONArray) content);

    for (Object actualValue : actualValues) {
      String actualValueString = (String) actualValue;
      boolean actualValueContainsAnExpectedValue =
          Arrays.stream(expectedValues).anyMatch(actualValueString::contains);

      if (!actualValueContainsAnExpectedValue) {
        failWithMessage("Found unexpected value: " + actualValue);
      }
    }

    return this;
  }


  /**
   * Verifies that each of the actual values in the column with the given header contains at least
   * one of the expectedValues.
   */
  public CommandResultAssert tableHasColumnOnlyWithValues(String header, String... expectedValues) {
    GfJsonObject resultContentJSON = actual.getCommandResult().getContent();
    Object content = getColumnContent(header, resultContentJSON);

    if (content == null) {
      failWithMessage("Command result did not contain a table with column header <" + header + ">: "
          + resultContentJSON.toString());
    }

    Object[] actualValues = toArray((JSONArray) content);
    assertThat(actualValues).containsOnly(expectedValues);
    return this;
  }

  private Object getColumnContent(String header, GfJsonObject resultContentJSON) {
    if (resultContentJSON.get(header) != null) {
      return resultContentJSON.get(header);
    }
    try {
      // Sometimes, the output is buried in a most questionable way.
      return resultContentJSON.getJSONObject("__sections__-0").getJSONObject("__tables__-0")
          .getJSONObject("content").get(header);
    } catch (NullPointerException ignored) {
    }
    return null;
  }

  public CommandResultAssert hasResult() {
    containsKeyValuePair("Result", "true");

    return this;
  }

  public CommandResultAssert hasNoResult() {
    containsKeyValuePair("Result", "false");

    return this;
  }

  private Object[] toArray(JSONArray array) {
    Object[] values = new Object[array.length()];

    for (int i = 0; i < array.length(); i++) {
      values[i] = array.get(i);
    }

    return values;
  }
}

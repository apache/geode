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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.CommandResponseBuilder;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.result.CompositeResultData;
import org.apache.geode.management.internal.cli.result.ErrorResultData;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.cli.result.TabularResultData;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class LegacyVsResultModelComparisonTest {

  @Test
  public void legacyTableComparison() {
    // Create the legacy results
    TabularResultData legacyTable = ResultBuilder.createTabularResultData();
    legacyTable.setHeader("Heads");
    legacyTable.accumulate("Name", "server1");
    legacyTable.accumulate("Name", "server2");
    legacyTable.accumulate("Id", "member name for server1");
    legacyTable.accumulate("Id", "member name for server2");
    legacyTable.setFooter("Tails");

    Result legacyResult = ResultBuilder.buildResult(legacyTable);
    String legacyString =
        CommandResponseBuilder.createCommandResponseJson("server1", (CommandResult) legacyResult);

    CommandResult clientLegacyResult = ResultBuilder.fromJson(legacyString);

    // Create the new model results
    TabularResultModel modelTable = new TabularResultModel();
    modelTable.setHeader("Heads");
    modelTable.accumulate("Name", "server1");
    modelTable.accumulate("Name", "server2");
    modelTable.accumulate("Id", "member name for server1");
    modelTable.accumulate("Id", "member name for server2");
    modelTable.setFooter("Tails");

    String newModelString = CommandResponseBuilder.createCommandResponseJson("server1", modelTable);
    CommandResult clientNewModelResult = ResultBuilder.fromJson(newModelString);

    assertThat(clientNewModelResult.getColumnValues("Name"))
        .isEqualTo(clientLegacyResult.getColumnValues("Name"));

    assertThat(readCommandOutput(clientNewModelResult))
        .isEqualTo(readCommandOutput(clientLegacyResult));
  }

  @Test
  public void legacyCompositeComparison() {
    // Create the legacy results
    CompositeResultData legacyCrd = ResultBuilder.createCompositeResultData();
    legacyCrd.setHeader("Heads");

    // section-0 table-0
    TabularResultData table1 = legacyCrd.addSection().addTable();
    table1.setHeader("section-0 table-1 header");
    table1.accumulate("Params", "value1");
    table1.setFooter("section-0 table-1 footer");

    // section-1
    CompositeResultData.SectionResultData section1 = legacyCrd.addSection();
    section1.setHeader("section 0 header");
    section1.addSeparator('-');
    section1.addData("param-1", "value-1");
    section1.addData("param-3", "value-3");
    section1.addData("param-2", "value-2");
    section1.setFooter("section 0 footer");

    // section-2
    CompositeResultData.SectionResultData section2 = legacyCrd.addSection("named-section");
    section2.setHeader("named section header");
    section2.addSeparator('-');
    section2.addData("param-A", "value-B");

    Result legacyResult = ResultBuilder.buildResult(legacyCrd);
    String legacyString =
        CommandResponseBuilder.createCommandResponseJson("server1", (CommandResult) legacyResult);

    CommandResult clientLegacyResult = ResultBuilder.fromJson(legacyString);

    // Create the new model results
    CompositeResultModel newCrd = new CompositeResultModel();
    newCrd.setHeader("Heads");

    TabularResultModel newTable1 = newCrd.addSection().addTable();
    newTable1.setHeader("section-0 table-1 header");
    newTable1.accumulate("Params", "value1");
    newTable1.setFooter("section-0 table-1 footer");

    SectionResultModel newSection1 = newCrd.addSection();
    newSection1.setHeader("section 0 header");
    newSection1.addSeparator('-');
    newSection1.addData("param-1", "value-1");
    newSection1.addData("param-3", "value-3");
    newSection1.addData("param-2", "value-2");
    newSection1.setFooter("section 0 footer");

    SectionResultModel newSection2 = newCrd.addSection("named-section");
    newSection2.setHeader("named section header");
    newSection2.addSeparator('-');
    newSection2.addData("param-A", "value-B");

    String newModelString = CommandResponseBuilder.createCommandResponseJson("server1", newCrd);
    CommandResult clientNewModelResult = ResultBuilder.fromJson(newModelString);

    assertThat(clientLegacyResult.getMapFromTableContent("0", "0"))
        .containsAllEntriesOf(clientNewModelResult.getMapFromTableContent("0", "0"));

    assertThat(clientLegacyResult.getMapFromSection("1"))
        .containsAllEntriesOf(clientNewModelResult.getMapFromSection("1"));

    assertThat(readCommandOutput(clientNewModelResult))
        .isEqualTo(readCommandOutput(clientLegacyResult));
  }

  @Test
  public void legacyErrorComparison() {
    // Create the legacy results
    ErrorResultData legacyError = ResultBuilder.createErrorResultData();
    legacyError.addLine("This is a bad line");
    legacyError.addLine("This is another bad line");

    Result legacyResult = ResultBuilder.buildResult(legacyError);
    String legacyString =
        CommandResponseBuilder.createCommandResponseJson("server1", (CommandResult) legacyResult);

    CommandResult legacyErrorResult = ResultBuilder.fromJson(legacyString);

    // Create the new model results
    ErrorResultModel newError = new ErrorResultModel();
    newError.addLine("This is a bad line");
    newError.addLine("This is another bad line");

    String newModelString = CommandResponseBuilder.createCommandResponseJson("server1", newError);
    CommandResult newErrorModelResult = ResultBuilder.fromJson(newModelString);

    assertThat(legacyErrorResult.getMessageFromContent())
        .isEqualTo(newErrorModelResult.getMessageFromContent());

    assertThat(readCommandOutput(newErrorModelResult))
        .isEqualTo(readCommandOutput(legacyErrorResult));
  }

  @Test
  public void legacyUserErrorComparison() {
    // Create the legacy results
    Result legacyResult = ResultBuilder.createUserErrorResult("This is an error message");
    String legacyString =
        CommandResponseBuilder.createCommandResponseJson("server1", (CommandResult) legacyResult);

    CommandResult legacyErrorResult = ResultBuilder.fromJson(legacyString);

    // Create the new model results
    ErrorResultModel newError = new ErrorResultModel(500, "This is an error message");

    String newModelString = CommandResponseBuilder.createCommandResponseJson("server1", newError);
    CommandResult newErrorModelResult = ResultBuilder.fromJson(newModelString);

    assertThat(legacyErrorResult.getMessageFromContent())
        .isEqualTo(newErrorModelResult.getMessageFromContent());

    assertThat(readCommandOutput(newErrorModelResult))
        .isEqualTo(readCommandOutput(legacyErrorResult));
  }

  private List<String> readCommandOutput(CommandResult cmd) {
    List<String> result = new ArrayList<>();
    while (cmd.hasNextLine()) {
      result.add(cmd.nextLine());
    }

    return result;
  }
}

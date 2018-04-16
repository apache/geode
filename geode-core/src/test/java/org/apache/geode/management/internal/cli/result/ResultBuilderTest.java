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

import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.management.cli.Result;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class ResultBuilderTest {

  @Test
  public void messageExistsForString() throws Exception {
    CommandResult result = (CommandResult) ResultBuilder.createInfoResult("test message");
    assertThat(result.getValueFromContent("message")).isEqualTo("[\"test message\"]");
  }

  @Test
  public void messageExistsForEmpty() throws Exception {
    CommandResult result = (CommandResult) ResultBuilder.createInfoResult("");
    assertThat(result.getValueFromContent("message")).isEqualTo("[\"\"]");

  }

  @Test
  public void messageExistsForNull() throws Exception {
    CommandResult result = (CommandResult) ResultBuilder.createInfoResult(null);
    assertThat(result.getValueFromContent("message")).isEqualTo("[null]");

  }

  @Test
  public void infoResultDataStructure() {
    InfoResultData result = ResultBuilder.createInfoResultData();
    result.addLine("line 1");
    result.addLine("line 2");
    result.setHeader("Heads");
    result.setFooter("Tails");
    CommandResult cmdResult = ResultBuilder.buildResult(result);

    assertThat(cmdResult.getHeader()).isEqualTo("Heads");
    assertThat(cmdResult.getListFromContent("message")).contains("line 1", "line 2");
    assertThat(cmdResult.getFooter()).isEqualTo("Tails");
    assertThat(cmdResult.getValueFromContent("message")).isNotEmpty();
    assertThat(cmdResult.getStatus()).isEqualTo(Result.Status.OK);
  }

  @Test
  public void errorResultDataStructure() {
    ErrorResultData result = ResultBuilder.createErrorResultData();
    result.addLine("line 1");
    result.addLine("line 2");
    result.setHeader("Heads");
    result.setFooter("Tails");
    CommandResult cmdResult = ResultBuilder.buildResult(result);

    assertThat(cmdResult.getHeader()).isEqualTo("Heads");
    assertThat(cmdResult.getListFromContent("message")).contains("line 1", "line 2");
    assertThat(cmdResult.getFooter()).isEqualTo("Tails");

    assertThat(cmdResult.getValueFromContent("message")).isNotEmpty();

    assertThat(cmdResult.getStatus()).isEqualTo(Result.Status.ERROR);
  }

  @Test
  public void tabularResultDataStructure() {
    TabularResultData result = ResultBuilder.createTabularResultData();
    result.accumulate("column1", "value11");
    result.accumulate("column1", "value12");
    result.accumulate("column2", "value21");
    result.accumulate("column2", "value22");

    result.setHeader("Heads");
    result.setFooter("Tails");
    CommandResult cmdResult = ResultBuilder.buildResult(result);

    assertThat(cmdResult.getHeader()).isEqualTo("Heads");
    assertThat(cmdResult.getFooter()).isEqualTo("Tails");

    assertThat(cmdResult.getListFromContent("column1")).contains("value11", "value12");
    assertThat(cmdResult.getListFromContent("column2")).contains("value21", "value22");
  }

  @Test
  public void compositeResultDataStructure() {
    CompositeResultData result = ResultBuilder.createCompositeResultData();

    result.setHeader("Heads");
    result.setFooter("Tails");
    // build up an example
    result.addSection().addData("section 0 key", "section 0 value");
    result.addSection().addTable().accumulate("table 1 column", "table 1 value");

    CommandResult cmdResult = ResultBuilder.buildResult(result);

    assertThat(cmdResult.getHeader()).isEqualTo("Heads");
    assertThat(cmdResult.getFooter()).isEqualTo("Tails");

    assertThat(cmdResult.getMapFromSection("0").get("section 0 key")).isEqualTo("section 0 value");

    Map<String, List<String>> table = cmdResult.getMapFromTableContent("1", "0");
    assertThat(table.get("table 1 column")).contains("table 1 value");
  }

  @Test
  public void errorCodeCorrectlyUpdated() {
    String json =
        "{\"contentType\":\"table\",\"data\":{\"content\":{\"Member\":[\"server\"],\"Status\":[\"ERROR: Bad.\"]},\"footer\":\"\",\"header\":\"\",\"type-class\":\"org.apache.geode.management.internal.cli.CommandResponse.Data\"},\"debugInfo\":\"\",\"failedToPersist\":false,\"fileToDownload\":null,\"page\":\"1/1\",\"sender\":\"server\",\"status\":-1,\"tokenAccessor\":\"__NULL__\",\"type-class\":\"org.apache.geode.management.internal.cli.CommandResponse\",\"version\":\"1.3.0-SNAPSHOT\",\"when\":\"10/17/17 8:17 AM\"}";

    CommandResult result = ResultBuilder.fromJson(json);
    assertThat(result.getStatus().getCode()).isEqualTo(-1);
  }
}
